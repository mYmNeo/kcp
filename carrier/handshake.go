package carrier

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// sessionAuthSize is the length of the nonce the listener issues and of every
// tag exchanged afterwards. A full HMAC-SHA256 tag is used rather than a
// truncation: the handshake runs once per stream, so the extra bytes cost
// nothing measurable, and there is no reason to hand an attacker a shorter
// search space.
const sessionAuthSize = sha256.Size

// authLabel and ackLabel domain-separate the two tags. Without distinct prefixes
// the listener's acknowledgement would also verify as the dialer's proof for the
// same (id, nonce), so a peer could pass its own acknowledgement back to itself.
//
// The "v1" suffix is part of the frozen label text, not a protocol version: it
// was chosen when the handshake was first written. Bumping protocolVersion is
// what signals an incompatible handshake, and redemanding the labels now would
// be a wire change for no gain — they only have to stay distinct from each
// other and stable for a given version.
const (
	authLabel = "carrier-handshake-v1"
	ackLabel  = "carrier-handshake-ack-v1"
)

// ErrAuth reports a stream that did not prove knowledge of the shared secret.
var ErrAuth = errors.New("carrier: handshake authentication failed")

// errNoSecret reports a listening endpoint configured without a secret, which
// means it cannot authenticate anything.
var errNoSecret = errors.New("carrier: listening requires Config.Secret")

// newNonce returns a fresh random nonce. A caller that cannot obtain randomness
// must not proceed: falling back to a predictable nonce would silently remove
// the carrier's replay resistance.
func newNonce() ([]byte, error) {
	buf := make([]byte, sessionAuthSize)
	if _, err := rand.Read(buf); err != nil {
		return nil, fmt.Errorf("carrier: handshake nonce: %w", err)
	}
	return buf, nil
}

// sessionTag derives HMAC-SHA256 over the label, the handshake header, and every
// following field. It is the one primitive behind both tags.
//
// Covering the id binds a tag to one session. Covering the nonce is what makes
// the handshake replay-resistant: the listener issues a fresh nonce for every
// connection, so a handshake recorded earlier in the same session does not
// verify against it. That matters because a tag whose only varying input were
// chosen by the peer — a client nonce, say — would be byte-for-byte replayable
// by anyone who captured it.
func sessionTag(secret []byte, label string, id uint64, fields ...[]byte) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(label))

	var hdr [handshakeBaseSize]byte
	binary.BigEndian.PutUint32(hdr[0:], magic)
	hdr[4] = protocolVersion
	binary.BigEndian.PutUint64(hdr[8:], id)
	mac.Write(hdr[:])

	for _, f := range fields {
		mac.Write(f)
	}
	return mac.Sum(nil)
}

// readHello consumes the unauthenticated opening frame and returns the session
// id it announces. It authenticates nothing: the id is not a credential, and the
// listener issues a challenge before trusting anything.
func readHello(conn io.Reader) (uint64, error) {
	var hdr [handshakeBaseSize]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return 0, err
	}

	if binary.BigEndian.Uint32(hdr[0:]) != magic {
		return 0, fmt.Errorf("%w: bad magic", ErrProtocol)
	}
	if hdr[4] != protocolVersion {
		return 0, fmt.Errorf("%w: unsupported version %d", ErrProtocol, hdr[4])
	}
	return binary.BigEndian.Uint64(hdr[8:]), nil
}

// writeHello announces the logical session id of a freshly dialed stream. All
// streams of one dialer announce the same id, which is what lets the listener
// reassemble parallel streams into one logical peer.
func writeHello(conn io.Writer, id uint64) error {
	var hdr [handshakeBaseSize]byte
	binary.BigEndian.PutUint32(hdr[0:], magic)
	hdr[4] = protocolVersion
	binary.BigEndian.PutUint64(hdr[8:], id)

	_, err := conn.Write(hdr[:])
	return err
}

// writeChallenge sends a fresh nonce and returns it, so the caller can verify
// the proof that comes back. Every connection gets its own nonce, which is what
// makes a captured handshake useless on a later one.
func writeChallenge(conn io.Writer) ([]byte, error) {
	nonce, err := newNonce()
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(nonce); err != nil {
		return nil, err
	}
	return nonce, nil
}

// readChallenge consumes the listener's nonce.
func readChallenge(conn io.Reader) ([]byte, error) {
	nonce := make([]byte, sessionAuthSize)
	if _, err := io.ReadFull(conn, nonce); err != nil {
		return nil, err
	}
	return nonce, nil
}

// writeProof answers the listener's challenge with proof of the shared secret.
func writeProof(conn io.Writer, secret []byte, id uint64, nonce []byte) error {
	_, err := conn.Write(sessionTag(secret, authLabel, id, nonce))
	return err
}

// readProof consumes and verifies the dialer's proof against the nonce this
// listener issued. It returns the proof so the acknowledgement can cover it.
//
// The comparison is constant time, so a wrong secret reveals nothing about how
// much of it was correct.
func readProof(conn io.Reader, secret []byte, id uint64, nonce []byte) ([]byte, error) {
	proof := make([]byte, sessionAuthSize)
	if _, err := io.ReadFull(conn, proof); err != nil {
		return nil, err
	}

	want := sessionTag(secret, authLabel, id, nonce)
	if subtle.ConstantTimeCompare(proof, want) != 1 {
		return nil, ErrAuth
	}
	return proof, nil
}

// ackTag is the acknowledgement both endpoints derive, so the dialer can verify
// that the listener saw this very exchange rather than a replayed one. It covers
// the nonce and the dialer's proof, which ties it to the handshake rather than to
// the session alone.
func ackTag(secret []byte, id uint64, nonce, proof []byte) []byte {
	return sessionTag(secret, ackLabel, id, nonce, proof)
}

// writeAck sends the acknowledgement that admits the dialer to the session.
//
// It is only reached for a stream that passed admit, because a refused peer is
// given no acknowledgement at all: attach closes the socket instead, so the
// dialer learns it was rejected by seeing the close rather than by reading a
// negative answer it could probe with.
func writeAck(conn io.Writer, secret []byte, id uint64, nonce, proof []byte) {
	conn.Write(ackTag(secret, id, nonce, proof))
}

// dialHandshake performs the dialing half of the handshake and verifies the
// listener's acknowledgement.
func dialHandshake(conn io.ReadWriter, secret []byte, id uint64) error {
	if err := writeHello(conn, id); err != nil {
		return err
	}

	nonce, err := readChallenge(conn)
	if err != nil {
		// A listener that refuses before challenging closes the stream, so this
		// is the ordinary path for a rejected dialer.
		return ErrAuth
	}

	proof := sessionTag(secret, authLabel, id, nonce)
	if err := writeProof(conn, secret, id, nonce); err != nil {
		return err
	}

	ack := make([]byte, sessionAuthSize)
	if _, err := io.ReadFull(conn, ack); err != nil {
		return ErrAuth
	}
	if subtle.ConstantTimeCompare(ack, ackTag(secret, id, nonce, proof)) != 1 {
		return ErrAuth
	}
	return nil
}

// serveHandshake performs the listening half: it issues a challenge and verifies
// the proof, returning the proof so the caller can acknowledge once admit has
// decided whether the stream is admissible.
func serveHandshake(conn io.ReadWriter, secret []byte, id uint64) (nonce, proof []byte, err error) {
	if nonce, err = writeChallenge(conn); err != nil {
		return nil, nil, err
	}
	if proof, err = readProof(conn, secret, id, nonce); err != nil {
		return nil, nil, err
	}
	return nonce, proof, nil
}
