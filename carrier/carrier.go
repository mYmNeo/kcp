// Package carrier carries KCP datagrams over TCP.
//
// kcp-go exposes a packet-oriented seam — kcp.ServeConn for a listener,
// kcp.NewConn4 for a dialer — so a carrier only has to be a net.PacketConn
// whose datagrams travel inside TCP streams instead of UDP datagrams. Every
// datagram becomes one length-prefixed frame, which keeps KCP's packet
// boundaries intact: the far side recovers the datagrams one for one, with no
// stream reassembly and no coalescing of two datagrams into one.
//
// # Why
//
// UDP is the right substrate for KCP on a healthy path, but it is the first
// thing a middlebox throttles or drops. TCP survives that, at the cost of
// head-of-line blocking inside each stream. Spreading datagrams over several
// parallel TCP streams keeps a single stalled stream from freezing the whole
// session, which is what makes the carrier usable for bulk transfer.
//
// # Wire format
//
// A listening endpoint accepts TCP connections. Each connection opens with a
// handshake that proves the dialer holds the shared secret and announces the
// logical session id, so several TCP streams can carry one session and still be
// reassembled into a single logical peer on the receiving side.
//
// The handshake is a challenge-response, because the session id travels in the
// clear and therefore is not a credential. Anyone can read an id — off the wire,
// or off the server log, which prints the peer address per session — so the id
// only names a session; the secret is what proves the right to join it. Four
// fixed-size frames — a round trip plus a final acknowledgement:
//
//	  dialer                                       listener
//	  ------                                       --------
//	  hello: magic | ver | reserved | id  -------->
//	                        <---------------------  nonce (sessionAuthSize)
//	  proof = HMAC(secret, authLabel|magic|ver|id|nonce)  -->
//	                        <---------------------  ack (sessionAuthSize)
//
//	 0                   1                   2                   3
//	+-------+-------+-------+-------+-------+-------+-------+-------+
//	|          magic "KCPT"         | ver   |      reserved         |
//	+-------+-------+-------+-------+-------+-------+-------+-------+
//	|                    session id (uint64, big endian)            |
//	+-------+-------+-------+-------+-------+-------+-------+-------+
//
// The listener issues the nonce per connection, and the proof covers it, so a
// handshake recorded earlier cannot be replayed onto a later connection: a tag
// whose only varying input were peer-chosen would be byte-for-byte reusable by
// whoever captured it. The acknowledgement is a second tag, under ackLabel over
// the nonce and the proof, so the dialer learns it was admitted instead of
// writing into a stream that will never be read, and no peer lacking the secret
// can forge it. A refused dialer gets no acknowledgement; it sees the close.
//
// See handshake.go for the exact transcript and the derivations.
//
// Every datagram after the handshake is framed as a 2 byte big-endian length
// followed by the payload:
//
//	+---------------+-----------------
//	| len (big end) | payload ...
//	+---------------+-----------------
//
// A zero length frame is legal: it carries an empty datagram, which a UDP
// socket accepts, and the carrier must not be stricter than the socket it
// replaces.
//
// # Ordering and loss
//
// A single stream preserves the order of the datagrams written to it. Datagrams
// striped across parallel streams may be reordered relative to each other; KCP
// tolerates reordering, which is what makes the striping safe. Nothing here
// retransmits: a broken TCP stream drops whatever was queued on it, and KCP
// retransmits exactly as it would over UDP.
//
// # Backpressure
//
// Each stream has a dedicated writer goroutine fed from a bounded queue, so a
// stalled TCP stream cannot block the others. When every queue is full, WriteTo
// blocks and that pressure propagates back into KCP's send window.
//
// A stream that stops draining must not be able to freeze the session — that is
// the whole point of spreading datagrams over several streams. A send that
// waits longer than Config.StallTimeout is therefore taken as evidence that the
// stream is wedged: it is dropped from the set and the datagram is retried on a
// healthy one. Only when every stream stalls does the endpoint fail, which is
// what releases KCP to reconnect.
package carrier

import (
	"encoding/binary"
	"errors"
	"net"
	"strconv"
	"time"

	crand "crypto/rand"
)

const (
	// magic is the four byte marker that opens every carrier stream.
	magic = 0x4b435054 // "KCPT"

	// protocolVersion is bumped whenever the wire format changes. Version 2
	// replaced the single-shot handshake with the client-hello / server-nonce /
	// client-proof / server-ack exchange; a version 1 peer is now rejected with
	// "unsupported version 1" instead of having its old fields misread as a
	// proof and reported as an authentication failure.
	protocolVersion = 2

	// handshakeBaseSize is the fixed part of the per-stream handshake header:
	// magic, version, reserved, and the session id.
	handshakeBaseSize = 16

	// frameHeaderSize is the size of the datagram length prefix.
	frameHeaderSize = 2

	// MaxDatagram is the largest datagram the wire format can describe. The
	// length prefix is a uint16, so this is the encoding's ceiling rather than a
	// tuning knob.
	MaxDatagram = 65535

	// kcpCeiling is the largest datagram KCP will actually hand the carrier.
	// kcp-go caps its own frames at 1500 bytes (mtuLimit), so pooled buffers are
	// sized to that reality plus headroom for a larger KCP MTU, instead of to
	// the encoding's maximum. Sizing the pool at MaxDatagram would pin 64 KiB per
	// queued datagram — two orders of magnitude more than a real packet — for
	// every stream under backpressure. A datagram larger than this still works;
	// it simply allocates instead of coming from the pool.
	kcpCeiling = 2048

	// readBufSize is the per-stream receive buffer. One syscall can therefore
	// deliver many frames at once without the reader having to grow it: it
	// covers the largest frame the sender may emit, which writeFrame holds to
	// the uint16 length prefix plus its own header.
	readBufSize = (1 << 16) + frameHeaderSize

	// defaultStreams is the number of parallel TCP streams per logical session
	// when Config.Streams is unset.
	defaultStreams = 1

	// defaultQueueDepth bounds the datagrams queued for transmission on one
	// stream when Config.QueueDepth is unset.
	defaultQueueDepth = 256

	// defaultRxQueue bounds the datagrams waiting to be read by the consumer.
	defaultRxQueue = 1024

	// defaultDialTimeout bounds dialing one stream and, on the listening side,
	// reading its handshake.
	defaultDialTimeout = 10 * time.Second

	// defaultKeepAlive is the TCP keepalive period used when Config.KeepAlive
	// is unset. It is how a peer that vanished without a FIN is eventually
	// reaped.
	defaultKeepAlive = 30 * time.Second

	// defaultMaxStreams caps how many concurrent TCP streams one peer may attach
	// when Config.MaxStreams is unset.
	defaultMaxStreams = 16

	// defaultMaxStreamsTotal caps how many streams every peer of one listening
	// endpoint may hold together, including streams still completing their
	// handshake. Per-peer caps alone leave the total unbounded, so this is the
	// bound that keeps an unauthenticated peer — or a crowd of them — from
	// consuming the listener's descriptors, goroutines and buffers.
	//
	// The value is sized from the memory it can pin, not from a peer count.
	// Each stream holds a readBufSize (64 KiB) receive buffer plus, under
	// backpressure, up to QueueDepth queued datagrams. At the defaults that is
	// roughly 64 KiB + 256x1.4 KiB ~= 0.4 MiB per stream, so the worst case here
	// is about 200 MiB — comparable to what the UDP path it replaces will
	// happily allocate for the same peer count. Raising this raises that ceiling
	// linearly, which is why it is a small number rather than a large one.
	defaultMaxStreamsTotal = 512

	// handshakeQueueDepth bounds the streams whose handshake has not been
	// verified yet. Unauthenticated streams are the cheapest thing to create and
	// the most expensive to hold, so they get a much smaller budget than
	// verified ones.
	handshakeQueueDepth = 64

	// defaultStallTimeout bounds how long one send waits behind a stream's full
	// queue before that stream is judged wedged. It exists because kcp-go never
	// sets a write deadline on the packet connection (its own SetDeadline only
	// stores the value), so without it a single stream that stops reading would
	// block KCP's post-processing goroutine forever.
	//
	// It is deliberately generous, and it is a liveness backstop rather than a
	// backpressure knob. A queue legitimately stays full for as long as the
	// writer is blocked in a socket write, and a KCP flush burst into a slow
	// link can keep a 256-deep queue full for many seconds: 350 KB drains in
	// ~3 s at 1 Mbit/s and ~11 s at 256 kbit/s, which is exactly the kind of
	// path this transport is meant to serve. Dropping a stream mid-transfer
	// there would kill a healthy session. Waiting a minute, by contrast, only
	// ever costs one retransmitted datagram, and a stream that is genuinely
	// dead — a peer that stopped reading — is still reaped. Values below 1 mean
	// the default.
	defaultStallTimeout = 60 * time.Second

	// closeDrainTimeout bounds how long a deliberate close waits for datagrams
	// already queued on a stream to reach the wire.
	closeDrainTimeout = 500 * time.Millisecond
)

// ErrProtocol reports a malformed stream: a bad magic, an unsupported version,
// or an out of range frame length. The stream is closed when it surfaces.
var ErrProtocol = errors.New("carrier: protocol error")

// Config tunes a carrier endpoint. The zero value is usable only on a dialing
// endpoint, which needs no secret; a listening endpoint requires Config.Secret
// and refuses every stream until one is set, because it cannot tell a genuine
// peer from an attacker without it.
type Config struct {
	// Secret authenticates the handshake. Both ends must be configured with the
	// same bytes, which in kcptun is the PBKDF2-derived session key both
	// binaries already compute.
	//
	// A listening endpoint rejects every stream while this is empty: the wire
	// carries the session id in the clear, so without the secret the listener
	// has no way to tell a peer from anyone who read the id.
	Secret []byte

	// Streams is the number of parallel TCP connections carrying one logical
	// session when dialing. Datagrams are spread across them so a single slow
	// stream does not become a head-of-line bottleneck. Values below 1 mean 1.
	//
	// On a listening endpoint this field is ignored: the dialer decides how
	// many streams a session uses, bounded by MaxStreams.
	Streams int

	// MaxStreamsTotal caps how many streams every peer of a listening endpoint
	// may hold together, including streams still completing their handshake.
	// It is the listener-wide admission bound: MaxStreams alone bounds one peer
	// while leaving the total unbounded. Values below 1 mean
	// defaultMaxStreamsTotal.
	MaxStreamsTotal int

	// MaxStreams caps how many concurrent TCP streams one peer may attach to a
	// listening endpoint. A stream beyond the cap is closed immediately.
	// Values below 1 mean defaultMaxStreams.
	MaxStreams int

	// QueueDepth bounds how many datagrams may wait for transmission on one
	// stream. It is the per-stream flow control window. Values below 1 mean
	// defaultQueueDepth.
	QueueDepth int

	// DialTimeout bounds establishing one TCP stream and, on the listening
	// side, reading its handshake.
	DialTimeout time.Duration

	// StallTimeout bounds how long one send may wait behind a stream's full
	// queue before the stream is dropped as wedged and the datagram is retried
	// on another. Values below 1 mean defaultStallTimeout.
	StallTimeout time.Duration

	// KeepAlive is the TCP keepalive period for every stream. Zero means
	// defaultKeepAlive; a negative value disables keepalives.
	KeepAlive time.Duration
}

// withDefaults returns the configuration with unset fields replaced by their
// defaults.
func (c Config) withDefaults() Config {
	if c.Streams < 1 {
		c.Streams = defaultStreams
	}
	if c.MaxStreams < 1 {
		c.MaxStreams = defaultMaxStreams
	}
	if c.QueueDepth < 1 {
		c.QueueDepth = defaultQueueDepth
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = defaultDialTimeout
	}
	if c.StallTimeout < 1 {
		c.StallTimeout = defaultStallTimeout
	}
	if c.MaxStreamsTotal < 1 {
		c.MaxStreamsTotal = defaultMaxStreamsTotal
	}
	if c.KeepAlive == 0 {
		c.KeepAlive = defaultKeepAlive
	}
	return c
}

// PeerAddr identifies one logical carrier peer: the set of TCP streams that
// announced a single session id, as the receiving endpoint sees it. KCP keys
// its sessions by the string form of the address it is handed, so every stream
// of one session has to resolve to the same value.
//
// It is used through the pointer form, because that is what this package hands
// to ReadFrom: a *PeerAddr fits in a net.Addr without boxing, so the receive
// path can reuse one address per peer instead of allocating one per datagram.
type PeerAddr struct {
	ID uint64
}

// Network implements net.Addr.
func (a *PeerAddr) Network() string { return "carrier" }

// String implements net.Addr.
func (a *PeerAddr) String() string { return "carrier:" + strconv.FormatUint(a.ID, 16) }

// peerID recovers the peer id from an address produced by this package.
func peerID(addr net.Addr) (uint64, bool) {
	if a, ok := addr.(*PeerAddr); ok && a != nil {
		return a.ID, true
	}
	return 0, false
}

// randID returns a fresh random session id. Ids are random rather than
// sequential so that a reconnecting dialer never reuses the address of a
// session the listener has not reaped yet.
func randID() uint64 {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		return uint64(time.Now().UnixNano())
	}
	return binary.LittleEndian.Uint64(b[:])
}
