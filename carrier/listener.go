package carrier

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Accept backoff bounds. A transient accept failure is retried with exponential
// delay, clamped so a persistent descriptor shortage settles into a slow poll
// rather than a busy loop or a long stall once the shortage clears.
const (
	acceptBackoffMin = 5 * time.Millisecond
	acceptBackoffMax = time.Second
)

// Listen is a net.PacketConn that recovers KCP datagrams from inbound TCP
// streams. It is the mirror image of Conn: where Conn stripes one session over
// several streams, Listen collects the streams that announce the same session
// id back into one logical peer and presents them to KCP as a single packet
// socket.
//
// Multiple peers are served from one listener. KCP distinguishes them by the
// address ReadFrom reports, so each peer gets a stable PeerAddr derived from its
// session id, and datagrams from all of its streams are interleaved into one
// receive queue.
//
// The handshake carries the session id in the clear, but it does not trust it:
// every stream must also prove knowledge of the shared secret, and the listener
// refuses to run without one. As defence in depth, a peer is additionally pinned
// to the source IP of the stream that created it, so a stolen id is not enough
// on its own, and a stream that fails authentication never reaches KCP.
//
// Listen implements net.PacketConn, so it can be handed straight to
// kcp.ServeConn. The caller owns it there (ownConn false), which means closing
// the kcp.Listener does not close the carrier; call Close explicitly.
type Listener struct {
	ln   net.Listener
	lcfg Config

	rx chan rxPacket
	rd deadline
	wd deadline

	peers  map[uint64]*peer
	peerMu sync.RWMutex

	// streams counts every stream the listener holds, including the ones still
	// completing their handshake, and is the listener-wide admission bound that
	// MaxStreams cannot provide: MaxStreams bounds one peer, this bounds them
	// all. pending counts only unverified streams, which get a much smaller
	// budget because they are cheap to create and expensive to hold.
	streams int
	pending int

	// handshaking holds the connections still completing their handshake, so
	// Close can release them instead of waiting out their read deadlines.
	handshaking map[net.Conn]struct{}

	opts sockOptions

	chClosed chan struct{}
	once     sync.Once

	// fatal carries the error that ended the accept loop, if any. It mirrors
	// Conn.fatal: a listener that has stopped accepting is useless, but it must
	// not be useless *silently*. Reporting it through ReadFrom sends the error
	// down the path the caller already watches — kcp-go's monitor turns a
	// ReadFrom failure into a socket read error, which surfaces from AcceptKCP
	// and reaches the server's log — instead of leaving an endpoint that is
	// bound, healthy-looking, and permanently deaf.
	fatal     atomic.Pointer[error]
	chFatal   chan struct{}
	fatalOnce sync.Once

	wg sync.WaitGroup
}

// fail records the error that ended the accept loop and wakes every blocked
// caller. It is idempotent, and the first error is the one that sticks.
func (l *Listener) fail(err error) {
	if err == nil {
		err = net.ErrClosed
	}
	l.fatalOnce.Do(func() {
		e := err
		l.fatal.Store(&e)
		close(l.chFatal)
	})
}

// fatalErr reports the sticky failure, if any.
func (l *Listener) fatalErr() error {
	if p := l.fatal.Load(); p != nil {
		return *p
	}
	return nil
}

// peer is the set of TCP streams that announced one session id.
//
// The streams live in a streamSet rather than a slice so that the send path
// picks a stream with no lock and no allocation; see streamSet for why that
// matters. add and remove are called with the listener's peerMu held, which is
// what keeps the "is this peer still alive" decision consistent with the map.
//
// ip is the source IP of the stream that created the peer; later streams must
// match it. It is deliberately not part of the peer's identity, so PeerAddr
// keeps reporting the session id alone.
//
// addr is the peer's address, stored as a pointer so that handing it to the
// receive queue fits in net.Addr without boxing. A fresh value (or a new peer)
// per datagram would put one heap allocation on the receive hot path.
type peer struct {
	ip      string
	addr    *PeerAddr
	streams *streamSet
}

// ListenWithConfig binds a TCP listener and starts accepting carrier streams.
// network must be a TCP network; an empty network means "tcp".
func ListenWithConfig(network, address string, cfg Config) (*Listener, error) {
	ln, err := net.Listen(normalizeNetwork(network), address)
	if err != nil {
		return nil, err
	}

	l := &Listener{
		ln:          ln,
		lcfg:        cfg.withDefaults(),
		rx:          make(chan rxPacket, defaultRxQueue),
		peers:       make(map[uint64]*peer),
		handshaking: make(map[net.Conn]struct{}),
		chClosed:    make(chan struct{}),
		chFatal:     make(chan struct{}),
	}

	// A listener cannot authenticate anything without the shared secret, so it
	// refuses to start rather than admitting every stream on the honour system.
	// Failing here is the only safe behavior: silently ignoring a missing secret
	// would leave authentication disabled while looking configured.
	if len(l.lcfg.Secret) == 0 {
		ln.Close()
		return nil, errNoSecret
	}

	l.wg.Add(1)
	go l.acceptLoop()

	return l, nil
}

// Addr returns the listener's local address.
func (l *Listener) Addr() net.Addr { return l.ln.Addr() }

// acceptLoop accepts TCP streams and attaches each one to its peer.
//
// A transient accept failure — EMFILE/ENFILE when the process runs out of
// descriptors, EINTR, a per-connection resource shortage — must not end the
// loop. The listening socket is deliberately still bound and healthy in those
// cases, so returning here would leave the endpoint permanently deaf while the
// kernel went on accepting connections into its backlog: an unauthenticated peer
// could take the whole transport down for good by exhausting descriptors once.
// Only a closed listener (or a permanent error) ends the loop.
func (l *Listener) acceptLoop() {
	defer l.wg.Done()

	backoff := acceptBackoffMin
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}

			// This decision is the whole reason the loop exists: an accept
			// failure the kernel will recover from must not end it, and one it
			// will not must not be retried forever. net.Error.Temporary is the
			// tempting form of that test, and has been deprecated since Go 1.18
			// precisely because it is not answerable: the predicate is defined
			// per platform and per errno, so a stdlib change could reclassify a
			// transient shortage as permanent — or the reverse — without this
			// package changing a line. The kinds are therefore named explicitly,
			// which is also what the comment above this loop has always claimed.
			if !isTransientAccept(err) {
				// A permanent error is as fatal as a closed listener: retrying
				// would spin without ever making progress. It is recorded rather
				// than only returned from, because returning silently would
				// leave the endpoint bound, accepting connections into the
				// kernel backlog, and never reading them again.
				l.fail(fmt.Errorf("carrier: accept: %w", err))
				return
			}

			// Back off so a persistent shortage does not become a busy loop.
			// The delay resets after the next successful accept.
			select {
			case <-time.After(backoff):
			case <-l.chClosed:
				return
			}
			if backoff < acceptBackoffMax {
				backoff *= 2
			}
			continue
		}
		backoff = acceptBackoffMin

		l.wg.Add(1)
		go l.attach(conn)
	}
}

// isTransientAccept reports whether an accept error is worth retrying.
//
// The kinds are named rather than delegated to net.Error.Temporary, whose
// definition is deprecated and platform-defined; see acceptLoop. Each one is
// classified by what it says about the listening socket, not by whether it is
// convenient to retry: ECONNABORTED and ECONNRESET mean a peer vanished between
// its SYN and this accept, EINTR means a signal interrupted the syscall, and
// EMFILE/ENFILE mean the process is out of descriptors. In every case the
// listening socket itself is still healthy, so giving up would leave it deaf
// over a condition that clears on its own.
func isTransientAccept(err error) bool {
	return errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EINTR) ||
		errors.Is(err, syscall.EMFILE) ||
		errors.Is(err, syscall.ENFILE)
}

// attach authenticates a stream handshake and files the stream under its peer.
// A stream whose handshake does not prove knowledge of the shared secret, or
// that would exceed one of the listener's budgets, is closed — after being told
// so — without ever reaching KCP.
//
// The handshake is read with io.ReadFull over a fixed-size slice, so it can
// never consume bytes belonging to the frames behind it: the first frame must
// still be intact when the reader goroutine starts.
func (l *Listener) attach(conn net.Conn) {
	defer l.wg.Done()

	// Tune the socket before anything else. This is the only place an accepted
	// stream gets TCP_NODELAY and keepalive, and the only place the socket
	// options KCP asked for (SO_RCVBUF/SO_SNDBUF/DSCP, which kcp-go forwards to
	// the PacketConn) are applied to a stream that did not exist yet.
	tune(conn, l.lcfg)
	l.opts.applyTo(conn)

	// Reserve before reading anything. This is where the listener commits
	// resources to an unauthenticated peer, so it is where both budgets must be
	// enforced.
	if !l.reserve(conn) {
		conn.Close()
		return
	}

	// The handshake runs under a deadline so a peer that connects and then says
	// nothing cannot hold its reservation indefinitely. It is a full deadline,
	// not a read one: attach writes the challenge and later the ack, and a peer
	// that advertises a tiny receive window and then stops reading must not be
	// able to block either write — attach holds a budget slot and a WaitGroup
	// slot across this whole section.
	if err := conn.SetDeadline(time.Now().Add(l.lcfg.DialTimeout)); err != nil {
		l.release(conn, false)
		conn.Close()
		return
	}
	id, err := readHello(conn)
	if err != nil {
		l.release(conn, false)
		conn.Close()
		return
	}
	// Issue a fresh challenge before accepting any proof. Because the nonce is
	// chosen here and used once, a handshake recorded earlier — even one from
	// this same session — cannot be replayed onto this connection.
	nonce, proof, err := serveHandshake(conn, l.lcfg.Secret, id)
	if err != nil {
		l.release(conn, false)
		conn.Close()
		return
	}
	// Build the stream completely before admit publishes it. onExit must be in
	// place by then: admit hands the stream to the peer's set under peerMu, and
	// Close walks that same set under the same lock, so assigning the field
	// afterwards would race a close that is already running.
	s := newStream(conn, l.lcfg.QueueDepth)
	s.onExit = func(dead *stream) { l.detach(id, dead) }

	p, ok := l.admit(id, sourceIP(conn.RemoteAddr()), s)
	if !ok {
		l.release(conn, false)
		conn.Close()
		return
	}

	// Only a stream that got in is acknowledged; a refused peer sees the close.
	// The ack is written while the handshake deadline is still armed, so a peer
	// that stopped reading cannot block this write.
	writeAck(conn, l.lcfg.Secret, id, nonce, proof)

	// The handshake is over, so the deadline goes away before the stream starts
	// running: leaving it armed would apply it to every later socket operation.
	if err := conn.SetDeadline(time.Time{}); err != nil {
		// admit has already published the stream, so it is the peer set that
		// owns it now and detach that must give its slot back. Retiring the
		// reservation as kept and tearing the stream down through fail is the
		// only path that reaches detach: returning the slot here instead would
		// leave the stream in the set forever, and hand the slot back a second
		// time when the close finally reaped it.
		l.release(conn, true)
		s.fail(err)
		return
	}

	// The stream now holds its slot until it exits; only the pending reservation
	// is retired here.
	l.release(conn, true)

	// p.addr is the peer's address as a pointer, so putting it into rxPacket's
	// net.Addr field does not box a fresh value per datagram.
	s.start(func(payload []byte) error {
		return pushRx(l.rx, l.chClosed, rxPacket{data: payload, addr: p.addr})
	})
}

// sourceIP returns the source IP of an accepted stream, used to pin a peer to
// the address that created it. It returns "" for a transport that exposes no IP.
func sourceIP(addr net.Addr) string {
	if tcp, ok := addr.(*net.TCPAddr); ok {
		return tcp.IP.String()
	}
	return ""
}

// reserve claims a stream slot for a connection that has not authenticated yet.
// Both budgets are checked here, under peerMu, so that concurrent arrivals
// cannot each pass a check that only one of them should have.
func (l *Listener) reserve(conn net.Conn) bool {
	l.peerMu.Lock()
	defer l.peerMu.Unlock()

	if l.isClosing() {
		return false
	}
	if l.streams >= l.lcfg.MaxStreamsTotal {
		return false
	}
	if l.pending >= handshakeQueueDepth {
		return false
	}

	l.streams++
	l.pending++
	l.handshaking[conn] = struct{}{}
	return true
}

// release retires a reservation. keep is true when the connection continues as
// a verified stream, which holds its slot until detach releases it, and false
// when the attempt is over and the slot goes back.
func (l *Listener) release(conn net.Conn, keep bool) {
	l.peerMu.Lock()
	defer l.peerMu.Unlock()

	delete(l.handshaking, conn)
	l.pending--
	if !keep {
		l.streams--
	}
}

// isClosing reports whether Close has begun. Callers must hold peerMu.
func (l *Listener) isClosing() bool {
	select {
	case <-l.chClosed:
		return true
	default:
		return false
	}
}

// admit files a stream under its peer, creating the peer on first sight, and
// reports whether it was accepted. It refuses a stream once the listener is
// closing, and refuses one whose source IP differs from the address that created
// the peer.
//
// The close check, the source check, the cap check and the insertion all share
// one critical section on purpose: separating any of them would let two streams
// arriving at the same instant both pass a check that only one should have — the
// cap would not bound anything, and a replayed id could slip in beside a genuine
// one.
func (l *Listener) admit(id uint64, ip string, s *stream) (*peer, bool) {
	l.peerMu.Lock()
	defer l.peerMu.Unlock()

	// Close swaps the peer map and then waits. A stream admitted after that
	// swap would never be closed by it, stranding its socket, its goroutines
	// and its read buffer.
	if l.isClosing() {
		return nil, false
	}

	p, ok := l.peers[id]
	if !ok {
		p = &peer{ip: ip, addr: &PeerAddr{ID: id}}
		p.streams = newStreamSet(nil)
		l.peers[id] = p
	} else if p.ip != "" && ip != "" && p.ip != ip {
		return nil, false
	}
	if p.streams.len() >= l.lcfg.MaxStreams {
		return nil, false
	}

	p.streams.add(s)
	return p, true
}

// detach removes a dead stream from its peer and forgets the peer once it holds
// no streams, so a reconnecting dialer that reuses an id starts clean. It also
// returns the stream's slot to the listener-wide budget.
//
// add and remove share the listener's peerMu, so the liveness decision below is
// made against a map that cannot change underneath it.
func (l *Listener) detach(id uint64, dead *stream) {
	l.peerMu.Lock()
	defer l.peerMu.Unlock()

	l.streams--

	p, ok := l.peers[id]
	if !ok {
		return
	}

	p.streams.remove(dead)

	if p.streams.len() == 0 {
		delete(l.peers, id)
	}
}

// ReadFrom reads one datagram from any peer into p, truncating it if p is too
// small. The reported address identifies the peer, so KCP demultiplexes sessions
// exactly as it would with distinct UDP sources.
func (l *Listener) ReadFrom(p []byte) (int, net.Addr, error) {
	// A listener whose accept loop has died reports that here, so the error
	// reaches kcp-go's monitor and surfaces from AcceptKCP. Without it the
	// endpoint stays bound and silent forever; see fail.
	if err := l.fatalErr(); err != nil {
		return 0, nil, err
	}

	timeout, stop := l.rd.timer()
	defer stop()

	select {
	case pkt := <-l.rx:
		return deliver(p, pkt), pkt.addr, nil
	case <-timeout:
		return 0, nil, errDeadline
	case <-l.chFatal:
		return 0, nil, l.fatalErr()
	case <-l.chClosed:
		return 0, nil, net.ErrClosed
	}
}

// WriteTo sends one datagram back to the peer addr identifies. The carrier
// chooses which of that peer's streams takes it, blocking while that stream's
// queue is full and honoring the write deadline while it waits.
func (l *Listener) WriteTo(p []byte, addr net.Addr) (int, error) {
	id, ok := peerID(addr)
	if !ok {
		return 0, errors.New("carrier: WriteTo requires a carrier peer address")
	}
	if len(p) > MaxDatagram {
		return 0, ErrProtocol
	}

	timeout, stop := l.wd.timer()
	defer stop()

	for {
		s, err := l.pick(id)
		if err != nil {
			return 0, err
		}

		if err := s.send(p, timeout, l.lcfg.StallTimeout); err == nil {
			return len(p), nil
		} else if errors.Is(err, errDeadline) {
			return 0, errDeadline
		}

		// Anything else is this stream's problem — it stalled, or it died with a
		// socket error. The datagram is still unsent, so try a sibling.
		// Narrowing this to net.ErrClosed would leak the raw errno of a broken
		// socket to the caller, and kcp-go turns a write error into a permanent
		// session teardown even when other streams are healthy. The loop
		// terminates because pick skips closed streams.
	}
}

// pick returns the least loaded live stream of a peer, without copying the
// stream list: the datagram path must not allocate.
func (l *Listener) pick(id uint64) (*stream, error) {
	l.peerMu.RLock()
	p, ok := l.peers[id]
	l.peerMu.RUnlock()

	if !ok {
		return nil, net.ErrClosed
	}
	if s := p.streams.pick(); s != nil {
		return s, nil
	}
	return nil, net.ErrClosed
}

// Close stops accepting and releases every stream. It is idempotent.
//
// A connection that is mid-handshake also has to be released, not just left to
// run out its read deadline: attach holds a WaitGroup slot across that read, so
// otherwise one peer that connects and says nothing would pin every Close for
// the full handshake timeout.
func (l *Listener) Close() error {
	l.once.Do(func() {
		close(l.chClosed)
		l.ln.Close()

		l.peerMu.Lock()
		peers := l.peers
		l.peers = make(map[uint64]*peer)
		handshaking := make([]net.Conn, 0, len(l.handshaking))
		for conn := range l.handshaking {
			handshaking = append(handshaking, conn)
		}
		l.peerMu.Unlock()

		for _, conn := range handshaking {
			// Closing the socket unblocks the handshake read inside attach, so
			// it reaches its exit path instead of waiting out its deadline.
			conn.Close()
		}
		for _, p := range peers {
			p.streams.closeAll()
		}

		drain(l.rx)
		l.fail(net.ErrClosed)
		l.wg.Wait()
	})
	return nil
}

// LocalAddr reports the listener's local address.
func (l *Listener) LocalAddr() net.Addr { return l.ln.Addr() }

// SetDeadline sets the read and write deadlines.
func (l *Listener) SetDeadline(t time.Time) error {
	l.rd.set(t)
	l.wd.set(t)
	return nil
}

// SetReadDeadline sets the deadline ReadFrom reports.
func (l *Listener) SetReadDeadline(t time.Time) error {
	l.rd.set(t)
	return nil
}

// SetWriteDeadline sets the deadline WriteTo reports while it waits for room on
// a peer's send queue.
func (l *Listener) SetWriteDeadline(t time.Time) error {
	l.wd.set(t)
	return nil
}

// SetReadBuffer applies SO_RCVBUF to every stream, present and future.
func (l *Listener) SetReadBuffer(bytes int) error {
	return l.opts.SetReadBuffer(bytes, l.forEachStream)
}

// SetWriteBuffer applies SO_SNDBUF to every stream, present and future.
func (l *Listener) SetWriteBuffer(bytes int) error {
	return l.opts.SetWriteBuffer(bytes, l.forEachStream)
}

// SetDSCP applies the DSCP field to every stream, present and future.
func (l *Listener) SetDSCP(dscp int) error {
	return l.opts.SetDSCP(dscp, l.forEachStream)
}

func (l *Listener) forEachStream(fn func(*stream)) {
	l.peerMu.RLock()
	peers := make([]*peer, 0, len(l.peers))
	for _, p := range l.peers {
		peers = append(peers, p)
	}
	l.peerMu.RUnlock()

	for _, p := range peers {
		p.streams.each(fn)
	}
}
