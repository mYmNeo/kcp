package carrier

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"
)

// flakyListener wraps a real listener and fails the first few Accept calls with
// EMFILE, which is what the kernel reports when a process runs out of
// descriptors. That is exactly the condition acceptLoop must survive: the
// socket stays bound and healthy, so exiting would leave the endpoint
// permanently deaf while connections kept queueing in the backlog.
type flakyListener struct {
	net.Listener
	remaining int
}

func (l *flakyListener) Accept() (net.Conn, error) {
	if l.remaining > 0 {
		l.remaining--
		return nil, emfileError()
	}
	return l.Listener.Accept()
}

// emfileError mimics syscall.EMFILE as the net package surfaces it: an
// *net.OpError wrapping the errno, which is the shape errors.Is unwraps.
func emfileError() error {
	return &net.OpError{Op: "accept", Err: syscall.EMFILE}
}

// permanentListener fails every Accept with an error the kernel will not
// recover from, which is the case acceptLoop must give up on.
type permanentListener struct{ net.Listener }

func (l *permanentListener) Accept() (net.Conn, error) {
	return nil, &net.OpError{Op: "accept", Err: syscall.EAFNOSUPPORT}
}

// TestAcceptLoopSurvivesTransientErrors pins the fix for the worst failure mode
// this package can have: a transient accept error used to end acceptLoop, which
// left the listener permanently deaf. The socket stays bound in that state, so
// clients still connect successfully and then hang forever — recoverable only by
// restarting the process.
func TestAcceptLoopSurvivesTransientErrors(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer inner.Close()

	l := newTestListener(&flakyListener{Listener: inner, remaining: 3})
	l.wg.Add(1)
	go l.acceptLoop()
	defer l.Close()

	// The listener must still serve streams after the transient failures.
	conn, err := net.Dial("tcp", inner.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// The handshake blocks on reads, so bound it: a regression must fail this
	// test rather than hang the suite until the package timeout.
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := dialHandshake(conn, testSecret, 0xABCD); err != nil {
		t.Fatalf("handshake after transient accept failures: %v", err)
	}
	conn.SetDeadline(time.Time{})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		l.peerMu.RLock()
		peers := len(l.peers)
		l.peerMu.RUnlock()
		if peers == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("accept loop did not recover: the stream was never admitted")
}

// TestPermanentAcceptErrorIsReported pins that a permanent accept failure ends
// the loop *and* surfaces: ReadFrom must report it, because otherwise the
// endpoint stays bound, accepts connections into the kernel backlog, and never
// reads them, with nothing in the log to say so.
func TestPermanentAcceptErrorIsReported(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer inner.Close()

	l := newTestListener(&permanentListener{Listener: inner})
	l.wg.Add(1)
	go l.acceptLoop()
	defer l.Close()

	// The read must unblock with the accept error rather than sit in the select
	// until a deadline or a shutdown.
	l.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 64)
	_, _, err = l.ReadFrom(buf)
	if !errors.Is(err, syscall.EAFNOSUPPORT) {
		t.Fatalf("ReadFrom after a permanent accept error = %v, want EAFNOSUPPORT", err)
	}

	// And it must stay reported, not clear on the next call.
	if _, _, err = l.ReadFrom(buf); !errors.Is(err, syscall.EAFNOSUPPORT) {
		t.Fatalf("second ReadFrom = %v, want the same sticky error", err)
	}
}

// TestTransientAcceptClassification pins the classification itself against the
// errnos it names, so a change to isTransientAccept has to be deliberate. A
// false negative leaves the endpoint deaf; a false positive turns a permanent
// failure into a spin.
func TestTransientAcceptClassification(t *testing.T) {
	transient := []error{
		&net.OpError{Op: "accept", Err: syscall.EMFILE},
		&net.OpError{Op: "accept", Err: syscall.ENFILE},
		&net.OpError{Op: "accept", Err: syscall.ECONNABORTED},
		&net.OpError{Op: "accept", Err: syscall.ECONNRESET},
		&net.OpError{Op: "accept", Err: syscall.EINTR},
		// net wraps the errno further in practice; the chain must still resolve.
		fmt.Errorf("outer: %w", &net.OpError{Op: "accept", Err: syscall.EMFILE}),
	}
	for _, err := range transient {
		if !isTransientAccept(err) {
			t.Errorf("isTransientAccept(%v) = false, want true", err)
		}
	}

	permanent := []error{
		&net.OpError{Op: "accept", Err: syscall.EAFNOSUPPORT},
		&net.OpError{Op: "accept", Err: syscall.EINVAL},
		&net.OpError{Op: "accept", Err: syscall.EBADF},
		errors.New("accept: no such device"),
	}
	for _, err := range permanent {
		if isTransientAccept(err) {
			t.Errorf("isTransientAccept(%v) = true, want false", err)
		}
	}
}

// TestCloseReleasesInFlightHandshake pins that Close does not wait out the
// handshake deadline for a peer that connects and then says nothing. attach
// holds a WaitGroup slot across that read, so without this an unauthenticated
// peer could pin every shutdown.
func TestCloseReleasesInFlightHandshake(t *testing.T) {
	cfg := testConfig()
	cfg.DialTimeout = 10 * time.Second // far longer than the assertion below

	ln, err := ListenWithConfig("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("ListenWithConfig: %v", err)
	}

	var silent []net.Conn
	for i := 0; i < 3; i++ {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		silent = append(silent, c)
	}
	defer func() {
		for _, c := range silent {
			c.Close()
		}
	}()

	// Let attach reach the blocking handshake read.
	time.Sleep(200 * time.Millisecond)

	done := make(chan struct{})
	go func() { ln.Close(); close(done) }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked on a silent peer's in-flight handshake")
	}
}

// TestCloseStrandsNoStream pins the shutdown contract: once Close returns, no
// peer may still hold a live stream.
//
// Close swaps the peer map and then walks the old one, while attach goroutines
// may still be finishing their handshakes. A stream admitted after that swap
// would never be released — its socket, its two goroutines and its read buffer
// would leak for the life of the process. The window is narrow, so this drives
// it many times rather than once.
//
// The peers must be snapshotted before Close runs: Close replaces the map
// before tearing the old one down, so asserting against ln.peers afterwards
// would inspect an empty map and pass no matter what leaked.
func TestCloseStrandsNoStream(t *testing.T) {
	const attempts = 300
	stranded := 0

	for i := 0; i < attempts; i++ {
		ln, err := ListenWithConfig("tcp", "127.0.0.1:0", testConfig())
		if err != nil {
			t.Fatalf("ListenWithConfig: %v", err)
		}

		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatalf("dial: %v", err)
		}

		// Complete the handshake but send no datagrams, then race Close against
		// the listener's attach goroutine.
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		if err := dialHandshake(conn, testSecret, uint64(i)+1); err != nil {
			t.Fatalf("handshake: %v", err)
		}
		conn.SetDeadline(time.Time{})

		// Snapshot the peers Close is about to walk. Close replaces the peer map
		// before it tears the old one down, so reading ln.peers afterwards would
		// inspect an empty map and assert nothing.
		ln.peerMu.RLock()
		before := make([]*peer, 0, len(ln.peers))
		for _, p := range ln.peers {
			before = append(before, p)
		}
		ln.peerMu.RUnlock()

		done := make(chan struct{})
		go func() { ln.Close(); close(done) }()
		<-done

		for _, p := range before {
			if s := p.streams.pick(); s != nil {
				stranded++
			}
		}

		conn.Close()
	}

	if stranded > 0 {
		t.Fatalf("%d of %d listeners kept a live stream after Close returned", stranded, attempts)
	}
}

// TestPeerRejectsReplayedIDFromAnotherSource pins the peer pinning rule.
//
// The session id is announced in the clear, so the source-IP pin is the defence
// in depth behind handshake authentication: an observer who learned an id, and
// who could sign a handshake, still cannot attach a stream from another address.
// A stream from the address that created the peer must still be accepted, or the
// parallel-stream fan-out would break.
func TestPeerRejectsReplayedIDFromAnotherSource(t *testing.T) {
	ln, err := ListenWithConfig("tcp", "127.0.0.1:0", testConfig())
	if err != nil {
		t.Fatalf("ListenWithConfig: %v", err)
	}
	defer ln.Close()

	const id = 0x1234
	const owner = "203.0.113.9"

	ln.peerMu.Lock()
	ln.peers[id] = &peer{ip: owner, addr: &PeerAddr{ID: id}, streams: newStreamSet(nil)}
	ln.peerMu.Unlock()

	// A stream from elsewhere claiming the same id is refused.
	stranger := newStream(&blockingConn{ch: make(chan struct{})}, 4)
	if _, ok := ln.admit(id, "127.0.0.1", stranger); ok {
		t.Error("stream from a different source was admitted to the peer")
	}

	// The peer's own address is still allowed to open another parallel stream.
	same := newStream(&blockingConn{ch: make(chan struct{})}, 4)
	if _, ok := ln.admit(id, owner, same); !ok {
		t.Error("stream from the peer's own source was refused")
	}
}

// TestHandshakeRejectsGarbage pins that a stream that does not speak the carrier
// protocol is dropped instead of being fed to KCP as datagrams.
func TestHandshakeRejectsGarbage(t *testing.T) {
	ln, err := ListenWithConfig("tcp", "127.0.0.1:0", testConfig())
	if err != nil {
		t.Fatalf("ListenWithConfig: %v", err)
	}
	defer ln.Close()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial listener: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Write(bytes.Repeat([]byte{0xFF}, handshakeBaseSize)); err != nil {
		t.Fatalf("write garbage handshake: %v", err)
	}

	// The contract is that the stream is closed without ever reaching KCP, so
	// assert that directly instead of polling the peer map for the absence of an
	// entry: a transient registration would be invisible to a poll, and a poll
	// that happens to run late would pass anyway.
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("garbage handshake was accepted: the stream was not closed")
	}

	ln.peerMu.RLock()
	peers := len(ln.peers)
	ln.peerMu.RUnlock()
	if peers != 0 {
		t.Fatalf("listener registered %d peer(s) for a garbage handshake", peers)
	}
}

// TestHandshakeAuthRequiresSecret pins the authentication boundary: a stream
// that speaks the framing protocol but cannot prove knowledge of the shared
// secret is refused, and the listener never registers it as a peer.
//
// The session id is carried in the clear, so this is the check that stops
// anyone who read it — off the wire, or off the server log, which prints the
// peer address per session — from attaching a stream to a live session and
// receiving or stalling its traffic.
func TestHandshakeAuthRequiresSecret(t *testing.T) {
	cfg := testConfig()
	cfg.Streams = 1

	ln, err := ListenWithConfig("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("ListenWithConfig: %v", err)
	}
	defer ln.Close()

	// A genuine dialer first, so there is a live session to attack.
	good, err := Dial("tcp", ln.Addr().String(), cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer good.Close()
	good.WriteTo([]byte("hello"), good.RemoteAddr())

	buf := make([]byte, 64)
	ln.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, addr, err := ln.ReadFrom(buf)
	if err != nil {
		t.Fatalf("listener ReadFrom: %v", err)
	}

	// Now replay that session id from the same source IP with the wrong secret,
	// which is exactly what an attacker who only read the id can do.
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial attacker: %v", err)
	}
	defer conn.Close()

	wrong := Config{Secret: []byte("not-the-secret")}
	if err := dialHandshake(conn, wrong.Secret, addr.(*PeerAddr).ID); err == nil {
		t.Fatal("listener accepted a handshake signed with the wrong secret")
	}

	// And the victim's own stream must survive the attempt untouched.
	if _, err := good.WriteTo([]byte("still alive"), good.RemoteAddr()); err != nil {
		t.Fatalf("victim stream broken by the replay attempt: %v", err)
	}
}

// TestListenRequiresSecret pins that a listener refuses to run unauthenticated
// rather than silently admitting every stream.
func TestListenRequiresSecret(t *testing.T) {
	if _, err := ListenWithConfig("tcp", "127.0.0.1:0", Config{Streams: 1}); !errors.Is(err, errNoSecret) {
		t.Fatalf("ListenWithConfig without a secret = %v, want %v", err, errNoSecret)
	}
}

// TestPeerStreamCap pins the bound that keeps one peer from consuming the whole
// listener's descriptor budget. The client asks for more streams than the cap
// allows; the excess must be refused without breaking the peer that obeyed.
func TestPeerStreamCap(t *testing.T) {
	cfg := testConfig()
	cfg.MaxStreams = 2

	ln, err := ListenWithConfig("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("ListenWithConfig: %v", err)
	}
	defer ln.Close()

	// The dialer uses more streams than the listener permits.
	clientCfg := testConfig()
	clientCfg.Streams = 5

	c, err := Dial("tcp", ln.Addr().String(), clientCfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	if _, err := c.WriteTo([]byte("still works"), c.RemoteAddr()); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	buf := make([]byte, 64)
	ln.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, addr, err := ln.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if string(buf[:n]) != "still works" {
		t.Fatalf("read %q", buf[:n])
	}

	ln.peerMu.RLock()
	held := 0
	for _, peer := range ln.peers {
		held += peer.streams.len()
	}
	ln.peerMu.RUnlock()

	if held > cfg.MaxStreams {
		t.Fatalf("listener holds %d streams for one peer, want at most %d", held, cfg.MaxStreams)
	}

	// The peer must still be usable for a reply in the other direction.
	if _, err := ln.WriteTo([]byte("reply"), addr); err != nil {
		t.Fatalf("listener WriteTo after cap: %v", err)
	}
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := c.ReadFrom(buf); err != nil {
		t.Fatalf("conn ReadFrom after cap: %v", err)
	}
}

// TestWriteToUnknownPeerFails pins that a datagram addressed to a peer the
// listener does not know about is refused rather than silently dropped, so KCP
// can react to the failure.
func TestWriteToUnknownPeerFails(t *testing.T) {
	ln, err := ListenWithConfig("tcp", "127.0.0.1:0", testConfig())
	if err != nil {
		t.Fatalf("ListenWithConfig: %v", err)
	}
	defer ln.Close()

	if _, err := ln.WriteTo([]byte("x"), &PeerAddr{ID: 0xdeadbeef}); err == nil {
		t.Fatal("WriteTo accepted an unknown peer address")
	}
}

// blockingConn is a net.Conn whose reads block until it is closed, so a test
// can hold a stream open without a live socket.
type blockingConn struct{ ch chan struct{} }

func (c *blockingConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *blockingConn) Read(p []byte) (int, error)       { <-c.ch; return 0, nil }
func (c *blockingConn) Close() error                     { return nil }
func (c *blockingConn) LocalAddr() net.Addr              { return testAddr{} }
func (c *blockingConn) RemoteAddr() net.Addr             { return testAddr{} }
func (c *blockingConn) SetDeadline(time.Time) error      { return nil }
func (c *blockingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *blockingConn) SetWriteDeadline(time.Time) error { return nil }

// testAddr stands in for a real socket address in tests that never dial.
type testAddr struct{}

func (testAddr) Network() string { return "test" }
func (testAddr) String() string  { return "test" }

// TestListenerGlobalStreamBudget pins the listener-wide admission bound. The
// per-peer cap alone leaves the total unbounded, so this is what keeps one
// unauthenticated peer — or a crowd of them — from consuming every descriptor,
// goroutine and read buffer the listener can hold.
func TestListenerGlobalStreamBudget(t *testing.T) {
	cfg := testConfig()
	cfg.MaxStreamsTotal = 4
	cfg.MaxStreams = 4

	ln, err := ListenWithConfig("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("ListenWithConfig: %v", err)
	}
	defer ln.Close()

	// Distinct session ids, so the per-peer cap cannot be what refuses them.
	var conns []net.Conn
	for i := 0; i < cfg.MaxStreamsTotal; i++ {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conns = append(conns, conn)
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		if err := dialHandshake(conn, testSecret, uint64(i)+1); err != nil {
			t.Fatalf("handshake %d: %v", i, err)
		}
		conn.SetDeadline(time.Time{})
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	// One more must be refused: the budget is full.
	extra, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial extra: %v", err)
	}
	defer extra.Close()
	// The refusal is detected by the read failing, so bound it too.
	extra.SetDeadline(time.Now().Add(5 * time.Second))
	if err := dialHandshake(extra, testSecret, 0xFFFF); !errors.Is(err, ErrAuth) {
		t.Fatalf("stream beyond MaxStreamsTotal = %v, want %v", err, ErrAuth)
	}

	// Let the refused attempt retire so the count reflects only admitted
	// streams, then confirm the admitted ones are exactly the budget.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ln.peerMu.RLock()
		got := ln.streams
		ln.peerMu.RUnlock()
		if got == cfg.MaxStreamsTotal {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	ln.peerMu.RLock()
	got := ln.streams
	ln.peerMu.RUnlock()
	t.Fatalf("listener holds %d streams, want %d", got, cfg.MaxStreamsTotal)
}

// failDeadlineClearConn forwards to a real connection but fails the second
// SetDeadline call. attach arms the handshake deadline first and clears it
// after admit, so this reproduces the one failure path that runs *after* the
// stream has been published into its peer's set — exactly what Close triggers
// when it closes the connections still sitting in l.handshaking.
type failDeadlineClearConn struct {
	net.Conn
	calls int
}

func (c *failDeadlineClearConn) SetDeadline(t time.Time) error {
	c.calls++
	if c.calls == 2 {
		return net.ErrClosed
	}
	return c.Conn.SetDeadline(t)
}

// TestAttachFailureAfterAdmitRetiresStream pins that a failure between admit
// and the start of a stream's goroutines does not strand it.
//
// admit publishes the stream into its peer's set and stops there; from that
// point the peer set is what owns the stream, and only detach may return its
// slot. The failure path used to call release(conn, false) and close the
// socket directly, which left the stream in the set forever — holding its
// read buffer and goroutine budget — and handed its slot back a second time
// when the close finally reaped it, driving l.streams negative.
//
// This drives attach itself rather than racing Close, so the failure is
// deterministic instead of a narrow-timing reproduction.
func TestAttachFailureAfterAdmitRetiresStream(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer raw.Close()

	l := newTestListener(raw)
	defer l.Close()

	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		c, err := net.Dial("tcp", raw.Addr().String())
		if err != nil {
			return
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(5 * time.Second))
		// The listener fails before acknowledging, so this reports ErrAuth and
		// the goroutine simply ends; the test asserts on listener state.
		dialHandshake(c, testSecret, 0xA11CE)
	}()

	server, err := raw.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}

	l.wg.Add(1)
	l.attach(&failDeadlineClearConn{Conn: server})
	<-clientDone

	l.peerMu.RLock()
	held, live := l.streams, 0
	for _, p := range l.peers {
		if p.streams.pick() != nil {
			live++
		}
	}
	peers := len(l.peers)
	l.peerMu.RUnlock()

	// The stream must not survive in the peer set, and the counters must agree
	// with what is actually held.
	if peers != 0 || live != 0 {
		t.Fatalf("failed attach stranded %d peer(s) holding %d live stream(s), want none", peers, live)
	}
	if held != 0 {
		t.Fatalf("listener holds %d stream slot(s) after a failed attach, want 0", held)
	}
}
