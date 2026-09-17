package carrier

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// testSecret is the shared secret the test endpoints authenticate with.
var testSecret = []byte("carrier-test-secret")

// testConfig is the timing profile the tests use. Timeouts are short so a
// regression that deadlocks fails fast instead of hanging the suite.
func testConfig() Config {
	return Config{
		Secret:      testSecret,
		Streams:     1,
		QueueDepth:  8,
		DialTimeout: 5 * time.Second,
		KeepAlive:   -1, // disable keepalives: these sockets live for milliseconds
	}
}

// pipe is one connected carrier endpoint pair: a listener with a dialed Conn
// attached, as the receiving and sending sides respectively.
type pipe struct {
	t  *testing.T
	ln *Listener
	c  *Conn
}

// newPipe starts a listener and dials it, returning both ends.
func newPipe(t *testing.T, cfg Config) *pipe {
	t.Helper()

	ln, err := ListenWithConfig("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("ListenWithConfig: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	c, err := Dial("tcp", ln.Addr().String(), cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	return &pipe{t: t, ln: ln, c: c}
}

// newTestListener builds a Listener around an already-bound socket, skipping
// the accept loop ListenWithConfig would start. attach, Close and the error
// paths are exercised directly against it.
//
// It exists so the invariant that every channel and map is initialised lives in
// one place: hand-building the literal at each call site means adding a field
// like chFatal silently produces a nil channel that only panics on the failure
// path, which is the path under test.
func newTestListener(ln net.Listener) *Listener {
	return &Listener{
		ln:          ln,
		lcfg:        testConfig().withDefaults(),
		rx:          make(chan rxPacket, defaultRxQueue),
		peers:       make(map[uint64]*peer),
		handshaking: make(map[net.Conn]struct{}),
		chClosed:    make(chan struct{}),
		chFatal:     make(chan struct{}),
	}
}

// serverRead reads one datagram on the listening side and reports the datagram
// and the peer address it arrived from.
func (p *pipe) serverRead(buf []byte, timeout time.Duration) (int, net.Addr) {
	p.t.Helper()

	p.ln.SetReadDeadline(time.Now().Add(timeout))
	n, addr, err := p.ln.ReadFrom(buf)
	if err != nil {
		p.t.Fatalf("listener ReadFrom: %v", err)
	}
	return n, addr
}

// serverWrite sends one datagram back to a peer.
func (p *pipe) serverWrite(payload []byte, addr net.Addr) {
	p.t.Helper()

	if _, err := p.ln.WriteTo(payload, addr); err != nil {
		p.t.Fatalf("listener WriteTo: %v", err)
	}
}

// clientRead reads one datagram on the dialing side.
func (p *pipe) clientRead(buf []byte, timeout time.Duration) int {
	p.t.Helper()

	p.c.SetReadDeadline(time.Now().Add(timeout))
	n, _, err := p.c.ReadFrom(buf)
	if err != nil {
		p.t.Fatalf("conn ReadFrom: %v", err)
	}
	return n
}

// WaitForPeer waits until the listener has registered the dialed peer, which is
// asynchronous with respect to Dial returning.
func (p *pipe) waitForPeer() net.Addr {
	p.t.Helper()

	// A one byte datagram both establishes the peer bookkeeping and yields the
	// address to reply to.
	p.clientWrite([]byte{0x01})
	buf := make([]byte, 64)
	_, addr := p.serverRead(buf, 5*time.Second)
	return addr
}

// clientWriteErr sends one datagram from the dialing side and reports the
// error. It is the form any caller outside the test goroutine must use:
// clientWrite below calls t.Fatalf, which from another goroutine kills only
// that goroutine and leaves the test to fail later with a timeout, hiding the
// real cause.
func (p *pipe) clientWriteErr(payload []byte) error {
	_, err := p.c.WriteTo(payload, p.c.RemoteAddr())
	return err
}

// clientWrite sends one datagram from the dialing side, failing the test on
// error. Only the test goroutine may call it; see clientWriteErr.
func (p *pipe) clientWrite(payload []byte) {
	p.t.Helper()

	if err := p.clientWriteErr(payload); err != nil {
		p.t.Fatalf("conn WriteTo: %v", err)
	}
}

// TestDatagramRoundTrip bounds the core contract: a datagram written on one end
// arrives on the other, byte for byte, in both directions.
func TestDatagramRoundTrip(t *testing.T) {
	p := newPipe(t, testConfig())

	want := []byte("hello carrier")
	p.clientWrite(want)

	buf := make([]byte, 1024)
	_, addr := p.serverRead(buf, 5*time.Second)
	if got := buf[:len(want)]; !bytes.Equal(got, want) {
		t.Fatalf("server received %q, want %q", got, want)
	}

	reply := []byte("hello kcp")
	p.serverWrite(reply, addr)

	got := make([]byte, 1024)
	n := p.clientRead(got, 5*time.Second)
	if !bytes.Equal(got[:n], reply) {
		t.Fatalf("client received %q, want %q", got[:n], reply)
	}
}

// TestPacketBoundariesPreserved is the invariant that makes the carrier usable
// as a packet socket: two datagrams written back to back must not be coalesced
// into one read, and one datagram must not be split across reads. A stream
// transport that ignored framing would fail this.
func TestPacketBoundariesPreserved(t *testing.T) {
	p := newPipe(t, testConfig())

	want := [][]byte{
		{0xAA},
		bytes.Repeat([]byte{0xBB}, 1500),
		{0xCC, 0xDD},
		bytes.Repeat([]byte{0xEE}, 4096),
	}
	for _, d := range want {
		p.clientWrite(d)
	}

	buf := make([]byte, MaxDatagram)
	for i, d := range want {
		p.c.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, addr, err := p.ln.ReadFrom(buf)
		if err != nil {
			t.Fatalf("datagram %d: listener ReadFrom: %v", i, err)
		}
		if !bytes.Equal(buf[:len(d)], d) {
			t.Fatalf("datagram %d: got %d bytes, want %d matching payload", i, len(d), len(d))
		}
		_ = addr
	}
}

// TestManyDatagramsPreserveContent checks that a burst larger than the send
// queue and larger than any single read buffer still round-trips exactly, which
// exercises the frame reader's buffer compaction and the queue backpressure.
func TestManyDatagramsPreserveContent(t *testing.T) {
	p := newPipe(t, testConfig())

	const count = 500
	payloads := make([][]byte, count)
	for i := range payloads {
		payloads[i] = bytes.Repeat([]byte{byte(i)}, 1+(i%1200))
	}

	// The write side runs on its own goroutine because the send queue would
	// otherwise fill and deadlock against this goroutine being the reader. Its
	// error comes back on a channel rather than through t.Fatalf: a failure
	// there would otherwise kill only the writer goroutine, and this test would
	// report a read timeout instead of the write error that caused it.
	writeErr := make(chan error, 1)
	go func() {
		for _, d := range payloads {
			if err := p.clientWriteErr(d); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- nil
	}()

	buf := make([]byte, MaxDatagram)
	for i, want := range payloads {
		p.ln.SetReadDeadline(time.Now().Add(10 * time.Second))
		n, _, err := p.ln.ReadFrom(buf)
		if err != nil {
			t.Fatalf("datagram %d: %v", i, err)
		}
		if !bytes.Equal(buf[:n], want) {
			t.Fatalf("datagram %d: got %d bytes, want %d; content differs", i, n, len(want))
		}
	}

	// Every payload has been read, so the writer has finished or failed; report
	// its error rather than leaving a write failure to look like a read hang.
	if err := <-writeErr; err != nil {
		t.Fatalf("conn WriteTo: %v", err)
	}
}

// TestMaxSizedDatagram covers the boundary where the frame length prefix is
// largest: a full sized datagram must round-trip without truncation.
func TestMaxSizedDatagram(t *testing.T) {
	p := newPipe(t, testConfig())

	want := make([]byte, MaxDatagram)
	if _, err := rand.Read(want); err != nil {
		t.Fatalf("rand: %v", err)
	}

	// Written synchronously: the queue is deeper than one datagram, so the send
	// cannot block on this goroutine, and a goroutine would only mean the write
	// error had to be reported through t.Fatalf from the wrong one.
	if err := p.clientWriteErr(want); err != nil {
		t.Fatalf("conn WriteTo: %v", err)
	}

	buf := make([]byte, MaxDatagram)
	p.ln.SetReadDeadline(time.Now().Add(10 * time.Second))
	n, _, err := p.ln.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if n != MaxDatagram {
		t.Fatalf("read %d bytes, want %d", n, MaxDatagram)
	}
	if !bytes.Equal(buf, want) {
		t.Fatalf("maximum sized datagram corrupted in transit")
	}
}

// TestEmptyDatagramAllowed pins that an empty datagram is carried rather than
// silently dropped: a UDP socket accepts one, and the carrier replaces a UDP
// socket, so callers must not observe a difference here.
func TestEmptyDatagramAllowed(t *testing.T) {
	p := newPipe(t, testConfig())

	p.clientWrite(nil)

	buf := make([]byte, 64)
	n, _ := p.serverRead(buf, 5*time.Second)
	if n != 0 {
		t.Fatalf("read %d bytes, want an empty datagram", n)
	}
}

// TestTruncationMatchesUDPSemantics pins the ReadFrom contract: a destination
// buffer smaller than the datagram yields the prefix that fits, not an error.
// KCP relies on this to size its read buffer.
func TestTruncationMatchesUDPSemantics(t *testing.T) {
	p := newPipe(t, testConfig())

	p.clientWrite([]byte("0123456789"))

	buf := make([]byte, 4)
	n, _ := p.serverRead(buf, 5*time.Second)
	if n != 4 {
		t.Fatalf("read %d bytes, want 4 (the capacity of the destination)", n)
	}
	if string(buf) != "0123" {
		t.Fatalf("truncated read = %q, want %q", buf, "0123")
	}
}

// TestParallelStreamsCarryOnePeer is the striping invariant: datagrams written
// through a connection with several streams must all arrive at exactly one peer
// address, because KCP demultiplexes sessions on that address. If the carrier
// reported a different address per stream, KCP would treat them as separate
// sessions and the data would be split across two broken ones.
func TestParallelStreamsCarryOnePeer(t *testing.T) {
	cfg := testConfig()
	cfg.Streams = 4

	p := newPipe(t, cfg)

	const count = 200
	for i := 0; i < count; i++ {
		p.clientWrite(bytes.Repeat([]byte{byte(i)}, 64))
	}

	buf := make([]byte, MaxDatagram)
	p.ln.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, first, err := p.ln.ReadFrom(buf)
	if err != nil {
		t.Fatalf("first ReadFrom: %v", err)
	}
	if _, ok := peerID(first); !ok {
		t.Fatalf("address %v is not a carrier peer address", first)
	}

	seen := map[string]int{first.String(): 1}
	for i := 1; i < count; i++ {
		if _, _, err := p.ln.ReadFrom(buf); err != nil {
			t.Fatalf("ReadFrom %d: %v", i, err)
		}
	}

	// Every stream the dialer opened must have been folded into the one peer,
	// so the listener must not have registered additional peers.
	p.ln.peerMu.RLock()
	peers := len(p.ln.peers)
	streams := 0
	for _, peer := range p.ln.peers {
		streams += peer.streams.len()
	}
	p.ln.peerMu.RUnlock()

	if peers != 1 {
		t.Fatalf("listener registered %d peers, want 1 (datagrams from all streams share one address)", peers)
	}
	if streams != cfg.Streams {
		t.Fatalf("listener holds %d streams, want %d", streams, cfg.Streams)
	}
	_ = seen
}

// TestSeparateDialersAreSeparatePeers is the complement: two connections must
// present two distinct addresses, or KCP would merge unrelated sessions.
func TestSeparateDialersAreSeparatePeers(t *testing.T) {
	ln, err := ListenWithConfig("tcp", "127.0.0.1:0", testConfig())
	if err != nil {
		t.Fatalf("ListenWithConfig: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()
	c1, err := Dial("tcp", addr, testConfig())
	if err != nil {
		t.Fatalf("Dial 1: %v", err)
	}
	defer c1.Close()

	c2, err := Dial("tcp", addr, testConfig())
	if err != nil {
		t.Fatalf("Dial 2: %v", err)
	}
	defer c2.Close()

	c1.WriteTo([]byte{0x11}, c1.RemoteAddr())
	c2.WriteTo([]byte{0x22}, c2.RemoteAddr())

	buf := make([]byte, 64)
	ln.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, a1, err := ln.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom 1: %v", err)
	}
	_, a2, err := ln.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom 2: %v", err)
	}

	if a1.String() == a2.String() {
		t.Fatalf("both dialers reported address %v; distinct sessions would collide", a1)
	}
}

// TestReadDeadlineExpires pins that a read deadline surfaces as the standard
// deadline error, which is what KCP's read loop expects from a packet socket.
func TestReadDeadlineExpires(t *testing.T) {
	p := newPipe(t, testConfig())

	p.ln.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	buf := make([]byte, 64)
	if _, _, err := p.ln.ReadFrom(buf); !errors.Is(err, errDeadline) {
		t.Fatalf("ReadFrom with an expired deadline = %v, want %v", err, errDeadline)
	}
}

// TestConnCloseUnblocksReader pins that closing a connection releases a blocked
// ReadFrom rather than wedging the goroutine that called it.
func TestConnCloseUnblocksReader(t *testing.T) {
	p := newPipe(t, testConfig())

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 64)
		_, _, err := p.c.ReadFrom(buf)
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	p.c.Close()

	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("ReadFrom after Close = %v, want %v", err, net.ErrClosed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ReadFrom did not return after Close")
	}
}

// TestWriteToRejectsOversizedDatagram pins that an oversized datagram is
// refused rather than silently truncated, which would corrupt KCP framing.
func TestWriteToRejectsOversizedDatagram(t *testing.T) {
	p := newPipe(t, testConfig())

	if _, err := p.c.WriteTo(make([]byte, MaxDatagram+1), p.c.RemoteAddr()); err == nil {
		t.Fatal("WriteTo accepted a datagram larger than MaxDatagram")
	}
}

// TestConcurrentBidirectionalTraffic drives both directions at once through
// several streams, which is the shape KCP actually produces: a full-duplex
// datagram stream that never stops. Every datagram must arrive exactly once and
// intact.
func TestConcurrentBidirectionalTraffic(t *testing.T) {
	cfg := testConfig()
	cfg.Streams = 3
	cfg.QueueDepth = 4

	p := newPipe(t, cfg)

	// Learn the peer address with a first datagram.
	peer := p.waitForPeer()

	const perDirection = 300
	payload := bytes.Repeat([]byte{0x5A}, 256)

	var wg sync.WaitGroup
	errs := make(chan error, 4)

	// client -> server
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < perDirection; i++ {
			if _, err := p.c.WriteTo(payload, p.c.RemoteAddr()); err != nil {
				errs <- err
				return
			}
		}
	}()

	// server -> client
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < perDirection; i++ {
			if _, err := p.ln.WriteTo(payload, peer); err != nil {
				errs <- err
				return
			}
		}
	}()

	// count reads on both ends
	wg.Add(2)
	go func() {
		defer wg.Done()
		buf := make([]byte, MaxDatagram)
		for i := 0; i < perDirection; i++ {
			p.ln.SetReadDeadline(time.Now().Add(15 * time.Second))
			n, _, err := p.ln.ReadFrom(buf)
			if err != nil {
				errs <- err
				return
			}
			if n != len(payload) {
				errs <- errors.New("server read short datagram")
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		buf := make([]byte, MaxDatagram)
		for i := 0; i < perDirection; i++ {
			p.c.SetReadDeadline(time.Now().Add(15 * time.Second))
			n, _, err := p.c.ReadFrom(buf)
			if err != nil {
				errs <- err
				return
			}
			if n != len(payload) {
				errs <- errors.New("client read short datagram")
				return
			}
		}
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("bidirectional traffic did not complete")
	}

	close(errs)
	for err := range errs {
		t.Fatalf("bidirectional traffic: %v", err)
	}
}

// TestFrameReaderRecoversAfterPartialWrites pins the framing logic against a
// peer that dribbles bytes out in arbitrary chunks, which is what a real TCP
// stream does. Frames must still be recovered exactly.
func TestFrameReaderRecoversAfterPartialWrites(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payloads := [][]byte{
		[]byte("a"),
		bytes.Repeat([]byte{0x01}, 300),
		bytes.Repeat([]byte{0x02}, 70000%MaxDatagram),
	}

	go func() {
		for _, p := range payloads {
			var hdr [frameHeaderSize]byte
			hdr[0] = byte(len(p) >> 8)
			hdr[1] = byte(len(p))
			frame := append(hdr[:], p...)

			// Write in awkward chunks to force the reader to reassemble.
			for _, chunk := range chunks(frame, 3) {
				if _, err := client.Write(chunk); err != nil {
					return
				}
			}
		}
		client.Close()
	}()

	fr := newFrameReader(server)
	for i, want := range payloads {
		got, err := fr.next()
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("frame %d: got %d bytes, want %d", i, len(got), len(want))
		}
		putBuffer(got)
	}

	if _, err := fr.next(); !errors.Is(err, io.EOF) {
		t.Fatalf("after the last frame, next() = %v, want EOF", err)
	}
}

// chunks splits b into slices of at most size bytes.
func chunks(b []byte, size int) [][]byte {
	var out [][]byte
	for len(b) > 0 {
		n := size
		if n > len(b) {
			n = len(b)
		}
		out = append(out, b[:n])
		b = b[n:]
	}
	return out
}

// TestStreamFailurePrunesSet pins the failure semantics a reconnecting client
// depends on: when a stream breaks, it is dropped from the set rather than
// staying around to swallow datagrams, and the survivors keep working. A dying
// stream must not take the healthy ones with it.
func TestStreamFailurePrunesSet(t *testing.T) {
	cfg := testConfig()
	cfg.Streams = 3

	p := newPipe(t, cfg)
	p.waitForPeer()

	if got := p.c.streams.len(); got != cfg.Streams {
		t.Fatalf("conn holds %d streams, want %d", got, cfg.Streams)
	}

	// Kill one stream the way a network failure would.
	var victim *stream
	p.c.streams.each(func(s *stream) {
		if victim == nil {
			victim = s
		}
	})
	victim.shutdown()

	// Both ends must converge. Waiting only for the dialing side leaves the
	// listener still holding the dead stream, and a reply sent in that window
	// may be routed onto it and dropped — which is the documented behavior
	// (KCP retransmits), not a failure. Asserting on both sides is what makes
	// the traffic checks below deterministic.
	waitStreams(t, p.c.streams, cfg.Streams-1)
	waitListenerStreams(t, p.ln, cfg.Streams-1)

	// The survivors must still carry traffic in both directions.
	if _, err := p.c.WriteTo([]byte("survivor"), p.c.RemoteAddr()); err != nil {
		t.Fatalf("WriteTo after a stream failed: %v", err)
	}

	buf := make([]byte, 64)
	n, addr := p.serverRead(buf, 5*time.Second)
	if string(buf[:n]) != "survivor" {
		t.Fatalf("read %q, want %q", buf[:n], "survivor")
	}

	p.serverWrite([]byte("ack"), addr)
	cgot := make([]byte, 64)
	cn := p.clientRead(cgot, 5*time.Second)
	if string(cgot[:cn]) != "ack" {
		t.Fatalf("read %q, want %q", cgot[:cn], "ack")
	}
}

// waitStreams blocks until the dialing side's stream set has reached want.
func waitStreams(t *testing.T, set *streamSet, want int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if set.len() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("conn holds %d streams, want %d", set.len(), want)
}

// waitListenerStreams blocks until the listening side holds want streams across
// all of its peers. It converges independently of the dialing side, because a
// dead stream is noticed locally on the end whose socket failed and only then
// propagates to the other.
func waitListenerStreams(t *testing.T, ln *Listener, want int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		total := 0
		ln.peerMu.RLock()
		for _, p := range ln.peers {
			total += p.streams.len()
		}
		ln.peerMu.RUnlock()
		if total == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("listener never released the failed stream")
}

// TestAllStreamsFailedFailsConnection pins the terminal condition: when the
// last stream dies, ReadFrom and WriteTo must start failing so KCP tears the
// session down instead of retransmitting into a dead transport forever.
func TestAllStreamsFailedFailsConnection(t *testing.T) {
	p := newPipe(t, testConfig())
	p.waitForPeer()

	p.c.streams.each(func(s *stream) { s.shutdown() })

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p.c.fatalErr() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := p.c.fatalErr(); err == nil {
		t.Fatal("connection did not fail after every stream died")
	}

	if _, err := p.c.WriteTo([]byte("x"), p.c.RemoteAddr()); err == nil {
		t.Fatal("WriteTo succeeded after every stream died")
	}

	buf := make([]byte, 64)
	if _, _, err := p.c.ReadFrom(buf); err == nil {
		t.Fatal("ReadFrom succeeded after every stream died")
	}
}

// TestListenerCloseReleasesStreams pins that closing the listener tears down
// the streams it accepted, so a server shutdown does not leave sockets behind.
func TestListenerCloseReleasesStreams(t *testing.T) {
	ln, err := ListenWithConfig("tcp", "127.0.0.1:0", testConfig())
	if err != nil {
		t.Fatalf("ListenWithConfig: %v", err)
	}

	c, err := Dial("tcp", ln.Addr().String(), testConfig())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	if _, err := c.WriteTo([]byte{0x01}, c.RemoteAddr()); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	buf := make([]byte, 64)
	ln.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := ln.ReadFrom(buf); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}

	if err := ln.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The dialing side must observe the loss of every stream, because the
	// listener closed them.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c.fatalErr() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := c.fatalErr(); err == nil {
		t.Fatal("dialing side did not observe the listener closing its streams")
	}
}

// TestConcurrentStreamsAndWrites runs writes from several goroutines at once.
// KCP can drive WriteTo from more than one goroutine (its flush and its OOB
// path), so the carrier must tolerate it without losing or interleaving frames.
func TestConcurrentStreamsAndWrites(t *testing.T) {
	cfg := testConfig()
	cfg.Streams = 2
	cfg.QueueDepth = 2

	p := newPipe(t, cfg)
	peer := p.waitForPeer()

	const writers = 8
	const each = 50

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			payload := bytes.Repeat([]byte{byte(id)}, 100+id)
			for i := 0; i < each; i++ {
				if _, err := p.c.WriteTo(payload, p.c.RemoteAddr()); err != nil {
					return
				}
			}
		}(w)
	}

	total := writers * each
	received := make(chan int, total)
	go func() {
		buf := make([]byte, MaxDatagram)
		for i := 0; i < total; i++ {
			p.ln.SetReadDeadline(time.Now().Add(20 * time.Second))
			n, _, err := p.ln.ReadFrom(buf)
			if err != nil {
				return
			}
			received <- n
		}
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	deadline := time.After(30 * time.Second)
	count := 0
	for count < total {
		select {
		case <-received:
			count++
		case <-deadline:
			t.Fatalf("received %d of %d datagrams before timing out", count, total)
		}
	}
	_ = peer
}

// TestSetDeadlineAppliesToBothDirections pins that SetDeadline covers writes as
// well as reads, matching the net.Conn contract kcp-go's session relies on.
// Asserting only the read half would let a regression that drops the write
// deadline pass — and the write deadline is the one that keeps a stalled stream
// from wedging the caller.
func TestSetDeadlineAppliesToBothDirections(t *testing.T) {
	p := newPipe(t, testConfig())

	c, err := Dial("tcp", p.ln.Addr().String(), testConfig())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	if err := c.SetDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}

	// Read direction.
	buf := make([]byte, 64)
	if _, _, err := c.ReadFrom(buf); !errors.Is(err, errDeadline) {
		t.Fatalf("ReadFrom = %v, want %v", err, errDeadline)
	}

	// Write direction, on a connection whose every stream has a full queue and
	// no writer draining it, so the send can only return by honoring a deadline.
	stalledConn := &Conn{
		remote:   testAddr{},
		rx:       make(chan rxPacket, defaultRxQueue),
		stall:    defaultStallTimeout,
		chClosed: make(chan struct{}),
		chFatal:  make(chan struct{}),
	}
	stalledConn.streams = newStreamSet(func() { stalledConn.failAll(net.ErrClosed) })
	stalledConn.streams.add(newStream(&blockingConn{ch: make(chan struct{})}, 1))

	stalledConn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
	if _, err := stalledConn.WriteTo([]byte{0x01}, stalledConn.remote); err != nil {
		t.Fatalf("first WriteTo: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := stalledConn.WriteTo([]byte{0x02}, stalledConn.remote)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, errDeadline) {
			t.Fatalf("WriteTo = %v, want %v", err, errDeadline)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WriteTo did not honor the write deadline set by SetDeadline")
	}
}

// TestStalledStreamReleasesCaller pins the liveness half of the stall bound:
// when the only stream's queue stays full past Config.StallTimeout, the caller is
// released and the wedged stream is dropped, rather than blocking forever.
//
// This is the half that matters when there is nothing to fail over to: without
// the bound, kcp-go — which never sets a write deadline on the packet connection
// — would have its post-processing goroutine blocked permanently.
func TestStalledStreamReleasesCaller(t *testing.T) {
	c := &Conn{
		remote:   testAddr{},
		rx:       make(chan rxPacket, defaultRxQueue),
		stall:    200 * time.Millisecond,
		chClosed: make(chan struct{}),
		chFatal:  make(chan struct{}),
	}
	c.streams = newStreamSet(func() { c.failAll(net.ErrClosed) })

	// A stream whose writer goroutine never runs, so its one queue slot stays
	// full from the first datagram on. It is the only stream, so pick chooses it.
	stalled := newStream(&blockingConn{ch: make(chan struct{})}, 1)
	stalled.onExit = c.streams.remove
	c.streams.add(stalled)

	if _, err := c.WriteTo([]byte{0x01}, c.remote); err != nil {
		t.Fatalf("filling the stalled queue: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := c.WriteTo([]byte{0x02}, c.remote)
		done <- err
	}()

	select {
	case err := <-done:
		// With no sibling to retry on and the set now empty the connection is
		// failed, so an error is the correct outcome; the contract under test is
		// that it returns at all.
		if err == nil {
			t.Fatal("WriteTo reported success with every stream stalled")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WriteTo never returned; the stall bound did not release the caller")
	}

	if !stalled.closed() {
		t.Fatal("stalled stream was left open rather than dropped")
	}
}

// TestStreamSocketErrorDoesNotBreakSend pins the failover contract for the case
// that actually happens in production: a stream dies with a real socket error
// (ECONNRESET and friends) while the datagram is still in flight.
//
// stream.send reports the stream's own sticky error, which for a broken socket is
// the underlying errno rather than net.ErrClosed. If the retry loop only matched
// net.ErrClosed, that raw error would reach the caller — and kcp-go turns any
// error from WriteTo into a permanent, sticky session teardown, killing a session
// whose other streams are perfectly healthy.
//
// The error is injected with fail rather than provoked through a real socket so
// the test is deterministic: it drives exactly the same code path (send returns a
// non-nil error for a stream that has shut down) without depending on when the
// kernel notices a closed peer.
func TestStreamSocketErrorDoesNotBreakSend(t *testing.T) {
	// The survivor is a real socket pair whose far end reads, so it drains and
	// carries the retried datagram.
	peer, survivorConn := tcpPair(t)
	defer peer.Close()

	c := &Conn{
		remote:   testAddr{},
		rx:       make(chan rxPacket, defaultRxQueue),
		stall:    defaultStallTimeout,
		chClosed: make(chan struct{}),
		chFatal:  make(chan struct{}),
	}
	c.streams = newStreamSet(func() { c.failAll(net.ErrClosed) })

	// One doomed stream, alone in the set, with its single queue slot full and no
	// writer draining it. Being the only stream, pick must return it.
	doomed := newStream(&blockingConn{ch: make(chan struct{})}, 1)
	doomed.onExit = c.streams.remove
	c.streams.add(doomed)

	if _, err := c.WriteTo([]byte{0x01}, c.remote); err != nil {
		t.Fatalf("filling the doomed queue: %v", err)
	}

	// The survivor is deliberately not in the set yet. It is added while the send
	// below is already blocked on the doomed stream, so the retry has somewhere
	// to go without pick ever preferring it up front — which is what makes this
	// test drive the retry path rather than bypass it.
	survivor := newStream(survivorConn, 8)
	survivor.onExit = c.streams.remove

	const want = "carried-by-survivor"
	errCh := make(chan error, 1)
	go func() {
		_, err := c.WriteTo([]byte(want), c.remote)
		errCh <- err
	}()

	// Let WriteTo pick the doomed stream and block on its full queue.
	time.Sleep(150 * time.Millisecond)

	// Now the survivor appears and the doomed stream dies with a socket-shaped
	// error. The blocked send returns that error, and the retry must find the
	// survivor and succeed instead of surfacing the raw errno.
	survivor.start(func([]byte) error { return nil })
	c.streams.add(survivor)
	doomed.fail(errors.New("write tcp: connection reset by peer"))

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("WriteTo while a sibling died with a socket error = %v, want it to retry", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WriteTo never returned after its stream died mid-send")
	}

	// The datagram must have arrived on the survivor as a framed payload, which
	// is the whole point: the session survived one broken stream.
	peer.SetReadDeadline(time.Now().Add(3 * time.Second))
	var hdr [frameHeaderSize]byte
	if _, err := io.ReadFull(peer, hdr[:]); err != nil {
		t.Fatalf("no frame reached the surviving stream: %v", err)
	}
	body := make([]byte, binary.BigEndian.Uint16(hdr[:]))
	if _, err := io.ReadFull(peer, body); err != nil {
		t.Fatalf("short frame body: %v", err)
	}
	if string(body) != want {
		t.Fatalf("survivor carried %q, want %q", body, want)
	}
}

// tcpPair returns two connected TCP endpoints, for tests that need a real
// socket whose far end can be read. Both ends are closed on cleanup: a stream
// reading from one of them only returns once its socket closes, so leaking them
// leaks a goroutine and a descriptor per run.
func tcpPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	type result struct {
		conn net.Conn
		err  error
	}
	accepted := make(chan result, 1)
	go func() {
		conn, err := ln.Accept()
		accepted <- result{conn, err}
	}()

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	got := <-accepted
	if got.err != nil {
		client.Close()
		t.Fatalf("accept: %v", got.err)
	}

	t.Cleanup(func() {
		got.conn.Close()
		client.Close()
	})
	return got.conn, client
}

// TestHandshakeReplayRefused is the regression test for the flaw the first
// authentication attempt had: a handshake whose only varying input is chosen by
// the peer is byte-for-byte replayable, so an on-path observer who recorded one
// could resend it verbatim and join the session.
//
// It drives the real path: a genuine dialer completes a handshake while its
// client-side bytes are recorded, then those exact bytes are replayed onto a
// second connection. The listener issues a fresh nonce per connection, so the
// recorded proof cannot verify there. This is what fails if the nonce is ever
// shared between streams, reused, or left out of the tag.
func TestHandshakeReplayRefused(t *testing.T) {
	ln, err := ListenWithConfig("tcp", "127.0.0.1:0", testConfig())
	if err != nil {
		t.Fatalf("ListenWithConfig: %v", err)
	}
	defer ln.Close()

	const id = 0x5eed

	// A genuine handshake, with the dialer's writes recorded.
	realConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer realConn.Close()

	recorder := &recordWriter{conn: realConn}
	realConn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := dialHandshake(recorder, testSecret, id); err != nil {
		t.Fatalf("genuine handshake: %v", err)
	}
	realConn.SetDeadline(time.Time{})

	// The dialer writes exactly two things: the hello, then the proof. The nonce
	// it reads in between is not recorded, which is the point.
	hello := recorder.written[:handshakeBaseSize]
	proof := recorder.written[handshakeBaseSize:]
	if len(proof) != sessionAuthSize {
		t.Fatalf("recorded %d proof bytes, want %d", len(proof), sessionAuthSize)
	}

	// Replay those bytes verbatim onto a fresh connection.
	replay, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial replay: %v", err)
	}
	defer replay.Close()
	replay.SetDeadline(time.Now().Add(5 * time.Second))

	if _, err := replay.Write(hello); err != nil {
		t.Fatalf("replay hello: %v", err)
	}

	// The listener issues its own fresh nonce for this connection.
	nonce := make([]byte, sessionAuthSize)
	if _, err := io.ReadFull(replay, nonce); err != nil {
		// Refused before challenging; still a refusal, and the stream never
		// reached a peer.
		return
	}

	if _, err := replay.Write(proof); err != nil {
		return // the listener closed rather than absorb the proof
	}

	// A replayed proof must not be answered with an acknowledgement: the listener
	// must close instead. Any successful read here means the handshake was
	// admitted on a stale proof.
	ack := make([]byte, sessionAuthSize)
	if _, err := io.ReadFull(replay, ack); err == nil {
		t.Fatal("listener acknowledged a replayed handshake: the proof verified against a fresh nonce")
	}
}

// recordWriter records everything written through it while forwarding to conn.
type recordWriter struct {
	conn    net.Conn
	written []byte
}

func (w *recordWriter) Write(p []byte) (int, error) {
	w.written = append(w.written, p...)
	return w.conn.Write(p)
}

func (w *recordWriter) Read(p []byte) (int, error) { return w.conn.Read(p) }
