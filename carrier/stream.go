package carrier

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// stream is one TCP connection carrying framed datagrams. A dedicated writer
// goroutine drains a bounded queue so a stalled stream cannot block the others,
// and a reader goroutine hands every received datagram to onPacket.
type stream struct {
	conn net.Conn
	fr   *frameReader

	chSend chan []byte
	die    chan struct{}
	once   sync.Once

	// onExit is called once, after the stream has stopped, so its endpoint can
	// drop it from the stream set. It must not block.
	onExit      func(*stream)
	releaseOnce sync.Once

	// graceful marks a deliberate close, which flushes the send queue before the
	// socket is released instead of dropping it.
	graceful atomic.Bool

	// startMu serializes start against signal. A stream is published into its
	// endpoint's set the moment it is admitted, which can happen before attach
	// has launched its goroutines; without this lock a close running in that
	// window would wait on WaitGroups that start had not incremented yet, and
	// start would then increment them after the wait had already passed.
	startMu sync.Mutex
	started bool

	mu   sync.Mutex
	werr error

	// Write scratch, reused for every datagram on the hot path. Owned by the
	// writer goroutine. wbufArr is stable storage that survives the writev;
	// wbufs is rebuilt from it per frame, because net.Buffers.WriteTo reslices
	// the slice it is handed and would otherwise consume the backing array.
	wbuf    [frameHeaderSize]byte
	wbufArr [2][]byte
	wbufs   net.Buffers

	// The writer and the reader are waited on separately: a graceful close has
	// to let the writer finish draining before the socket is closed, whereas
	// the reader only returns once the socket is closed.
	wgWriter sync.WaitGroup
	wgReader sync.WaitGroup
}

// newStream wraps an established TCP connection. Call start to begin I/O.
func newStream(conn net.Conn, queueDepth int) *stream {
	if queueDepth < 1 {
		queueDepth = defaultQueueDepth
	}

	return &stream{
		conn:   conn,
		fr:     newFrameReader(conn),
		chSend: make(chan []byte, queueDepth),
		die:    make(chan struct{}),
	}
}

// start launches the reader and writer goroutines. It must be called exactly
// once, and does nothing if the stream has already been shut down.
func (s *stream) start(onPacket func(payload []byte) error) {
	s.startMu.Lock()
	defer s.startMu.Unlock()

	if s.started || s.closed() {
		return
	}
	s.started = true
	s.wgWriter.Add(1)
	s.wgReader.Add(1)

	go s.writeLoop()
	go s.readLoop(onPacket)
}

// readLoop turns frames back into datagrams.
func (s *stream) readLoop(onPacket func(payload []byte) error) {
	defer s.wgReader.Done()

	for {
		payload, err := s.fr.next()
		if err != nil {
			s.fail(err)
			return
		}
		if err := onPacket(payload); err != nil {
			putBuffer(payload)
			s.fail(err)
			return
		}
	}
}

// writeLoop hands queued datagrams to the socket one frame at a time. Datagrams
// still queued when a stream fails are dropped, exactly as a UDP send would be;
// KCP retransmits them.
func (s *stream) writeLoop() {
	defer s.wgWriter.Done()

	for {
		select {
		case payload := <-s.chSend:
			if err := s.write(payload); err != nil {
				return
			}
		case <-s.die:
			// On a graceful close the queued datagrams are flushed before the
			// socket is released; see close.
			if s.graceful.Load() {
				s.flush()
			}
			return
		}
	}
}

// write emits one queued datagram and releases its buffer.
func (s *stream) write(payload []byte) error {
	err := s.writeFrame(payload)
	putBuffer(payload)
	if err != nil {
		s.fail(err)
		return err
	}
	return nil
}

// flush writes the datagrams already queued, stopping at the first write error.
// close sets a write deadline first, so a peer that has stopped reading bounds
// this rather than stalling the close.
func (s *stream) flush() {
	for {
		select {
		case payload := <-s.chSend:
			if err := s.write(payload); err != nil {
				return
			}
		default:
			return
		}
	}
}

// send enqueues one datagram. The payload is copied because the caller recycles
// its buffer as soon as send returns.
//
// The common case — room in the queue — is handled without touching a timer, so
// the uncontended hot path allocates nothing. Only a full queue arms the stall
// bound. A nil timeout channel means the caller set no write deadline.
func (s *stream) send(payload []byte, timeout <-chan time.Time, stall time.Duration) error {
	if len(payload) > MaxDatagram {
		return fmt.Errorf("%w: datagram size %d", ErrProtocol, len(payload))
	}
	if s.closed() {
		return s.err()
	}

	// allocBuffer guarantees room for the whole payload, so the append never
	// reallocates and the result is still the pool's size class when the payload
	// fits one.
	buf := append(allocBuffer(len(payload)), payload...)

	select {
	case s.chSend <- buf:
		return nil
	default:
	}

	// The queue is full, so this send has to wait and may need to consult a
	// deadline. Both timers are created here rather than on every datagram:
	// time.NewTimer allocates, and kcp-go never sets a write deadline on the
	// packet connection, so arming one unconditionally would put an allocation
	// back on the hottest path this package has.
	wedge := time.NewTimer(stall)
	defer wedge.Stop()

	select {
	case s.chSend <- buf:
		return nil
	case <-timeout:
		putBuffer(buf)
		return errDeadline
	case <-wedge.C:
		// The queue stayed full for the whole bound. Drop the stream so the
		// datagram can be retried on a healthy one: leaving it in place would
		// let one stalled stream freeze the session, which is exactly what the
		// parallel-stream design exists to prevent.
		putBuffer(buf)
		s.fail(errStalled)
		return errStalled
	case <-s.die:
		putBuffer(buf)
		return s.err()
	}
}

// depth reports how many datagrams are waiting for this stream.
func (s *stream) depth() int { return len(s.chSend) }

// closed reports whether the stream has shut down.
func (s *stream) closed() bool {
	select {
	case <-s.die:
		return true
	default:
		return false
	}
}

// err reports the sticky error that ended the stream, or net.ErrClosed when the
// stream was closed deliberately.
func (s *stream) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.werr != nil {
		return s.werr
	}
	return net.ErrClosed
}

// fail records the error that ended the stream and shuts it down.
func (s *stream) fail(err error) {
	if err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
		s.mu.Lock()
		if s.werr == nil {
			s.werr = err
		}
		s.mu.Unlock()
	}
	s.shutdown()
}

// shutdown releases the socket abruptly, stops both goroutines, and notifies the
// endpoint. Datagrams still queued are dropped; this is the failure path, where
// KCP retransmits them on the next session. It is idempotent.
func (s *stream) shutdown() {
	s.signal()
	s.release()
}

// signal closes die exactly once, telling both loops to stop.
//
// It is deliberately free of any wait: the writer and reader loops reach
// shutdown through fail when they hit an I/O error, so a lock held across a wait
// here would deadlock against the loop that just failed.
func (s *stream) signal() {
	s.once.Do(func() { close(s.die) })
}

// release drops the socket and reports the stream gone, exactly once.
func (s *stream) release() {
	s.releaseOnce.Do(func() {
		s.conn.Close()
		if s.onExit != nil {
			s.onExit(s)
		}
	})
}

// close shuts the stream down deliberately.
//
// Order matters. KCP flushes its final packets and then closes the transport, so
// at this moment the send queue usually holds datagrams that have not reached
// the wire yet — typically the session's goodbye. The sequence is therefore:
// mark the close graceful, bound the flush with a write deadline, signal the
// writer, wait for the writer to finish, and only then release the socket.
// Closing the socket first would make every flush fail against a closed
// connection, which is how a clean shutdown comes to look like a dead peer.
//
// No lock is held across either wait; see signal.
func (s *stream) close() error {
	// Take startMu so a concurrent start either completes its Add calls before
	// the waits below, or sees the closed stream and launches nothing. Without
	// this the two would race: Wait could pass before Add ran, and the goroutines
	// would then be launched onto a stream nobody waits for.
	s.startMu.Lock()

	// The write deadline is set before the writer is signalled, not after: once
	// die closes the writer is already draining the queue, and a flush started
	// in that window would run unbounded against a peer that stopped reading.
	// Ordering it first means the deadline is always in place before the flush
	// it is meant to bound.
	s.graceful.Store(true)
	s.conn.SetWriteDeadline(time.Now().Add(closeDrainTimeout))
	s.signal()
	s.startMu.Unlock()

	s.wgWriter.Wait()
	s.release()
	s.wgReader.Wait()
	return nil
}

// localAddr reports the local address of the underlying connection.
func (s *stream) localAddr() net.Addr { return s.conn.LocalAddr() }

// deadline holds one optional deadline as unix nanoseconds; zero means none. It
// is safe for concurrent use and for concurrent ReadFrom/WriteTo callers.
type deadline struct{ ns atomic.Int64 }

// set replaces the deadline; the zero time disables it.
func (d *deadline) set(t time.Time) {
	if t.IsZero() {
		d.ns.Store(0)
		return
	}
	d.ns.Store(t.UnixNano())
}

// timer returns a channel that fires once the deadline elapses, plus its stop
// function. A nil channel means no deadline is set, so the channel never fires.
func (d *deadline) timer() (<-chan time.Time, func() bool) {
	ns := d.ns.Load()
	if ns == 0 {
		return nil, func() bool { return false }
	}

	if remaining := time.Until(time.Unix(0, ns)); remaining > 0 {
		t := time.NewTimer(remaining)
		return t.C, t.Stop
	}

	expired := make(chan time.Time)
	close(expired)
	return expired, func() bool { return false }
}

// errDeadline is returned when a deadline elapses with no data, matching what
// the net package reports for a socket deadline.
var errDeadline = os.ErrDeadlineExceeded

// errStalled reports that a stream's send queue stayed full past the stall
// bound, so the stream was dropped and the datagram must be retried on another.
// It is deliberately not a write error: the endpoint must not hand it to KCP,
// whose write-error path is sticky and would tear the session down over one bad
// stream.
var errStalled = errors.New("carrier: stream stalled")
