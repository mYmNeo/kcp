package carrier

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Conn is a net.PacketConn that carries KCP datagrams inside one or more TCP
// streams to a single remote endpoint.
//
// KCP sees a normal packet socket: WriteTo sends one datagram, ReadFrom receives
// one. The remote address is fixed at dial time, so WriteTo ignores the address
// it is passed and ReadFrom always reports the dialed endpoint.
//
// Datagrams are striped across Config.Streams TCP streams, each with its own
// writer goroutine and bounded queue. A stream that breaks is dropped from the
// set; when the last one goes, ReadFrom and WriteTo start failing so that KCP
// tears the session down and the caller reconnects.
type Conn struct {
	remote net.Addr
	local  net.Addr
	rx     chan rxPacket
	rd     deadline
	wd     deadline
	stall  time.Duration
	opts   sockOptions

	streams *streamSet

	chClosed  chan struct{}
	closeOnce sync.Once

	fatal     atomic.Pointer[error]
	chFatal   chan struct{}
	fatalOnce sync.Once
}

// Dial opens a carrier connection to a listening endpoint, establishing
// Config.Streams TCP streams that together form one logical peer.
//
// network must be a TCP network ("tcp", "tcp4", "tcp6"); an empty network means
// "tcp". The returned Conn is ready to be handed to kcp.NewConn4 as the
// net.PacketConn, with the session owning it (ownConn true) so that closing the
// session releases every stream.
func Dial(network, address string, cfg Config) (*Conn, error) {
	cfg = cfg.withDefaults()

	raddr, err := net.ResolveTCPAddr(normalizeNetwork(network), address)
	if err != nil {
		return nil, err
	}

	c := &Conn{
		remote:   raddr,
		rx:       make(chan rxPacket, defaultRxQueue),
		stall:    cfg.StallTimeout,
		chClosed: make(chan struct{}),
		chFatal:  make(chan struct{}),
	}
	c.streams = newStreamSet(func() { c.failAll(net.ErrClosed) })

	// Every stream of one dialer belongs to the same logical peer, so they all
	// announce one shared id: that is what lets the receiving side reassemble
	// parallel streams into a single KCP session.
	id := randID()

	streams := make([]*stream, 0, cfg.Streams)
	for i := 0; i < cfg.Streams; i++ {
		conn, err := net.DialTimeout(raddr.Network(), raddr.String(), cfg.DialTimeout)
		if err != nil {
			if i == 0 {
				closeStreams(streams)
				return nil, err
			}
			// A listener that stopped accepting extra streams mid-dial is not a
			// reason to lose the session: the streams already established carry
			// the same session, and the set tolerates being smaller than asked.
			break
		}
		tune(conn, cfg)
		c.opts.applyTo(conn)

		// The handshake is written and read under the dial timeout, so a peer
		// that accepts but never answers cannot pin the dialer.
		if err := conn.SetDeadline(time.Now().Add(cfg.DialTimeout)); err != nil {
			conn.Close()
			closeStreams(streams)
			return nil, err
		}
		err = dialHandshake(conn, cfg.Secret, id)
		if err != nil {
			conn.Close()
			// A refused stream is reported to the dialer as an error, which is
			// how it learns the listener did not accept this stream. Only the
			// first failure is fatal: it means the session itself was refused,
			// whereas a later one means the peer is already at its stream cap.
			// Failing the whole dial there would turn a capacity limit into a
			// total outage.
			if i == 0 {
				closeStreams(streams)
				return nil, err
			}
			break
		}
		if err := conn.SetDeadline(time.Time{}); err != nil {
			conn.Close()
			closeStreams(streams)
			return nil, err
		}

		streams = append(streams, newStream(conn, cfg.QueueDepth))
	}

	// LocalAddr is reported from the first stream; every stream is dialed to the
	// same endpoint, so this is only ever used for logging.
	c.local = streams[0].localAddr()

	// Publish every stream before starting any of them. The set fires onEmpty
	// the instant it becomes empty, so starting a stream before the others are
	// added would let the very first one dying — a listener that rejects it, a
	// peer that closes immediately — mark the whole connection permanently
	// failed while its siblings were still perfectly healthy.
	for _, s := range streams {
		s.onExit = c.streams.remove
		c.streams.add(s)
	}
	for _, s := range streams {
		s.start(c.onPacket)
	}

	return c, nil
}

// closeStreams releases a partially built stream list.
func closeStreams(streams []*stream) {
	for _, s := range streams {
		s.close()
	}
}

// onPacket hands a received datagram to the consumer. The connection owns the
// buffer until a reader takes it with ReadFrom.
func (c *Conn) onPacket(payload []byte) error {
	return pushRx(c.rx, c.chClosed, rxPacket{data: payload, addr: c.remote})
}

// failAll records the error that killed the connection and wakes every blocked
// caller.
func (c *Conn) failAll(err error) {
	if err == nil {
		err = net.ErrClosed
	}
	c.fatalOnce.Do(func() {
		e := err
		c.fatal.Store(&e)
		close(c.chFatal)
	})
}

// fatalErr reports the sticky failure, if any.
func (c *Conn) fatalErr() error {
	if p := c.fatal.Load(); p != nil {
		return *p
	}
	return nil
}

// ReadFrom reads one datagram into p, truncating it if p is too small — the
// same contract a UDP socket follows. The address reported is always the dialed
// endpoint, because a carrier connection has exactly one peer.
func (c *Conn) ReadFrom(p []byte) (int, net.Addr, error) {
	if err := c.fatalErr(); err != nil {
		return 0, nil, err
	}

	timeout, stop := c.rd.timer()
	defer stop()

	select {
	case pkt := <-c.rx:
		return deliver(p, pkt), c.remote, nil
	case <-timeout:
		return 0, nil, errDeadline
	case <-c.chFatal:
		return 0, nil, c.fatalErr()
	case <-c.chClosed:
		return 0, nil, net.ErrClosed
	}
}

// WriteTo sends one datagram on the least loaded stream, retrying on another
// when one has failed or has stopped making progress. It blocks while the
// chosen stream's queue is full — that is how backpressure reaches KCP's send
// window — and honors the write deadline while it waits.
func (c *Conn) WriteTo(p []byte, addr net.Addr) (int, error) {
	if err := c.fatalErr(); err != nil {
		return 0, err
	}
	if len(p) > MaxDatagram {
		return 0, ErrProtocol
	}

	timeout, stop := c.wd.timer()
	defer stop()

	for {
		// Re-checked each pass, not just once: a retry only makes sense while the
		// connection is still viable. The stream set can empty and fail the
		// connection between passes, and sending on a fatally failed connection
		// would hide that from KCP.
		if err := c.fatalErr(); err != nil {
			return 0, err
		}

		s := c.streams.pick()
		if s == nil {
			return 0, net.ErrClosed
		}

		switch err := s.send(p, timeout, c.stall); {
		case err == nil:
			return len(p), nil
		case errors.Is(err, errDeadline):
			// The caller's own deadline: report it as a socket would.
			return 0, errDeadline
		default:
			// Everything else is this stream's problem — it stalled, or it died
			// with a socket error like ECONNRESET. The datagram is still unsent,
			// so try a sibling.
			//
			// This must not be narrowed to net.ErrClosed: send returns the
			// stream's own sticky error, which for a broken socket is the
			// underlying errno, not net.ErrClosed. Handing that back would reach
			// kcp-go's sticky write-error path and tear down a session whose
			// other streams are perfectly healthy. The loop still terminates,
			// because pick skips streams that have shut down.
			//
			// ErrProtocol needs no case of its own: send raises it only for an
			// oversized datagram, and the check above has already rejected one.
			continue
		}
	}
}

// Close releases every stream and unblocks all pending callers. It is
// idempotent.
func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		close(c.chClosed)
		c.streams.closeAll()
		drain(c.rx)
		c.failAll(net.ErrClosed)
	})
	return nil
}

// LocalAddr reports the local address of the first established stream.
func (c *Conn) LocalAddr() net.Addr { return c.local }

// RemoteAddr reports the address the connection was dialed to.
func (c *Conn) RemoteAddr() net.Addr { return c.remote }

// SetDeadline sets the read and write deadlines.
func (c *Conn) SetDeadline(t time.Time) error {
	c.rd.set(t)
	c.wd.set(t)
	return nil
}

// SetReadDeadline sets the deadline ReadFrom reports.
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.rd.set(t)
	return nil
}

// SetWriteDeadline sets the deadline WriteTo reports.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.wd.set(t)
	return nil
}

// SetReadBuffer applies SO_RCVBUF to the established streams and records it for
// any stream established later.
func (c *Conn) SetReadBuffer(bytes int) error {
	return c.opts.SetReadBuffer(bytes, c.streams.each)
}

// SetWriteBuffer applies SO_SNDBUF to the established streams and records it for
// any stream established later.
func (c *Conn) SetWriteBuffer(bytes int) error {
	return c.opts.SetWriteBuffer(bytes, c.streams.each)
}

// SetDSCP applies the DSCP field to the established streams and records it for
// any stream established later.
func (c *Conn) SetDSCP(dscp int) error {
	return c.opts.SetDSCP(dscp, c.streams.each)
}
