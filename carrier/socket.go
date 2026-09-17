package carrier

import (
	"errors"
	"net"
	"sync"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

// tune applies the transport level options every carrier stream inherits from
// the endpoint configuration. Socket buffers and DSCP are not set here: KCP
// asks for those through SetReadBuffer/SetWriteBuffer/SetDSCP, which reach the
// live streams through sockOptions.
func tune(conn net.Conn, cfg Config) {
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}

	tcp.SetNoDelay(true)
	if cfg.KeepAlive > 0 {
		tcp.SetKeepAlive(true)
		tcp.SetKeepAlivePeriod(cfg.KeepAlive)
	} else {
		tcp.SetKeepAlive(false)
	}
}

// sockOptions remembers the socket options KCP asks for so that they can be
// applied to every stream an endpoint establishes afterwards.
type sockOptions struct {
	mu     sync.Mutex
	rcvBuf int
	sndBuf int
	dscp   int
	hasDS  bool
}

// walkStreams enumerates the live streams an endpoint currently holds.
type walkStreams func(func(*stream))

// SetReadBuffer records the receive buffer and applies it to every stream the
// endpoint already holds. A transport that does not support the option is
// skipped rather than failing the call, because a stream established later may
// support it.
func (o *sockOptions) SetReadBuffer(bytes int, walk walkStreams) error {
	o.mu.Lock()
	o.rcvBuf = bytes
	o.mu.Unlock()

	walk(func(s *stream) { setSockReadBuffer(s.conn, bytes) })
	return nil
}

// SetWriteBuffer records the send buffer and applies it to every stream the
// endpoint already holds.
func (o *sockOptions) SetWriteBuffer(bytes int, walk walkStreams) error {
	o.mu.Lock()
	o.sndBuf = bytes
	o.mu.Unlock()

	walk(func(s *stream) { setSockWriteBuffer(s.conn, bytes) })
	return nil
}

// SetDSCP records the DSCP value and applies it to every stream the endpoint
// already holds. It reports an error only when streams exist and every one of
// them rejected the option, so the caller is not told about a failure that
// might still succeed on a stream established later. With no streams yet the
// request is simply pending, and applyTo will carry it out.
func (o *sockOptions) SetDSCP(dscp int, walk walkStreams) error {
	o.mu.Lock()
	o.dscp = dscp
	o.hasDS = true
	o.mu.Unlock()

	attempted, rejected := 0, 0
	walk(func(s *stream) {
		attempted++
		if err := applyDSCP(s.conn, dscp); err != nil {
			rejected++
		}
	})
	if attempted > 0 && rejected == attempted {
		return errDSCPUnsupported
	}
	return nil
}

// setSockReadBuffer applies SO_RCVBUF to one stream, ignoring a transport that
// does not support the option.
func setSockReadBuffer(conn net.Conn, bytes int) {
	if s, ok := conn.(interface{ SetReadBuffer(int) error }); ok {
		s.SetReadBuffer(bytes)
	}
}

// setSockWriteBuffer applies SO_SNDBUF to one stream, ignoring a transport that
// does not support the option.
func setSockWriteBuffer(conn net.Conn, bytes int) {
	if s, ok := conn.(interface{ SetWriteBuffer(int) error }); ok {
		s.SetWriteBuffer(bytes)
	}
}

// applyTo applies every recorded option to a freshly accepted or dialed stream.
func (o *sockOptions) applyTo(conn net.Conn) {
	o.mu.Lock()
	rcvBuf, sndBuf, dscp, hasDS := o.rcvBuf, o.sndBuf, o.dscp, o.hasDS
	o.mu.Unlock()

	if rcvBuf > 0 {
		setSockReadBuffer(conn, rcvBuf)
	}
	if sndBuf > 0 {
		setSockWriteBuffer(conn, sndBuf)
	}
	if hasDS {
		// A failure here is deliberately not fatal: the stream is already
		// established and the caller chose this transport, so refusing the
		// whole connection over a QoS hint would be worse than proceeding
		// without it. SetDSCP on an established endpoint is where it surfaces.
		applyDSCP(conn, dscp)
	}
}

// applyDSCP sets the DSCP field on a TCP stream. It mirrors what kcp-go does
// for a UDP socket: try the IPv4 TOS byte and the IPv6 traffic class, and
// report success if either sticks.
func applyDSCP(conn net.Conn, dscp int) error {
	var succeeded bool
	if err := ipv4.NewConn(conn).SetTOS(dscp << 2); err == nil {
		succeeded = true
	}
	if err := ipv6.NewConn(conn).SetTrafficClass(dscp); err == nil {
		succeeded = true
	}
	if succeeded {
		return nil
	}
	return errDSCPUnsupported
}

// errDSCPUnsupported reports that no stream accepted the DSCP option.
var errDSCPUnsupported = errors.New("carrier: SetDSCP unsupported")

// normalizeNetwork maps the network names a carrier endpoint accepts onto the
// TCP network they all resolve to. The empty string and PeerAddr's own network
// name are both accepted so a caller never has to special-case either.
func normalizeNetwork(network string) string {
	switch network {
	case "", "tcp", "carrier":
		return "tcp"
	default:
		return network
	}
}
