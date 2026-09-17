package carrier

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
)

// rxPacket is one received datagram together with the peer it came from. data
// is a pooled buffer owned by the receiver until deliver or putBuffer releases
// it.
type rxPacket struct {
	data []byte
	addr net.Addr
}

// bufPool recycles datagram buffers sized for a real KCP packet.
//
// It holds *[kcpCeiling]byte rather than []byte so that Put avoids
// interface-boxing: a slice stored in a sync.Pool boxes its 24 byte header onto
// the heap on every Put, which would put an allocation straight back onto the
// per-datagram path this pool exists to clear. A pointer fits in an interface's
// data word, so a round trip costs nothing. This mirrors the technique
// kcp-go's own defaultBufferPool uses.
var bufPool = sync.Pool{New: func() any { return new([kcpCeiling]byte) }}

// getBuffer returns an empty buffer with room for one datagram. It comes from
// the pool when possible, so the common path — a sub-2048 byte KCP packet —
// reuses memory instead of allocating per packet. A datagram larger than the
// pool's size class gets a fresh allocation and is simply not recycled.
func getBuffer() []byte {
	return bufPool.Get().(*[kcpCeiling]byte)[:0]
}

// putBuffer returns a buffer to the pool. Only the pool's own size class is
// recycled, which keeps the pool homogeneous; anything else is left to the
// garbage collector.
//
// The capacity check is what makes the slice-to-array-pointer conversion sound:
// only a slice of the pool's own size class was ever backed by a
// *[kcpCeiling]byte, so any other capacity belongs to a different allocation
// and is dropped rather than reinterpreted.
func putBuffer(b []byte) {
	if cap(b) != kcpCeiling {
		return
	}
	bufPool.Put((*[kcpCeiling]byte)(b[:cap(b)]))
}

// allocBuffer returns a zero length buffer with room for n bytes, preferring the
// pool. Frames at or below kcpCeiling come from the pool; anything larger
// allocates, which cannot happen for a KCP session but keeps the carrier honest
// about the framing limit it advertises.
func allocBuffer(n int) []byte {
	if n <= kcpCeiling {
		return getBuffer()
	}
	return make([]byte, 0, n)
}

// writeFrame writes one datagram as a length-prefixed frame. The header and the
// payload go out as a single writev, so a datagram costs one syscall and no
// extra copy. A zero length frame is legal: it carries an empty datagram, which
// a UDP socket accepts, and the carrier must not be stricter than the socket it
// replaces.
//
// The scratch lives on the stream because this is the per-datagram hot path, and
// it is rebuilt from a persistent array rather than reused as a slice:
// net.Buffers.WriteTo takes the writev fast path, and the poll layer advances
// that slice by reslicing it, which drops its capacity to zero and would force
// the next append to allocate. Rebuilding the slice header over the stable array
// sidesteps that, so a datagram costs no allocation at all. The writer goroutine
// is the only caller.
func (s *stream) writeFrame(payload []byte) error {
	if len(payload) > MaxDatagram {
		return fmt.Errorf("%w: datagram size %d", ErrProtocol, len(payload))
	}

	binary.BigEndian.PutUint16(s.wbuf[:], uint16(len(payload)))
	s.wbufArr[0] = s.wbuf[:]
	s.wbufArr[1] = payload
	s.wbufs = s.wbufArr[:]

	_, err := s.wbufs.WriteTo(s.conn)
	return err
}

// frameReader turns a TCP byte stream back into datagrams. It reads into a
// reusable buffer so that one syscall covers as many frames as the peer
// delivered, and copies each payload out into its own pooled buffer because the
// consumer (KCP) keeps received datagrams.
type frameReader struct {
	conn net.Conn
	buf  []byte
	r, w int
	err  error
}

// newFrameReader wraps conn.
func newFrameReader(conn net.Conn) *frameReader {
	return &frameReader{conn: conn, buf: make([]byte, readBufSize)}
}

// next returns the next datagram in a buffer the caller owns. A truncated or
// malformed frame surfaces as ErrProtocol; a dead connection surfaces as the
// connection's own error. A pending error is only reported once the bytes
// already buffered have been framed.
func (fr *frameReader) next() ([]byte, error) {
	for {
		avail := fr.w - fr.r
		if avail >= frameHeaderSize {
			// The length prefix is a uint16, so n <= MaxDatagram and the frame
			// is at most readBufSize — exactly the size of fr.buf. The buffer
			// therefore never has to grow, which is what lets prepare be pure
			// compaction and what makes a peer unable to name a length this
			// reader cannot hold.
			n := int(binary.BigEndian.Uint16(fr.buf[fr.r:]))
			if avail >= frameHeaderSize+n {
				payload := allocBuffer(n)[:n]
				copy(payload, fr.buf[fr.r+frameHeaderSize:fr.r+frameHeaderSize+n])
				fr.r += frameHeaderSize + n
				if fr.r == fr.w {
					fr.r, fr.w = 0, 0
				}
				return payload, nil
			}
			fr.prepare(frameHeaderSize + n)
		} else {
			fr.prepare(frameHeaderSize)
		}

		if fr.err != nil {
			return nil, fr.err
		}

		nr, err := fr.conn.Read(fr.buf[fr.w:])
		if nr > 0 {
			fr.w += nr
		}
		if err != nil {
			fr.err = err
		}
	}
}

// prepare makes room for need bytes by moving unread bytes to the front of the
// buffer. need is always at most the buffer's length (see next), so this is pure
// compaction and never allocates.
func (fr *frameReader) prepare(need int) {
	if fr.r == 0 {
		return
	}
	copy(fr.buf, fr.buf[fr.r:fr.w])
	fr.w -= fr.r
	fr.r = 0
}

// pushRx hands a received datagram to the consumer, blocking while the queue is
// full so that TCP flow control reaches the peer.
//
// Buffer ownership is unconditional: the caller keeps ownership of pkt.data
// regardless of the outcome, and it is the caller that releases it. Only a
// successful hand-off transfers the buffer to the consumer. Having exactly one
// rule is what keeps a datagram from being returned to the pool twice, which
// would let two consumers share one array.
func pushRx(ch chan rxPacket, die <-chan struct{}, pkt rxPacket) error {
	select {
	case ch <- pkt:
		return nil
	case <-die:
		return net.ErrClosed
	}
}

// deliver copies one queued datagram into p and releases its buffer. It
// implements the ReadFrom contract: a datagram larger than p is truncated, as
// it would be on a UDP socket.
func deliver(p []byte, pkt rxPacket) int {
	n := copy(p, pkt.data)
	putBuffer(pkt.data)
	return n
}

// drain releases every datagram still queued for a closed endpoint.
func drain(ch chan rxPacket) {
	for {
		select {
		case pkt := <-ch:
			putBuffer(pkt.data)
		default:
			return
		}
	}
}
