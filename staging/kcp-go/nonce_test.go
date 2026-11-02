package kcp

import (
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"
)

// nonceIdleConn keeps a session's read loop blocked until Close.
type nonceIdleConn struct {
	closed chan struct{}
	once   sync.Once
}

func newNonceIdleConn() *nonceIdleConn {
	return &nonceIdleConn{closed: make(chan struct{})}
}

func (c *nonceIdleConn) ReadFrom([]byte) (int, net.Addr, error) {
	<-c.closed
	return 0, nil, net.ErrClosed
}

func (c *nonceIdleConn) WriteTo([]byte, net.Addr) (int, error) { return 0, net.ErrClosed }
func (c *nonceIdleConn) LocalAddr() net.Addr                   { return nonceAddr("local") }
func (c *nonceIdleConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}
func (c *nonceIdleConn) SetDeadline(time.Time) error      { return nil }
func (c *nonceIdleConn) SetReadDeadline(time.Time) error  { return nil }
func (c *nonceIdleConn) SetWriteDeadline(time.Time) error { return nil }

type nonceAddr string

func (a nonceAddr) Network() string { return "mem" }
func (a nonceAddr) String() string  { return string(a) }

func newNonceSession(t *testing.T, block BlockCrypt) *UDPSession {
	t.Helper()
	conn := newNonceIdleConn()
	s := newUDPSession(1, 0, 0, nil, conn, false, nonceAddr("peer"), block)
	t.Cleanup(func() {
		conn.Close()
		s.Close()
	})
	return s
}

// TestFillNonceConcurrentDistinct checks the transmit nonce. The writer and
// the updater both seal, so fillNonce must stay unique under concurrent calls.
// Two sessions that share a key must not share a prefix.
func TestFillNonceConcurrentDistinct(t *testing.T) {
	block, err := NewAESGCMCrypt([]byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	a := newNonceSession(t, block)
	b := newNonceSession(t, block)
	if a.nonceHi == b.nonceHi {
		t.Fatalf("sessions sharing a key got the same prefix %08x", a.nonceHi)
	}

	const (
		goroutines = 8
		perG       = 4096
	)
	got := make([][12]byte, goroutines*perG)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			base := g * perG
			for i := 0; i < perG; i++ {
				a.fillNonce(got[base+i][:])
			}
		}(g)
	}
	wg.Wait()

	seen := make(map[[12]byte]struct{}, len(got))
	for i, n := range got {
		if p := binary.BigEndian.Uint32(n[:4]); p != a.nonceHi {
			t.Fatalf("nonce %d prefix %08x, session prefix %08x", i, p, a.nonceHi)
		}
		if _, ok := seen[n]; ok {
			t.Fatalf("duplicate nonce %x", n)
		}
		seen[n] = struct{}{}
	}

	var other [12]byte
	b.fillNonce(other[:])
	if p := binary.BigEndian.Uint32(other[:4]); p != b.nonceHi {
		t.Fatalf("second session prefix %08x, nonce prefix %08x", b.nonceHi, p)
	}
	if _, ok := seen[other]; ok {
		t.Fatalf("second session nonce collided with the first: %x", other)
	}
}
