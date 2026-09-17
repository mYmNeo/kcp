package carrier

import (
	"errors"
	"net"
	"testing"
	"time"
)

// TestWriteToHonorsWriteDeadline pins the net.PacketConn contract for writes.
//
// WriteTo blocks while a stream's send queue is full — that is how backpressure
// reaches KCP — so without consulting the deadline a stalled peer would wedge
// the caller forever. The queue is deliberately depth 1 with no writer draining
// it, which makes the second write block deterministically.
func TestWriteToHonorsWriteDeadline(t *testing.T) {
	c := &Conn{
		remote:   testAddr{},
		rx:       make(chan rxPacket, defaultRxQueue),
		stall:    defaultStallTimeout,
		chClosed: make(chan struct{}),
		chFatal:  make(chan struct{}),
	}
	c.streams = newStreamSet(func() { c.failAll(net.ErrClosed) })

	// No start(): the writer goroutine never runs, so the single queue slot
	// stays full after the first write.
	s := newStream(&blockingConn{ch: make(chan struct{})}, 1)
	c.streams.add(s)

	if _, err := c.WriteTo([]byte{0x01}, c.remote); err != nil {
		t.Fatalf("first WriteTo: %v", err)
	}

	c.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))

	// Run the blocking write on its own goroutine so a regression reports a
	// clear failure instead of hanging the whole package until the test binary
	// times out.
	type result struct {
		err     error
		elapsed time.Duration
	}
	done := make(chan result, 1)
	go func() {
		start := time.Now()
		_, err := c.WriteTo([]byte{0x02}, c.remote)
		done <- result{err: err, elapsed: time.Since(start)}
	}()

	select {
	case got := <-done:
		if !errors.Is(got.err, errDeadline) {
			t.Fatalf("WriteTo = %v after %v, want %v", got.err, got.elapsed, errDeadline)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WriteTo did not return within 5s despite a 100ms write deadline")
	}
}
