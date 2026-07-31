// Per-operation benchmarks for the hottest per-frame paths in staging/smux.
//
// These benchmarks measure the steady-state allocation cost of the write,
// read, frame-header encode/decode, writeFrameInternal, and allocator hot
// paths. They drive the optimization decision: a path that already shows
// ~0 extra allocs/op is reported as already-optimal rather than churned.
//
// This file lives in package smux (white-box) so it can exercise unexported
// symbols (rawHeader, defaultAllocator, msb) directly while still using a
// real TCP-backed stream pair for the end-to-end paths.

package smux

import (
	"encoding/binary"
	"io"
	"testing"
	"time"
)

// benchStreamPairVersion returns a bidirectional client/server stream pair
// running the requested protocol version (1 or 2) over a real TCP connection.
// It mirrors getSmuxStreamPair but lets the caller force the version so v1
// and v2 paths can be benchmarked independently.
func benchStreamPairVersion(tb testing.TB, version int) (*Session, *Session, *Stream, *Stream) {
	tb.Helper()
	c1, c2, err := getTCPConnectionPair()
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		_ = c1.Close()
		_ = c2.Close()
	})

	cfg := DefaultConfig()
	cfg.Version = version
	cfg.KeepAliveDisabled = true // keep the benchmark quiet

	serverSess, err := Server(c2, cfg)
	if err != nil {
		tb.Fatal(err)
	}
	clientSess, err := Client(c1, cfg)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		_ = serverSess.Close()
		_ = clientSess.Close()
	})

	type acceptResult struct {
		ss  *Stream
		err error
	}
	done := make(chan acceptResult, 1)
	go func() {
		ss, err := serverSess.AcceptStream()
		done <- acceptResult{ss, err}
	}()
	cs, err := clientSess.OpenStream()
	if err != nil {
		tb.Fatal(err)
	}
	res := <-done
	if res.err != nil {
		tb.Fatal(res.err)
	}
	return clientSess, serverSess, cs, res.ss
}

// drainInBackground drains all bytes written to rc into a throwaway buffer
// on a background goroutine so the writer benchmark never blocks on a full
// receive buffer. The returned stop function closes rc (which unblocks the
// in-flight Read with an error) and then blocks until the drain goroutine has
// exited. Closing here — rather than relying on tb.Cleanup — is required
// because tb.Cleanup runs only after the bench function returns, so waiting
// on the drain goroutine before that would deadlock.
func drainInBackground(tb testing.TB, rc io.ReadCloser) func() {
	tb.Helper()
	buf := make([]byte, 32*1024)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, err := rc.Read(buf); err != nil {
				return
			}
		}
	}()
	return func() {
		_ = rc.Close()
		<-done
	}
}

// ---------------------------------------------------------------------------
// Stream.Write: writeV1 / writeV2, with and without a write deadline.
//
// The deadline variants specifically exercise the time.NewTimer path
// (stream.go:504 in writeV1, stream.go:561 in writeV2). Without a deadline
// neither path allocates a timer.
// ---------------------------------------------------------------------------

func benchStreamWrite(b *testing.B, version int, deadline bool) {
	_, _, cs, ss := benchStreamPairVersion(b, version)
	stop := drainInBackground(b, ss)

	// payload small enough to fit in a single frame so each Write is one frame.
	payload := make([]byte, 1024)
	total := 0

	if deadline {
		// A far-future deadline keeps the timer path active every Write
		// without ever firing (no timeout reset churn mid-loop).
		if err := cs.SetWriteDeadline(time.Now().Add(time.Hour)); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := cs.Write(payload)
		if err != nil {
			b.Fatalf("write: %v", err)
		}
		total += n
	}
	b.StopTimer()

	// correctness assert outside the timed loop
	if total != b.N*len(payload) {
		b.Fatalf("wrote %d, want %d", total, b.N*len(payload))
	}
	// clear the deadline before cleanup so Close is clean
	_ = cs.SetWriteDeadline(time.Time{})
	stop()
}

func BenchmarkStreamWriteV1(b *testing.B) {
	benchStreamWrite(b, 1, false)
}

func BenchmarkStreamWriteV1Deadline(b *testing.B) {
	benchStreamWrite(b, 1, true)
}

func BenchmarkStreamWriteV2(b *testing.B) {
	benchStreamWrite(b, 2, false)
}

func BenchmarkStreamWriteV2Deadline(b *testing.B) {
	benchStreamWrite(b, 2, true)
}

// ---------------------------------------------------------------------------
// Stream.Read: tryReadV1 / tryReadV2 draining a bufferRing.
//
// A background writer keeps the stream fed so each Read copies out of the
// ring and recycles buffers to defaultAllocator. This exercises
// bufferRing.consumeFront and the defaultAllocator.Put recycle path.
// ---------------------------------------------------------------------------

func benchStreamRead(b *testing.B, version int) {
	_, _, cs, ss := benchStreamPairVersion(b, version)

	// Feed the stream from a background writer.
	payload := make([]byte, 1024)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for {
			if _, err := cs.Write(payload); err != nil {
				return
			}
		}
	}()

	readBuf := make([]byte, 1024)
	total := 0

	// Let some data land before timing.
	warmup := make([]byte, 1024)
	for i := 0; i < 16; i++ {
		n, err := ss.Read(warmup)
		if err != nil {
			b.Fatalf("warmup read: %v", err)
		}
		total += n
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := ss.Read(readBuf)
		if err != nil {
			b.Fatalf("read: %v", err)
		}
		total += n
	}
	b.StopTimer()

	if total == 0 {
		b.Fatal("read no bytes")
	}
	// Closing the client stream stops the background writer.
	_ = cs.Close()
	<-writerDone
}

func BenchmarkStreamReadV1(b *testing.B) {
	benchStreamRead(b, 1)
}

func BenchmarkStreamReadV2(b *testing.B) {
	benchStreamRead(b, 2)
}

// ---------------------------------------------------------------------------
// Frame header encode/decode (frame.go rawHeader / binary.LittleEndian).
//
// Confirms there is no reflection and no per-frame header allocation: the
// header lives in a stack [headerSize]byte and is filled with PutUint16/
// PutUint32.
// ---------------------------------------------------------------------------

func BenchmarkFrameHeaderEncode(b *testing.B) {
	var hdr rawHeader
	const ver byte = 1
	const cmd byte = cmdPSH
	const sid uint32 = 42
	const length uint16 = 1024
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hdr[0] = ver
		hdr[1] = cmd
		binary.LittleEndian.PutUint16(hdr[2:], length)
		binary.LittleEndian.PutUint32(hdr[4:], sid)
	}
	b.StopTimer()
	// correctness assert
	if hdr.Version() != ver || hdr.Cmd() != cmd ||
		hdr.Length() != length || hdr.StreamID() != sid {
		b.Fatalf("header mismatch: %+v", hdr)
	}
}

func BenchmarkFrameHeaderDecode(b *testing.B) {
	var hdr rawHeader
	hdr[0] = 1
	hdr[1] = cmdPSH
	binary.LittleEndian.PutUint16(hdr[2:], 1024)
	binary.LittleEndian.PutUint32(hdr[4:], 42)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hdr.Version()
		_ = hdr.Cmd()
		_ = hdr.Length()
		_ = hdr.StreamID()
	}
	b.StopTimer()
	if hdr.StreamID() != 42 {
		b.Fatal("decode mismatch")
	}
}

// ---------------------------------------------------------------------------
// Session.writeFrameInternal: the per-frame session write path.
//
// sendLoop pre-allocates its header buffer once (session.go:636) and the
// result channel is pooled (resultChanPool). This benchmark confirms the
// per-call allocation cost of writeFrameInternal in steady state by driving
// it through the public Stream.Write path with a single-frame payload.
// ---------------------------------------------------------------------------

func BenchmarkWriteFrameInternal(b *testing.B) {
	_, _, cs, ss := benchStreamPairVersion(b, 1)
	stop := drainInBackground(b, ss)

	payload := make([]byte, 1024)
	total := 0

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := cs.Write(payload)
		if err != nil {
			b.Fatalf("write: %v", err)
		}
		total += n
	}
	b.StopTimer()

	if total != b.N*len(payload) {
		b.Fatalf("wrote %d, want %d", total, b.N*len(payload))
	}
	stop()
}

// ---------------------------------------------------------------------------
// Allocator.Get/Put steady-state (alloc.go).
//
// Mirrors the existing BenchmarkAlloc but reports explicitly here so the
// before/after artifact contains the allocator confirmation alongside the
// other hot paths. Confirms msb De Bruijn is O(1) with no loop fallback.
// ---------------------------------------------------------------------------

func BenchmarkAllocatorGetPut(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pbuf := defaultAllocator.Get(i % 65536)
		defaultAllocator.Put(pbuf)
	}
}

func BenchmarkHotPathMSB(b *testing.B) {
	// Exercise msb across the whole power-of-two range plus the awkward
	// non-power-of-two sizes that determine the bits+1 pool selection.
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = msb((i % 65536) + 1)
	}
}

// TestConcurrentWriteSameStream exercises the writeTimer field under
// concurrent goroutines writing to the SAME stream — the precise pattern
// that would race without writeTimerMu. Run with -race.
func TestConcurrentWriteSameStream(t *testing.T) {
	_, _, cs, ss := benchStreamPairVersion(t, 1)
	drain := drainInBackground(t, ss)

	cs.SetWriteDeadline(time.Now().Add(10 * time.Second))

	const goroutines = 4
	const writesEach = 64
	payload := []byte("concurrent-race-test-payload")

	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < writesEach; j++ {
				if _, err := cs.Write(payload); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
	}

	for i := 0; i < goroutines; i++ {
		err := <-errs
		if err != nil {
			t.Fatalf("concurrent write %d: %v", i, err)
		}
	}

	drain()
}
