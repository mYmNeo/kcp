// The MIT License (MIT)
//
// Copyright (c) 2015 xtaci
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT ANY WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package kcp

import (
	"encoding/binary"
	"sync"
	"testing"
	"time"
)

// hotpathBenchPacket builds a single valid KCP PUSH segment wire packet
// with the given conv, sn and payload, matching the on-wire header layout
// consumed by KCP.Input. The returned slice is owned by the caller.
func hotpathBenchPacket(conv uint32, sn uint32, payload []byte) []byte {
	buf := make([]byte, IKCP_OVERHEAD+len(payload))
	binary.LittleEndian.PutUint32(buf, conv)                      // conv
	buf[4] = IKCP_CMD_PUSH                                        // cmd
	buf[5] = 0                                                    // frg
	binary.LittleEndian.PutUint16(buf[6:], 128)                   // wnd
	binary.LittleEndian.PutUint32(buf[8:], 0)                     // ts
	binary.LittleEndian.PutUint32(buf[12:], sn)                   // sn
	binary.LittleEndian.PutUint32(buf[16:], 0)                    // una
	binary.LittleEndian.PutUint32(buf[20:], uint32(len(payload))) // len
	copy(buf[IKCP_OVERHEAD:], payload)
	return buf
}

// --- 1. UDPSession.Write per-message allocation -----------------------------

// BenchmarkUDPSessionWrite_NoDeadline measures the per-message cost of
// UDPSession.Write with no write deadline set. The deadline path in
// WriteBuffers (sess.go:371-384) is skipped entirely here, so this is the
// lower bound for per-call overhead.
func BenchmarkUDPSessionWrite_NoDeadline(b *testing.B) {
	port := nextPort()
	l := sinkServer(port)
	defer l.Close()

	b.ReportAllocs()
	cli, err := dialSink(port)
	if err != nil {
		b.Fatal(err)
	}
	defer cli.Close()

	// dialSink sets SetDeadline (both read+write). Clear the write
	// deadline so this benchmark measures the true no-deadline path.
	cli.SetWriteDeadline(time.Time{})

	msg := make([]byte, 64)
	b.SetBytes(int64(len(msg)))
	b.ResetTimer()
	for b.Loop() {
		if _, err := cli.Write(msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUDPSessionWrite_WithDeadline measures the per-message cost of
// UDPSession.Write with a write deadline set. This exercises the time.NewTimer
// allocation in WriteBuffers (sess.go:373). The deadline is far in the future
// so the timer never fires; only the per-call NewTimer allocation is measured.
func BenchmarkUDPSessionWrite_WithDeadline(b *testing.B) {
	port := nextPort()
	l := sinkServer(port)
	defer l.Close()

	b.ReportAllocs()
	cli, err := dialSink(port)
	if err != nil {
		b.Fatal(err)
	}
	defer cli.Close()

	// Far-future deadline so the timer channel never fires during the run.
	cli.SetWriteDeadline(time.Now().Add(time.Hour))

	msg := make([]byte, 64)
	b.SetBytes(int64(len(msg)))
	b.ResetTimer()
	for b.Loop() {
		if _, err := cli.Write(msg); err != nil {
			b.Fatal(err)
		}
	}
}

// --- 2. KCP.Input per-segment cost -----------------------------------------

// BenchmarkKCPInput measures the per-packet cost of feeding valid KCP PUSH
// segments into a KCP instance's Input (kcp.go:634). Each iteration feeds one
// in-window segment with a monotonically increasing sn, then drains the
// receive queue so the receiver state stays bounded and steady-state is
// measured rather than window-growth artifacts. Correctness (Recv returns the
// payload) is asserted outside the timed loop.
func BenchmarkKCPInput(b *testing.B) {
	const conv = uint32(0x55667788)
	kcp := NewKCP(conv, func(buf []byte, size int) {})
	kcp.WndSize(128, 128)
	kcp.NoDelay(1, 10, 2, 1)

	payload := []byte("bench-input-payload-32-bytes!!") // 32 bytes
	pkt := hotpathBenchPacket(conv, 0, payload)

	// Pre-warm the buffer pool so the first Input's pool.Get doesn't
	// allocate a fresh *[mtuLimit]byte and skew the measurement.
	warm := defaultBufferPool.Get()
	defaultBufferPool.Put(warm)

	recvBuf := make([]byte, len(payload))
	var (
		payloadOK  bool   // set to true inside drain loop
		gotPayload string // snapshot of first recv'd payload
		recvCount  int    // number of Recv calls across all iterations
	)

	b.ReportAllocs()
	b.SetBytes(int64(len(pkt)))
	b.ResetTimer()

	var sn uint32
	for b.Loop() {
		// Rewrite the sn field of the reused packet for an in-window segment.
		binary.LittleEndian.PutUint32(pkt[12:], sn)
		if ret := kcp.Input(pkt, IKCP_PACKET_REGULAR, false); ret != 0 {
			b.Fatalf("Input returned %d for sn=%d", ret, sn)
		}
		// Drain the receive queue to keep rcv_nxt advancing in lock-step,
		// bounding rcv_buf/rcv_queue size and isolating per-segment cost.
		// Capture the first recv'd payload for correctness verification.
		for kcp.PeekSize() > 0 {
			if !payloadOK {
				n := kcp.Recv(recvBuf)
				gotPayload = string(recvBuf[:n])
				payloadOK = true
			} else {
				kcp.Recv(recvBuf)
			}
			recvCount++
		}
		sn++
	}
	b.StopTimer()

	// Correctness assertions outside the timed region.
	if !payloadOK {
		b.Fatal("no payload received — drain loop never executed Recv")
	}
	if gotPayload != string(payload) {
		b.Fatalf("recv mismatch: got %q want %q", gotPayload, payload)
	}
	if recvCount != b.N {
		b.Fatalf("recv count %d != iterations %d", recvCount, b.N)
	}
}

// --- 3. KCP.flush per-flush cost (acklist + snd_buf mix) -------------------

// BenchmarkFlush_AcklistSndBuf measures flush() cost with a realistic mix:
// a populated acklist (acks pending from received data) and a populated
// snd_buf (in-flight segments eligible for retransmit). The existing
// BenchmarkFlush (kcp_test.go:150) only covers an empty-acklist snd_buf
// retransmit path; this variant exercises Phase 1 (ack flush) together with
// Phase 4 (segment transmission) so the acklist encode path is covered.
func BenchmarkFlush_AcklistSndBuf(b *testing.B) {
	const ackCount = 32
	const sndCount = 64

	b.ReportAllocs()
	var mu sync.Mutex

	// Build a fresh KCP per iteration group would skew allocs; instead we
	// rebuild state before the timed loop and reset acklist after each flush
	// (flush itself zeroes acklist via acklist[0:0]).
	kcp := NewKCP(1, func(buf []byte, size int) {})
	kcp.snd_buf = NewRingBuffer[segment](1024)
	for range sndCount {
		kcp.snd_buf.Push(segment{xmit: 1, resendts: currentMs() + 10000})
	}
	// Pre-populate the acklist with ackItems within the receiver window.
	now := currentMs()
	for i := range ackCount {
		kcp.ack_push(uint32(i), now)
	}
	// Keep rcv_nxt below the ack sns so the bufferbloat filter emits them.
	kcp.rcv_nxt = 0

	b.ResetTimer()
	for b.Loop() {
		mu.Lock()
		kcp.flush(IKCP_FLUSH_FULL)
		mu.Unlock()
		// Re-arm acklist so each iteration measures the same work; flush
		// cleared it via acklist = acklist[0:0].
		for i := range ackCount {
			kcp.ack_push(uint32(i), now)
		}
	}
}
