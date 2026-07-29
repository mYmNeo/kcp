package kcp

import (
	"encoding/binary"
	"sync/atomic"
	"testing"
)

// --- Heap peek optimization tests ---

// TestRecvBufPeekOptimization verifies that the peek-before-pop optimization
// in parse_data and Recv correctly handles out-of-order segment delivery.
// It feeds segments with non-sequential sequence numbers and confirms
// that Recv delivers them in order.
func TestRecvBufPeekOptimization(t *testing.T) {
	// Save and restore SNMP state.
	prev := DefaultSnmp.Copy()

	kcp1 := NewKCP(0x11223344, func(buf []byte, size int) {})
	kcp1.NoDelay(1, 10, 2, 1)
	kcp1.WndSize(128, 128)

	// Send data in order so snd_nxt and snd_una advance together.
	kcp1.Send([]byte("hello world"))

	// Extract the segment from snd_queue to feed into Input as "received".
	seg, ok := kcp1.snd_queue.Pop()
	if !ok {
		t.Fatal("expected segment in snd_queue")
	}

	// Encode it as a wire packet.
	buf := make([]byte, IKCP_OVERHEAD+len(seg.data))
	seg.conv = kcp1.conv
	seg.cmd = IKCP_CMD_PUSH
	seg.wnd = kcp1.wnd_unused()
	seg.una = kcp1.rcv_nxt
	seg.encode(buf)
	copy(buf[IKCP_OVERHEAD:], seg.data)

	// Feed the data into another KCP instance.
	kcp2 := NewKCP(0x11223344, func(buf []byte, size int) {})
	kcp2.NoDelay(1, 10, 2, 1)
	kcp2.WndSize(128, 128)

	// Feed segments in REVERSE order (sn=3, sn=2, sn=1, sn=0).
	// This exercises the rcv_buf peek optimization: the heap root
	// won't match rcv_nxt until all earlier segments arrive.
	for sn := 3; sn >= 0; sn-- {
		binary.LittleEndian.PutUint32(buf[12:], uint32(sn)) // set sn in header
		// Reset seg data to a known string.
		payload := []byte("hello world")
		binary.LittleEndian.PutUint32(buf[20:], uint32(len(payload))) // set length
		copy(buf[IKCP_OVERHEAD:], payload)
		kcp2.Input(buf, IKCP_PACKET_REGULAR, false)
	}

	// Now read all 4 segments back. They should be in order.
	recvBuf := make([]byte, 4096)
	total := 0
	for i := 0; i < 4; i++ {
		n := kcp2.Recv(recvBuf)
		if n <= 0 {
			t.Fatalf("Recv %d returned %d, expected > 0", i, n)
		}
		total += n
	}
	if total != 4*11 {
		t.Fatalf("total received %d, expected %d", total, 4*11)
	}

	// Verify the peek optimization didn't corrupt internal state.
	if kcp2.rcv_buf.Len() != 0 {
		t.Errorf("rcv_buf not empty after Recv: %d segments remain", kcp2.rcv_buf.Len())
	}

	// Restore SNMP.
	DefaultSnmp.Reset()
	*DefaultSnmp = *prev
}

// TestRecvBufPeek_GapInMiddle tests that the peek optimization handles
// a gap in the middle of the receive window correctly.
func TestRecvBufPeek_GapInMiddle(t *testing.T) {
	prev := DefaultSnmp.Copy()

	kcp1 := NewKCP(0x55667788, func(buf []byte, size int) {})
	kcp1.NoDelay(1, 10, 2, 1)
	kcp1.WndSize(128, 128)

	payload := []byte("abcd")

	kcp2 := NewKCP(0x55667788, func(buf []byte, size int) {})
	kcp2.NoDelay(1, 10, 2, 1)
	kcp2.WndSize(128, 128)

	// Helper to create a KCP packet with given sn.
	makePacket := func(sn uint32) []byte {
		buf := make([]byte, IKCP_OVERHEAD+len(payload))
		binary.LittleEndian.PutUint32(buf, 0x55667788)                // conv
		buf[4] = IKCP_CMD_PUSH                                        // cmd
		binary.LittleEndian.PutUint16(buf[6:], 128)                   // wnd
		binary.LittleEndian.PutUint32(buf[12:], sn)                   // sn
		binary.LittleEndian.PutUint32(buf[16:], 0)                    // una
		binary.LittleEndian.PutUint32(buf[20:], uint32(len(payload))) // len
		copy(buf[IKCP_OVERHEAD:], payload)
		return buf
	}

	// Feed sn=0 first (should go rcv_buf -> rcv_queue after peek confirms match).
	kcp2.Input(makePacket(0), IKCP_PACKET_REGULAR, false)

	// Feed sn=3 and sn=2 (out of order, stay in rcv_buf).
	kcp2.Input(makePacket(3), IKCP_PACKET_REGULAR, false)
	kcp2.Input(makePacket(2), IKCP_PACKET_REGULAR, false)

	// sn=0 should be readable now.
	recvBuf := make([]byte, 1024)
	if n := kcp2.Recv(recvBuf); n != 4 {
		t.Fatalf("first Recv: got %d, want 4 (payload len)", n)
	}

	// Now feed sn=1 (fills the gap).
	kcp2.Input(makePacket(1), IKCP_PACKET_REGULAR, false)

	// sn=1, sn=2, sn=3 should all be readable now, in order.
	for i := 1; i <= 3; i++ {
		if n := kcp2.Recv(recvBuf); n != 4 {
			t.Fatalf("Recv for sn=%d: got %d, want 4", i, n)
		}
	}

	if kcp2.rcv_buf.Len() != 0 {
		t.Errorf("rcv_buf not empty: %d segments remain", kcp2.rcv_buf.Len())
	}

	DefaultSnmp.Reset()
	*DefaultSnmp = *prev
}

// --- OutSegs batching tests ---

// TestOutSegsBatching verifies that the OutSegs SNMP counter is updated
// once per flush() call, not once per segment encoded.
func TestOutSegsBatching(t *testing.T) {
	DefaultSnmp.Reset()

	var outCalled []int // sizes passed to output callback
	kcp1 := NewKCP(0xABCDEF01, func(buf []byte, size int) {
		outCalled = append(outCalled, size)
	})
	kcp1.NoDelay(1, 10, 2, 1)
	kcp1.WndSize(128, 128)

	// Queue up multiple segments.
	const numSegs = 10
	for i := 0; i < numSegs; i++ {
		kcp1.Send([]byte("data"))
	}

	// Record OutSegs before flush.
	before := atomic.LoadUint64(&DefaultSnmp.OutSegs)

	// Flush everything.
	kcp1.flush(IKCP_FLUSH_FULL)

	// Record OutSegs after flush.
	after := atomic.LoadUint64(&DefaultSnmp.OutSegs)

	// OutSegs should have increased by exactly numSegs.
	delta := after - before
	if delta != numSegs {
		t.Errorf("OutSegs delta = %d, expected %d (one per segment)", delta, numSegs)
	}

	// Verify output callback was called.
	if len(outCalled) == 0 {
		t.Error("output callback was never called")
	}
}

// TestOutSegsBatching_BulkFlush verifies that OutSegs is correct
// even when flush() is called from the normal update path.
func TestOutSegsBatching_BulkFlush(t *testing.T) {
	DefaultSnmp.Reset()

	kcp1 := NewKCP(0xDEADBEEF, func(buf []byte, size int) {})
	kcp1.NoDelay(1, 10, 2, 1)
	kcp1.WndSize(128, 128)

	const numSegs = 20
	for i := 0; i < numSegs; i++ {
		kcp1.Send(make([]byte, 100))
	}

	before := atomic.LoadUint64(&DefaultSnmp.OutSegs)
	kcp1.flush(IKCP_FLUSH_FULL)
	after := atomic.LoadUint64(&DefaultSnmp.OutSegs)

	if delta := after - before; delta != numSegs {
		t.Errorf("OutSegs delta = %d, expected %d", delta, numSegs)
	}
}

// --- acklist pre-allocation test ---

// TestAcklistPreAllocated verifies that NewKCP pre-allocates
// the acklist backing array to avoid repeated growth on first use.
func TestAcklistPreAllocated(t *testing.T) {
	kcp1 := NewKCP(0x12345678, func(buf []byte, size int) {})

	if kcp1.acklist == nil {
		t.Fatal("acklist is nil after NewKCP")
	}

	initialCap := cap(kcp1.acklist)
	expected := int(kcp1.mtu / IKCP_OVERHEAD)
	if initialCap < expected {
		t.Errorf("acklist cap = %d, expected at least %d (mtu/OVERHEAD)", initialCap, expected)
	}

	// Verify ack_push works without reallocation for the first N acks.
	kcp1.ack_push(100, currentMs())
	if len(kcp1.acklist) != 1 {
		t.Errorf("acklist len = %d after one push, expected 1", len(kcp1.acklist))
	}
	if cap(kcp1.acklist) != initialCap {
		t.Errorf("acklist cap changed from %d to %d after first push", initialCap, cap(kcp1.acklist))
	}
}

// TestParseFastackSkipAcked verifies that parse_fastack skips segments
// marked as acked=1 (by parse_ack), so their fastack counters are not
// incremented. Tested segments should only accumulate fastack when they
// are unacknowledged.
func TestParseFastackSkipAcked(t *testing.T) {
	// Save and restore SNMP state.
	prev := DefaultSnmp.Copy()
	defer func() {
		DefaultSnmp.Reset()
		*DefaultSnmp = *prev
	}()

	kcp1 := NewKCP(0xAABBCCDD, func(buf []byte, size int) {})
	kcp1.NoDelay(1, 10, 2, 1)
	kcp1.WndSize(128, 128)

	// Send 5 segments — they enter snd_queue, then we move them to snd_buf.
	for range 5 {
		kcp1.Send([]byte("data"))
	}

	// Move all segments from snd_queue into snd_buf (simulating what flush Phase 4 does).
	for {
		seg, ok := kcp1.snd_queue.Pop()
		if !ok {
			break
		}
		seg.conv = kcp1.conv
		seg.cmd = IKCP_CMD_PUSH
		seg.sn = kcp1.snd_nxt
		seg.xmit = 1    // mark as transmitted (so fastack can apply)
		seg.acked = 0   // not acked
		seg.fastack = 0 // reset
		kcp1.snd_buf.Push(seg)
		kcp1.snd_nxt++
	}

	// Now simulate ACKing segment 0 and 2 via parse_ack. This sets acked=1 on those.
	kcp1.parse_ack(kcp1.snd_una)     // segment sn=0
	kcp1.parse_ack(kcp1.snd_una + 2) // segment sn=2

	// Call parse_fastack with sn covering all segments.
	// Segments with acked=1 (sn 0 and 2) should NOT get fastack incremented.
	kcp1.parse_fastack(kcp1.snd_una+4, currentMs())

	// Enumerate snd_buf and verify fastack counters.
	var fastackSum int
	for seg := range kcp1.snd_buf.ForEach {
		fastackSum += int(seg.fastack)
		// All segments with acked=1 must have fastack==0.
		if seg.acked == 1 && seg.fastack != 0 {
			t.Errorf("segment sn=%d has acked=1 but fastack=%d (should be 0)", seg.sn, seg.fastack)
		}
		// Non-acked segments (except the one matching the trigger SN) may have fastack > 0.
		if seg.acked == 0 && seg.sn != kcp1.snd_una+4 && seg.fastack == 0 {
			t.Errorf("segment sn=%d has acked=0 but fastack=0 (should be > 0)", seg.sn)
		}
	}

	// We expect fastack on segments 1 and 3 (acked segments 0 and 2 skipped).
	if fastackSum != 2 {
		t.Errorf("total fastack sum = %d, expected 2 (segments 1 and 3)", fastackSum)
	}
}

// BenchmarkSegmentHeapPushPop measures the cost of pushing and popping
// segments through the rcv_buf heap. This happens on every received data
// packet — the any-boxing elimination should show 0 allocs/op after optimization.
func BenchmarkSegmentHeapPushPop(b *testing.B) {
	h := newSegmentHeap()
	segs := make([]segment, 256)
	for i := range segs {
		segs[i].sn = uint32(i)
		segs[i].data = []byte("test data payload for benchmark")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for _, s := range segs {
			h.push(s)
		}
		for range segs {
			h.pop()
		}
	}
}
