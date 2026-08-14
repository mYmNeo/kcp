package kcp

import "testing"

// sinkBufPair forces the wrapper to escape, matching postProcess assigning
// pair[:] into ipv4.Message.Buffers (otherwise the compiler elides allocs).
var sinkBufPair [][]byte

// BenchmarkBufPairPool measures the TX-path wrapper used for
// ipv4.Message.Buffers: pooled *[1][]byte vs allocating a fresh
// [][]byte{buf} / new([1][]byte) each time. ReportAllocs decides whether
// bufPairPool is worth keeping (review: "先 bench 再保留").
func BenchmarkBufPairPool(b *testing.B) {
	payload := make([]byte, 1200)

	b.Run("Pooled", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			pair := bufPairPool.Get().(*[1][]byte)
			pair[0] = payload
			sinkBufPair = pair[:]
			pair[0] = nil
			bufPairPool.Put(pair)
		}
	})

	b.Run("AllocSlice", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buffers := [][]byte{payload}
			sinkBufPair = buffers
		}
	})

	b.Run("AllocArrayPtr", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			pair := new([1][]byte)
			pair[0] = payload
			sinkBufPair = pair[:]
		}
	})
}

func TestBufferPoolGetSize(t *testing.T) {
	bp := newBufferPool()

	buf := bp.Get()

	// Check length
	if len(buf) != mtuLimit {
		t.Fatalf("expected len=%d, got %d", mtuLimit, len(buf))
		return
	}

	// Check capacity
	if cap(buf) != mtuLimit {
		t.Fatalf("expected cap=%d, got %d", mtuLimit, cap(buf))
		return
	}
}

func TestBufferPoolPutAndReuse(t *testing.T) {
	bp := newBufferPool()

	buf := bp.Get()
	// Modify buffer to track it
	buf[0] = 99

	// Put back to pool
	if err := bp.Put(buf); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Get again. sync.Pool reuse is best-effort (especially under -race/GC),
	// so only require correct capacity; check prior data if the same buffer
	// was reused.
	buf2 := bp.Get()
	if cap(buf2) != mtuLimit {
		t.Fatalf("expected cap=%d, got %d", mtuLimit, cap(buf2))
	}
	if &buf2[0] == &buf[0] && buf2[0] != 99 {
		t.Fatalf("expected reused buffer to keep previous data")
	}
}

func TestBufferPoolPutWrongSizeIgnored(t *testing.T) {
	bp := newBufferPool()

	// Make a buffer with wrong capacity
	wrongBuf := make([]byte, 100)

	bp.Put(wrongBuf)

	// Get should still return a buffer with mtuLimit capacity
	buf := bp.Get()

	if cap(buf) != mtuLimit {
		t.Fatalf("pool accepted wrong-sized buffer; expected cap=%d, got %d", mtuLimit, cap(buf))
		return
	}
}

func TestBufferPoolPutReturnsError(t *testing.T) {
	bp := newBufferPool()

	// 1. Correct size
	buf := make([]byte, mtuLimit)
	if err := bp.Put(buf); err != nil {
		t.Fatalf("expected nil error for correct size, got %v", err)
	}

	// 2. Incorrect size
	wrongBuf := make([]byte, mtuLimit+1)
	if err := bp.Put(wrongBuf); err == nil {
		t.Fatalf("expected error for wrong size, got nil")
	}
}
