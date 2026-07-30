// The MIT License (MIT)
//
// # Copyright (c) 2015 xtaci
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package kcp

import (
	"errors"
	"sync"
)

// pre-allocated error to avoid repeated allocations
var errBufferSizeMismatch = errors.New("buffer size mismatch")

// A system-wide packet buffer shared among sending, receiving and FEC
// to mitigate high-frequency memory allocation of packets.
var defaultBufferPool = newBufferPool()

type bufferPool struct {
	xmitBuf sync.Pool
}

// newBufferPool creates a system-wide packet buffer pool.
// Buffers are stored as *[mtuLimit]byte array pointers (8 bytes) so that
// Put/Get into sync.Pool avoids interface-boxing allocations — the same
// technique used by bufPairPool below.
func newBufferPool() *bufferPool {
	return &bufferPool{
		xmitBuf: sync.Pool{
			New: func() any {
				return new([mtuLimit]byte)
			},
		},
	}
}

// Get retrieves a buffer from the pool as a []byte slice.
func (bp *bufferPool) Get() []byte {
	arr := bp.xmitBuf.Get().(*[mtuLimit]byte)
	return arr[:]
}

// Put returns a buffer to the pool.
func (bp *bufferPool) Put(buf []byte) error {
	// Only put back buffers of the correct size.
	if cap(buf) != mtuLimit {
		return errBufferSizeMismatch
	}
	// Convert slice back to array pointer — 8 bytes, no boxing allocation.
	bp.xmitBuf.Put((*[mtuLimit]byte)(buf[:cap(buf)]))
	return nil
}

// bufPairPool reduces allocation of [][]byte wrapper slices used in
// ipv4.Message.Buffers during TX batching. Each entry is a *[1][]byte
// (8-byte pointer) that fits inline in any — no boxing allocation.
var bufPairPool = sync.Pool{
	New: func() any {
		var a [1][]byte
		return &a
	},
}
