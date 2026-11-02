// The MIT License (MIT)
//
// Copyright (c) 2015 xtaci
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
	"encoding/binary"
	"hash/crc32"
	"runtime"
	"sync"
	"time"

	"golang.org/x/net/ipv4"
	"golang.org/x/time/rate"
)

// ppBudget is how many packets a worker takes from one session before it
// moves on, so a busy session cannot starve the others.
const ppBudget = 4 * maxBatchSize

// ppQueue holds the sessions with packets waiting for postProcess. A fixed
// set of workers serves every session, so an idle session holds no goroutine.
var ppQueue struct {
	once sync.Once
	mu   sync.Mutex
	cond *sync.Cond
	q    []*UDPSession
	head int
}

// ppWorker holds the TX batch of one worker, reused for every session it serves.
type ppWorker struct {
	txqueue []ipv4.Message
	bufs    [][]byte
}

// oneBuf returns a one-element Buffers slice for b, backed by w.bufs.
func (w *ppWorker) oneBuf(b []byte) [][]byte {
	w.bufs = append(w.bufs, b)
	return w.bufs[len(w.bufs)-1 : len(w.bufs) : len(w.bufs)]
}

func (w *ppWorker) recycle() {
	for k := range w.txqueue {
		defaultBufferPool.Put(w.txqueue[k].Buffers[0])
		w.txqueue[k].Buffers = nil
	}
	w.txqueue = w.txqueue[:0]
	clear(w.bufs)
	w.bufs = w.bufs[:0]
}

// ppWorkers recycles batches handed to a rate limited session and back.
var ppWorkers = sync.Pool{New: func() any {
	return &ppWorker{
		txqueue: make([]ipv4.Message, 0, maxBatchSize),
		bufs:    make([][]byte, 0, maxBatchSize),
	}
}}

// flushTx sends the batch in w. When the rate limiter needs time first, s
// keeps the batch and flushTx reports true, so that no worker sleeps while
// other sessions wait for it.
func (s *UDPSession) flushTx(w *ppWorker, bytesToSend int) bool {
	if limiter, _ := s.rateLimiter.Load().(*rate.Limiter); limiter != nil {
		now := time.Now()
		// A batch larger than the burst cannot be reserved, so it is sent
		// immediately without rate limiting.
		if r := limiter.ReserveN(now, bytesToSend); r.OK() {
			if d := r.DelayFrom(now); d > 0 {
				s.ppHeld.w, s.ppHeld.bytes, s.ppHeld.until = w, bytesToSend, now.Add(d)
				return true
			}
		}
	}
	s.sendTx(w, bytesToSend)
	return false
}

func (s *UDPSession) sendTx(w *ppWorker, bytesToSend int) {
	s.tx(w.txqueue)
	if kcpTrace {
		s.mu.Lock()
		s.kcp.debugLog(IKCP_LOG_OUTPUT, "conv", s.kcp.conv, "datalen", bytesToSend)
		s.mu.Unlock()
	}
	w.recycle()
}

// kickPostProcess queues s for a worker unless it is already queued or being
// served. Callers queue their packet first. An empty queue is left alone so
// a speculative kick does not claim the session.
func (s *UDPSession) kickPostProcess() {
	if s.postQueue.len() == 0 {
		return
	}
	if s.claimPostProcess() {
		ppEnqueue(s)
	}
}

// dispatchStaged transmits packets Write collected while it held s.mu.
// n == 0 falls back to the queue. A non-empty queue or a busy worker takes
// the packets onto the queue so they stay behind whatever is already there.
func (s *UDPSession) dispatchStaged(n int, lone sendRequest, many []sendRequest) {
	if n == 0 {
		s.dispatchPost()
		return
	}
	if s.postQueue.len() != 0 || !s.claimPostProcess() {
		s.handoffStaged(n, lone, many)
		s.kickPostProcess()
		return
	}
	w := ppWorkers.Get().(*ppWorker)
	shipped := false
	for {
		if h := s.ppHeld.w; h != nil {
			s.ppHeld.w = nil
			s.sendTx(h, s.ppHeld.bytes)
			ppWorkers.Put(h)
		}
		// Older queued packets go out first. The fast path is an empty queue,
		// which is an atomic load and does not take the queue lock.
		if s.postQueue.len() > 0 {
			more := s.postProcess(w)
			if s.ppHeld.w == w {
				SystemTimedSched.Put(func() { ppEnqueue(s) }, s.ppHeld.until)
				return
			}
			if more {
				continue
			}
		}
		if !shipped {
			shipped = true
			if s.shipStaged(w, n, lone, many) {
				SystemTimedSched.Put(func() { ppEnqueue(s) }, s.ppHeld.until)
				return
			}
			continue
		}
		if !s.finishPostProcess() {
			ppWorkers.Put(w)
			return
		}
	}
}

// shipStaged encodes staged packets and flushes them. Packets that do not fit
// in the held batch are put on the queue for the worker that resumes s.
func (s *UDPSession) shipStaged(w *ppWorker, n int, lone sendRequest, many []sendRequest) bool {
	if n == 1 {
		return s.flushTx(w, s.txAdd(w, lone))
	}
	bytes := 0
	for i, req := range many {
		bytes += s.txAdd(w, req)
		if len(w.txqueue) < maxBatchSize && i != len(many)-1 {
			continue
		}
		if s.flushTx(w, bytes) {
			for _, rest := range many[i+1:] {
				if !s.postQueue.push(rest) {
					defaultBufferPool.Put(rest.buffer)
				}
			}
			return true
		}
		bytes = 0
	}
	return false
}

func (s *UDPSession) handoffStaged(n int, lone sendRequest, many []sendRequest) {
	if n == 1 {
		if !s.postQueue.push(lone) {
			defaultBufferPool.Put(lone.buffer)
		}
		return
	}
	for _, req := range many {
		if !s.postQueue.push(req) {
			defaultBufferPool.Put(req.buffer)
		}
	}
}

// fillNonce writes a unique GCM nonce. The 12-byte size is the transmit path.
func (s *UDPSession) fillNonce(dst []byte) {
	lo := s.nonceLo.Add(1)
	if len(dst) == 12 {
		binary.BigEndian.PutUint32(dst[:4], s.nonceHi)
		binary.BigEndian.PutUint64(dst[4:], lo)
		return
	}
	var buf [12]byte
	binary.BigEndian.PutUint32(buf[:4], s.nonceHi)
	binary.BigEndian.PutUint64(buf[4:], lo)
	if len(dst) <= len(buf) {
		copy(dst, buf[len(buf)-len(dst):])
		return
	}
	clear(dst)
	copy(dst, buf[:])
}

// txAdd runs FEC and encryption for one packet and appends it to w.
func (s *UDPSession) txAdd(w *ppWorker, req sendRequest) int {
	buf := req.buffer
	oob := req.oob

	var ecc [][]byte

	if s.fecEncoder != nil {
		if !oob {
			now := req.ms
			if now == 0 {
				now = int64(currentMs())
			}
			ecc = s.fecEncoder.encodeAt(buf, maxFECEncodeLatency, now)
		} else {
			s.fecEncoder.encodeOOB(buf)
		}
	}

	switch block := s.block.(type) {
	case nil:
	case *aeadCrypt:
		nonceSize := block.NonceSize()

		dst := buf[:nonceSize]
		nonce := buf[:nonceSize]
		plaintext := buf[nonceSize:]

		s.fillNonce(nonce)
		buf = block.Seal(dst, nonce, plaintext, nil)

		for k := range ecc {
			dst := ecc[k][:nonceSize]
			nonce := ecc[k][:nonceSize]
			plaintext := ecc[k][nonceSize:]

			s.fillNonce(nonce)
			ecc[k] = block.Seal(dst, nonce, plaintext, nil)
		}
	default:
		fillRand(buf[:nonceSize])
		checksum := crc32.ChecksumIEEE(buf[cryptHeaderSize:])
		binary.LittleEndian.PutUint32(buf[nonceSize:], checksum)
		block.Encrypt(buf, buf)

		for k := range ecc {
			fillRand(ecc[k][:nonceSize])
			checksum := crc32.ChecksumIEEE(ecc[k][cryptHeaderSize:])
			binary.LittleEndian.PutUint32(ecc[k][nonceSize:], checksum)
			block.Encrypt(ecc[k], ecc[k])
		}
	}

	bytes := 0
	var msg ipv4.Message
	msg.Addr = s.remote
	msg.Buffers = w.oneBuf(buf)
	bytes += len(buf)
	w.txqueue = append(w.txqueue, msg)

	for i := 0; i < s.dup; i++ {
		bts := defaultBufferPool.Get()[:len(buf)]
		copy(bts, buf)
		msg.Buffers = w.oneBuf(bts)
		bytes += len(bts)
		w.txqueue = append(w.txqueue, msg)
	}

	for k := range ecc {
		bts := defaultBufferPool.Get()[:len(ecc[k])]
		copy(bts, ecc[k])
		msg.Buffers = w.oneBuf(bts)
		bytes += len(bts)
		w.txqueue = append(w.txqueue, msg)
	}
	return bytes
}

// dispatchPost transmits queued packets on the calling goroutine. Write uses
// it so a just-flushed data packet is not waiting on a worker wakeup. A
// worker that already owns s drains anything queued here.
func (s *UDPSession) dispatchPost() {
	if s.postQueue.len() == 0 {
		return
	}
	if !s.claimPostProcess() {
		return
	}
	w := ppWorkers.Get().(*ppWorker)
	for {
		if h := s.ppHeld.w; h != nil {
			s.ppHeld.w = nil
			s.sendTx(h, s.ppHeld.bytes)
			ppWorkers.Put(h)
		}
		more := s.postProcess(w)
		if s.ppHeld.w == w {
			// s keeps the batch, and stays queued, until the limiter allows it.
			SystemTimedSched.Put(func() { ppEnqueue(s) }, s.ppHeld.until)
			return
		}
		if !more && !s.finishPostProcess() {
			ppWorkers.Put(w)
			return
		}
	}
}

func (s *UDPSession) claimPostProcess() bool {
	return s.ppRunning.CompareAndSwap(false, true)
}

func (s *UDPSession) finishPostProcess() bool {
	s.ppRunning.Store(false)
	return s.postQueue.len() > 0 && s.claimPostProcess()
}

func ppEnqueue(s *UDPSession) {
	ppQueue.once.Do(startPostProcessWorkers)
	ppQueue.mu.Lock()
	if ppQueue.head > len(ppQueue.q)/2 {
		n := copy(ppQueue.q, ppQueue.q[ppQueue.head:])
		clear(ppQueue.q[n:])
		ppQueue.q, ppQueue.head = ppQueue.q[:n], 0
	}
	ppQueue.q = append(ppQueue.q, s)
	ppQueue.mu.Unlock()
	ppQueue.cond.Signal()
}

func startPostProcessWorkers() {
	ppQueue.cond = sync.NewCond(&ppQueue.mu)
	n := runtime.NumCPU()
	if n < 2 {
		n = 2
	}
	if n > 8 {
		n = 8
	}
	for i := 0; i < n; i++ {
		go postProcessWorker()
	}
}

func postProcessWorker() {
	w := ppWorkers.Get().(*ppWorker)
	for {
		ppQueue.mu.Lock()
		for ppQueue.head == len(ppQueue.q) {
			ppQueue.cond.Wait()
		}
		s := ppQueue.q[ppQueue.head]
		ppQueue.q[ppQueue.head] = nil
		ppQueue.head++
		if ppQueue.head == len(ppQueue.q) {
			ppQueue.q, ppQueue.head = ppQueue.q[:0], 0
		}
		ppQueue.mu.Unlock()

		// The limiter allowed a held batch when it was held, so send it now.
		if h := s.ppHeld.w; h != nil {
			s.ppHeld.w = nil
			s.sendTx(h, s.ppHeld.bytes)
			ppWorkers.Put(h)
		}

		more := s.postProcess(w)
		if s.ppHeld.w == w {
			// s keeps the batch, and stays queued, until the limiter allows it.
			w = ppWorkers.Get().(*ppWorker)
			SystemTimedSched.Put(func() { ppEnqueue(s) }, s.ppHeld.until)
			continue
		}
		if more {
			ppEnqueue(s)
			continue
		}
		// Hand back to kickPostProcess, unless a packet arrived meanwhile.
		if s.finishPostProcess() {
			ppEnqueue(s)
		}
	}
}
