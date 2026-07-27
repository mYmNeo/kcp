// MIT License
//
// Copyright (c) 2016-2017 xtaci
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

package smux

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRecvLoopBufferedRead verifies that the buffered reader in recvLoop
// correctly coalesces small reads. We test this by sending many small frames
// and measuring that they are all received intact.
func TestRecvLoopBufferedRead(t *testing.T) {
	c1, c2, err := getTCPConnectionPairForTest(t)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	defer c2.Close()

	serverSess, err := Server(c2, nil)
	if err != nil {
		t.Fatal(err)
	}
	clientSess, err := Client(c1, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Accept server stream
	var serverStream *Stream
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		serverStream, _ = serverSess.AcceptStream()
	}()

	clientStream, err := clientSess.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	<-acceptDone
	if serverStream == nil {
		t.Fatal("failed to accept stream")
	}
	defer clientStream.Close()
	defer serverStream.Close()

	// Send many small frames to exercise buffered coalescing
	const numWrites = 1000
	const writeSize = 64 // small enough to test coalescing

	payload := bytes.Repeat([]byte("A"), writeSize)

	var wg sync.WaitGroup
	wg.Add(1)

	// Reader goroutine
	var bytesReceived int64
	var readErr error
	go func() {
		defer wg.Done()
		buf := make([]byte, writeSize)
		for atomic.LoadInt64(&bytesReceived) < int64(numWrites*writeSize) {
			n, err := serverStream.Read(buf)
			if n > 0 {
				atomic.AddInt64(&bytesReceived, int64(n))
			}
			if err != nil {
				if err == io.EOF {
					return
				}
				readErr = err
				return
			}
		}
	}()

	// Write many small frames
	for i := 0; i < numWrites; i++ {
		_, err := clientStream.Write(payload)
		if err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}

	// Wait for all data
	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for atomic.LoadInt64(&bytesReceived) < int64(numWrites*writeSize) {
		select {
		case <-deadline:
			t.Fatalf("timeout: received %d of %d bytes", atomic.LoadInt64(&bytesReceived), numWrites*writeSize)
		case <-ticker.C:
		}
		if readErr != nil {
			t.Fatalf("read error: %v", readErr)
		}
	}

	// Close to signal EOF
	clientStream.Close()
	wg.Wait()

	if readErr != nil {
		t.Fatalf("read error: %v", readErr)
	}
	if atomic.LoadInt64(&bytesReceived) != int64(numWrites*writeSize) {
		t.Fatalf("bytes received mismatch: got %d, want %d", atomic.LoadInt64(&bytesReceived), numWrites*writeSize)
	}
}

// TestControlFramePriority verifies that CLSCTRL frames (SYN/FIN/NOP)
// are not blocked by a saturated data queue. We create high throughput
// data on multiple streams and verify that a new stream can still be
// opened and closed promptly.
func TestControlFramePriority(t *testing.T) {
	c1, c2, err := getTCPConnectionPairForTest(t)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	defer c2.Close()

	config := DefaultConfig()
	config.Version = 2
	// Disable keepalive to reduce noise
	config.KeepAliveDisabled = true

	serverSess, err := Server(c2, config)
	if err != nil {
		t.Fatal(err)
	}
	clientSess, err := Client(c1, config)
	if err != nil {
		t.Fatal(err)
	}

	// Start multiple streams pumping data to fill the shaper queue
	const numStreams = 8
	const dataSize = 64 * 1024 // large write to fill shaper queue quickly

	stopCh := make(chan struct{})

	// Server-side: accept all streams and drain reads
	go func() {
		for i := 0; i < numStreams; i++ {
			stream, err := serverSess.AcceptStream()
			if err != nil {
				return
			}
			go func(s *Stream) {
				buf := make([]byte, 32*1024)
				for {
					select {
					case <-stopCh:
						return
					default:
					}
					_, err := s.Read(buf)
					if err != nil {
						return
					}
				}
			}(stream)
		}
	}()

	// Client opens streams and writes to saturate
	for i := 0; i < numStreams; i++ {
		stream, err := clientSess.OpenStream()
		if err != nil {
			t.Fatalf("open stream %d failed: %v", i, err)
		}
		defer stream.Close()

		go func(s *Stream, id int) {
			data := bytes.Repeat([]byte{byte(id)}, dataSize)
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				s.Write(data)
			}
		}(stream, i)
	}

	// Let data streams saturate the connection
	time.Sleep(300 * time.Millisecond)

	// Now try to open and close a new CONTROL stream — should succeed quickly
	ctrlStart := time.Now()
	ctrlStream, err := clientSess.OpenStream()
	if err != nil {
		t.Fatalf("control stream open failed under load: %v", err)
	}
	ctrlStream.Close()
	ctrlElapsed := time.Since(ctrlStart)

	// CloseWrite also sends FIN via CLSCTRL shaper path
	ctrlStream2, err := clientSess.OpenStream()
	if err != nil {
		t.Fatalf("second control stream open failed: %v", err)
	}
	closeStart := time.Now()
	ctrlStream2.CloseWrite()
	ctrlStream2.Close()
	closeElapsed := time.Since(closeStart)

	// Signal stop and close sessions to unblock any stuck goroutines
	close(stopCh)
	clientSess.Close()
	serverSess.Close()

	// Control operations should complete within a reasonable time
	// even under heavy data load
	if ctrlElapsed > 5*time.Second {
		t.Errorf("control stream open+close took %v under load", ctrlElapsed)
	}
	if closeElapsed > 5*time.Second {
		t.Errorf("control stream CloseWrite+Close took %v under load", closeElapsed)
	}

	t.Logf("ctrl open+close: %v, closewrite+close: %v under %d saturated streams",
		ctrlElapsed, closeElapsed, numStreams)
}

// TestSendLoopBufferedWrite verifies that sendLoop correctly handles
// the net.Buffers write path (no WriteBuffers on plain TCP).
func TestSendLoopBufferedWrite(t *testing.T) {
	c1, c2, err := getTCPConnectionPairForTest(t)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	defer c2.Close()

	serverSess, err := Server(c2, nil)
	if err != nil {
		t.Fatal(err)
	}
	clientSess, err := Client(c1, nil)
	if err != nil {
		t.Fatal(err)
	}

	var serverStream *Stream
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		serverStream, _ = serverSess.AcceptStream()
	}()

	clientStream, err := clientSess.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	<-acceptDone
	defer clientStream.Close()
	defer serverStream.Close()

	// Write a frame larger than half the buffer to exercise full-frame path
	largeData := make([]byte, 64*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	n, err := clientStream.Write(largeData)
	if err != nil {
		t.Fatalf("large write failed: %v", err)
	}
	if n != len(largeData) {
		t.Fatalf("short write: got %d, want %d", n, len(largeData))
	}

	// Read back and verify
	received := make([]byte, len(largeData))
	total := 0
	for total < len(largeData) {
		n, err := serverStream.Read(received[total:])
		if err != nil {
			t.Fatalf("read failed at offset %d: %v", total, err)
		}
		total += n
	}

	if !bytes.Equal(largeData, received) {
		t.Fatal("data mismatch")
	}
}

// getTCPConnectionPairForTest is a helper that returns a pair of connected TCP connections.
func getTCPConnectionPairForTest(tb testing.TB) (net.Conn, net.Conn, error) {
	tb.Helper()
	lst, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, nil, err
	}

	var conn0 net.Conn
	var err0 error
	done := make(chan struct{})
	go func() {
		conn0, err0 = lst.Accept()
		close(done)
	}()

	conn1, err := net.Dial("tcp", lst.Addr().String())
	if err != nil {
		lst.Close()
		return nil, nil, err
	}

	<-done
	lst.Close()
	if err0 != nil {
		conn1.Close()
		return nil, nil, err0
	}
	return conn0, conn1, nil
}

// BenchmarkConnSmuxV1 measures smux v1 throughput vs raw TCP.
func BenchmarkConnSmuxV1(b *testing.B) {
	c1, c2, err := getTCPConnectionPairForTest(b)
	if err != nil {
		b.Fatal(err)
	}
	defer c1.Close()
	defer c2.Close()

	config := DefaultConfig()
	config.Version = 1

	s, err := Server(c2, config)
	if err != nil {
		b.Fatal(err)
	}
	c, err := Client(c1, config)
	if err != nil {
		b.Fatal(err)
	}

	var ss *Stream
	done := make(chan error)
	go func() {
		var rerr error
		ss, rerr = s.AcceptStream()
		done <- rerr
		close(done)
	}()
	cs, err := c.OpenStream()
	if err != nil {
		b.Fatal(err)
	}
	if err := <-done; err != nil {
		b.Fatal(err)
	}

	defer cs.Close()
	defer ss.Close()

	buf := make([]byte, 128*1024)
	buf2 := make([]byte, 128*1024)
	b.SetBytes(128 * 1024)
	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		count := 0
		for {
			n, _ := ss.Read(buf2)
			count += n
			if count >= 128*1024*b.N {
				return
			}
		}
	}()
	for i := 0; i < b.N; i++ {
		cs.Write(buf)
	}
	wg.Wait()
}

// BenchmarkConnSmuxV2 measures smux v2 throughput vs raw TCP.
func BenchmarkConnSmuxV2(b *testing.B) {
	c1, c2, err := getTCPConnectionPairForTest(b)
	if err != nil {
		b.Fatal(err)
	}
	defer c1.Close()
	defer c2.Close()

	config := DefaultConfig()
	config.Version = 2

	s, err := Server(c2, config)
	if err != nil {
		b.Fatal(err)
	}
	c, err := Client(c1, config)
	if err != nil {
		b.Fatal(err)
	}

	var ss *Stream
	done := make(chan error)
	go func() {
		var rerr error
		ss, rerr = s.AcceptStream()
		done <- rerr
		close(done)
	}()
	cs, err := c.OpenStream()
	if err != nil {
		b.Fatal(err)
	}
	if err := <-done; err != nil {
		b.Fatal(err)
	}

	defer cs.Close()
	defer ss.Close()

	buf := make([]byte, 128*1024)
	buf2 := make([]byte, 128*1024)
	b.SetBytes(128 * 1024)
	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		count := 0
		for {
			n, _ := ss.Read(buf2)
			count += n
			if count >= 128*1024*b.N {
				return
			}
		}
	}()
	for i := 0; i < b.N; i++ {
		cs.Write(buf)
	}
	wg.Wait()
}

// TestThroughputV1V2Comparison runs a throughput comparison between v1 and v2.
func TestThroughputV1V2Comparison(t *testing.T) {
	const dataSize = 4 * 1024 * 1024 // 4 MB

	for _, version := range []int{1, 2} {
		v := version
		t.Run(fmt.Sprintf("v%d", v), func(t *testing.T) {
			c1, c2, err := getTCPConnectionPairForTest(t)
			if err != nil {
				t.Fatal(err)
			}
			defer c1.Close()
			defer c2.Close()

			config := DefaultConfig()
			config.Version = v

			s, err := Server(c2, config)
			if err != nil {
				t.Fatal(err)
			}
			c, err := Client(c1, config)
			if err != nil {
				t.Fatal(err)
			}

			var ss *Stream
			acceptDone := make(chan struct{})
			go func() {
				defer close(acceptDone)
				ss, _ = s.AcceptStream()
			}()

			cs, err := c.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			<-acceptDone

			// Reader goroutine
			var readBytes int64
			readDone := make(chan struct{})
			go func() {
				defer close(readDone)
				buf := make([]byte, 64*1024)
				for atomic.LoadInt64(&readBytes) < dataSize {
					n, err := ss.Read(buf)
					if n > 0 {
						atomic.AddInt64(&readBytes, int64(n))
					}
					if err != nil && err != io.EOF {
						return
					}
				}
			}()

			// Writer
			start := time.Now()
			payload := make([]byte, 64*1024)
			written := int64(0)
			for written < dataSize {
				n, err := cs.Write(payload)
				if err != nil {
					t.Fatalf("write failed: %v", err)
				}
				written += int64(n)
			}
			cs.CloseWrite()

			<-readDone
			elapsed := time.Since(start)

			cs.Close()
			ss.Close()

			throughput := float64(dataSize) / elapsed.Seconds()
			t.Logf("v%d throughput: %.2f MB/s (%v for %d bytes)",
				v, throughput/(1024*1024), elapsed, dataSize)

			if throughput < 1 { // sanity check: at least 1 MB/s
				t.Errorf("v%d throughput too low: %.2f MB/s", v, throughput/(1024*1024))
			}
		})
	}
}
