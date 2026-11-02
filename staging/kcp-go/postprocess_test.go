package kcp

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestPostProcessWorkerHandoff(t *testing.T) {
	for i := 0; i < 1000; i++ {
		s := &UDPSession{}
		s.postQueue.push(sendRequest{})
		s.ppRunning.Store(true)

		start := make(chan struct{})
		var wg sync.WaitGroup
		var claims atomic.Int32
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			if s.claimPostProcess() {
				claims.Add(1)
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			if s.finishPostProcess() {
				claims.Add(1)
			}
		}()
		close(start)
		wg.Wait()

		if got := claims.Load(); got != 1 {
			t.Fatalf("iteration %d: got %d scheduling claims, want exactly one", i, got)
		}
		if !s.ppRunning.Load() {
			t.Fatalf("iteration %d: pending packet has no worker scheduled", i)
		}
	}
}

// Rate limited sessions waiting for tokens must not hold the shared
// postProcess workers from a session without a limit.
func TestPostProcessRateLimitedMixed(t *testing.T) {
	port := nextPort()
	l := sinkServer(port)
	defer l.Close()

	// More limited sessions than workers, each with more queued than its
	// limit lets out.
	stop := make(chan struct{})
	defer close(stop)
	for i := 0; i < 16; i++ {
		s, err := dialSink(port)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		s.SetRateLimit(32 * 1024)
		go func() {
			buf := make([]byte, 64*1024)
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := s.Write(buf); err != nil {
					return
				}
			}
		}()
	}
	// Let the limiters spend their bursts.
	time.Sleep(time.Second)

	eport := nextPort()
	el := echoServer(eport, nil)
	defer el.Close()
	cli, err := dialEcho(eport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	cli.SetRateLimit(0)
	cli.SetLogger(0, nil)

	start := time.Now()
	if err := echo_tester(cli, 1024, 256); err != nil {
		t.Fatal(err)
	}
	took := time.Since(start)
	t.Logf("unlimited session echoed 256 KiB in %v", took)
	if took > 2*time.Second {
		t.Fatalf("unlimited session took %v behind rate limited ones", took)
	}
}

// TestRateLimitedWriteQueuesStagedTail covers the branch in shipStaged that
// pushes packets past a held batch onto the send queue. A non-zero limit is
// what makes flushTx keep the batch instead of writing it.
func TestRateLimitedWriteQueuesStagedTail(t *testing.T) {
	port := nextPort()
	l := echoServer(port, nil)
	defer l.Close()

	cli, err := DialWithOptions(fmt.Sprintf("127.0.0.1:%d", port), nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	cli.SetStreamMode(true)
	cli.SetWriteDelay(false)
	cli.SetNoDelay(1, 20, 2, 1)
	cli.SetWindowSize(4096, 4096)
	if !cli.SetMtu(1400) {
		t.Fatal("SetMtu rejected")
	}
	cli.SetACKNoDelay(false)
	// The bucket starts full (maxBatchSize*mtuLimit). A full transmit batch
	// fits in that burst, so flushTx would not hold and the tail would never
	// be queued. Spend the burst up front; the next batch then has to wait.
	cli.SetRateLimit(256 * 1024)
	lim := cli.rateLimiter.Load().(*rate.Limiter)
	if r := lim.ReserveN(time.Now(), lim.Burst()); !r.OK() {
		t.Fatal("could not spend limiter burst")
	}
	deadline := time.Now().Add(20 * time.Second)
	cli.SetDeadline(deadline)

	cli.mu.Lock()
	// The peer has not advertised a window yet, so rmt_wnd is still the
	// default 32. Pin it so this write can emit a full batch in one flush.
	cli.kcp.rmt_wnd = 4096
	mss := int(cli.kcp.mss)
	cli.mu.Unlock()
	if mss <= 0 {
		t.Fatalf("mss = %d", mss)
	}
	seg := bytes.Repeat([]byte{0x5a}, mss)

	const extra = 2
	payload := bytes.Repeat(seg, maxBatchSize+extra)
	if _, err := cli.Write(payload); err != nil {
		t.Fatal(err)
	}
	queued := cli.postQueue.len()
	if queued < extra {
		t.Fatalf("staged tail queued = %d, want at least %d", queued, extra)
	}

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(cli, got); err != nil {
		t.Fatalf("echo: %v (queued tail %d)", err, queued)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("echoed payload mismatch")
	}
}
