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

import "testing"

func TestWinMax(t *testing.T) {
	var m winMax
	m.reset(0, 100)
	m.update(10, 1, 50)
	m.update(10, 5, 60)
	if got := m.get(); got != 100 {
		t.Fatalf("inside window: max = %d, want 100", got)
	}
	m.update(10, 11, 40)
	if got := m.get(); got != 60 {
		t.Fatalf("after the 100 sample expires: max = %d, want 60", got)
	}
	m.update(10, 12, 500)
	if got := m.get(); got != 500 {
		t.Fatalf("a new maximum must win at once: max = %d, want 500", got)
	}
}

func TestRTOBeforeAndAfterFirstSample(t *testing.T) {
	kcp := NewKCP(1, func([]byte, int) {})
	kcp.NoDelay(1, 10, 2, 1)

	kcp.Send(make([]byte, 100))
	kcp.flush(IKCP_FLUSH_FULL)
	if got := kcp.snd_buf.At(0).rto; got < IKCP_RTO_INIT {
		t.Fatalf("first flight RTO = %d ms; a 200-300 ms path would retransmit it before any ACK, want >= %d", got, IKCP_RTO_INIT)
	}

	// A smooth 300 ms path drives the variance term to a few ms. The RTO
	// must still leave room for the queueing a 1.25x pacing probe adds.
	for range 200 {
		kcp.update_ack(300)
	}
	if want := uint32(300 + 300/4); kcp.rx_rto < want {
		t.Fatalf("steady RTO = %d ms, want >= %d (srtt + srtt/4)", kcp.rx_rto, want)
	}
	kcp.Send(make([]byte, 100))
	kcp.flush(IKCP_FLUSH_FULL)
	if got := kcp.snd_buf.At(1).rto; got >= IKCP_RTO_INIT {
		t.Fatalf("RTO after RTT samples = %d ms, want the measured RTO %d", got, kcp.rx_rto)
	}
}

// TestPacingSpreadsBurst checks that a window-sized burst leaves at the
// pacing rate instead of all at once.
func TestPacingSpreadsBurst(t *testing.T) {
	kcp := NewKCP(1, func([]byte, int) {})
	kcp.NoDelay(1, 10, 2, 1)
	kcp.WndSize(1024, 1024)
	kcp.rmt_wnd = 1024
	kcp.min_rtt = 100
	kcp.btlbw.reset(0, 1_000_000) // bytes/s
	kcp.full_pipe = true
	kcp.cycle_idx = 2 // gain 1

	const segs = 200
	for range segs {
		kcp.Send(make([]byte, kcp.mss))
	}
	sent := func() (n int) {
		for i := range kcp.snd_buf.Len() {
			if kcp.snd_buf.At(i).xmit > 0 {
				n++
			}
		}
		return n
	}

	// The bucket holds paceBurstMs of the rate: 10 KB, about 8 segments.
	bucket := int(1_000_000*paceBurstMs/1000)/int(kcp.mtu+IKCP_OVERHEAD) + 1
	kcp.flush(IKCP_FLUSH_FULL)
	first := sent()
	if first == 0 || first > bucket {
		t.Fatalf("first flush sent %d of %d segments, want at most one %d-segment bucket", first, segs, bucket)
	}
	if kcp.pace_wait == 0 {
		t.Fatal("segments are held but pace_wait is 0, so nothing would release them early")
	}

	// 100 ms later the bucket has refilled only to its depth, not 100 ms worth.
	kcp.pace_ts -= 100
	kcp.flush(IKCP_FLUSH_FULL)
	if second := sent(); second <= first || second-first > bucket {
		t.Fatalf("after a 100 ms gap %d more segments left, want at most one %d-segment bucket", second-first, bucket)
	}
}

// startupRound feeds one round trip of deliveries into the startup state
// machine: bytes delivered over rtt ms, closed by a sample with the given
// flags.
func startupRound(kcp *KCP, now *uint32, rtt uint32, bytes uint64, held, applim bool, sent, rexmit uint32) {
	kcp.round_held = held
	kcp.round_sent, kcp.round_rexmit = sent, rexmit
	seg := segment{xmit: 1, deliv: kcp.delivered, deliv_ts: *now - rtt, first_ts: *now - rtt, ts: *now - rtt, applim: applim}
	kcp.delivered += bytes - IKCP_OVERHEAD
	kcp.on_delivered(&seg, *now)
	kcp.take_rate_sample(*now)
	*now += rtt
}

func newStartupKCP() (*KCP, uint32) {
	kcp := NewKCP(1, func([]byte, int) {})
	kcp.min_rtt = 100
	return kcp, 100000
}

func TestStartupExit(t *testing.T) {
	const rtt = 100

	t.Run("bandwidth plateau", func(t *testing.T) {
		kcp, now := newStartupKCP()
		for _, b := range []uint64{10_000, 20_000, 40_000, 80_000} {
			startupRound(kcp, &now, rtt, b, true, false, 50, 0)
		}
		for i := range 3 {
			if kcp.full_pipe {
				t.Fatalf("startup ended after %d flat rounds, want %d", i, paceFullBwRnds)
			}
			startupRound(kcp, &now, rtt, 80_000, true, false, 50, 0)
		}
		if !kcp.full_pipe {
			t.Fatalf("startup still running after %d flat rounds", paceFullBwRnds)
		}
	})

	t.Run("flat app-limited or unheld rounds keep startup", func(t *testing.T) {
		kcp, now := newStartupKCP()
		startupRound(kcp, &now, rtt, 20_000, true, false, 50, 0)
		for range 6 {
			startupRound(kcp, &now, rtt, 20_000, true, true, 50, 0)
			startupRound(kcp, &now, rtt, 20_000, false, false, 50, 0)
		}
		if kcp.full_pipe {
			t.Fatal("startup ended on rounds the application limited; a later bulk transfer would be stuck at this rate")
		}
	})

	t.Run("lossy held round ends startup at the last clean estimate", func(t *testing.T) {
		kcp, now := newStartupKCP()
		for _, b := range []uint64{10_000, 20_000, 40_000} {
			startupRound(kcp, &now, rtt, b, true, false, 50, 0)
		}
		clean := kcp.btlbw.get()
		startupRound(kcp, &now, rtt, 48_000, true, true, 300, 30)
		if !kcp.full_pipe {
			t.Fatal("a held round with 10% retransmitted did not end startup")
		}
		if got := kcp.btlbw.get(); got != clean {
			t.Fatalf("bandwidth after loss exit = %d, want the last clean estimate %d", got, clean)
		}
	})

	t.Run("loss in unheld round keeps startup", func(t *testing.T) {
		kcp, now := newStartupKCP()
		for _, b := range []uint64{10_000, 20_000, 40_000} {
			startupRound(kcp, &now, rtt, b, true, false, 50, 0)
		}
		startupRound(kcp, &now, rtt, 40_000, false, false, 300, 30)
		if kcp.full_pipe {
			t.Fatal("random loss while the sender was not pushing ended startup")
		}
	})

	t.Run("retransmitted segment gives no rate sample", func(t *testing.T) {
		kcp, now := newStartupKCP()
		seg := segment{xmit: 2, deliv: 0, deliv_ts: now - 1, first_ts: now - 1, ts: now - 1}
		kcp.delivered += 1_000_000
		kcp.on_delivered(&seg, now)
		kcp.take_rate_sample(now)
		if got := kcp.btlbw.get(); got != 0 {
			t.Fatalf("retransmission produced bandwidth sample %d; its ACK may answer the first transmission", got)
		}
	})
}

// TestResetRTOShorterPath checks that after ResetRTO, as when connectivity
// returns over a shorter path, rate samples from the new path are accepted.
func TestResetRTOShorterPath(t *testing.T) {
	kcp, now := newStartupKCP()
	kcp.min_rtt, kcp.min_rtt_ts = 300, now
	kcp.btlbw.reset(0, 50_000)
	kcp.full_pipe = true
	kcp.pace_tokens = 1 << 20

	kcp.ResetRTO()
	if kcp.full_pipe || kcp.btlbw.get() != 0 {
		t.Fatal("pacing kept the old path's bandwidth model after ResetRTO")
	}
	if kcp.pace_tokens <= 0 || kcp.pace_tokens > 2*int64(kcp.mtu) {
		t.Fatalf("ResetRTO left %d bytes of pacing budget, want (0, %d]: none stalls the resend, more lets it burst", kcp.pace_tokens, 2*kcp.mtu)
	}

	// The new path's first rate sample, over a 150 ms interval, closes
	// before any RTT sample from it has been taken.
	startupRound(kcp, &now, 150, 150_000, true, false, 50, 0)
	if got := kcp.btlbw.get(); got == 0 {
		t.Fatal("a 150 ms sample on the new path was rejected against the old path's 300 ms min RTT")
	}
}
