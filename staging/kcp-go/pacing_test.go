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
	// must still leave room for the queueing a pacing probe adds.
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

	// The bucket holds paceBurstMs of the rate (a few segments at 1 MB/s). A
	// burst is the whole bucket, give or take the segment that overdraws it.
	bucket := int(1_000_000*paceBurstMs/1000)/int(kcp.mtu+IKCP_OVERHEAD) + 1
	inBucket := func(n int) bool { return n >= bucket-1 && n <= bucket }

	// pace_ts is set relative to the clock flush reads, so the refill is the
	// same however long the process has run.
	kcp.pace_tokens, kcp.pace_ts = 0, currentMs()-100
	kcp.flush(IKCP_FLUSH_FULL)
	first := sent()
	if !inBucket(first) {
		t.Fatalf("first flush sent %d of %d segments, want one %d-segment bucket", first, segs, bucket)
	}
	if kcp.pace_wait == 0 {
		t.Fatal("segments are held but pace_wait is 0, so nothing would release them early")
	}

	// Another 100 ms refills to the bucket depth, not 100 ms worth (~74).
	kcp.pace_tokens, kcp.pace_ts = 0, currentMs()-100
	kcp.flush(IKCP_FLUSH_FULL)
	if n := sent() - first; !inBucket(n) {
		t.Fatalf("after a 100 ms gap %d more segments left, want one %d-segment bucket", n, bucket)
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

	t.Run("FEC-repaired segment is not counted as delivered", func(t *testing.T) {
		kcp, now := newStartupKCP()
		kcp.inflight = 2 * (1000 + IKCP_OVERHEAD)
		repaired := segment{xmit: 1, fastack: 1, data: make([]byte, 1000)}
		kcp.on_delivered(&repaired, now)
		if kcp.delivered != 0 {
			t.Fatalf("delivered = %d after an FEC-repaired segment; parity-absorbed overflow would inflate the bandwidth estimate", kcp.delivered)
		}
		if kcp.inflight != 1000+IKCP_OVERHEAD {
			t.Fatalf("inflight = %d, want %d: the repaired segment left the network", kcp.inflight, 1000+IKCP_OVERHEAD)
		}
		clean := segment{xmit: 1, data: make([]byte, 1000)}
		kcp.on_delivered(&clean, now)
		if kcp.delivered != 1000+IKCP_OVERHEAD {
			t.Fatalf("delivered = %d after an in-order segment, want %d", kcp.delivered, 1000+IKCP_OVERHEAD)
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

// TestSendBacklogCap checks that Write admission stops once the unsent data
// covers paceUnsentLong at the bandwidth estimate on a long path after
// startup, and reopens as the pacer sends it, so data written later (an
// interactive frame) waits behind a bounded queue. Startup and short paths
// keep the larger paceUnsentMs.
func TestSendBacklogCap(t *testing.T) {
	kcp := NewKCP(1, func([]byte, int) {})
	kcp.NoDelay(1, 10, 2, 1)
	kcp.WndSize(4096, 4096)
	kcp.rmt_wnd = 4096
	kcp.min_rtt = 100
	kcp.btlbw.reset(0, 1_000_000)
	kcp.full_pipe = true
	kcp.cycle_idx = 1 // drain phase, gain below 1: the cap must not shrink with it

	limit := uint64(1_000_000) * paceUnsentLong / 1000
	queued := uint64(0)
	for !kcp.send_backlog_full() {
		kcp.Send(make([]byte, kcp.mss))
		queued += uint64(kcp.mss)
		if queued > 4*limit {
			t.Fatalf("admission still open with %d unsent bytes, cap %d", queued, limit)
		}
	}
	if kcp.unsent != queued || queued < limit || queued > limit+uint64(kcp.mss) {
		t.Fatalf("admission closed at %d unsent bytes (counter %d), want the first segment past %d", queued, kcp.unsent, limit)
	}
	if kcp.WaitSnd() >= int(kcp.snd_wnd) {
		t.Fatal("cap test hit the send window instead of the unsent cap")
	}

	// Stream mode appends small writes into the last queued segment instead
	// of pushing a new one; those bytes are unsent too.
	kcp.stream = 1
	last := kcp.snd_queue.At(kcp.snd_queue.Len() - 1)
	last.data = last.data[:len(last.data)-100]
	kcp.unsent -= 100
	before := kcp.unsent
	kcp.Send(make([]byte, 40))
	if got := kcp.unsent - before; got != 40 {
		t.Fatalf("stream-mode append counted %d unsent bytes, want 40", got)
	}

	// One pacer burst goes out; the unsent count drops and admission reopens.
	kcp.pace_tokens, kcp.pace_ts = 0, currentMs()-paceBurstMs
	kcp.flush(IKCP_FLUSH_FULL)
	if kcp.unsent >= queued {
		t.Fatalf("unsent = %d after a flush, want below %d", kcp.unsent, queued)
	}
	if kcp.send_backlog_full() {
		t.Fatalf("admission still closed with %d unsent bytes, cap %d", kcp.unsent, limit)
	}

	// Startup and a path where ACK delay dominates the RTT (min RTT 15 ms
	// against a 10 ms interval) still admit past the long-path cap.
	for _, tc := range []struct {
		name     string
		minRtt   uint32
		fullPipe bool
	}{{"startup", 100, false}, {"short path", 15, true}} {
		k := NewKCP(1, func([]byte, int) {})
		k.NoDelay(1, 10, 2, 1)
		k.WndSize(4096, 4096)
		k.min_rtt = tc.minRtt
		k.btlbw.reset(0, 1_000_000)
		k.full_pipe = tc.fullPipe
		for k.unsent < limit+2*uint64(k.mss) {
			k.Send(make([]byte, k.mss))
		}
		if k.send_backlog_full() {
			t.Fatalf("%s: admission closed at %d unsent bytes, the long-path cap; want paceUnsentMs", tc.name, k.unsent)
		}
	}
}

// TestStandingQueueCorrection checks that after startup, a pushing round in
// which no RTT sample came back near min RTT lowers the bandwidth estimate,
// while rounds that drained to min RTT, or whose excess fits within one flush
// interval of ACK delay, leave it alone.
func TestStandingQueueCorrection(t *testing.T) {
	setup := func() (*KCP, uint32) {
		kcp, now := newStartupKCP()
		kcp.interval = 30
		kcp.min_rtt, kcp.min_rtt_ts = 200, now
		kcp.btlbw.reset(0, 2_000_000)
		kcp.full_pipe = true
		return kcp, now
	}
	// The sample's own rate is far below the estimate, so it cannot move the
	// max filter; only the correction can.
	round := func(kcp *KCP, now *uint32, roundMin uint32) {
		kcp.round_min_rtt = roundMin
		startupRound(kcp, now, 300, 30_000, true, false, 50, 0)
	}

	kcp, now := setup()
	cut := func(bw uint64) uint64 { return bw * (100 - paceQueueCut) / 100 }
	round(kcp, &now, 250)
	if got, want := kcp.btlbw.get(), cut(2_000_000); got != want {
		t.Fatalf("after a round with a 50 ms standing queue, btlbw = %d, want %d (one paceQueueCut step)", got, want)
	}
	round(kcp, &now, 400)
	if got, want := kcp.btlbw.get(), cut(cut(2_000_000)); got != want {
		t.Fatalf("after a second standing-queue round, btlbw = %d, want %d (one paceQueueCut step per round)", got, want)
	}

	kcp, now = setup()
	round(kcp, &now, 201)
	round(kcp, &now, 229) // within min RTT + interval: ACK delay, not queueing
	if got := kcp.btlbw.get(); got != 2_000_000 {
		t.Fatalf("rounds that drained (or only saw ACK delay) changed btlbw to %d", got)
	}

	kcp, now = setup()
	kcp.full_pipe = false
	round(kcp, &now, 300)
	if got := kcp.btlbw.get(); got < 2_000_000 {
		t.Fatalf("startup's own queue lowered btlbw to %d; startup must keep growing", got)
	}

	// A LAN path: min RTT 4 ms against a 30 ms flush interval. RTT samples
	// there mostly measure ACK delay, so even a 60 ms round minimum is not
	// read as a standing queue.
	kcp, now = setup()
	kcp.min_rtt = 4
	round(kcp, &now, 60)
	if got := kcp.btlbw.get(); got != 2_000_000 {
		t.Fatalf("on a path with RTT below the flush interval btlbw changed to %d", got)
	}
}

// TestProbeEndsOnQueue checks that the probe phase ends at the first RTT
// sample showing a queue, that the drain then lasts as long as the probe ran,
// and that ACK delay alone (or a path where it dominates) ends nothing.
func TestProbeEndsOnQueue(t *testing.T) {
	setup := func(minRtt uint32) (*KCP, uint32) {
		kcp, now := newStartupKCP()
		kcp.interval = 30
		kcp.min_rtt = minRtt
		kcp.full_pipe = true
		kcp.cycle_idx, kcp.cycle_ts = 0, now
		return kcp, now
	}

	kcp, now := setup(200)
	kcp.end_probe_on_queue(229, now+50) // within min RTT + interval: ACK delay
	if kcp.cycle_idx != 0 {
		t.Fatalf("an RTT sample within one flush interval of min RTT ended the probe (phase %d)", kcp.cycle_idx)
	}
	kcp.end_probe_on_queue(240, now+50)
	if kcp.cycle_idx != 1 {
		t.Fatalf("a 40 ms queue 50 ms into the probe left phase %d, want the drain phase 1", kcp.cycle_idx)
	}
	// The drain should end 50 ms later, after as long as the probe ran.
	if got := _itimediff(now+100, kcp.cycle_ts); got != int32(kcp.min_rtt) {
		t.Fatalf("drain ends %d ms after the early exit, want 50 (the probe's length)", int32(kcp.min_rtt)-_itimediff(now+50, kcp.cycle_ts))
	}

	kcp, now = setup(200)
	kcp.cycle_idx = 2
	kcp.end_probe_on_queue(400, now+50)
	if kcp.cycle_idx != 2 {
		t.Fatal("a queueing RTT sample outside the probe changed the gain phase")
	}

	// LAN: min RTT 4 ms against a 30 ms flush interval; samples there
	// mostly measure ACK delay.
	kcp, now = setup(4)
	kcp.end_probe_on_queue(60, now+2)
	if kcp.cycle_idx != 0 {
		t.Fatal("on a path with RTT below the flush interval an RTT sample ended the probe")
	}
}
