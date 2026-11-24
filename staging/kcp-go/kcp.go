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
	"container/heap"
	"encoding/binary"
	"time"
)

// KCP Protocol Constants
const (
	// Retransmission Timeout (RTO) bounds, in milliseconds
	IKCP_RTO_NDL  = 30    // no-delay mode: minimum RTO (ms)
	IKCP_RTO_MIN  = 100   // normal mode: minimum RTO (ms)
	IKCP_RTO_DEF  = 200   // default RTO (ms)
	IKCP_RTO_INIT = 1000  // RTO for segments sent before any RTT sample (ms), as in RFC 6298
	IKCP_RTO_MAX  = 60000 // maximum RTO (ms), 60 seconds

	// Command types for the KCP segment header (cmd field)
	IKCP_CMD_PUSH = 81 // cmd: push data
	IKCP_CMD_ACK  = 82 // cmd: acknowledge
	IKCP_CMD_WASK = 83 // cmd: window probe request (ask)
	IKCP_CMD_WINS = 84 // cmd: window size response (tell)

	// Probe flags (bitfield), set in kcp.probe to schedule probe commands
	IKCP_ASK_SEND = 1 // schedule sending IKCP_CMD_WASK
	IKCP_ASK_TELL = 2 // schedule sending IKCP_CMD_WINS

	// Default window and MTU sizes
	IKCP_WND_SND = 32   // default send window size (packets)
	IKCP_WND_RCV = 32   // default receive window size (packets)
	IKCP_MTU_DEF = 1400 // default MTU (bytes, not including UDP/IP header)

	// Protocol parameters
	IKCP_ACK_FAST    = 3      // number of later ACKs needed to trigger fast retransmit
	IKCP_INTERVAL    = 100    // default flush interval (ms)
	IKCP_OVERHEAD    = 24     // per-segment header size: conv(4) + cmd(1) + frg(1) + wnd(2) + ts(4) + sn(4) + una(4) + len(4)
	IKCP_DEADLINK    = 20     // maximum transmissions before declaring the link dead
	IKCP_THRESH_INIT = 2      // initial slow-start threshold (packets)
	IKCP_THRESH_MIN  = 2      // minimum slow-start threshold (packets)
	IKCP_PROBE_INIT  = 500    // initial window probe timeout (ms)
	IKCP_PROBE_LIMIT = 120000 // maximum window probe timeout (ms), 120 seconds
	IKCP_SN_OFFSET   = 12     // byte offset of sequence number (sn) within the segment header
)

// Pacing parameters. Transmission is paced at a gain times the bottleneck
// bandwidth estimated from delivery-rate samples, so bursts are spread over
// time instead of overflowing shallow router queues and socket buffers.
const (
	paceStartupGain = 289   // percent; 2/ln2, doubles delivery rate each round until bandwidth plateaus
	paceFullBwRnds  = 3     // rounds without 25% bandwidth growth that end startup
	paceStartupLoss = 5     // percent of a round's transmissions retransmitted that ends startup
	paceBurstMs     = 5     // token bucket depth, in ms at the pacing rate
	paceBwWinRounds = 10    // bandwidth max-filter window, in round trips
	paceMinRttWinMs = 10000 // min RTT filter window (ms)
	paceInitWnd     = 32    // segments per min RTT assumed before any bandwidth sample
	paceUnsentMs    = 50    // writes wait while unsent data exceeds this many ms at the bandwidth estimate
	paceQueueCut    = 10    // percent; most the bandwidth estimate drops per round with a standing queue
	paceUnsentLong  = 25    // paceUnsentMs after startup where min RTT > 2*interval, with a paceInitWnd/2 floor
)

// paceCycleGain is the steady-state pacing gain cycle in percent, one phase
// per min RTT. Pacing at gain 1 bounds delivery-rate samples by the send
// rate, so ACK bursts cannot ratchet the bandwidth estimate upward; the 1.15
// phase probes for more bandwidth and the 0.85 phase drains the queue that
// probing built. A gentler probe than BBR's 1.25 keeps the probe's queue
// within shallow bottleneck buffers on long paths.
var paceCycleGain = [8]uint64{115, 85, 100, 100, 100, 100, 100, 100}

type winMaxSample struct {
	t uint32
	v uint64
}

// winMax is a windowed max filter that tracks the best, second best and
// third best samples (Kathleen Nichols' algorithm, as in Linux lib/minmax.c).
type winMax struct {
	s [3]winMaxSample
}

func (m *winMax) get() uint64 { return m.s[0].v }

func (m *winMax) reset(t uint32, v uint64) {
	m.s[0] = winMaxSample{t, v}
	m.s[1], m.s[2] = m.s[0], m.s[0]
}

// update adds sample v taken at time t and keeps the max over window win.
func (m *winMax) update(win, t uint32, v uint64) {
	val := winMaxSample{t, v}
	if v >= m.s[0].v || t-m.s[2].t > win {
		m.reset(t, v)
		return
	}
	if v >= m.s[1].v {
		m.s[1], m.s[2] = val, val
	} else if v >= m.s[2].v {
		m.s[2] = val
	}
	dt := t - m.s[0].t
	switch {
	case dt > win:
		m.s[0], m.s[1], m.s[2] = m.s[1], m.s[2], val
		if t-m.s[0].t > win {
			m.s[0], m.s[1], m.s[2] = m.s[1], m.s[2], val
		}
	case m.s[1].t == m.s[0].t && dt > win/4:
		m.s[1], m.s[2] = val, val
	case m.s[2].t == m.s[1].t && dt > win/2:
		m.s[2] = val
	}
}

type PacketType int8

const (
	IKCP_PACKET_REGULAR PacketType = iota
	IKCP_PACKET_FEC
)

type FlushType int8

const (
	IKCP_FLUSH_ACKONLY FlushType = 1 << iota
	IKCP_FLUSH_FULL
)

type KCPLogType int32

const (
	IKCP_LOG_OUTPUT KCPLogType = 1 << iota
	IKCP_LOG_INPUT
	IKCP_LOG_SEND
	IKCP_LOG_RECV
	IKCP_LOG_OUT_ACK
	IKCP_LOG_OUT_PUSH
	IKCP_LOG_OUT_WASK
	IKCP_LOG_OUT_WINS
	IKCP_LOG_IN_ACK
	IKCP_LOG_IN_PUSH
	IKCP_LOG_IN_WASK
	IKCP_LOG_IN_WINS
)

const (
	IKCP_LOG_OUTPUT_ALL = IKCP_LOG_OUTPUT | IKCP_LOG_OUT_ACK | IKCP_LOG_OUT_PUSH | IKCP_LOG_OUT_WASK | IKCP_LOG_OUT_WINS
	IKCP_LOG_INPUT_ALL  = IKCP_LOG_INPUT | IKCP_LOG_IN_ACK | IKCP_LOG_IN_PUSH | IKCP_LOG_IN_WASK | IKCP_LOG_IN_WINS
	IKCP_LOG_ALL        = IKCP_LOG_OUTPUT_ALL | IKCP_LOG_INPUT_ALL | IKCP_LOG_SEND | IKCP_LOG_RECV
)

// refTime is the monotonic reference time point for KCP timestamps.
var refTime time.Time = time.Now()

// currentMs returns elapsed monotonic milliseconds since package initialization.
func currentMs() uint32 { return uint32(time.Since(refTime) / time.Millisecond) }

// output_callback receives packet data emitted by KCP.
type output_callback func(buf []byte, size int)

// logoutput_callback logs KCP trace output.
type logoutput_callback func(msg string, args ...any)

func _itimediff(later, earlier uint32) int32 {
	return (int32)(later - earlier)
}

// segment defines a KCP segment
type segment struct {
	conv     uint32
	cmd      uint8
	frg      uint8
	wnd      uint16
	ts       uint32
	sn       uint32
	una      uint32
	rto      uint32
	xmit     uint32
	resendts uint32
	fastack  uint32
	acked    uint32 // whether the segment has been acknowledged

	// delivery-rate sample state, stamped at each transmission
	deliv    uint64 // kcp.delivered when sent
	deliv_ts uint32 // kcp.delivered_ts when sent
	first_ts uint32 // kcp.first_sent_ts when sent
	applim   bool   // sent while the sender was application-limited
	data     []byte
}

// encode a segment header into buffer
func (seg *segment) encode(ptr []byte) []byte {
	_ = ptr[IKCP_OVERHEAD-1] // BCE hint
	binary.LittleEndian.PutUint32(ptr, seg.conv)
	ptr[4] = seg.cmd
	ptr[5] = seg.frg
	binary.LittleEndian.PutUint16(ptr[6:], seg.wnd)
	binary.LittleEndian.PutUint32(ptr[8:], seg.ts)
	binary.LittleEndian.PutUint32(ptr[12:], seg.sn)
	binary.LittleEndian.PutUint32(ptr[16:], seg.una)
	binary.LittleEndian.PutUint32(ptr[20:], uint32(len(seg.data)))
	DefaultSnmp.increaseOutSegs()
	return ptr[IKCP_OVERHEAD:]
}

// segmentHeap is a min-heap of segments, used for receiving segments in order
type segmentHeap struct {
	segments []segment
	marks    map[uint32]struct{} // to avoid duplicates
}

func newSegmentHeap() *segmentHeap {
	h := &segmentHeap{
		marks: make(map[uint32]struct{}),
	}
	heap.Init(h)
	return h
}

func (h *segmentHeap) Len() int { return len(h.segments) }

func (h *segmentHeap) Less(i, j int) bool {
	return _itimediff(h.segments[j].sn, h.segments[i].sn) > 0
}

func (h *segmentHeap) Swap(i, j int) { h.segments[i], h.segments[j] = h.segments[j], h.segments[i] }
func (h *segmentHeap) Push(x any) {
	h.segments = append(h.segments, x.(segment))
	h.marks[x.(segment).sn] = struct{}{}
}

func (h *segmentHeap) Pop() any {
	n := len(h.segments)
	x := h.segments[n-1]
	h.segments[n-1] = segment{} // clear reference to avoid memory leak
	h.segments = h.segments[0 : n-1]
	delete(h.marks, x.sn)
	return x
}

func (h *segmentHeap) Has(sn uint32) bool {
	_, exists := h.marks[sn]
	return exists
}

// KCP defines a single KCP connection's protocol state machine.
// It is a pure ARQ (Automatic Repeat reQuest) implementation with no I/O.
type KCP struct {
	// Connection identity and framing
	conv  uint32 // conversation id, must be equal on both sides
	mtu   uint32 // maximum transmission unit (bytes)
	mss   uint32 // maximum segment size = mtu - IKCP_OVERHEAD
	state uint32 // connection state, 0 = active, 0xFFFFFFFF = dead link

	// Sequence numbers and acknowledgment tracking
	snd_una uint32 // oldest unacknowledged sequence number
	snd_nxt uint32 // next sequence number to send
	rcv_nxt uint32 // next expected sequence number to receive

	// Congestion control
	ssthresh           uint32 // slow-start threshold (packets)
	rx_rttvar, rx_srtt int32  // RTT variance and smoothed RTT (ms), per RFC 6298
	rx_rto, rx_minrto  uint32 // retransmission timeout and its lower bound (ms)
	snd_wnd            uint32 // local send window size (packets)
	rcv_wnd            uint32 // local receive window size (packets)
	rmt_wnd            uint32 // remote advertised window size (packets)
	cwnd               uint32 // congestion window (packets)
	incr               uint32 // bytes accumulated for cwnd increment

	// Window probing
	probe      uint32 // probe flags (IKCP_ASK_SEND / IKCP_ASK_TELL)
	ts_probe   uint32 // timestamp for next window probe (ms)
	probe_wait uint32 // current probe timeout (ms), increases on each retry

	// Timers and scheduling
	interval uint32 // flush interval (ms)
	ts_flush uint32 // next flush timestamp (ms)
	nodelay  uint32 // 0: normal, 1: no-delay mode (reduces RTO aggressively)
	updated  uint32 // whether Update() has been called at least once

	// Reliability
	dead_link  uint32 // transmission count at which the link is considered dead
	fastresend int32  // fast retransmit trigger count, 0 = disabled
	nocwnd     int32  // 1 = disable congestion control
	stream     int32  // 1 = stream mode (no message boundaries), 0 = message mode

	// Logging
	logmask KCPLogType

	// Data queues and buffers
	snd_queue *RingBuffer[segment] // send queue: segments waiting to enter the send window
	rcv_queue *RingBuffer[segment] // receive queue: ordered segments ready for user read
	snd_buf   *RingBuffer[segment] // send buffer: segments in-flight (sent but unacknowledged)
	rcv_buf   *segmentHeap         // receive buffer: out-of-order segments awaiting reordering

	acklist []ackItem // pending ACKs to be flushed

	buffer []byte          // pre-allocated encoding buffer for flush()
	output output_callback // callback to write data to the underlying transport

	log logoutput_callback // trace log callback

	// Delivery-rate estimation (draft-cheng-iccrg-delivery-rate-estimation)
	// and pacing. Byte counts include the KCP segment header.
	delivered     uint64 // bytes acknowledged so far
	delivered_ts  uint32 // when delivered last advanced (ms)
	first_sent_ts uint32 // send time of the newest acknowledged segment (ms)
	app_limited   uint64 // nonzero: samples are app-limited until delivered passes it
	inflight      uint64 // bytes sent and not yet acknowledged
	unsent        uint64 // bytes queued by Send and not yet transmitted once
	min_rtt       uint32 // windowed minimum RTT (ms), 0 = unknown
	min_rtt_ts    uint32 // when min_rtt was taken (ms)
	btlbw         winMax // windowed max delivery rate (bytes/s); t is kcp.round
	round         uint32 // round trips counted by delivery
	round_deliv   uint64 // delivered value that ends the current round
	full_bw       uint64 // startup: bandwidth when growth last exceeded 25%
	full_bw_cnt   uint32 // startup: rounds since then; paceFullBwRnds ends startup
	full_pipe     bool   // the bandwidth estimate has plateaued
	round_held    bool   // the pacer held segments during the current round
	round_rexmit  uint32 // segments retransmitted during the current round
	round_sent    uint32 // segments transmitted during the current round
	round_min_rtt uint32 // smallest RTT sample (ms) during the current round, 0 = none
	cycle_idx     uint32 // index into paceCycleGain once full_pipe is set
	cycle_ts      uint32 // when the current gain phase began (ms)
	pace_tokens   int64  // pacing budget (bytes)
	pace_ts       uint32 // when pace_tokens was last refilled (ms)
	pace_wait     uint32 // ms until held segments may leave, 0 = nothing held

	// rate sample from the most recently sent segment acked by one Input call
	rs_have     bool
	rs_deliv    uint64
	rs_deliv_ts uint32
	rs_first_ts uint32
	rs_sent_ts  uint32
	rs_applim   bool
}

type ackItem struct {
	sn uint32
	ts uint32
}

// NewKCP creates a new KCP state machine.
//
// conv must be the same at both endpoints, or data will be silently rejected.
//
// The output function is called whenever data needs to be sent on the wire.
func NewKCP(conv uint32, output output_callback) *KCP {
	kcp := new(KCP)
	kcp.conv = conv
	kcp.snd_wnd = IKCP_WND_SND
	kcp.rcv_wnd = IKCP_WND_RCV
	kcp.rmt_wnd = IKCP_WND_RCV
	kcp.mtu = IKCP_MTU_DEF
	kcp.mss = kcp.mtu - IKCP_OVERHEAD
	kcp.buffer = make([]byte, (kcp.mtu+IKCP_OVERHEAD)*3)
	kcp.rx_rto = IKCP_RTO_DEF
	kcp.rx_minrto = IKCP_RTO_MIN
	kcp.interval = IKCP_INTERVAL
	kcp.ts_flush = IKCP_INTERVAL
	kcp.ssthresh = IKCP_THRESH_INIT
	kcp.dead_link = IKCP_DEADLINK
	kcp.output = output
	kcp.snd_buf = NewRingBuffer[segment](IKCP_WND_SND * 2)
	kcp.rcv_queue = NewRingBuffer[segment](IKCP_WND_RCV * 2)
	kcp.snd_queue = NewRingBuffer[segment](IKCP_WND_SND * 2)
	kcp.rcv_buf = newSegmentHeap()
	return kcp
}

// newSegment creates a KCP segment
func (kcp *KCP) newSegment(size int) (seg segment) {
	seg.data = defaultBufferPool.Get()[:size]
	return
}

// recycleSegment recycles a KCP segment
func (kcp *KCP) recycleSegment(seg *segment) {
	if seg.data != nil {
		defaultBufferPool.Put(seg.data)
		seg.data = nil
	}
}

// PeekSize checks the size of next message in the recv queue
func (kcp *KCP) PeekSize() (length int) {
	seg, ok := kcp.rcv_queue.Peek()
	if !ok {
		return -1
	}

	if seg.frg == 0 {
		return len(seg.data)
	}

	if kcp.rcv_queue.Len() < int(seg.frg+1) {
		return -1
	}

	for ri := 0; ri < kcp.rcv_queue.Len(); ri++ {

		seg := kcp.rcv_queue.At(ri)
		length += len(seg.data)
		if seg.frg == 0 {
			break
		}
	}
	return
}

// Recv reads data from the KCP state machine.
//
// It returns the number of bytes read.
//
// It returns -1 when there is no readable data.
//
// It returns -2 if len(buffer) is smaller than kcp.PeekSize().
func (kcp *KCP) Recv(buffer []byte) (n int) {
	peeksize := kcp.PeekSize()
	if peeksize < 0 {
		return -1
	}

	if peeksize > len(buffer) {
		return -2
	}

	var fast_recover bool
	if kcp.rcv_queue.Len() >= int(kcp.rcv_wnd) {
		fast_recover = true
	}

	// merge fragment
	for {
		seg, ok := kcp.rcv_queue.Pop()
		if !ok {
			break
		}

		copy(buffer, seg.data)
		buffer = buffer[len(seg.data):]
		n += len(seg.data)
		kcp.recycleSegment(&seg)
		if seg.frg == 0 {
			if kcpTrace {
				kcp.debugLog(IKCP_LOG_RECV, "stream", kcp.stream, "conv", kcp.conv, "sn", seg.sn, "ts", seg.ts, "datalen", n)
			}
			break
		}
	}

	// move available data from rcv_buf -> rcv_queue
	for kcp.rcv_buf.Len() > 0 {
		seg := heap.Pop(kcp.rcv_buf).(segment)
		if seg.sn == kcp.rcv_nxt && kcp.rcv_queue.Len() < int(kcp.rcv_wnd) {
			kcp.rcv_queue.Push(seg)
			kcp.rcv_nxt++
		} else {
			// push back segment
			heap.Push(kcp.rcv_buf, seg)
			break
		}
	}

	// fast recover
	if kcp.rcv_queue.Len() < int(kcp.rcv_wnd) && fast_recover {
		// The next flush will send IKCP_CMD_WINS to advertise the receive
		// window to the peer.
		kcp.probe |= IKCP_ASK_TELL
	}
	return
}

// Send sends data from the user level and returns a negative value on error.
func (kcp *KCP) Send(buffer []byte) int {
	var count int
	if len(buffer) == 0 {
		return -1
	}

	if kcpTrace {
		kcp.debugLog(IKCP_LOG_SEND, "stream", kcp.stream, "conv", kcp.conv, "datalen", len(buffer))
	}

	// append to previous segment in streaming mode (if possible)
	if kcp.stream != 0 {
		if n := kcp.snd_queue.Len(); n > 0 {
			for ri := kcp.snd_queue.Len() - 1; ri >= 0; ri-- {
				seg := kcp.snd_queue.At(ri)
				if len(seg.data) < int(kcp.mss) {
					capacity := int(kcp.mss) - len(seg.data)
					extend := min(len(buffer), capacity)

					// grow slice, the underlying cap is guaranteed to
					// be larger than kcp.mss
					oldlen := len(seg.data)
					seg.data = seg.data[:oldlen+extend]
					copy(seg.data[oldlen:], buffer)
					kcp.unsent += uint64(extend)
					buffer = buffer[extend:]
				}
				break
			}
		}

		if len(buffer) == 0 {
			return 0
		}
	}

	if len(buffer) <= int(kcp.mss) {
		count = 1
	} else {
		count = (len(buffer) + int(kcp.mss) - 1) / int(kcp.mss)
	}

	if count > 255 {
		return -2
	}

	if count == 0 {
		count = 1
	}

	for i := 0; i < count; i++ {
		var size int
		size = min(len(buffer), int(kcp.mss))
		seg := kcp.newSegment(size)
		copy(seg.data, buffer[:size])
		if kcp.stream == 0 { // message mode
			seg.frg = uint8(count - i - 1)
		} else { // stream mode
			seg.frg = 0
		}

		kcp.snd_queue.Push(seg)
		kcp.unsent += uint64(size)
		buffer = buffer[size:]
	}
	return 0
}

// update_ack updates the smoothed RTT and RTO based on a new RTT sample.
// The estimator is based on RFC 6298, with reduced weight for RTT samples well
// below the current smoothed RTT.
func (kcp *KCP) update_ack(rtt int32) {
	var rto uint32
	if kcp.rx_srtt == 0 {
		kcp.rx_srtt = rtt
		kcp.rx_rttvar = rtt >> 1
	} else {
		delta := rtt - kcp.rx_srtt
		kcp.rx_srtt += delta >> 3
		if delta < 0 {
			delta = -delta
		}
		if rtt < kcp.rx_srtt-kcp.rx_rttvar {
			// If the RTT sample is below SRTT minus the current RTT variance,
			// give it one-eighth of the usual weight.
			kcp.rx_rttvar += (delta - kcp.rx_rttvar) >> 5
		} else {
			kcp.rx_rttvar += (delta - kcp.rx_rttvar) >> 2
		}
	}
	// The variance term alone is a few ms on a smooth long path, so a pacing
	// probe that queues even srtt/8 extra would fire RTO for the whole
	// flight. Keep at least srtt/4 of margin, as such probes queue less.
	rto = uint32(kcp.rx_srtt) + max(kcp.interval, uint32(kcp.rx_rttvar)<<2, uint32(kcp.rx_srtt)>>2)
	kcp.rx_rto = min(max(kcp.rx_minrto, rto), IKCP_RTO_MAX)
}

// update_minrtt feeds an RTT sample (ms) into the windowed min RTT filter.
func (kcp *KCP) update_minrtt(rtt int32, now uint32) {
	r := uint32(max(rtt, 1))
	if kcp.min_rtt == 0 || r <= kcp.min_rtt || _itimediff(now, kcp.min_rtt_ts) > paceMinRttWinMs {
		kcp.min_rtt = r
		kcp.min_rtt_ts = now
	}
}

// on_delivered accounts a newly acknowledged segment for delivery-rate
// sampling. It must run before the segment's data is recycled.
func (kcp *KCP) on_delivered(seg *segment, now uint32) {
	if seg.xmit == 0 {
		return
	}
	size := uint64(len(seg.data) + IKCP_OVERHEAD)
	kcp.inflight -= min(size, kcp.inflight)
	// A first transmission acknowledged after later segments (fastack > 0)
	// was lost and repaired by FEC parity. Counting it would credit the
	// overflow that parity absorbed as delivered bandwidth. A lost ACK also
	// leaves fastack > 0; skipping that segment only under-counts.
	if seg.xmit == 1 && seg.fastack > 0 {
		return
	}
	kcp.delivered += size
	kcp.delivered_ts = now
	// Keep the sample of the most recently sent segment, as RFC draft-cheng
	// does, so one sample covers everything acknowledged by this Input call.
	// As in Karn's algorithm, a retransmitted segment gives no sample: its
	// ACK may answer an earlier transmission, so its newer send stamp would
	// overstate the rate and end rounds early.
	if seg.xmit == 1 && (!kcp.rs_have || seg.deliv >= kcp.rs_deliv) {
		kcp.rs_have = true
		kcp.rs_deliv = seg.deliv
		kcp.rs_deliv_ts = seg.deliv_ts
		kcp.rs_first_ts = seg.first_ts
		kcp.rs_sent_ts = seg.ts
		kcp.rs_applim = seg.applim
	}
}

// take_rate_sample turns the pending sample into a delivery rate and feeds
// the bottleneck bandwidth filter.
func (kcp *KCP) take_rate_sample(now uint32) {
	if !kcp.rs_have {
		return
	}
	kcp.rs_have = false
	if kcp.app_limited != 0 && kcp.delivered > kcp.app_limited {
		kcp.app_limited = 0
	}
	newRound, roundHeld := false, false
	if kcp.rs_deliv >= kcp.round_deliv {
		kcp.round++
		kcp.round_deliv = kcp.delivered
		newRound, roundHeld = true, kcp.round_held
		kcp.round_held = false
	}
	kcp.first_sent_ts = kcp.rs_sent_ts

	// The slower of the send and ACK rates bounds the delivery rate. Intervals
	// shorter than min RTT come from ACK compression and are discarded.
	interval := max(_itimediff(kcp.rs_sent_ts, kcp.rs_first_ts), _itimediff(now, kcp.rs_deliv_ts))
	if interval > 0 && (kcp.min_rtt == 0 || uint32(interval) >= kcp.min_rtt) {
		rate := (kcp.delivered - kcp.rs_deliv) * 1000 / uint64(interval)
		if !kcp.rs_applim || rate >= kcp.btlbw.get() {
			kcp.btlbw.update(paceBwWinRounds, kcp.round, rate)
		}
	}

	// A round that had to retransmit more than paceStartupLoss of what it
	// sent overflowed the bottleneck, so the pipe is already full. Only
	// rounds in which the pacer held segments count: in others the sender
	// was not pushing at the startup gain, so losses there say nothing
	// about the pipe. Retransmits in the first round come from the initial
	// RTO guess, which can be shorter than the path RTT, and a few segments
	// are too small a sample: random loss of one in fifty would end startup.
	lossy := newRound && roundHeld && kcp.round >= 2 && kcp.round_sent >= 8*paceInitWnd &&
		kcp.round_rexmit*100 > kcp.round_sent*paceStartupLoss
	if lossy && !kcp.full_pipe {
		// Samples from the round that overflowed include the queue it built
		// draining; keep the last estimate that grew cleanly.
		if kcp.full_bw > 0 {
			kcp.btlbw.reset(kcp.round, kcp.full_bw)
		}
		kcp.full_pipe = true
		kcp.cycle_idx = 2
		kcp.cycle_ts = now
	}

	// Otherwise startup ends once the bandwidth estimate fails to grow by 25%
	// for paceFullBwRnds rounds in a row. Only rounds in which the pacer held
	// segments and the sample was not app-limited count: elsewhere the
	// application, not the path, set the delivery rate.
	if newRound && !kcp.full_pipe && roundHeld && !kcp.rs_applim {
		if bw := kcp.btlbw.get(); bw >= kcp.full_bw*5/4 {
			kcp.full_bw = bw
			kcp.full_bw_cnt = 0
		} else if kcp.full_bw_cnt++; kcp.full_bw_cnt >= paceFullBwRnds {
			kcp.full_pipe = true
			kcp.cycle_idx = 2
			kcp.cycle_ts = now
		}
	}
	// A queue that stayed up for a whole round while the sender was pushing
	// means the pacing rate exceeds the bottleneck: with a correct estimate
	// the gain-1 phases drain what the probe built. Max-filtered samples
	// taken while recovery bursts drain read high, so cut the estimate by
	// paceQueueCut for each such round. A round counts when its best RTT
	// sample sat over min RTT by more than min_rtt/8 and one flush interval
	// (ACKs may wait that long); bursty traffic drains the queue each round,
	// so its round minimum stays at min RTT. Only paths whose min RTT is
	// well above the flush interval qualify: where ACK delay is comparable
	// to the RTT, RTT samples cannot tell a standing queue from ACK delay,
	// and short rounds would apply the cut many times per second.
	if newRound && kcp.full_pipe && roundHeld && !kcp.rs_applim && kcp.min_rtt > 2*kcp.interval && kcp.round_min_rtt > 0 &&
		kcp.round_min_rtt > kcp.min_rtt+max(kcp.min_rtt/8, kcp.interval) {
		kcp.btlbw.reset(kcp.round, kcp.btlbw.get()*(100-paceQueueCut)/100)
	}
	if newRound {
		kcp.round_rexmit, kcp.round_sent, kcp.round_min_rtt = 0, 0, 0
	}

	if kcp.full_pipe && kcp.min_rtt > 0 && _itimediff(now, kcp.cycle_ts) >= int32(kcp.min_rtt) {
		kcp.cycle_idx = (kcp.cycle_idx + 1) % uint32(len(paceCycleGain))
		kcp.cycle_ts = now
	}
}

// end_probe_on_queue ends the probe phase early once an RTT sample shows
// queueing: above min RTT by more than min_rtt/8 and one flush interval (ACK
// delay). The probe phase lasts one min RTT, but on a shallow buffer it fills
// the queue well before then, and the overflow costs both drops and latency
// for everything behind it. The drain phase that follows lasts as long as the
// probe ran: with symmetric gains that removes what the probe added, where a
// full min RTT of drain would leave the link idle. Paths where ACK delay
// dominates the RTT keep the timed phase, as for the standing-queue
// correction.
func (kcp *KCP) end_probe_on_queue(rtt, now uint32) {
	if !kcp.full_pipe || kcp.cycle_idx != 0 || kcp.min_rtt <= 2*kcp.interval ||
		rtt <= kcp.min_rtt+max(kcp.min_rtt/8, kcp.interval) {
		return
	}
	probed := min(uint32(max(_itimediff(now, kcp.cycle_ts), 0)), kcp.min_rtt)
	kcp.cycle_idx, kcp.cycle_ts = 1, now-kcp.min_rtt+probed
}

// pacing_floor is paceInitWnd segments per RTT, in bytes/s.
func (kcp *KCP) pacing_floor() uint64 {
	rtt := kcp.min_rtt
	if rtt == 0 {
		rtt = uint32(max(kcp.rx_srtt, 0))
	}
	if rtt == 0 {
		rtt = IKCP_RTO_DEF
	}
	return paceInitWnd * uint64(kcp.mtu) * 1000 / uint64(rtt)
}

// pacing_rate returns the transmit rate in bytes/s: a gain times the
// bottleneck bandwidth estimate, but never less than pacing_floor. The floor
// keeps short request/response flows, whose delivery-rate samples only
// measure the application, from being throttled by them.
func (kcp *KCP) pacing_rate() uint64 {
	gain := uint64(paceStartupGain)
	if kcp.full_pipe {
		gain = paceCycleGain[kcp.cycle_idx]
	}
	return gain * max(kcp.btlbw.get(), kcp.pacing_floor()) / 100
}

// reset_pacing forgets the path model, so pacing measures the path again
// from startup. Delivery and in-flight accounting are kept. Min RTT is
// cleared too: after a move to a shorter path a stale, larger value would
// reject every honest rate sample (interval < min_rtt) for its 10 s window.
// Input refills it from the next ACK before taking a rate sample.
func (kcp *KCP) reset_pacing(now uint32) {
	kcp.btlbw = winMax{}
	kcp.min_rtt, kcp.min_rtt_ts = 0, 0
	kcp.full_bw, kcp.full_bw_cnt, kcp.full_pipe = 0, 0, false
	kcp.cycle_idx, kcp.cycle_ts = 0, now
	kcp.app_limited = 0
	kcp.round_held, kcp.round_rexmit, kcp.round_sent, kcp.round_min_rtt = false, 0, 0, 0
	// Two segments may leave at once, as from a fresh connection; the rest
	// of the resend follows at the startup rate instead of in one burst.
	kcp.pace_tokens, kcp.pace_ts = 2*int64(kcp.mtu), now
}

// shrink_buf advances snd_una to the oldest unacknowledged segment in snd_buf.
func (kcp *KCP) shrink_buf() {
	if seg, ok := kcp.snd_buf.Peek(); ok {
		kcp.snd_una = seg.sn
	} else {
		kcp.snd_una = kcp.snd_nxt
	}
}

// parse_ack marks a segment as acknowledged in snd_buf by sequence number.
// The segment is not removed immediately; it stays until snd_una advances past it,
// avoiding expensive shifts in the ring buffer.
func (kcp *KCP) parse_ack(sn, now uint32) {
	if _itimediff(sn, kcp.snd_una) < 0 || _itimediff(sn, kcp.snd_nxt) >= 0 {
		return
	}

	for ri := 0; ri < kcp.snd_buf.Len(); ri++ {

		seg := kcp.snd_buf.At(ri)
		if sn == seg.sn {
			// Mark it acknowledged and recycle its data, but leave the slot
			// until `una` advances. This avoids shifting later segments in the
			// ring buffer.
			if seg.acked == 0 {
				kcp.on_delivered(seg, now)
			}
			seg.acked = 1
			kcp.recycleSegment(seg)
			break
		}
		if _itimediff(sn, seg.sn) < 0 {
			break
		}
	}
}

// parse_fastack increments the fast-ack counter for segments with sn < the given sn.
// Returns 1 if any segment's fastack counter has reached the fast retransmit threshold.
func (kcp *KCP) parse_fastack(sn, ts uint32) int {
	shouldFastAck := 0
	if _itimediff(sn, kcp.snd_una) < 0 || _itimediff(sn, kcp.snd_nxt) >= 0 {
		return 0
	}

	for ri := 0; ri < kcp.snd_buf.Len(); ri++ {

		seg := kcp.snd_buf.At(ri)
		if _itimediff(sn, seg.sn) < 0 {
			break
		} else if sn != seg.sn && _itimediff(seg.ts, ts) <= 0 {
			if seg.fastack != 0xFFFFFFFF {
				seg.fastack++
				if seg.fastack >= uint32(kcp.fastresend) {
					shouldFastAck = 1
				}
			}
		}
	}

	return shouldFastAck
}

// parse_una removes all segments from snd_buf that have been cumulatively acknowledged
// (i.e., segments with sn < una). Returns the number of segments removed.
func (kcp *KCP) parse_una(una, now uint32) int {
	count := 0
	for ri := 0; ri < kcp.snd_buf.Len(); ri++ {
		seg := kcp.snd_buf.At(ri)
		if _itimediff(una, seg.sn) > 0 {
			if seg.acked == 0 {
				kcp.on_delivered(seg, now)
			}
			kcp.recycleSegment(seg)
			count++
		} else {
			break
		}
	}
	kcp.snd_buf.Discard(count)
	return count
}

// ack_push appends an ACK to the pending ACK list.
func (kcp *KCP) ack_push(sn, ts uint32) {
	kcp.acklist = append(kcp.acklist, ackItem{sn, ts})
}

// parse_data returns true if the segment is outside the receive window or is a duplicate.
func (kcp *KCP) parse_data(newseg segment) bool {
	sn := newseg.sn
	if _itimediff(sn, kcp.rcv_nxt+kcp.rcv_wnd) >= 0 ||
		_itimediff(sn, kcp.rcv_nxt) < 0 {
		return true
	}

	repeat := false
	if !kcp.rcv_buf.Has(sn) {
		// replicate the content if it's new
		dataCopy := defaultBufferPool.Get()[:len(newseg.data)]
		copy(dataCopy, newseg.data)
		newseg.data = dataCopy

		// insert the new segment into rcv_buf
		heap.Push(kcp.rcv_buf, newseg)
	} else {
		repeat = true
	}

	// move available data from rcv_buf -> rcv_queue
	for kcp.rcv_buf.Len() > 0 {
		seg := heap.Pop(kcp.rcv_buf).(segment)
		if seg.sn == kcp.rcv_nxt && kcp.rcv_queue.Len() < int(kcp.rcv_wnd) {
			kcp.rcv_queue.Push(seg)
			kcp.rcv_nxt++
		} else {
			// push back segment
			heap.Push(kcp.rcv_buf, seg)
			break
		}
	}

	return repeat
}

// Input feeds a packet into the KCP state machine.
//
// pktType distinguishes regular packets from packets recovered by FEC.
//
// ackNoDelay causes pending ACKs to be flushed immediately, at the cost of
// increased bandwidth.
func (kcp *KCP) Input(data []byte, pktType PacketType, ackNoDelay bool) int {
	snd_una := kcp.snd_una
	if len(data) < IKCP_OVERHEAD {
		return -1
	}

	var latest uint32 // the latest ack packet
	var updateRTT int
	var inSegs uint64
	var flushSegments int // signal to flush segments
	now := currentMs()

	for {
		if len(data) < int(IKCP_OVERHEAD) {
			break
		}

		_ = data[IKCP_OVERHEAD-1] // BCE hint
		conv := binary.LittleEndian.Uint32(data)
		cmd := data[4]
		frg := data[5]
		wnd := binary.LittleEndian.Uint16(data[6:])
		ts := binary.LittleEndian.Uint32(data[8:])
		sn := binary.LittleEndian.Uint32(data[12:])
		una := binary.LittleEndian.Uint32(data[16:])
		length := binary.LittleEndian.Uint32(data[20:])
		data = data[IKCP_OVERHEAD:]

		if conv != kcp.conv {
			return -1
		}

		if kcpTrace {
			kcp.debugLog(IKCP_LOG_INPUT, "conv", conv, "cmd", cmd, "frg", frg, "wnd", wnd, "ts", ts, "sn", sn, "una", una, "len", length, "datalen", len(data))
		}

		if len(data) < int(length) {
			return -2
		}

		if cmd != IKCP_CMD_PUSH && cmd != IKCP_CMD_ACK &&
			cmd != IKCP_CMD_WASK && cmd != IKCP_CMD_WINS {
			return -3
		}

		// Accept remote-window updates only from regular packets.
		if pktType == IKCP_PACKET_REGULAR {
			kcp.rmt_wnd = uint32(wnd)
		}
		if kcp.parse_una(una, now) > 0 {
			flushSegments |= 1
		}
		kcp.shrink_buf()

		switch cmd {
		case IKCP_CMD_ACK:
			if kcpTrace {
				kcp.debugLog(IKCP_LOG_IN_ACK, "conv", conv, "sn", sn, "una", una, "ts", ts, "rto", kcp.rx_rto)
			}
			kcp.parse_ack(sn, now)
			flushSegments |= kcp.parse_fastack(sn, ts)
			updateRTT |= 1
			latest = ts
		case IKCP_CMD_PUSH:
			repeat := true
			if _itimediff(sn, kcp.rcv_nxt+kcp.rcv_wnd) < 0 {
				kcp.ack_push(sn, ts)
				if _itimediff(sn, kcp.rcv_nxt) >= 0 {
					repeat = kcp.parse_data(segment{
						conv: conv, cmd: cmd, frg: frg, wnd: wnd,
						ts: ts, sn: sn, una: una,
						data: data[:length], // delayed data copying
					})
				}
			}
			if pktType == IKCP_PACKET_REGULAR && repeat {
				DefaultSnmp.increaseRepeatSegs()
			}
			if kcpTrace {
				kcp.debugLog(IKCP_LOG_IN_PUSH, "conv", conv, "sn", sn, "una", una, "ts", ts, "packettype", pktType, "repeat", repeat)
			}
		case IKCP_CMD_WASK:
			// ready to send back IKCP_CMD_WINS in Ikcp_flush
			// tell remote my window size
			kcp.probe |= IKCP_ASK_TELL
			if kcpTrace {
				kcp.debugLog(IKCP_LOG_IN_WASK, "conv", conv, "wnd", wnd, "ts", ts)
			}
		case IKCP_CMD_WINS:
			if kcpTrace {
				kcp.debugLog(IKCP_LOG_IN_WINS, "conv", conv, "wnd", wnd, "ts", ts)
			}
		default:
			return -3
		}

		inSegs++
		data = data[length:]
	}
	DefaultSnmp.addInSegs(inSegs)

	// update rtt with the latest ts
	// ignore the FEC packet
	if updateRTT != 0 && pktType == IKCP_PACKET_REGULAR {
		if rtt := _itimediff(now, latest); rtt >= 0 {
			kcp.update_ack(rtt)
			kcp.update_minrtt(rtt, now)
			if r := uint32(max(rtt, 1)); kcp.round_min_rtt == 0 || r < kcp.round_min_rtt {
				kcp.round_min_rtt = r
			}
			kcp.end_probe_on_queue(uint32(rtt), now)
		}
	}
	kcp.take_rate_sample(now)

	// Congestion window (cwnd) update on ACK arrival.
	// Uses Reno-style algorithm: slow-start below ssthresh, then AIMD.
	if kcp.nocwnd == 0 {
		if _itimediff(kcp.snd_una, snd_una) > 0 {
			if kcp.cwnd < kcp.rmt_wnd {
				mss := kcp.mss
				if kcp.cwnd < kcp.ssthresh {
					kcp.cwnd++
					kcp.incr += mss
				} else {
					if kcp.incr < mss {
						kcp.incr = mss
					}
					kcp.incr += (mss*mss)/kcp.incr + (mss / 16)
					if (kcp.cwnd+1)*mss <= kcp.incr {
						if mss > 0 {
							kcp.cwnd = (kcp.incr + mss - 1) / mss
						} else {
							kcp.cwnd = kcp.incr + mss - 1
						}
					}
				}
				if kcp.cwnd > kcp.rmt_wnd {
					kcp.cwnd = kcp.rmt_wnd
					kcp.incr = kcp.rmt_wnd * mss
				}
			}
		}
	}

	// Determine if we need to flush data segments or acks
	if flushSegments != 0 {
		// If the window has slid or a fast ACK should be triggered,
		// Flush immediately. In previous implementations, we only
		// send out fastacks when interval timeouts, so the resending packets
		// have to wait until then. Now, we try to flush as soon as we can.
		kcp.flush(IKCP_FLUSH_FULL)
	} else if len(kcp.acklist) >= int(kcp.mtu/IKCP_OVERHEAD) { // clocking
		// This threshold serves as a clock for low-latency networks, where
		// latency is below the flush interval. Otherwise, the peer would wait
		// for the next interval-triggered flush to send UNA and ACKs. In such
		// networks, that interval is too long, so flush ACKs immediately once
		// the list grows large.
		kcp.flush(IKCP_FLUSH_ACKONLY)
	} else if ackNoDelay && len(kcp.acklist) > 0 { // Test-only: flush ACKs immediately when ackNoDelay is set.
		kcp.flush(IKCP_FLUSH_ACKONLY)
	}
	return 0
}

func (kcp *KCP) wnd_unused() uint16 {
	if kcp.rcv_queue.Len() < int(kcp.rcv_wnd) {
		return uint16(int(kcp.rcv_wnd) - kcp.rcv_queue.Len())
	}
	return 0
}

// flush sends pending data through the KCP output callback.
// This is the core scheduling function, organized in 6 phases:
//
//	Phase 1: Flush pending ACKs
//	Phase 2: Window probing (when remote window is zero)
//	Phase 3: Send window probe commands (WASK/WINS)
//	Phase 4: Move segments from snd_queue to snd_buf (sliding window)
//	Phase 5: Retransmit segments (initial, fast, early, RTO)
//	Phase 6: Update SNMP counters and congestion window
//
// Returns the suggested interval (ms) until the next flush call.
func (kcp *KCP) flush(flushType FlushType) (nextUpdate uint32) {
	var seg segment
	seg.conv = kcp.conv
	seg.cmd = IKCP_CMD_ACK
	seg.wnd = kcp.wnd_unused()
	seg.una = kcp.rcv_nxt

	buffer := kcp.buffer
	ptr := buffer

	// --- Phase 1: Flush pending ACKs ---
	if flushType == IKCP_FLUSH_ACKONLY || flushType == IKCP_FLUSH_FULL {
		for i, ack := range kcp.acklist {
			ptr = kcp.makeSpace(buffer, ptr, IKCP_OVERHEAD)
			// filter jitters caused by bufferbloat
			if _itimediff(ack.sn, kcp.rcv_nxt) >= 0 || len(kcp.acklist)-1 == i {
				seg.sn, seg.ts = ack.sn, ack.ts
				ptr = seg.encode(ptr)
				if kcpTrace {
					kcp.debugLog(IKCP_LOG_OUT_ACK, "conv", seg.conv, "sn", seg.sn, "una", seg.una, "ts", seg.ts)
				}
			}
		}
		kcp.acklist = kcp.acklist[0:0]
	}

	// --- Phase 2: Window probing (when remote window is zero) ---
	if kcp.rmt_wnd == 0 {
		current := currentMs()
		if kcp.probe_wait == 0 {
			kcp.probe_wait = IKCP_PROBE_INIT
			kcp.ts_probe = current + kcp.probe_wait
		} else {
			if _itimediff(current, kcp.ts_probe) >= 0 {
				if kcp.probe_wait < IKCP_PROBE_INIT {
					kcp.probe_wait = IKCP_PROBE_INIT
				}
				kcp.probe_wait += kcp.probe_wait / 2
				if kcp.probe_wait > IKCP_PROBE_LIMIT {
					kcp.probe_wait = IKCP_PROBE_LIMIT
				}
				kcp.ts_probe = current + kcp.probe_wait
				kcp.probe |= IKCP_ASK_SEND
			}
		}
	} else {
		kcp.ts_probe = 0
		kcp.probe_wait = 0
	}

	// --- Phase 3: Flush window probing commands ---
	if (kcp.probe & IKCP_ASK_SEND) != 0 {
		seg.cmd = IKCP_CMD_WASK
		ptr = kcp.makeSpace(buffer, ptr, IKCP_OVERHEAD)
		ptr = seg.encode(ptr)
		if kcpTrace {
			kcp.debugLog(IKCP_LOG_OUT_WASK, "conv", seg.conv, "wnd", seg.wnd, "ts", seg.ts)
		}
	}

	// flush window probing commands
	if (kcp.probe & IKCP_ASK_TELL) != 0 {
		seg.cmd = IKCP_CMD_WINS
		ptr = kcp.makeSpace(buffer, ptr, IKCP_OVERHEAD)
		ptr = seg.encode(ptr)
		if kcpTrace {
			kcp.debugLog(IKCP_LOG_OUT_WINS, "conv", seg.conv, "wnd", seg.wnd, "ts", seg.ts)
		}
	}

	kcp.probe = 0

	// --- Phase 4: Move segments from snd_queue to snd_buf (sliding window) ---
	// Effective window = min(snd_wnd, rmt_wnd, cwnd)
	cwnd := min(kcp.snd_wnd, kcp.rmt_wnd)
	if kcp.nocwnd == 0 {
		cwnd = min(kcp.cwnd, cwnd)
	}

	for {
		if _itimediff(kcp.snd_nxt, kcp.snd_una+cwnd) >= 0 {
			break
		}

		newseg, ok := kcp.snd_queue.Pop()
		if !ok {
			break
		}

		newseg.conv = kcp.conv
		newseg.cmd = IKCP_CMD_PUSH
		newseg.sn = kcp.snd_nxt
		kcp.snd_buf.Push(newseg)
		kcp.snd_nxt++
	}

	// Early retransmit recovers the tail of a flight, so it waits while
	// unsent segments sit at the tail of snd_buf: segments Phase 4 just moved
	// in (upstream KCP's newSegsCount == 0 gate) and segments the pacer held.
	// Either way more transmissions, and so more ACKs, will follow.
	tailUnsent := false
	if n := kcp.snd_buf.Len(); n > 0 {
		tailUnsent = kcp.snd_buf.At(n-1).xmit == 0
	}

	// calculate resent
	resent := uint32(kcp.fastresend)
	if kcp.fastresend <= 0 {
		resent = 0xffffffff
	}

	// --- Phase 5: Retransmit segments from snd_buf ---
	// Determines which segments need (re)transmission:
	// - Initial transmit (xmit == 0)
	// - Fast retransmit (fastack >= fastresend threshold)
	// - Early retransmit (fastack > 0, no new segments queued)
	// - RTO-based retransmit (current >= resendts)
	current := currentMs()
	var change, lostSegs, fastRetransSegs, earlyRetransSegs uint64
	nextUpdate = kcp.interval

	// Refill the pacing budget. The bucket holds paceBurstMs of the pacing
	// rate, but at least two segments so a lone segment is never delayed.
	rate := kcp.pacing_rate()
	burst := max(int64(rate*paceBurstMs/1000), 2*int64(kcp.mtu))
	if elapsed := _itimediff(current, kcp.pace_ts); elapsed > 0 {
		kcp.pace_tokens = min(kcp.pace_tokens+int64(rate*uint64(elapsed)/1000), burst)
		kcp.pace_ts = current
	}
	held := false

	if flushType == IKCP_FLUSH_FULL {
		for ri := 0; ri < kcp.snd_buf.Len(); ri++ {
			segment := kcp.snd_buf.At(ri)
			needsend := false
			if segment.acked == 1 {
				continue
			}
			if kcp.pace_tokens <= 0 {
				// Out of budget: this and later segments wait for the pacer.
				held = true
				break
			}
			if segment.xmit == 0 { // initial transmit
				needsend = true
				segment.rto = kcp.rx_rto
				if kcp.rx_srtt == 0 {
					// No RTT sample yet. On a long path the default RTO is
					// shorter than one round trip and would resend the first
					// flight before its ACKs could arrive; RFC 6298 waits 1 s.
					segment.rto = max(segment.rto, IKCP_RTO_INIT)
				}
				segment.resendts = current + segment.rto
			} else if segment.fastack >= resent && segment.fastack != 0xFFFFFFFF { // fast retransmit
				needsend = true
				segment.fastack = 0xFFFFFFFF // must wait until RTO to reset
				segment.rto = kcp.rx_rto
				segment.resendts = current + segment.rto
				change++
				fastRetransSegs++
			} else if segment.fastack > 0 && segment.fastack != 0xFFFFFFFF && !tailUnsent { // early retransmit
				needsend = true
				segment.fastack = 0xFFFFFFFF
				segment.rto = kcp.rx_rto
				segment.resendts = current + segment.rto
				change++
				earlyRetransSegs++
			} else if _itimediff(current, segment.resendts) >= 0 { // RTO
				needsend = true
				if kcp.nodelay == 0 {
					segment.rto += kcp.rx_rto
				} else {
					segment.rto += kcp.rx_rto / 2
				}
				segment.fastack = 0
				segment.resendts = current + segment.rto
				lostSegs++
			}

			if needsend {
				current = currentMs()
				need := IKCP_OVERHEAD + len(segment.data)
				if segment.xmit == 0 {
					if kcp.inflight == 0 {
						// Restart the delivery-rate interval after idle.
						kcp.first_sent_ts = current
						kcp.delivered_ts = current
					}
					kcp.inflight += uint64(need)
					kcp.unsent -= min(uint64(len(segment.data)), kcp.unsent)
				}
				segment.deliv = kcp.delivered
				segment.deliv_ts = kcp.delivered_ts
				segment.first_ts = kcp.first_sent_ts
				segment.applim = kcp.app_limited != 0
				kcp.pace_tokens -= int64(need)
				kcp.round_sent++

				segment.xmit++
				segment.ts = current
				segment.wnd = seg.wnd
				segment.una = seg.una

				ptr = kcp.makeSpace(buffer, ptr, need)
				ptr = segment.encode(ptr)
				copy(ptr, segment.data)
				ptr = ptr[len(segment.data):]

				if kcpTrace {
					kcp.debugLog(IKCP_LOG_OUT_PUSH, "conv", segment.conv, "sn", segment.sn, "frg", segment.frg, "una", segment.una, "ts", segment.ts, "xmit", segment.xmit, "datalen", len(segment.data))
				}

				if segment.xmit >= kcp.dead_link {
					kcp.state = 0xFFFFFFFF // mark connection as dead
				}
			}

			// get the nearest rto
			if rto := _itimediff(segment.resendts, current); rto > 0 && uint32(rto) < nextUpdate {
				nextUpdate = uint32(rto)
			}
		}
	}

	if flushType == IKCP_FLUSH_FULL {
		if held {
			kcp.round_held = true
			// Time until the budget covers one full segment.
			deficit := uint64(int64(kcp.mtu) - kcp.pace_tokens)
			kcp.pace_wait = max(uint32(deficit*1000/rate), 1)
		} else {
			kcp.pace_wait = 0
			if kcp.snd_queue.Len() == 0 {
				// Nothing left to send: later rate samples reflect the
				// application, not the network, until this data is delivered.
				kcp.app_limited = max(kcp.delivered+kcp.inflight, 1)
			}
		}
	}

	// --- Phase 6: Update SNMP counters and congestion window ---
	sum := lostSegs
	if lostSegs > 0 {
		DefaultSnmp.addLostSegs(lostSegs)
	}
	if fastRetransSegs > 0 {
		DefaultSnmp.addFastRetransSegs(fastRetransSegs)
		sum += fastRetransSegs
	}
	if earlyRetransSegs > 0 {
		DefaultSnmp.addEarlyRetransSegs(earlyRetransSegs)
		sum += earlyRetransSegs
	}
	if sum > 0 {
		DefaultSnmp.addRetransSegs(sum)
		kcp.round_rexmit += uint32(sum)
	}

	// cwnd update
	if kcp.nocwnd == 0 {
		// Update ssthresh after fast or early retransmit.
		// Reduce the slow-start threshold to half the inflight window.
		if change > 0 {
			inflight := kcp.snd_nxt - kcp.snd_una
			kcp.ssthresh = max(inflight/2, IKCP_THRESH_MIN)
			kcp.cwnd = kcp.ssthresh + resent
			kcp.incr = kcp.cwnd * kcp.mss
		}

		// Congestion control after RTO: reset cwnd per RFC 5681
		if lostSegs > 0 {
			kcp.ssthresh = max(cwnd/2, IKCP_THRESH_MIN)
			kcp.cwnd = 1
			kcp.incr = kcp.mss
		}

		if kcp.cwnd < 1 {
			kcp.cwnd = 1
			kcp.incr = kcp.mss
		}
	}

	// flush bytes in buffer if there is any
	if size := len(buffer) - len(ptr); size > 0 {
		kcp.output(buffer, size)
	}

	DefaultSnmp.storeRingBufferSndQueue(kcp.snd_queue.Len())
	DefaultSnmp.storeRingBufferRcvQueue(kcp.rcv_queue.Len())
	DefaultSnmp.storeRingBufferSndBuffer(kcp.snd_buf.Len())
	return nextUpdate
}

// makeSpace sends buffered bytes when adding space would exceed one MTU, then
// returns where to write next. Using a method avoids a closure allocation in
// TinyGo.
func (kcp *KCP) makeSpace(buffer, ptr []byte, space int) []byte {
	if size := len(buffer) - len(ptr); size+space > int(kcp.mtu) {
		kcp.output(buffer, size)
		return buffer
	}
	return ptr
}

// (deprecated)
//
// Update advances the protocol state and flushes pending data. Call it
// periodically; use Check to determine when the next call is due. Internal
// timestamps are in milliseconds.
func (kcp *KCP) Update() {
	var slap int32

	current := currentMs()
	if kcp.updated == 0 {
		kcp.updated = 1
		kcp.ts_flush = current
	}

	slap = _itimediff(current, kcp.ts_flush)

	if slap >= 10000 || slap < -10000 {
		kcp.ts_flush = current
		slap = 0
	}

	if slap >= 0 {
		kcp.ts_flush += kcp.interval
		if _itimediff(current, kcp.ts_flush) >= 0 {
			kcp.ts_flush = current + kcp.interval
		}
		kcp.flush(IKCP_FLUSH_FULL)
	}
}

// (deprecated)
//
// Check returns the elapsed-millisecond timestamp when Update should next be
// called. Scheduling updates this way avoids unnecessary calls and can help
// manage large numbers of KCP connections.
func (kcp *KCP) Check() uint32 {
	current := currentMs()
	ts_flush := kcp.ts_flush
	tm_flush := int32(0x7fffffff)
	tm_packet := int32(0x7fffffff)
	minimal := uint32(0)
	if kcp.updated == 0 {
		return current
	}

	if _itimediff(current, ts_flush) >= 10000 ||
		_itimediff(current, ts_flush) < -10000 {
		ts_flush = current
	}

	if _itimediff(current, ts_flush) >= 0 {
		return current
	}

	tm_flush = _itimediff(ts_flush, current)

	for ri := 0; ri < kcp.snd_buf.Len(); ri++ {

		seg := kcp.snd_buf.At(ri)
		diff := _itimediff(seg.resendts, current)
		if diff <= 0 {
			return current
		}
		if diff < tm_packet {
			tm_packet = diff
		}
	}

	minimal = uint32(tm_packet)
	if tm_packet >= tm_flush {
		minimal = uint32(tm_flush)
	}
	if minimal >= kcp.interval {
		minimal = kcp.interval
	}

	return current + minimal
}

// SetMtu changes the MTU size. The default is 1400 bytes.
func (kcp *KCP) SetMtu(mtu int) int {
	if mtu <= IKCP_OVERHEAD {
		return -1
	}

	bufferSize := (mtu + IKCP_OVERHEAD) * 3
	for segment := range kcp.snd_buf.ForEach {
		if size := IKCP_OVERHEAD + len(segment.data); size > bufferSize {
			bufferSize = size
		}
	}
	for segment := range kcp.snd_queue.ForEach {
		if size := IKCP_OVERHEAD + len(segment.data); size > bufferSize {
			bufferSize = size
		}
	}

	kcp.mtu = uint32(mtu)
	kcp.mss = kcp.mtu - IKCP_OVERHEAD
	kcp.buffer = make([]byte, bufferSize)
	return 0
}

// NoDelay configures no-delay mode, the update interval, fast retransmit, and
// congestion control.
// fastest: ikcp_nodelay(kcp, 1, 20, 2, 1)
// nodelay: 0 disables (default); 1 enables
// interval: internal update interval in milliseconds (default 100 ms)
// resend: fast retransmit threshold; 0 disables it (default)
// nc: 0 enables congestion control (default); 1 disables it
func (kcp *KCP) NoDelay(nodelay, interval, resend, nc int) int {
	if nodelay >= 0 {
		kcp.nodelay = uint32(nodelay)
		if nodelay != 0 {
			kcp.rx_minrto = IKCP_RTO_NDL
		} else {
			kcp.rx_minrto = IKCP_RTO_MIN
		}
	}
	if interval >= 0 {
		if interval > 5000 {
			interval = 5000
		} else if interval < 10 {
			interval = 10
		}
		kcp.interval = uint32(interval)
	}
	if resend >= 0 {
		kcp.fastresend = int32(resend)
	}
	if nc >= 0 {
		kcp.nocwnd = int32(nc)
	}
	return 0
}

// WndSize sets the send and receive window sizes. The defaults are 32 segments
// for each.
func (kcp *KCP) WndSize(sndwnd, rcvwnd int) int {
	if sndwnd > 0 {
		kcp.snd_wnd = uint32(sndwnd)
	}
	if rcvwnd > 0 {
		kcp.rcv_wnd = uint32(rcvwnd)
	}
	return 0
}

// WaitSnd returns the number of segments in the send buffer and send queue.
func (kcp *KCP) WaitSnd() int {
	return kcp.snd_buf.Len() + kcp.snd_queue.Len()
}

// send_backlog_full reports whether Write should wait. Besides the send
// window, unsent data is capped at paceUnsentMs of the bottleneck bandwidth
// estimate (like TCP_NOTSENT_LOWAT). Data queued beyond that only adds
// queueing delay for everything written after it, such as interactive frames
// multiplexed on the same session; the application can hold it and schedule
// it instead. The estimate, unlike the pacing rate, does not swing with the
// gain cycle, so the backlog stays the same size through probe and drain.
func (kcp *KCP) send_backlog_full() bool {
	if kcp.WaitSnd() >= int(kcp.snd_wnd) {
		return true
	}
	bw := max(kcp.btlbw.get(), kcp.pacing_floor())
	if !kcp.full_pipe {
		bw = kcp.pacing_rate()
	}
	// After startup, where RTT samples resolve queueing (min RTT above two
	// flush intervals, as for the standing-queue correction), the estimate
	// is steady enough to hold half the backlog. On those paths a frame
	// written behind the backlog already waits for a deep bottleneck queue.
	// During startup the rate doubles each round, and on short paths ACK
	// delay makes the estimate noisy; a short backlog there starves the
	// pacer and costs goodput.
	ms, floor := uint64(paceUnsentMs), uint64(paceInitWnd)
	if kcp.full_pipe && kcp.min_rtt > 2*kcp.interval {
		ms, floor = paceUnsentLong, paceInitWnd/2
	}
	limit := max(bw*ms/1000, floor*uint64(kcp.mss))
	return kcp.unsent >= limit
}

// SetLogger configures the trace logger
func (kcp *KCP) SetLogger(mask KCPLogType, logger logoutput_callback) {
	if logger == nil {
		kcp.logmask = 0
		return
	}
	kcp.logmask = mask
	kcp.log = logger
}

// ResetRTO resets the retransmission timeout (RTO) and resend timestamp
// for all segments currently in the send buffer.
//
// This forces all unacknowledged segments to be eligible for immediate
// retransmission during the next flush. It also resets the pacing model, so
// the possibly new path is measured from startup and the resent segments
// leave paced rather than in one burst.
func (kcp *KCP) ResetRTO() {
	current := currentMs()

	for segment := range kcp.snd_buf.ForEach {
		segment.rto = kcp.rx_rto
		segment.resendts = current
	}

	// The caller knows connectivity was restored, possibly over another
	// path. Bandwidth and RTT estimates from before no longer hold, so
	// measure again instead of resending everything at the old rate.
	kcp.reset_pacing(current)
}
