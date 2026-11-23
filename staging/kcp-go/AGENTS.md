# kcp-go Project Guide for AI Agents

## 1. Project Overview
**kcp-go** is a production-grade, reliable UDP library for Go. It implements the KCP protocol, providing a stream-based interface (like TCP) over UDP with low latency, high throughput, and packet-level control. It is widely used in gaming, streaming, and network acceleration (e.g., kcptun).

**Key Features:**
- **Reliable ARQ:** Automatic Repeat reQuest for reliable delivery.
- **FEC (Forward Error Correction):** Reed-Solomon codes to recover lost packets without retransmission.
- **Encryption:** Packet-level encryption (AES, TEA, Salsa20, etc.).
- **Performance:** Optimized for high concurrency and low memory footprint using buffer pools and platform-specific syscalls (sendmmsg/recvmmsg).

## 2. Architecture

The project is layered as follows:

1.  **Application Layer:** Uses `net.Conn` interface (`UDPSession`) to Read/Write streams.
2.  **Session Layer (`sess.go`):** Manages the connection lifecycle, encryption/decryption, and FEC encoding/decoding. It acts as the glue between the raw UDP socket and the KCP protocol state.
3.  **Protocol Layer (`kcp.go`):** The core KCP state machine. Handles sequence numbers, retransmission queues, flow control, and congestion control.
4.  **Transport Layer:** Raw UDP sockets (`net.PacketConn`).

### Data Pipeline

**Outgoing (Write):**
`App Stream` -> `UDPSession.Write` -> `KCP.Send` -> `KCP.flush` (Phase 5 token bucket; held segments set `pace_wait`, released by the one-shot `UDPSession.pace` timer) -> `UDPSession.output` -> `chPostProcessing` -> `postProcess` (FEC Encode -> Encrypt) -> `UDP Socket`

**Incoming (Read):**
`UDP Socket` -> `readLoop` -> `packetInput` (Decrypt -> FEC Decode) -> `KCP.Input` -> `UDPSession.Read` -> `App Stream`

## 3. Key Files & Components

| File | Component | Description |
| :--- | :--- | :--- |
| **`kcp.go`** | **Core Protocol** | Implements the KCP protocol logic (ARQ, RTO calculation, window management). Pure logic, no I/O. |
| **`sess.go`** | **Session Management** | Defines `UDPSession` (implements `net.Conn`) and `Listener`. Handles the "plumbing" of data. |
| **`fec.go`** | **FEC** | Forward Error Correction implementation using Reed-Solomon codes. |
| **`crypt.go`** | **Encryption** | Block ciphers and stream ciphers wrapper for packet encryption. |
| **`tx.go`** | **Transmission** | Platform-specific packet transmission logic (e.g., `tx_linux.go` uses `sendmmsg`). |
| **`readloop.go`** | **Reception** | Platform-specific packet reception loops (e.g., `readloop_linux.go` uses `recvmmsg`). |
| **`timedsched.go`** | **Scheduler** | A global timer scheduler to drive `KCP.update` for all sessions, reducing goroutine overhead. |
| **`bufferpool.go`** | **Memory Management** | `sync.Pool` wrappers to minimize GC pressure during high-throughput data transfer. |

## 4. Core Concepts

### KCP Protocol
- **Conversation ID (`conv`):** Uniquely identifies a session.
- **Command (`cmd`):** PUSH (data), ACK (acknowledgment), WASK (window probe), WINS (window size).
- **Fast Retransmit:** Retransmits lost packets faster than RTO if out-of-order packets are received.
- **No Delay:** Configurable mode to minimize RTO and disable Reno congestion control (`cwnd`) for lowest latency. `nc=1` does not disable pacing (below), which is the only rate control left in every kcptun mode. Do not strip it as dead code.
- **RTO:** `rto = srtt + max(interval, 4*rttvar, srtt/4)`. The `srtt/4` term keeps pacing probes from firing spurious RTOs on smooth long paths. Segments sent before the first RTT sample use `IKCP_RTO_INIT` (1 s, RFC 6298), because `IKCP_RTO_DEF` (200 ms) is shorter than a 200-300 ms path RTT.

### Pacing (`kcp.go`, always on, independent of `nc`)
- **Why:** with `nc=1` (every kcptun mode), the effective window is `min(snd_wnd, rmt_wnd)`, which can be hundreds of segments. Unpaced, that window bursts into shallow bottleneck queues and causes tail drops and retransmit storms.
- **Rate model:** BBR-like.
  - Delivery-rate samples per draft-cheng (`on_delivered` in `parse_ack`/`parse_una`, `take_rate_sample` after `Input`). Karn rule: only `xmit == 1` segments produce samples.
  - `btlbw` is a windowed max over 10 rounds (`winMax`, keyed by `kcp.round`); `min_rtt` uses a 10 s window.
  - **Standing-queue correction:** after startup, a held, non-app-limited round whose smallest RTT sample (`round_min_rtt`) stayed above `min_rtt + max(min_rtt/8, interval)` cuts `btlbw` by `paceQueueCut` (10%). Samples taken while recovery bursts drain overestimate the rate by about 20% on shallow buffers, and the gain-1 phases would refill the queue; delay, unlike loss, is not triggered by random loss. The interval term keeps ACK delay from counting as queueing.
- **Gain phases:**
  - Startup paces at 2.89×. It ends when bandwidth plateaus (3 held, non-app-limited rounds under 25% growth), or when a held round from round 2 on retransmits more than 5% of ≥ `8*paceInitWnd` sends. The loss exit resets `btlbw` to the last clean estimate.
  - Then ProbeBW cycles `{1.25, 0.75, 1×6}`, one phase per `min_rtt`.
  - The floor is `paceInitWnd` (32) segments per RTT, so request/response flows are never throttled by their own app-limited samples.
- **Mechanics:** a token bucket (10 ms deep, at least 2 MTU) gates Phase 5 of `flush` for new data and retransmits alike; ACK-only flushes are not paced. Held segments set `kcp.pace_wait`. `UDPSession.armPaceLocked` (under `s.mutex`) schedules a one-shot `pace()` flush on `SystemTimedSched` after write, both `Input` paths, and `update()`.
- **Early retransmit** additionally waits while unsent (pacer-held) segments sit at the tail of `snd_buf`.
- **Write admission (`send_backlog_full`)**, like `TCP_NOTSENT_LOWAT`: besides `snd_wnd`, `Write` blocks while unsent bytes (`kcp.unsent`, counted in `Send` and cleared at first transmission) reach `paceUnsentMs` (150 ms) of the bandwidth estimate (the pacing rate during startup), with a floor of 32 MSS; keyed to `btlbw`, the cap stays steady through the 1.25/0.75 gain cycle. Without this cap a saturating writer queues the whole window (~500 segments) of unsent data, and a frame written later, such as an smux frame from an interactive stream, waits seconds behind it. Every `notifyWriteEvent` site uses the same predicate.
- **`ResetRTO`** calls `reset_pacing`, which clears the bandwidth model, `min_rtt` and the token budget so that a new path is measured from scratch.
- **Wire format unchanged:** pacing is sender-local, and old peers interoperate.
- **Tests:** `pacing_test.go` covers the state machine.
  - `KCP_HL_BENCH=1 go test -run TestHighLatencyBench` (`highlatency_bench_test.go`) is the end-to-end long-RTT/shallow-buffer benchmark.
  - `KCP_BULK_BENCH=1 go test -run TestBulkBench` (`bulk_bench_test.go`) runs a saturating download with interactive pings on the same session.

### FEC (Forward Error Correction)
- Splits data into shards and generates parity shards.
- Allows recovery of `N` lost packets if `N <= parity_shards`.
- Increases bandwidth usage but reduces latency caused by retransmissions.

### Concurrency Model
- **Per-Session Goroutines:**
    - `readLoop`: Reads from the UDP socket (for clients).
    - `postProcess`: Handles encryption and FEC encoding before sending.
- **Global Scheduler:** `SystemTimedSched` (in `timedsched.go`) manages the `update` tick for all sessions to avoid creating a ticker goroutine per session.
- **Locking:** `UDPSession` uses a `sync.Mutex` to protect KCP state.

## 5. Development Guidelines for AI

1.  **Context Awareness:** When modifying `kcp.go`, remember it is a pure state machine. Do not introduce I/O operations directly into `KCP` methods.
2.  **Performance:** Be mindful of memory allocations. Use `defaultBufferPool` (`bufferpool.go`) for temporary byte slices.
3.  **Platform Compatibility:** If modifying network I/O, check `platform_*.go`, `tx_*.go`, and `readloop_*.go` to ensure cross-platform compatibility (Linux, BSD, Windows, Generic).
4.  **Testing:**
    - Use `kcp_test.go` for protocol logic tests.
    - Use `sess_test.go` for integration tests involving `UDPSession`.
    - Use `examples/` to verify end-to-end functionality.
5.  **Encryption:** New encryption methods should implement the `BlockCrypt` or `aeadCrypt` interfaces in `crypt.go`.

## 6. Common Tasks

- **Tuning Parameters:** Look at `SetNoDelay`, `SetWindowSize`, `SetMtu` in `sess.go`.
- **Debugging:** `KCP` struct has `reserved` fields and logging constants (`IKCP_LOG_*`) that can be enabled for tracing.
- **Adding Metrics:** `snmp.go` contains the `Snmp` struct for global statistics. Update this struct to add new metrics.
