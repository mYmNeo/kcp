# Repository Guidelines

## Project Overview

A practical reference for AI assistants working in the **kcp-go** codebase. kcp-go is a production-grade reliable UDP transport library for Go — it provides `net.Conn` and `net.Listener` interfaces over UDP with ARQ, FEC, encryption, and congestion control. Module: `github.com/xtaci/kcp-go/v5`. Licensed MIT (Copyright xtaci, 2015). Go 1.24+.

## Architecture & Data Flow

The library is layered as three subsystems, all in `package kcp`:

| Layer | Files | Responsibility |
|-------|-------|----------------|
| **Transport (I/O)** | `readloop*.go`, `tx*.go`, `platform*.go` | Raw packet I/O with platform batching |
| **Session** | `sess.go` | `net.Conn` facade, crypto, FEC, pipeline goroutines |
| **Protocol** | `kcp.go` | Pure ARQ state machine — sequence numbers, RTO, congestion, retransmit |

### Data pipeline (documented in `sess.go:23-44`)

**Write path**: `App → UDPSession.Write → KCP.Send → KCP.flush → output callback → chPostProcessing → postProcess goroutine → [FEC Encode] → [Encrypt/CRC32] → TxQueue → tx(txqueue) → net.PacketConn → UDP`

**Read path**: `UDP → readLoop goroutine → packetInput → [Decrypt] → [CRC32] → kcpInput → demux FEC type marker → [FEC Decode] / [OOB] → KCP.Input → UDPSession.Read → App`

### KCP state machine (`kcp.go:194-248`)

`KCP` is a pure ARQ implementation with **no I/O** — it calls a user-supplied `output callback` to emit packets. Key fields:

- **Sequence space**: `snd_una` (oldest unacked), `snd_nxt` (next to send), `rcv_nxt` (next expected)
- **Congestion control** (RFC 5681/6298): `ssthresh`, `rx_srtt`/`rx_rttvar` (smoothed RTT), `rx_rto`/`rx_minrto`, `snd_wnd`/`rcv_wnd`/`rmt_wnd`/`cwnd`, `incr`
- **Window probing**: `probe`, `ts_probe`, `probe_wait` — sends WASK/WINS probes when remote window is zero
- **Queues**: `snd_queue` / `snd_buf` / `rcv_queue` (`RingBuffer[segment]`), `rcv_buf` (`segmentHeap` for reorder), `acklist []ackItem`

Key methods: `NewKCP(conv, output_callback)`, `Send(buf)`, `Recv(buf)`, `Input(data, pktType, ackNoDelay)`, `Update()`, `Check()`, `flush(flushType)`, `NoDelay(nodelay, interval, resend, nc)`, `WndSize(sndwnd, rcvwnd)`.

`flush()` (`kcp.go:759-996`) is the core scheduler — 6 phases: flush ACKs, window probing, move snd_queue→snd_buf, retransmit (initial/fast/early/RTO), update SNMP/cwnd.

### Session (`sess.go:121-176`)

`UDPSession` implements `net.Conn`. Key fields:
- `conn net.PacketConn`, `kcp *KCP`, `block BlockCrypt` — the transport/protocol/crypto triad
- `fecEncoder *fecEncoder`, `fecDecoder *fecDecoder` — forward error correction
- `mu sync.Mutex` — guards KCP state (all `Read`/`Write`/`update` serialize on this)
- `chReadEvent`, `chWriteEvent` — wakeup channels for blocking Read/Write
- `chPostProcessing chan sendRequest` — feeds the postProcess pipeline
- `die`, `dieOnce`, `chSocketReadError`, `chSocketWriteError` — lifecycle/error signals
- `rd`, `wd atomic.Value` — read/write deadlines
- `ackNoDelay bool`, `writeDelay bool` — protocol tuning

The `Listener` type (`sess.go:1129-1149`) demuxes incoming packets by remote address (`packetInput` → lookup or create session), enabling a single UDP socket to serve many connections.

### Global timer scheduler (`timedsched.go`)

`var SystemTimedSched *TimedSched` — a global two-stage parallel scheduler shared by all sessions to avoid per-session timer goroutines. Stage 1 ("prepend") collects `Put(f, deadline)` calls; Stage 2 ("sched", `runtime.NumCPU()` workers) maintains local min-heaps of timed tasks. Each session registers its `update()` callback via `SystemTimedSched.Put(s.update, nextUpdate)`.

### Platform I/O (`readloop*.go`, `tx*.go`, `platform*.go`)

Build-tag-gated (`//go:build linux` vs `//go:build !linux`):

| Function | Generic | Linux |
|----------|---------|-------|
| Incoming | `defaultReadLoop` — `ReadFrom` single packet | `readLoop` — `recvmmsg` batch (256 pkts, `golang.org/x/net/ipv4/6`) |
| Outgoing | `defaultTx` — `WriteTo` single packet | `tx` — `WriteBatch` via `sendmmsg` (`ipv4.PacketConn`) |
| Platform | `platform` struct empty | `platform.batchConn` (if underlying conn is `*net.UDPConn`) |

Batch ops degrade gracefully: if `batchConn` is nil (non-UDP conn, mock), it falls back to the generic path. Linux batch size is 256 packets per syscall, with pre-allocated 256×1500 byte buffers (384 KB per session).

## Key Directories

Flat Go package — all source lives at the repo root.

- `.` (root) — all `.go` files, all in `package kcp`
- `assets/` — images and diagrams (layermodel.jpg, frame.png, flame graphs)
- `examples/` — `echo.go` (working client-server demo)
- `wireshark/` — Lua dissector plugin for Wireshark packet inspection

## Development Commands

```bash
go build ./...                                    # compile
go vet ./...                                      # lint
go test ./...                                     # run all tests
go test -bench=. -benchmem                        # benchmarks with allocation stats
go test -coverprofile=coverage.txt -covermode=atomic -bench . -timeout 10m   # CI coverage + benchmarks

# Debug logging (rebuilds with trace output)
go test -tags debug -run ^TestSetLogger$

# Run the echo example (terminal 1 = server, terminal 2 = client)
go run examples/echo.go
```

Travis CI (`go 1.11.x`–`1.13.x`) runs the coverage command above, uploading to codecov. Local dev uses any modern Go (`go 1.24.0` in `go.mod`).

## Code Conventions & Common Patterns

### Concurrency

- **`UDPSession.mu` is the central lock** — it serializes `Read`, `Write`, and `update()` calls. KCP is NOT thread-safe internally; all access must be behind this mutex.
- **Goroutines per session**: `readLoop` (reads from `net.PacketConn`, calls `packetInput` → `kcpInput` → `KCP.Input`), `postProcess` (FEC encode → encrypt → TX), and the scheduler callback `update()`. `readLoop` and `Write` contend on `mu`; the scheduler runs `update()` on a pooled goroutine from `SystemTimedSched`.
- **Channel-as-signal pattern**: `chReadEvent`, `chWriteEvent`, `chSocketReadError`, `chSocketWriteError` are `chan struct{}` used for wakeup/notification, NOT as data queues. `chPostProcessing` (`chan sendRequest`) is the only buffered data-carrying channel.
- **Error propagation**: `atomic.Value` stores the first error; `sync.Once` + `chan struct{}` signals exactly once.
- **`sync.Once` for one-shot channel close** — `dieOnce`, `socketReadErrorOnce`, `socketWriteErrorOnce`.

### KCP is a pure state machine

`KCP` has **no I/O**, no channels, no goroutines. It calls `output(buf, size)` (a callback set at construction) when it needs to emit packets. This property is load-bearing — the session layer wraps the output callback to inject packets into `chPostProcessing`.

### v1/v2 protocol constants

The 4 KCP packet types (`kcp.go:69-72`): `IKCP_PACKET_DATA=81`, `IKCP_PACKET_ACK=82`, `IKCP_PACKET_WASK=83` (window probe request), `IKCP_PACKET_WINS=84` (window size report). FEC-encoded packets use type markers `0xf1` (typeData), `0xf2` (typeParity), `0xf3` (typeOOB) at offset 4 — these don't collide with KCP cmd values in little-endian.

### Memory management

- `bufferpool.go`: `defaultBufferPool` — `sync.Pool` of `[]byte` with cap 1500 (`mtuLimit`). Used for all packet allocations on the hot path (rx, tx, FEC). Always `Put` back.
- `ringbuffer.go`: `RingBuffer[T]` — generic O(1) push/pop ring with automatic growth. Used for `snd_queue`, `snd_buf`, `rcv_queue` on `KCP`.
- `allocator.go` / `snmp.go`: SNMP stats (`DefaultSnmp`) track packet/byte/retransmit counts atomically — update counters after every I/O operation.

### Version-compatible API

Multiple `NewConn` variants exist for backward compat: `NewConn4`, `NewConn3`, `NewConn2`, `NewConn`. Each varies in what parameters it accepts. Prefer `DialWithOptions` / `ListenWithOptions` for new code.

## Important Files

| File | Size | Role |
|------|------|------|
| `sess.go` | 43.6 KB | `UDPSession` (`net.Conn`), `Listener`, `postProcess`, `packetInput`, `kcpInput`, public API (`Dial`, `Listen`, `NewConn*`) |
| `kcp.go` | 31.5 KB | `KCP` state machine — `Send`, `Recv`, `Input`, `flush` (6-phase), `Update`, `Check`, `NoDelay`, `WndSize` |
| `crypt.go` | 18.1 KB | `BlockCrypt` interface, implementations (AES, TEA, Salsa20, SM4, etc.), AEAD support |
| `fec.go` | 15.7 KB | `fecEncoder`/`fecDecoder`, Reed-Solomon via `klauspost/reedsolomon`, marshal/unmarshal |
| `timedsched.go` | 5.3 KB | `SystemTimedSched` — two-stage global timer for `KCP.Update` |
| `autotune.go` | 5.2 KB | Auto-bandwidth tuning based on RTT/loss measurement |
| `bufferpool.go` | 2.1 KB | `defaultBufferPool` — `sync.Pool` for 1500-byte packet buffers |
| `readloop*.go` | 1–3 KB each | Platform-gated packet receive (generic: `ReadFrom`, Linux: `recvmmsg` batch) |
| `tx*.go` | 1–2 KB each | Platform-gated packet send (generic: `WriteTo`, Linux: `sendmmsg` batch) |
| `snmp.go` | 8.0 KB | `Snmp` struct — global counters (packets in/out, bytes, retransmits, FEC stats, etc.) |

## Runtime/Tooling Preferences

- **Runtime**: Go 1.24+ (per `go.mod`). No CGo required — all platform ops use `golang.org/x/net/ipv4`/`ipv6`, not raw syscalls.
- **Dependencies**: `klauspost/reedsolomon` (FEC), `tjfoc/gmsm` (SM4 crypto), `pkg/errors` (error wrapping), `x/crypto`, `x/net`, `x/sys`, `x/time`. Test deps: `stretchr/testify` (only in autotune_test.go), `xtaci/lossyconn` (simulated packet loss).
- **CI**: Travis CI (legacy, Go 1.11–1.13). Codecov for coverage.
- **Debug**: Build tag `debug` enables KCP trace logging (`kcp_trace_on.go` vs `kcp_trace_off.go`).
- **PGO**: Profile-guided optimization supported — configure in `build.sh` (kcptun parent project).

## Testing & QA

- **Framework**: stdlib `testing` predominantly; `testify/assert` used only in `autotune_test.go`.
- **Harness**: Test helpers in `sess_test.go` build real UDP echo servers over `localhost:0`:
  - `nextPort()` — atomic port allocator starting at 10000
  - `dialEcho`/`listenEcho` constructors with crypto + FEC config
  - `echoServer` goroutines → `handleEcho` per-conn
  - `echo_tester(msg, count)` verifies N-round-trip correctness
  - `randomEchoTest(cli, N)` — parallel writer/reader with deterministic random streams
- **Loss simulation**: `lossyconn.NewLossyConn(lossRate, minDelayMs)` wraps a `net.PacketConn` with configurable loss/delay. Used in `kcp_test.go`, `fec_test.go`, and `sess_test.go`.
- **Representative tests**: `Test1GBEcho` (flow control, ~1 GB transfer), `TestParallel1024CLIENT_64BMSG_64CNT` (1024 concurrent clients), `TestFECDecodeLoss` (100 groups, 3 random losses, verified recovery), `TestAES256GCM` (AEAD round-trip), `TestAutoTune`, `TestTinyBufferReceiver` (2-byte server buffer).
- **Benchmarks**: `BenchmarkEchoSpeed4K` (echo throughput), `BenchmarkSinkSpeed1M` (sink throughput), `BenchmarkFlush` (flush with 1024 segments), `BenchmarkFECDecode`, `BenchmarkAES256`, `BenchmarkAEAD_Chacha20_Poly1035`.
- **Conventions**: Tests are function-per-case, no `t.Parallel()` (sessions use real UDP), no build-tag gated tests except `-tags debug` for trace logging. Timeouts may be real (echo transfers take seconds).

## Editing checklist

When modifying this codebase, an AI assistant should:

1. **Never add I/O to `KCP`** — it's a pure state machine. All I/O lives in `sess.go`, `readloop*.go`, `tx*.go`.
2. **Respect `UDPSession.mu`** — `Read`, `Write`, and `update()` all hold it. Adding a new path that touches KCP state outside the lock is a concurrency bug.
3. **Touch both platform branches** when modifying I/O — any change to `readLoop` or `tx` must have a corresponding change in `readloop_generic.go`/`tx_generic.go` or vice versa.
4. **Always `Put` back pool buffers** — `defaultBufferPool.Get()` must be paired with `.Put()`. A leaked buffer is a GC pressure leak.
5. **Preserve the output callback contract** — `KCP.output` is called from `flush()` and must never block. The session implementation (`postProcess`) is non-blocking with a buffered channel.
6. **Add a test** using the `dialEcho`/`listenEcho` harness for integration changes, or `lossyconn` for loss-simulation scenarios. Match the flat `TestXxx` naming convention.
