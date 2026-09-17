# Repository Guidelines

## Project Overview

A practical reference for AI assistants working in the **smux** codebase. smux (Simple MultipleXing) is a multiplexing library for Go that runs multiple logical streams over a single reliable, ordered connection (TCP or KCP). It powers connection management for [kcp-go](https://github.com/xtaci/kcp-go) and [kcptun](https://github.com/xtaci/kcptun). Licensed MIT (Copyright xtaci, 2016–2017).

## Architecture & Data Flow

smux is a single flat package — every `.go` file lives at the repo root, all in `package smux`. Three participants form the runtime:

1. **`Session`** (`session.go`) — owns the underlying `io.ReadWriteCloser`, the stream registry, the token bucket, and four long-lived goroutines.
2. **`Stream`** (`stream.go`) — one logical stream; implements `net.Conn`.
3. **`Frame`** (`frame.go`) — the 8-byte-header wire unit exchanged over the connection.

### Wire format (little-endian, `binary.LittleEndian`)

8-byte header (`headerSize = 8`, `frame.go:53`):

| Offset | Bytes | Field | Type |
|--------|-------|-------|------|
| 0 | 1 | VERSION | `byte` |
| 1 | 1 | CMD | `byte` |
| 2 | 2 | LENGTH | `uint16` |
| 4 | 4 | STREAMID | `uint32` |

Commands (`frame.go:31-39`): `cmdSYN=0` (open), `cmdFIN=1` (close/EOF), `cmdPSH=2` (data), `cmdNOP=3` (keepalive), `cmdUPD=4` (v2 window update). The `cmdUPD` body is 8 bytes: 4-byte `consumed` + 4-byte `window` (`updHeader`, `frame.go:99-106`).

### Session lifecycle and goroutines

`Client(conn, cfg)` / `Server(conn, cfg)` (`mux.go`) → `newSession` (`session.go`) spawns four goroutines:

- **`recvLoop`** (`session.go`) — reads frames from `conn` under token-bucket control (`bucket int32`); dispatches by CMD: `SYN` → register stream, push to `chAccepts`; `PSH` → `stream.pushBytes` + wake reader; `FIN` → `stream.fin()`; `UPD` (v2) → `stream.update(consumed, window)` + wake writer; `NOP` → keepalive ack. On any read/protocol error it closes the session so `shaperLoop`/`sendLoop`/`keepalive` are reaped.
- **`keepalive`** (`session.go`) — sends `NOP` on `KeepAliveInterval`; closes session on `KeepAliveTimeout`.
- **`shaperLoop`** (`session.go`) — drains `shaper`/`shaperCtrl` into a fair round-robin `shaperQueue`; CLSCTRL is preferred but both classes are admission-capped (`maxShaperSize` / `maxCtrlShaperSize`). Channels are nil'd at cap so Push never overshoots. Exits when `s.die` closes and signals via `shaperLoopDone`.
- **`sendLoop`** (`session.go`) — pops from `shaperQueue` and writes to `conn`.

### Write path

`Stream.Write` → `writeFrameInternal` → `writeRequest{class, ...}` on `shaper` (CLSDATA) or `shaperCtrl` (CLSCTRL) → `shaperLoop` → `shaperQueue` → `sendLoop` → `conn`. SYN/NOP/UPD use CLSCTRL; FIN uses CLSDATA.

### Flow control

- **v1**: token bucket on the receive side only (`bucket` int32, `bucketNotify` chan). `Stream.returnTokens` feeds tokens back to the session after reads.
- **v2**: adds a per-stream sliding window. Each stream tracks `peerConsumed` / `peerWindow` (init 256 KB); `sendWindowUpdate` (`stream.go:411-428`) emits `cmdUPD` once `incr` crosses `windowUpdateThreshold` (MaxStreamBuffer/2).

## Key Directories

Flat layout — no `src/`, `cmd/`, or `internal/` subtrees.

- `.` (root) — all Go sources, tests, config, docs.
- `assets/` — images only (`mux.jpg`, `smux.png`, `curve.jpg`); not code.

## Development Commands

```bash
go build ./...                                          # compile
go vet ./...                                            # lint (no external linter configured)
go test ./...                                           # run all tests
go test -v -run=^$ -bench .                            # benchmarks only
go test -coverprofile=coverage.txt -covermode=atomic -bench .   # CI coverage run (Travis)
```

The `.travis.yml` matrix targets Go `1.9.x`–`1.11.x` and runs the coverage command above, uploading to codecov. The `go.mod` directive is `go 1.18`; any Go ≥ 1.18 builds cleanly.

## Code Conventions & Common Patterns

- **Concurrency is the central concern.** Every mutable struct is guarded explicitly. When adding a field to `Session` or `stream`, decide its synchronization owner first.
  - `Session`: `nextStreamIDLock` guards `nextStreamID`; `streamLock` guards the `streams` map; `bucket`/`closed`/`goAway`/`sessionIsActive` are `atomic` int32; `die` chan + `dieOnce` for lifecycle; `chAccepts`, `shaper`, `chShaperPending/Consumed`, `bucketNotify`, and the per-error `ch*Error` chans drive the goroutines (`session.go:93-134`).
  - `stream`: `bufferLock` guards `bufferRing`; `die`+`dieOnce`, `chFinEvent`+`finEventOnce`, `chWriteClosed`+`writeClosedOnce` for half-close lifecycle; `readDeadline`/`writeDeadline` are `atomic.Value`; `chReaderWakeup`/`chWriterWakeup`/`chUpdate` chans for blocking read/write coordination (`stream.go:40-79`).
- **Channel-as-signal, not channel-as-queue.** Wakeup chans are `chan struct{}` closed or sent-to once to signal; `sync.Once` guarantees single close. `chAccepts` (`chan *stream`) is the only buffered-value channel.
- **Error propagation** uses `atomic.Value` to store the first error + a `chan struct{}` + `sync.Once` to notify once (socket read/write errors, proto error). Sentinel errors: `ErrInvalidProtocol`, `ErrConsumed`, `ErrGoAway`, `ErrTimeout` (implements `net.Error`), `ErrWouldBlock` (`session.go:71-75`).
- **v1/v2 split** is pervasive: `tryReadV1`/`tryReadV2`, `writeV1`/`writeV2`, `writeToV1`/`writeToV2`. Touching read/write logic means touching both paths unless the change is version-agnostic.
- **`net.Conn` on `Stream`**: `Read`, `Write`, `Close`, `CloseWrite` (half-close, like `TCPConn`), `SetReadDeadline`/`SetWriteDeadline`/`SetDeadline`, `LocalAddr`, `RemoteAddr`, plus `WriteTo` (io.WriterTo) and `GetDieCh`.
- **Buffer pooling**: `alloc.go` provides `Allocator` (array of `sync.Pool`, one per 2^n size class) with De Bruijn `msb()` for O(1) bucket selection; `defaultAllocator` is initialized in `init()`. `Put` requires `cap == 2^n`. `resultChanPool` (`session.go`) pools write-result channels.
- **Version compatibility is a hard constraint** — the wire format must stay backward-compatible; existing v1 peers must still interoperate.

## Important Files

- `mux.go` — `Config` struct (Version, KeepAlive*, MaxFrameSize, MaxReceiveBuffer, MaxStreamBuffer), `DefaultConfig`, `VerifyConfig`, entry points `Server`/`Client`.
- `session.go` — `Session` manager; the four goroutines (`recvLoop`, `keepalive`, `shaperLoop`, `sendLoop`); `OpenStream`/`AcceptStream`/`Close`/`NumStreams`/`IsClosed`.
- `stream.go` — `Stream` (`*stream` wrapper) + `stream` impl; `bufferRing`; v1/v2 read/write; half-close; `sendWindowUpdate`.
- `frame.go` — wire types: `Frame`, `rawHeader` (8-byte decode via LittleEndian), `updHeader`; all CMD/size constants.
- `shaper.go` — `shaperHeap` (min-heap by class then seq), `shaperQueue` (round-robin fair queue, atomic count).
- `alloc.go` — `Allocator`, `msb()`, `defaultAllocator`.
- `pkg.go` — minimal package doc (7 lines).

## Runtime/Tooling Preferences

- **Runtime**: Go toolchain, `go 1.18` minimum (per `go.mod`).
- **Dependencies**: none — stdlib only (`go.sum` is empty; no `require` block). Do not add external dependencies without strong justification; this is a deliberate zero-dep library.
- **CI**: Travis CI (legacy matrix Go 1.9–1.11). Local dev uses any modern Go.
- **Package manager**: `go mod` (modules). No vendoring.

## Testing & QA

- **Framework**: stdlib `testing` only — no testify, gomock, or any third-party test lib (confirmed by the empty `go.sum`).
- **Test files** (all at root): `alloc_test.go`, `mux_test.go`, `session_test.go` (~1500 lines, the bulk), `shaper_test.go`, `stream_test.go`, `stream_internal_test.go`.
- **Harness patterns**:
  - `setupServer(tb)` / `setupServerV2(tb)` (`session_test.go:51-114`) — TCP echo server on `localhost:0`, wraps a real `net.Dial` connection in smux. V1 and V2 variants. Most end-to-end tests use these.
  - `getSmuxStreamPair()` (`session_test.go:1194`) — bidirectional stream pair over TCP for benchmarks.
  - `net.Pipe()` — in-memory pipe for tests that don't need echo (addr/deadline/close-chan tests).
  - `newUnitTestStream()` / `newUnitTestStreamV2()` (`stream_internal_test.go:11-24,159-174`) — constructs a bare `session` struct and `stream` directly, bypassing networking entirely.
- **`stream_internal_test.go`** is in `package smux` (not `smux_test`) specifically for **white-box access to unexported** symbols: `waitRead`, `fin()`, `pushBytes`, `peerWindow`, `incr`, etc. The other `*_test.go` files use `package smux_test` (black-box) or `package smux`.
- **Representative tests**: `TestEcho` (echo round-trip), `TestParallel` (1000 concurrent streams × 100 msgs — the goroutine-safety stress test), `TestKeepAliveTimeout`, `TestRandomFrame` (fuzzes the parser with random bytes / bad frames), `TestHalfCloseBasic` / `TestHalfCloseAutoCleanup` (half-close semantics).
- **Benchmarks**: `BenchmarkMSB`, `BenchmarkAlloc` (alloc.go); `BenchmarkAcceptClose`, `BenchmarkConnSmux`, `BenchmarkConnTCP` (session_test.go — smux vs raw TCP throughput).
- **Conventions**: no `t.Parallel()` (sequential by design), no `t.Run` subtests, no `testing.Short()` skips, no build tags. Tests are flat function-per-case. Expect timeouts to be real (`TestKeepAliveTimeout` waits ~3s).
- **Coverage**: no in-repo coverage threshold; CI uploads to codecov. The `go test -bench .` invocation in Travis runs benchmarks as part of the coverage run.

## Editing checklist

When modifying this codebase, an AI assistant should:

1. **Identify the goroutine ownership** of any field you touch — reads from `recvLoop`/`sendLoop` vs. user-goroutine calls (`Read`/`Write`/`Close`) cross each other constantly.
2. **Handle both v1 and v2 paths** unless the change is strictly version-agnostic.
3. **Preserve wire-format compatibility** — header layout and CMD values are a public protocol.
4. **Use `sync.Once` for one-shot channel closes** — never close a shared signal channel without it.
5. **Recycle buffers** via `defaultAllocator.Put` (cap must be 2^n) and `resultChanPool` — allocations on the hot path are intentional avoidances.
6. **Add a test** that exercises the changed behavior through the existing TCP-echo or `net.Pipe` harness; match the flat `TestXxx` style.
