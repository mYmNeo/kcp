# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

kcptun is a Go network tunneling tool that wraps TCP connections with KCP (UDP-based reliable transport) and SMUX stream multiplexing to improve performance over congested links. It operates as a client-server pair. Requires Go 1.26+.

## Build Commands

```bash
# Build client and server (linux amd64 + arm64, darwin arm64)
# Uses -pgo=auto (Go 1.26+ profile-guided optimization)
./build.sh

# Build manually
go build -ldflags "-X main.VERSION=$(date -u +%Y%m%d) -s -w" -o build/client_linux_amd64 github.com/xtaci/kcptun/client
go build -ldflags "-X main.VERSION=$(date -u +%Y%m%d) -s -w" -o build/server_linux_amd64 github.com/xtaci/kcptun/server

# Multi-platform release build with UPX compression
./build-release.sh

# Dependency management
go mod tidy

# Download latest release binary for current OS/arch
./download.sh
```

The `SALT` env var sets the PBKDF2 salt baked into binaries. If unset, `build.sh` and `build-release.sh` generate a random 18-byte base64 salt at build time. `main.VERSION` is stamped from `date -u +%Y%m%d`.

## Testing

```bash
go test ./...                                    # Run all tests
go test ./std                                    # Test a specific package
go test ./std -run TestCopyPrefersWriterTo        # Run a single test
go test -v -cover ./...                          # Verbose with coverage
```

## Linting

```bash
go fmt ./...
go vet ./...
```

## Debugging & Profiling

```bash
# pprof — expose Go's net/http/pprof on :6060 (both client and server)
client -r <remote> --pprof
server -l <listen> --pprof
# Then: go tool pprof http://localhost:6060/debug/pprof/profile

# SNMP stats logging — periodic KCP metrics dump to file
client -r <remote> --snmplog ./snmp.log --snmpperiod 60

# Runtime stats dump — send SIGUSR1 to print KCP SNMP counters to console
kill -USR1 $(pgrep client_linux_amd64)
kill -USR1 $(pgrep server_linux_amd64)
```

## Architecture

**Packages:**

- **`client/`** — Listens on a local TCP port (default `:12948`), dials a remote KCP server, creates a SMUX multiplexer over the KCP session, and forwards data bidirectionally between local TCP connections and SMUX streams.

- **`server/`** — Listens on a UDP port (default `:29900`), accepts KCP sessions from clients, demultiplexes SMUX streams, and forwards each stream to a target. Supports three target types (`TGT_UNIX`, `TGT_TCP`, `TGT_SOCKS5`): TCP address, Unix socket path, or built-in SOCKS5 proxy. With `--tcp` it listens on TCP instead, via the carrier.

- **`carrier/`** — Optional transport that carries the KCP datagrams inside TCP instead of UDP, selected with `--tcp`. It implements `net.PacketConn`, so kcp-go's `ServeConn`/`NewConn4` seam accepts it unchanged: KCP, FEC, SMUX, compression and the proxy modes are all untouched above the transport. Datagrams are length-prefixed frames, striped across several TCP streams so one stalled stream cannot freeze a session. `--tcp` must be set on **both** ends; a mismatch is a silent black hole rather than an error, since the flag only selects a transport per side.
  - `carrier.go` — Package doc (wire format, ordering and loss model), `Config` and its defaults (`defaultStreams` 1, `defaultQueueDepth` 256, `defaultMaxStreams` 16, `defaultMaxStreamsTotal` 512, `defaultStallTimeout` 60s), `PeerAddr` — a `*PeerAddr` fits in a `net.Addr` without boxing, so the receive path reuses one address per peer
  - `conn.go` — `Dial` + `Conn`: the dialing-side `net.PacketConn`, one logical peer over `Config.Streams` TCP streams
  - `listener.go` — `ListenWithConfig` + `Listener`: the accepting side, collecting the streams that announce one session id back into one peer. Caps per-peer and listener-wide streams, gives unverified handshakes a smaller budget, and pins a peer to the source IP that created it
  - `stream.go` / `streamset.go` — One TCP stream with a dedicated writer goroutine and a bounded queue; `streamSet` publishes the live slice behind an atomic pointer so the per-datagram read path takes no lock and allocates nothing
  - `frame.go` — 2-byte big-endian length prefix framing that preserves KCP packet boundaries; the buffer pool holds `*[kcpCeiling]byte` rather than `[]byte` to avoid interface-boxing on every `Put`
  - `handshake.go` — Four-frame challenge-response (hello → nonce → HMAC proof → ack) over HMAC-SHA256 under domain-separated labels. The nonce is issued per connection and covered by the proof, so a recorded handshake cannot be replayed onto a later one
  - `socket.go` — `tune` (TCP nodelay and keepalive) and `sockOptions`, which records what KCP asks for via `SetReadBuffer`/`SetWriteBuffer`/`SetDSCP` and replays it onto every stream established later; DSCP goes through `golang.org/x/net/ipv4`/`ipv6`

- **`std/`** — Shared library used by both client and server:
  - `config.go` — `BaseConfig` struct embedded by both client and server Config; predefined KCP mode profiles (normal, fast, fast2, fast3); `ParseJSONConfig` generic loader
  - `crypt.go` — Cipher registry mapping names (sm4, tea, aes-128, aes-128-gcm, aes-192, blowfish, twofish, cast5, 3des, xtea, salsa20) to `kcp.BlockCrypt` implementations via PBKDF2-HMAC-SHA256 key derivation (600,000 iterations). Weak/null ciphers (none, null, xor) removed. Defaults to `aes-128-gcm` (AEAD).
  - `copy.go` — Optimized bidirectional I/O forwarding (`Copy`/`Pipe`) using `io.WriterTo`/`io.ReaderFrom` interfaces
  - `proxy.go` — SOCKS5 protocol implementation (RFC 1928) with buffer pooling
  - `multiport.go` — Parses `host:min-max` port range format for multiport dialing
  - `comp.go` — LZ4 compression wrapper with 64KB block size for low-latency bulk transfer
  - `smuxcfg.go` — SMUX configuration (v1/v2 selection, buffer sizes)
  - `snmp.go` — Periodic SNMP stats logging to file (`--snmplog`/`--snmpperiod`)
  - `signal.go` — Signal handling (Unix only): SIGUSR1 dumps KCP SNMP stats, SIGTERM/SIGINT terminates the process

**Data flow:**
```
App → Client (TCP :12948) → [KCP + SMUX over internet] → Server (:29900) → Target service
```

The bracketed hop is UDP by default and TCP with `--tcp` on both ends. `--mtu`, `--sndwnd`/`--rcvwnd` and `--datashard`/`--parityshard` tune KCP itself either way; `--sockbuf` and `--dscp` apply to whichever socket now carries the datagrams.

**Key dependencies:** `github.com/xtaci/kcp-go/v5` (KCP transport), `github.com/xtaci/smux` (stream multiplexing), `github.com/urfave/cli` (CLI framework), `golang.org/x/crypto` (PBKDF2 key derivation), `golang.org/x/net` (DSCP on TCP sockets, in the carrier), `github.com/fatih/color` (colored console output).

**FEC (Forward Error Correction):** `--datashard N` and `--parityshard M` configure Reed-Solomon erasure codes. N data packets + M parity packets sent together; up to M can be lost without retransmission. Default: 10/3.


## Key Patterns

- **Configuration**: CLI flags (`urfave/cli`) with optional JSON config file override (`-c config.json`). Both client and server embed `std.BaseConfig` for shared KCP/SMUX parameters. Per-side carrier settings live on the binary that owns them: `--carrierstreams` on the client, `--carriermaxstreams`/`--carriermaxstreamstotal` on the server. `--carrierqueuedepth` is shared, in `BaseConfig`, because both ends should throttle at the same point. `ParseJSONConfig` does not reject unknown fields, so a per-side key placed in the wrong config file is silently ignored rather than reported.
- **Platform-specific files**: Build-constrained files for original-destination lookup (`client/origdst_linux.go` uses `SO_ORIGINAL_DST`; `client/origdst_stub.go` is the no-op fallback) and signal handling (`std/signal.go` is `//go:build linux || darwin || freebsd`). The Linux implementation detects the original destination of redirected TCP connections for transparent SOCKS5 proxying.
- **Version injection**: Build-time linker flags set `main.VERSION` and `main.SALT`.
- **Buffer pooling**: `sync.Pool` used in proxy.go, copy.go and the carrier's frame.go to reduce GC pressure.
- **Lazy init**: `sync.Once` in `client/dial.go` for one-time multiport address parsing.
- **Session scavenging**: Client periodically purges expired KCP sessions (controlled by `--autoexpire` and `--scavengettl` flags).
