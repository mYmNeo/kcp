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

- **`server/`** — Listens on a UDP port (default `:29900`), accepts KCP sessions from clients, demultiplexes SMUX streams, and forwards each stream to a target. Supports three target types (`TGT_UNIX`, `TGT_TCP`, `TGT_SOCKS5`): TCP address, Unix socket path, or built-in SOCKS5 proxy.

- **`std/`** — Shared library used by both client and server:
  - `config.go` — `BaseConfig` struct embedded by both client and server Config; predefined KCP mode profiles (normal, fast, fast2, fast3); `ParseJSONConfig` generic loader
  - `crypt.go` — Cipher registry mapping names (sm4, tea, aes-128, aes-128-gcm, aes-192, blowfish, twofish, cast5, 3des, xtea, salsa20) to `kcp.BlockCrypt` implementations via PBKDF2-HMAC-SHA256 key derivation (600,000 iterations). Weak/null ciphers (none, null, xor) removed. Defaults to `aes-128-gcm` (AEAD).
  - `copy.go` — Optimized bidirectional I/O forwarding (`Copy`/`Pipe`) using `io.WriterTo`/`io.ReaderFrom` interfaces
  - `proxy.go` — SOCKS5 protocol implementation (RFC 1928) with buffer pooling
  - `multiport.go` — Parses `host:min-max` port range format for multiport dialing
  - `comp.go` — LZ4 compression wrapper with 64KB block size for low-latency bulk transfer
  - `smuxcfg.go` — SMUX configuration (v1/v2 selection, buffer sizes)
  - `snmp.go` — Periodic SNMP stats logging to file (`--snmplog`/`--snmpperiod`)
  - `signal.go` — Signal handling (Unix only): SIGUSR1 dumps KCP SNMP stats, SIGTERM/SIGINT runs registered exit handlers
  - `atexit.go` — Exit handler registration for graceful shutdown

- **`dns/`** — Minimal config struct (`DNSConfig`) for local interface name binding, used by client.

**Data flow:**
```
App → Client (TCP :12948) → [KCP/UDP + SMUX over internet] → Server (UDP :29900) → Target service
```

**Key dependencies:** `github.com/xtaci/kcp-go/v5` (KCP transport), `github.com/xtaci/smux` (stream multiplexing), `github.com/urfave/cli` (CLI framework), `golang.org/x/crypto` (PBKDF2 key derivation), `github.com/fatih/color` (colored console output), `github.com/jellydator/ttlcache/v3` (TTL cache for Linux conntrack).

**FEC (Forward Error Correction):** `--datashard N` and `--parityshard M` configure Reed-Solomon erasure codes. N data packets + M parity packets sent together; up to M can be lost without retransmission. Default: 10/3.


## Key Patterns

- **Configuration**: CLI flags (`urfave/cli`) with optional JSON config file override (`-c config.json`). Both client and server embed `std.BaseConfig` for shared KCP/SMUX parameters.
- **Platform-specific files**: Build-constrained files for conntrack (`contrack_darwin.go`, `contrack_linux.go`) and signal handling (`std/signal.go` is `//go:build linux || darwin || freebsd`). The Linux conntrack implementation uses `ttlcache` and netfilter to detect original destination of redirected TCP connections (SOCKS5 proxy mode). The Darwin variant is a no-op stub.
- **Version injection**: Build-time linker flags set `main.VERSION` and `main.SALT`.
- **Buffer pooling**: `sync.Pool` used in proxy.go and copy.go to reduce GC pressure.
- **Lazy init**: `sync.Once` in `client/dial.go` for one-time multiport address parsing.
- **Session scavenging**: Client periodically purges expired KCP sessions (controlled by `--autoexpire` and `--scavengettl` flags).
