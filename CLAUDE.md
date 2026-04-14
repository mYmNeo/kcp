# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

kcptun is a Go network tunneling tool that wraps TCP connections with KCP (UDP-based reliable transport) and SMUX stream multiplexing to improve performance over congested links. It operates as a client-server pair.

## Build Commands

```bash
# Build client and server (linux amd64 + arm64, darwin arm64)
./build.sh

# Build manually
go build -ldflags "-X main.VERSION=$(date -u +%Y%m%d) -s -w" -o build/client_linux_amd64 github.com/xtaci/kcptun/client
go build -ldflags "-X main.VERSION=$(date -u +%Y%m%d) -s -w" -o build/server_linux_amd64 github.com/xtaci/kcptun/server

# Multi-platform release build with UPX compression
./build-release.sh
```

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

## Architecture

**Three main packages:**

- **`client/`** — Listens on a local TCP port (default `:12948`), dials a remote KCP server, creates a SMUX multiplexer over the KCP session, and forwards data bidirectionally between local TCP connections and SMUX streams.

- **`server/`** — Listens on a UDP port (default `:29900`), accepts KCP sessions from clients, demultiplexes SMUX streams, and forwards each stream to a target (TCP address, Unix socket, or SOCKS5 proxy).

- **`std/`** — Shared library used by both client and server:
  - `config.go` — `BaseConfig` struct embedded by both ClientConfig and ServerConfig; predefined KCP mode profiles (normal, fast, fast2, fast3)
  - `crypt.go` — Cipher registry mapping names (aes, salsa20, blowfish, etc.) to `kcp.BlockCrypt` implementations via PBKDF2 key derivation
  - `copy.go` — Optimized bidirectional I/O forwarding (`Copy`/`Pipe`) using `io.WriterTo`/`io.ReaderFrom` interfaces
  - `proxy.go` — SOCKS5 protocol implementation (RFC 1928) with buffer pooling
  - `multiport.go` — Parses `host:min-max` port range format for multiport dialing
  - `comp.go` — Snappy compression wrapper
  - `smuxcfg.go` — SMUX configuration (v1/v2 selection, buffer sizes)

**Data flow:**
```
App → Client (TCP :12948) → [KCP/UDP + SMUX over internet] → Server (UDP :29900) → Target service
```

## Key Patterns

- **Configuration**: CLI flags (`urfave/cli`) with optional JSON config file override (`-c config.json`). Both client and server embed `std.BaseConfig` for shared KCP/SMUX parameters.
- **Platform-specific files**: Build-constrained files for conntrack (`contrack_darwin.go`, `contrack_linux.go`).
- **Version injection**: Build-time linker flags set `main.VERSION` and `main.SALT`.
- **Buffer pooling**: `sync.Pool` used in proxy.go and copy.go to reduce GC pressure.
- **Lazy init**: `sync.Once` in `client/dial.go` for one-time multiport address parsing.
