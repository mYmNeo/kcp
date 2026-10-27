#!/usr/bin/env bash
# Collect a CPU profile from a live kcptun client and server, install it as
# default.pgo, and rebuild release packages with ./build.sh (-pgo=auto).
#
# Both binaries serve net/http/pprof on 127.0.0.1:6060, so they cannot be
# profiled together. The same bulk echo runs twice — once sampling the server,
# once sampling the client — and go tool pprof merges the protobuf profiles.
# -pgo=auto reads client/default.pgo and server/default.pgo, so the merged
# profile is installed next to each main package (gitignored via *.pgo).
#
# Workload: both sides get the same flags, matching test.json: FEC off,
# --nocomp, windows 2048/8192, 16 MiB socket and smux buffers, 8 MiB stream
# buffer, smux v2, frame size 8192, DSCP 46, and --acknodelay. --conn 1 is
# client-only. -mode ${PGO_MODE:-fast3} is applied to both. Each echoed TCP
# stream writes and reads at the same time.
#
#   PGO_SECONDS   CPU sample length per process, in seconds (default 30)
#   PGO_CONNS     parallel echoed TCP streams (default 4)
#   PGO_MODE      value passed to kcptun -mode (default fast3)
#   KCPTUN_KEY    pre-shared key; a random one is generated when unset
#   SALT          PBKDF2 salt build.sh bakes into release binaries via
#                 -X main.SALT; build.sh generates a random one when unset
#   SINK_PORT     TCP echo target behind the server (default 43991)
#   KCP_PORT      UDP port the server listens on (default 43990)
#   LOCAL_PORT    TCP port the client listens on (default 43992)
set -euo pipefail

ROOT=$(cd "$(dirname "$0")" && pwd)
cd "$ROOT"

PGO_SECONDS=${PGO_SECONDS:-30}
PGO_CONNS=${PGO_CONNS:-4}
PGO_MODE=${PGO_MODE:-fast3}
SINK_PORT=${SINK_PORT:-43991}
KCP_PORT=${KCP_PORT:-43990}
LOCAL_PORT=${LOCAL_PORT:-43992}

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/kcptun-pgo.XXXXXX")
SESSION_PIDS=()
SINK_PID=""

dump_logs() {
	local f
	shopt -s nullglob
	for f in "$WORKDIR"/*.log "$WORKDIR"/*.out; do
		echo "===== $(basename "$f") =====" >&2
		tail -n 40 "$f" >&2 || true
	done
}

die() {
	trap - ERR
	echo "pgo.sh: $*" >&2
	dump_logs
	exit 1
}

cleanup() {
	trap - ERR
	local pid
	if ((${#SESSION_PIDS[@]} > 0)); then
		for pid in "${SESSION_PIDS[@]}"; do
			kill -TERM "$pid" 2>/dev/null || true
		done
	fi
	if [[ -n "$SINK_PID" ]]; then
		kill -TERM "$SINK_PID" 2>/dev/null || true
	fi
	sleep 0.2
	if ((${#SESSION_PIDS[@]} > 0)); then
		for pid in "${SESSION_PIDS[@]}"; do
			kill -KILL "$pid" 2>/dev/null || true
			wait "$pid" 2>/dev/null || true
		done
	fi
	if [[ -n "$SINK_PID" ]]; then
		kill -KILL "$SINK_PID" 2>/dev/null || true
		wait "$SINK_PID" 2>/dev/null || true
	fi
	rm -rf "$WORKDIR"
}

on_err() {
	trap - ERR
	echo "pgo.sh failed" >&2
	dump_logs
}

trap on_err ERR
trap cleanup EXIT

if ! [[ "$PGO_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
	die "PGO_SECONDS must be a positive integer"
fi
if ! [[ "$PGO_CONNS" =~ ^[1-9][0-9]*$ ]]; then
	die "PGO_CONNS must be a positive integer"
fi
for port in "$SINK_PORT" "$KCP_PORT" "$LOCAL_PORT"; do
	if ! [[ "$port" =~ ^[1-9][0-9]*$ ]] || ((port < 1 || port > 65535)); then
		die "invalid port: $port"
	fi
done
if ((SINK_PORT == KCP_PORT || SINK_PORT == LOCAL_PORT || KCP_PORT == LOCAL_PORT)); then
	die "SINK_PORT, KCP_PORT, and LOCAL_PORT must be distinct"
fi
if ((SINK_PORT == 6060 || KCP_PORT == 6060 || LOCAL_PORT == 6060)); then
	die "port 6060 is reserved for pprof"
fi

command -v go >/dev/null 2>&1 || die "go is required"
command -v ss >/dev/null 2>&1 || die "ss (iproute2) is required"
command -v tar >/dev/null 2>&1 || die "tar is required"
command -v gofmt >/dev/null 2>&1 || die "gofmt is required"
# PATH grep is ripgrep on some machines; ripgrep rejects grep -E. This script
# already requires Linux (ss), so use the system grep.
if [[ -x /usr/bin/grep ]]; then
	GREP=/usr/bin/grep
elif [[ -x /bin/grep ]]; then
	GREP=/bin/grep
else
	die "GNU grep is required"
fi

if [[ -z "${KCPTUN_KEY:-}" ]]; then
	KCPTUN_KEY=$(dd if=/dev/urandom bs=24 count=1 2>/dev/null | base64 | tr -d '\n')
fi
export KCPTUN_KEY

listening() {
	local proto=$1 port=$2 out
	# ss -H prints one line per socket and nothing when the port is free.
	if [[ "$proto" == tcp ]]; then
		out=$(ss -H -ltn "sport = :${port}" 2>/dev/null || true)
	else
		out=$(ss -H -lun "sport = :${port}" 2>/dev/null || true)
	fi
	[[ -n "$out" ]]
}

ensure_free() {
	local proto=$1 port=$2
	if listening "$proto" "$port"; then
		die "${proto} port ${port} is already in use"
	fi
}

wait_listen() {
	local proto=$1 port=$2 i
	for ((i = 0; i < 200; i++)); do
		if listening "$proto" "$port"; then
			return 0
		fi
		if ((${#SESSION_PIDS[@]} > 0)) && ! kill -0 "${SESSION_PIDS[0]}" 2>/dev/null; then
			die "server exited while waiting for ${proto} ${port}"
		fi
		if ((${#SESSION_PIDS[@]} > 1)) && ! kill -0 "${SESSION_PIDS[1]}" 2>/dev/null; then
			die "client exited while waiting for ${proto} ${port}"
		fi
		if [[ -n "$SINK_PID" ]] && ! kill -0 "$SINK_PID" 2>/dev/null; then
			die "echo sink exited while waiting for ${proto} ${port}"
		fi
		sleep 0.1
	done
	die "timed out waiting for ${proto} port ${port}"
}

stop_session() {
	local pid i alive
	if ((${#SESSION_PIDS[@]} == 0)); then
		return 0
	fi
	for pid in "${SESSION_PIDS[@]}"; do
		kill -TERM "$pid" 2>/dev/null || true
	done
	for ((i = 0; i < 50; i++)); do
		alive=0
		for pid in "${SESSION_PIDS[@]}"; do
			if kill -0 "$pid" 2>/dev/null; then
				alive=1
			fi
		done
		if ((alive == 0)); then
			break
		fi
		sleep 0.1
	done
	for pid in "${SESSION_PIDS[@]}"; do
		kill -KILL "$pid" 2>/dev/null || true
		wait "$pid" 2>/dev/null || true
	done
	SESSION_PIDS=()
	for ((i = 0; i < 50; i++)); do
		if ! listening tcp "$LOCAL_PORT" && ! listening udp "$KCP_PORT" && ! listening tcp 6060; then
			return 0
		fi
		sleep 0.1
	done
	die "ports stayed busy after stopping kcptun"
}

sample_seconds() {
	local line num unit
	while IFS= read -r line; do
		# pprof prints "230ms", "1.50s", "2s" — the unit is part of the value.
		if [[ "$line" =~ Total\ samples\ =\ ([0-9]+([.][0-9]+)?)(ns|us|ms|s)? ]]; then
			num=${BASH_REMATCH[1]}
			unit=${BASH_REMATCH[3]}
			awk -v n="$num" -v u="$unit" 'BEGIN {
				scale = 1
				if (u == "ms") scale = 0.001
				else if (u == "us") scale = 0.000001
				else if (u == "ns") scale = 0.000000001
				printf "%.3f", n * scale
			}'
			return 0
		fi
	done < <(go tool pprof -top -nodecount=1 "$1" </dev/null 2>"$WORKDIR/pprof-top.log" || true)
	return 0
}

require_samples() {
	local got
	got=$(sample_seconds "$1")
	if [[ -z "$got" || "$got" == 0 || "$got" == 0.* ]]; then
		die "no CPU samples in $1"
	fi
	echo "  $(basename "$1"): ${got}s of CPU samples"
}

write_bench() {
	mkdir -p "$WORKDIR/benchsrc"
	cat >"$WORKDIR/benchsrc/main.go" <<'EOF'
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	mode := flag.String("mode", "", "sink or drive")
	addr := flag.String("addr", "127.0.0.1:43991", "listen or dial address")
	conns := flag.Int("conns", 4, "parallel connections")
	dur := flag.Duration("duration", 45*time.Second, "drive duration")
	flag.Parse()

	var err error
	switch *mode {
	case "sink":
		err = sink(*addr)
	case "drive":
		err = drive(*addr, *conns, *dur)
	default:
		err = errors.New("set -mode sink or -mode drive")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func sink(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go echo(c)
	}
}

func echo(c net.Conn) {
	defer c.Close()
	_, _ = io.Copy(c, c)
}

func drive(addr string, conns int, dur time.Duration) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	timer := time.AfterFunc(dur, cancel)
	defer timer.Stop()

	payload := make([]byte, 32*1024)
	state := uint32(0xA5A5A5A5)
	for i := range payload {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		payload[i] = byte(state)
	}

	var (
		transferred atomic.Int64
		ready       sync.Once
		errMu       sync.Mutex
		lastErr     error
		wg          sync.WaitGroup
	)
	note := func(err error) {
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		errMu.Lock()
		lastErr = err
		errMu.Unlock()
	}

	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pump(ctx, addr, payload, &transferred, &ready, note)
		}()
	}
	wg.Wait()

	n := transferred.Load()
	fmt.Printf("bytes %d\n", n)
	if n == 0 {
		errMu.Lock()
		err := lastErr
		errMu.Unlock()
		if err == nil {
			err = errors.New("benchmark transferred no bytes")
		}
		return err
	}
	return nil
}

func pump(ctx context.Context, addr string, payload []byte, transferred *atomic.Int64, ready *sync.Once, note func(error)) {
	buf := append([]byte(nil), payload...)
	readBuf := make([]byte, len(buf))
	for ctx.Err() == nil {
		dialCtx, dialCancel := context.WithTimeout(ctx, 2*time.Second)
		var dialer net.Dialer
		conn, err := dialer.DialContext(dialCtx, "tcp", addr)
		dialCancel()
		if err != nil {
			note(err)
			if !sleepCtx(ctx, 50*time.Millisecond) {
				return
			}
			continue
		}
		exchange(ctx, conn, buf, readBuf, transferred, ready, note)
		_ = conn.Close()
	}
}

func exchange(ctx context.Context, conn net.Conn, buf, readBuf []byte, transferred *atomic.Int64, ready *sync.Once, note func(error)) {
	// Unblock a Read or Write stuck on a full window when the drive stops.
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			n, err := conn.Write(buf)
			if n > 0 {
				transferred.Add(int64(n))
			}
			if err != nil {
				note(err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		scratch := readBuf
		for ctx.Err() == nil {
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			n, err := conn.Read(scratch)
			if n > 0 {
				transferred.Add(int64(n))
				ready.Do(func() { fmt.Fprintln(os.Stderr, "ready") })
			}
			if err != nil {
				note(err)
				return
			}
		}
	}()
	wg.Wait()
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
EOF
	gofmt -w "$WORKDIR/benchsrc/main.go"
	(
		cd "$WORKDIR/benchsrc"
		go mod init kcptun-pgo-bench >/dev/null
		CGO_ENABLED=0 go build -o "$WORKDIR/bench" .
	)
}

build_kcptun() {
	local pgo=$1
	echo "Building client and server with -pgo=${pgo}"
	CGO_ENABLED=0 go build -pgo="$pgo" -o "$WORKDIR/kcptun-server" ./server
	CGO_ENABLED=0 go build -pgo="$pgo" -o "$WORKDIR/kcptun-client" ./client
}

print_throughput() {
	local role=$1 bytes=$2
	awk -v role="$role" -v bytes="$bytes" -v secs="$PGO_SECONDS" 'BEGIN {
		printf "  %s pass echoed %.1f MiB (%.1f MiB/s, both directions)\n", role, bytes / 1048576, bytes / secs / 1048576
	}'
}

profile_pass() {
	local role=$1
	local server_cmd client_cmd drive_pid bytes i
	# Same values as test.json. conn is not a server flag.
	local -a shared=(
		--datashard 0
		--parityshard 0
		--rcvwnd 8192
		--sndwnd 2048
		--nocomp
		--sockbuf 16777216
		--smuxbuf 16777216
		--streambuf 8388608
		--smuxver 2
		--dscp 46
		--framesize 8192
		--acknodelay
	)
	server_cmd=(
		"$WORKDIR/kcptun-server"
		-l "127.0.0.1:${KCP_PORT}"
		-t "127.0.0.1:${SINK_PORT}"
		--mode "$PGO_MODE"
		--quiet
		"${shared[@]}"
	)
	client_cmd=(
		"$WORKDIR/kcptun-client"
		-l "127.0.0.1:${LOCAL_PORT}"
		-r "127.0.0.1:${KCP_PORT}"
		--mode "$PGO_MODE"
		--quiet
		--conn 1
		"${shared[@]}"
	)
	if [[ "$role" == server ]]; then
		server_cmd+=(--pprof)
	else
		client_cmd+=(--pprof)
	fi

	echo "Benchmarking with ${role} pprof on :6060 for ${PGO_SECONDS}s"

	"${server_cmd[@]}" >"$WORKDIR/${role}-server.log" 2>&1 </dev/null &
	SESSION_PIDS+=($!)
	"${client_cmd[@]}" >"$WORKDIR/${role}-client.log" 2>&1 </dev/null &
	SESSION_PIDS+=($!)

	wait_listen udp "$KCP_PORT"
	wait_listen tcp "$LOCAL_PORT"
	wait_listen tcp 6060

	"$WORKDIR/bench" -mode drive \
		-addr "127.0.0.1:${LOCAL_PORT}" \
		-conns "$PGO_CONNS" \
		-duration "$((PGO_SECONDS + 30))s" \
		>"$WORKDIR/${role}-drive.out" 2>"$WORKDIR/${role}-drive.log" </dev/null &
	drive_pid=$!
	SESSION_PIDS+=("$drive_pid")

	for ((i = 0; i < 200; i++)); do
		if "$GREP" -q '^ready$' "$WORKDIR/${role}-drive.log"; then
			break
		fi
		if ! kill -0 "$drive_pid" 2>/dev/null; then
			die "benchmark exited before the tunnel carried data (${role})"
		fi
		sleep 0.1
	done
	if ! "$GREP" -q '^ready$' "$WORKDIR/${role}-drive.log"; then
		die "timed out waiting for traffic through the tunnel (${role})"
	fi

	if ! go tool pprof -proto \
		-output "$WORKDIR/${role}.pprof" \
		-seconds "$PGO_SECONDS" \
		-timeout "$((PGO_SECONDS + 60))" \
		"$WORKDIR/kcptun-${role}" \
		"http://127.0.0.1:6060/debug/pprof/profile" \
		>"$WORKDIR/${role}-pprof.log" 2>&1 </dev/null; then
		die "go tool pprof failed while profiling the ${role}"
	fi

	kill -TERM "$drive_pid" 2>/dev/null || true
	for ((i = 0; i < 80; i++)); do
		if ! kill -0 "$drive_pid" 2>/dev/null; then
			break
		fi
		sleep 0.1
	done
	kill -KILL "$drive_pid" 2>/dev/null || true
	wait "$drive_pid" 2>/dev/null || true

	bytes=$(awk '/^bytes / {print $2; exit}' "$WORKDIR/${role}-drive.out" || true)
	if ! [[ "${bytes:-}" =~ ^[0-9]+$ ]] || ((bytes < 1048576)); then
		die "benchmark moved too little traffic while profiling the ${role} (${bytes:-0} bytes)"
	fi
	print_throughput "$role" "$bytes"
	require_samples "$WORKDIR/${role}.pprof"

	stop_session
}

pgo_plan_uses_profile() {
	local main=$1 plan="$WORKDIR/${1}-plan.txt"
	# go build -n prints the recipe on stderr. preprofile consumes default.pgo.
	go build -pgo=auto -n -o /dev/null "./${main}" >"$plan" 2>&1
	if ! "$GREP" -q "preprofile" "$plan" || ! "$GREP" -q "${main}/default.pgo" "$plan"; then
		die "go build -pgo=auto did not apply ${main}/default.pgo"
	fi
}

newest_archive() {
	local name=$1
	local newest="" f
	shopt -s nullglob
	for f in "$ROOT"/build/kcptun-${name}-*.tar.gz; do
		if [[ -z "$newest" || "$f" -nt "$newest" ]]; then
			newest=$f
		fi
	done
	shopt -u nullglob
	if [[ -z "$newest" ]]; then
		die "build.sh did not produce build/kcptun-${name}-*.tar.gz"
	fi
	printf '%s\n' "$newest"
}

confirm_pgo_archive() {
	local os=$1 arch=$2 archive bin
	archive=$(newest_archive "${os}-${arch}")
	for bin in "server_${os}_${arch}" "client_${os}_${arch}"; do
		tar -xOf "$archive" "$bin" >"$WORKDIR/$bin"
		if ! go version -m "$WORKDIR/$bin" | "$GREP" -q -- '-pgo'; then
			die "$bin was not built with a PGO profile"
		fi
	done
	echo "PGO profile confirmed in $(basename "$archive")"
}

ensure_free tcp 6060
ensure_free tcp "$SINK_PORT"
ensure_free tcp "$LOCAL_PORT"
ensure_free udp "$KCP_PORT"

echo "Writing the echo benchmark"
write_bench

if [[ -f "$ROOT/client/default.pgo" && -f "$ROOT/server/default.pgo" ]]; then
	build_kcptun auto
else
	build_kcptun off
fi

"$WORKDIR/bench" -mode sink -addr "127.0.0.1:${SINK_PORT}" \
	>"$WORKDIR/sink.log" 2>&1 </dev/null &
SINK_PID=$!
wait_listen tcp "$SINK_PORT"

profile_pass server
profile_pass client

echo "Merging client and server profiles into default.pgo"
if ! go tool pprof -proto \
	-output "$WORKDIR/default.pgo" \
	"$WORKDIR/server.pprof" \
	"$WORKDIR/client.pprof" \
	>"$WORKDIR/merge-pprof.log" 2>&1 </dev/null; then
	die "go tool pprof failed to merge profiles"
fi
require_samples "$WORKDIR/default.pgo"

cp -f "$WORKDIR/default.pgo" "$ROOT/client/default.pgo"
cp -f "$WORKDIR/default.pgo" "$ROOT/server/default.pgo"

echo "Hottest functions in the merged profile:"
go tool pprof -top -nodecount=12 "$ROOT/server/default.pgo" </dev/null

echo "Checking that -pgo=auto selects default.pgo"
pgo_plan_uses_profile server
pgo_plan_uses_profile client

echo "Building optimized release packages with ./build.sh"
SALT="${SALT:-}" "$ROOT/build.sh"

confirm_pgo_archive linux amd64
confirm_pgo_archive linux arm64
confirm_pgo_archive darwin amd64
confirm_pgo_archive darwin arm64

echo "Installed merged profile:"
ls -lh "$ROOT/client/default.pgo" "$ROOT/server/default.pgo"
echo "Optimized packages are in ${ROOT}/build/"
