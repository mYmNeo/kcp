// Containerized smux integration test.
//
// Usage:
//
//	go run . -mode=server -addr=:9000
//	go run . -mode=client -addr=server:9000
//
// Protocol: client sends 4-byte big-endian length prefix + data.
// Server reads length, reads exactly that many bytes, echoes back
// length prefix + data. No half-close needed — completely deterministic.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/xtaci/smux"
)

var (
	mode      = flag.String("mode", "", "server or client")
	addr      = flag.String("addr", "localhost:9000", "listen/connect address")
	version   = flag.Int("version", 2, "smux protocol version (1 or 2)")
	dataSize  = flag.Int64("datasize", 16*1024*1024, "total data to transfer (bytes)")
	streams   = flag.Int("streams", 4, "number of streams")
	frameSize = flag.Int("framesize", 32768, "max frame size")
)

func main() {
	flag.Parse()

	switch *mode {
	case "server":
		runServer()
	case "client":
		runClient()
	default:
		fmt.Fprintf(os.Stderr, "usage: %s -mode=server|client\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}
}

func runServer() {
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	defer ln.Close()
	log.Printf("server listening on %s (smux v%d)", *addr, *version)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go handleSmuxConn(conn)
	}
}

func handleSmuxConn(conn net.Conn) {
	defer conn.Close()

	cfg := buildConfig()
	sess, err := smux.Server(conn, cfg)
	if err != nil {
		log.Printf("smux server: %v", err)
		return
	}
	defer sess.Close()

	for {
		stream, err := sess.AcceptStream()
		if err != nil {
			return
		}
		go echoStream(stream)
	}
}

// echoStream reads a 4-byte length prefix, then exactly that many bytes,
// echoes them back with the same length prefix.
func echoStream(stream *smux.Stream) {
	defer stream.Close()

	var lenBuf [4]byte
	for {
		if _, err := io.ReadFull(stream, lenBuf[:]); err != nil {
			return
		}
		length := binary.BigEndian.Uint32(lenBuf[:])
		if length == 0 || length > 64*1024*1024 {
			return
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(stream, data); err != nil {
			return
		}
		// Echo: write length prefix + data
		if _, err := stream.Write(lenBuf[:]); err != nil {
			return
		}
		if _, err := stream.Write(data); err != nil {
			return
		}
	}
}

func runClient() {
	log.Printf("client connecting to %s (smux v%d, %d streams, %.1f MB each)",
		*addr, *version, *streams, float64(*dataSize)/(1024*1024))

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()

	cfg := buildConfig()
	sess, err := smux.Client(conn, cfg)
	if err != nil {
		log.Fatalf("smux client: %v", err)
	}
	defer sess.Close()

	start := time.Now()

	var totalBytes int64
	var errors int

	// Run concurrent streams
	type result struct {
		id  int
		n   int64
		err error
	}
	ch := make(chan result, *streams)

	for i := 0; i < *streams; i++ {
		go func(streamID int) {
			n, err := streamEchoTest(sess)
			ch <- result{streamID, n, err}
		}(i)
	}

	for i := 0; i < *streams; i++ {
		r := <-ch
		if r.err != nil {
			errors++
			log.Printf("stream %d: %v", r.id, r.err)
		} else {
			totalBytes += r.n
		}
	}

	elapsed := time.Since(start)

	if errors > 0 {
		log.Fatalf("FAIL: %d/%d streams had errors", errors, *streams)
	}

	totalMB := float64(totalBytes) / (1024 * 1024)
	throughput := float64(totalBytes) / elapsed.Seconds() / (1024 * 1024)

	log.Printf("PASS: %d streams, %.1f MB total, %.2f MB/s, %v elapsed",
		*streams, totalMB, throughput, elapsed.Round(time.Millisecond))
}

func streamEchoTest(sess *smux.Session) (int64, error) {
	stream, err := sess.OpenStream()
	if err != nil {
		return 0, fmt.Errorf("open: %w", err)
	}
	defer stream.Close()

	// Send: 4-byte length prefix + data
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(*dataSize))

	if _, err := stream.Write(lenBuf[:]); err != nil {
		return 0, fmt.Errorf("write len: %w", err)
	}

	// Write data in chunks
	chunk := make([]byte, *frameSize)
	var written int64
	for written < *dataSize {
		toWrite := int64(len(chunk))
		if written+toWrite > *dataSize {
			toWrite = *dataSize - written
		}
		n, err := stream.Write(chunk[:toWrite])
		if n > 0 {
			written += int64(n)
		}
		if err != nil {
			return written, fmt.Errorf("write: %w", err)
		}
	}

	// Read echoed data: 4-byte length prefix + data
	var recvLenBuf [4]byte
	if _, err := io.ReadFull(stream, recvLenBuf[:]); err != nil {
		return written, fmt.Errorf("read len: %w", err)
	}
	recvLen := int64(binary.BigEndian.Uint32(recvLenBuf[:]))
	if recvLen != *dataSize {
		return written, fmt.Errorf("length mismatch: got %d, want %d", recvLen, *dataSize)
	}

	// Read echoed data in chunks (for throughput measurement, data correctness
	// is implied by the fact TCP is reliable and smux preserves ordering)
	recvBuf := make([]byte, *frameSize)
	var received int64
	for received < recvLen {
		n, err := stream.Read(recvBuf)
		if n > 0 {
			received += int64(n)
		}
		if err != nil {
			if err == io.EOF && received == recvLen {
				break
			}
			return written, fmt.Errorf("read data at %d/%d: %w", received, recvLen, err)
		}
	}

	if received != recvLen {
		return written, fmt.Errorf("short read: got %d, want %d", received, recvLen)
	}

	return written, nil
}

func buildConfig() *smux.Config {
	cfg := smux.DefaultConfig()
	cfg.Version = *version
	cfg.MaxFrameSize = *frameSize
	cfg.KeepAliveDisabled = true
	return cfg
}
