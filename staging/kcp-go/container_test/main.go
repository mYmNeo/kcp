// Container integration test for kcp-go.
// Usage:
//
//	-mode server -addr :9999         (run echo server)
//	-mode client -addr server:9999   (run echo client, exit 0 on success)
package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/xtaci/kcp-go/v5"
	"golang.org/x/crypto/pbkdf2"
)

var (
	mode = flag.String("mode", "", "server or client")
	addr = flag.String("addr", ":9999", "listen/dial address")
)

func main() {
	flag.Parse()

	key := pbkdf2.Key([]byte("kcp-test-key"), []byte("kcp-test-salt"), 1024, 32, sha1.New)
	block, err := kcp.NewAESBlockCrypt(key)
	if err != nil {
		log.Fatal(err)
	}

	switch *mode {
	case "server":
		runServer(*addr, block)
	case "client":
		runClient(*addr, block)
	default:
		log.Fatalf("unknown mode %q, expected server or client", *mode)
	}
}

func runServer(addr string, block kcp.BlockCrypt) {
	ln, err := kcp.ListenWithOptions(addr, block, 0, 0)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	log.Printf("server listening on %s", addr)
	fmt.Println("READY") // signal to docker-compose healthcheck

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			return
		}
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 65536)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("read: %v", err)
			}
			return
		}
		_, err = conn.Write(buf[:n])
		if err != nil {
			log.Printf("write: %v", err)
			return
		}
	}
}

func runClient(addr string, block kcp.BlockCrypt) {
	// Retry connecting with backoff since the server container may not be ready.
	var conn *kcp.UDPSession
	var err error
	for i := 0; i < 10; i++ {
		conn, err = kcp.DialWithOptions(addr, block, 0, 0)
		if err == nil {
			break
		}
		log.Printf("dial attempt %d: %v", i+1, err)
		time.Sleep(time.Duration(i+1) * 200 * time.Millisecond)
	}
	if err != nil {
		log.Fatalf("failed to dial after retries: %v", err)
	}
	defer conn.Close()

	// Configure for low latency.
	conn.SetNoDelay(1, 10, 2, 1)
	conn.SetWindowSize(128, 128)

	payload := []byte("kcp container integration test payload - round trip verification")
	buf := make([]byte, len(payload))

	// Run 100 round-trips to exercise the protocol.
	for i := 0; i < 100; i++ {
		_, err = conn.Write(payload)
		if err != nil {
			log.Fatalf("write %d: %v", i, err)
		}

		_, err = io.ReadFull(conn, buf)
		if err != nil {
			log.Fatalf("read %d: %v", i, err)
		}

		if string(buf) != string(payload) {
			log.Fatalf("round-trip %d: data mismatch\ngot:  %s\nwant: %s", i, buf, payload)
		}
	}

	fmt.Println("PASS: 100 round-trips verified")
}
