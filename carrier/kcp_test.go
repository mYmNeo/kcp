package carrier

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"
)

// TestKCPOverCarrier is the integration contract: the carrier must be usable as
// the net.PacketConn behind a real KCP session, which is the only reason it
// exists. It stands up both halves the way the client and server do — ServeConn
// on the listening side, NewConn4 on the dialing side — and then moves data
// through the resulting sessions.
//
// This is what pins the peer-address semantics. kcp-go's listener keys sessions
// on the address string and its client-side read loop discards datagrams from
// an unexpected source, so the carrier must report one stable address per peer
// on the receiving side and the exact dialed address on the sending side.
func TestKCPOverCarrier(t *testing.T) {
	cases := []struct {
		name    string
		block   kcp.BlockCrypt
		shards  [2]int
		streams int
	}{
		{name: "plain", block: nil, shards: [2]int{0, 0}, streams: 1},
		{name: "fec-no-crypt", block: nil, shards: [2]int{10, 3}, streams: 1},
		{name: "fec-and-crypt", block: aesBlock(t), shards: [2]int{10, 3}, streams: 1},
		{name: "striped", block: aesBlock(t), shards: [2]int{10, 3}, streams: 4},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				Secret:      testSecret,
				Streams:     tc.streams,
				QueueDepth:  64,
				DialTimeout: 5 * time.Second,
				KeepAlive:   -1,
			}

			ln, err := ListenWithConfig("tcp", "127.0.0.1:0", cfg)
			if err != nil {
				t.Fatalf("ListenWithConfig: %v", err)
			}
			defer ln.Close()

			// Server half: exactly what server/main.go does on the --tcp path.
			lis, err := kcp.ServeConn(tc.block, tc.shards[0], tc.shards[1], ln)
			if err != nil {
				t.Fatalf("ServeConn: %v", err)
			}
			defer lis.Close()

			// Client half: exactly what client/main.go does on the --tcp path.
			pconn, err := Dial("tcp", ln.Addr().String(), cfg)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			defer pconn.Close()

			var convid uint32
			if err := binary.Read(rand.Reader, binary.LittleEndian, &convid); err != nil {
				t.Fatalf("convid: %v", err)
			}

			client, err := kcp.NewConn4(convid, pconn.RemoteAddr(), tc.block,
				tc.shards[0], tc.shards[1], true, pconn)
			if err != nil {
				t.Fatalf("NewConn4: %v", err)
			}
			defer client.Close()

			client.SetStreamMode(true)
			client.SetWriteDelay(false)
			client.SetNoDelay(1, 10, 2, 1)
			client.SetWindowSize(128, 128)
			client.SetMtu(1350)
			client.SetACKNoDelay(true)

			// Client -> server: a payload big enough to span many datagrams, so
			// the test exercises splitting, windowing and sequencing rather than
			// a single packet.
			//
			// The write starts before the accept because a fresh KCP session is
			// silent until it has data to send: the first datagram the listener
			// ever sees is this payload, and that is what creates the session.
			// The production server accepts in a loop for exactly this reason.
			payload := make([]byte, 512<<10)
			if _, err := rand.Read(payload); err != nil {
				t.Fatalf("rand: %v", err)
			}
			want := sha256.Sum256(payload)

			go func() {
				if _, err := client.Write(payload); err != nil {
					t.Errorf("client Write: %v", err)
				}
			}()

			// The server must accept a session for the carrier peer.
			lis.SetDeadline(time.Now().Add(10 * time.Second))
			server, err := lis.AcceptKCP()
			if err != nil {
				t.Fatalf("AcceptKCP: %v", err)
			}
			defer server.Close()

			server.SetStreamMode(true)
			server.SetWriteDelay(false)
			server.SetNoDelay(1, 10, 2, 1)
			server.SetWindowSize(128, 128)
			server.SetMtu(1350)
			server.SetACKNoDelay(true)

			got := make([]byte, len(payload))
			if _, err := io.ReadFull(server, got); err != nil {
				t.Fatalf("server ReadFull: %v", err)
			}
			if sha256.Sum256(got) != want {
				t.Fatal("client -> server payload corrupted in transit")
			}

			// Server -> client: proves the return path, which is the direction
			// that depends on the carrier reporting the peer address KCP can
			// route a reply to.
			reply := make([]byte, 128<<10)
			if _, err := rand.Read(reply); err != nil {
				t.Fatalf("rand: %v", err)
			}
			replyWant := sha256.Sum256(reply)

			go func() {
				if _, err := server.Write(reply); err != nil {
					t.Errorf("server Write: %v", err)
				}
			}()

			replyGot := make([]byte, len(reply))
			if _, err := io.ReadFull(client, replyGot); err != nil {
				t.Fatalf("client ReadFull: %v", err)
			}
			if sha256.Sum256(replyGot) != replyWant {
				t.Fatal("server -> client payload corrupted in transit")
			}
		})
	}
}

// TestKCPMultipleSessionsOverCarrier pins that two independent dialers get two
// independent KCP sessions from one carrier listener. If the carrier collapsed
// distinct peers into one address, kcp-go would hand both clients' datagrams to
// one session and this would corrupt or stall.
func TestKCPMultipleSessionsOverCarrier(t *testing.T) {
	cfg := Config{Secret: testSecret, Streams: 2, QueueDepth: 32, DialTimeout: 5 * time.Second, KeepAlive: -1}

	ln, err := ListenWithConfig("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("ListenWithConfig: %v", err)
	}
	defer ln.Close()

	lis, err := kcp.ServeConn(nil, 0, 0, ln)
	if err != nil {
		t.Fatalf("ServeConn: %v", err)
	}
	defer lis.Close()

	addr := ln.Addr().String()
	const sessions = 2

	clients := make([]*kcp.UDPSession, sessions)
	for i := range clients {
		pconn, err := Dial("tcp", addr, cfg)
		if err != nil {
			t.Fatalf("Dial %d: %v", i, err)
		}
		defer pconn.Close()

		var convid uint32
		binary.Read(rand.Reader, binary.LittleEndian, &convid)

		c, err := kcp.NewConn4(convid, pconn.RemoteAddr(), nil, 0, 0, true, pconn)
		if err != nil {
			t.Fatalf("NewConn4 %d: %v", i, err)
		}
		defer c.Close()

		c.SetStreamMode(true)
		c.SetNoDelay(1, 10, 2, 1)
		c.SetWindowSize(128, 128)
		c.SetMtu(1350)
		clients[i] = c
	}

	// Each client sends a distinct, self-identifying payload. Acceptance order
	// is not tied to dial order, so the test verifies the real invariant
	// instead: every session must deliver exactly one client's payload, whole
	// and unmixed. If the carrier merged distinct peers into one address, kcp-go
	// would feed both clients' datagrams to one session and a read would come
	// back mixed or short.
	payloads := make([][]byte, sessions)
	for i := range payloads {
		payloads[i] = bytes.Repeat([]byte{byte('A' + i)}, 64<<10)
	}
	for i, c := range clients {
		go func(c *kcp.UDPSession, p []byte) {
			if _, err := c.Write(p); err != nil {
				t.Errorf("client %d Write: %v", i, err)
			}
		}(c, payloads[i])
	}

	servers := make([]*kcp.UDPSession, 0, sessions)
	lis.SetDeadline(time.Now().Add(15 * time.Second))
	for i := 0; i < sessions; i++ {
		s, err := lis.AcceptKCP()
		if err != nil {
			t.Fatalf("AcceptKCP %d: %v", i, err)
		}
		defer s.Close()
		s.SetStreamMode(true)
		s.SetNoDelay(1, 10, 2, 1)
		s.SetWindowSize(128, 128)
		s.SetMtu(1350)
		servers = append(servers, s)
	}

	// Read one payload per accepted session, concurrently, and check that what
	// arrives is exactly one client's payload.
	type seen struct {
		idx int
		tag byte
	}
	results := make(chan seen, sessions)
	for i, s := range servers {
		go func(i int, s *kcp.UDPSession) {
			buf := make([]byte, 64<<10)
			s.SetReadDeadline(time.Now().Add(15 * time.Second))
			if _, err := io.ReadFull(s, buf); err != nil {
				t.Errorf("server %d ReadFull: %v", i, err)
				results <- seen{-1, 0}
				return
			}
			// The payload must be uniform: a mix means two sessions collided.
			tag := buf[0]
			for _, b := range buf {
				if b != tag {
					t.Errorf("server %d received a payload mixing multiple clients", i)
					results <- seen{-1, 0}
					return
				}
			}
			results <- seen{i, tag}
		}(i, s)
	}

	got := make(map[byte]int, sessions)
	for i := 0; i < sessions; i++ {
		r := <-results
		if r.idx < 0 {
			t.Fatal("a session failed to deliver its payload")
		}
		got[r.tag]++
	}

	// Every client's distinct payload must show up exactly once, across
	// sessions that were accepted in an arbitrary order.
	for i := range payloads {
		tag := byte('A' + i)
		if got[tag] != 1 {
			t.Fatalf("payload %q delivered %d times across sessions, want exactly 1", tag, got[tag])
		}
	}
	if len(got) != sessions {
		t.Fatalf("observed %d distinct payloads across %d sessions", len(got), sessions)
	}
}

// aesBlock returns an AES cipher for the crypt-variants of the integration test.
func aesBlock(t *testing.T) kcp.BlockCrypt {
	t.Helper()

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand key: %v", err)
	}
	block, err := kcp.NewAESBlockCrypt(key)
	if err != nil {
		t.Fatalf("NewAESBlockCrypt: %v", err)
	}
	return block
}

// TestCarrierAddressDistinguishesPeers pins the property the integration test
// depends on: the address a datagram reports is a function of its session, so
// one dialer's streams collapse to a single address while two dialers stay
// distinct. KCP keys its sessions on that string, so collapsing two peers into
// one address would feed both clients' datagrams to a single session.
//
// The stability half forces every stream of a peer to agree; the
// differentiation half is the part a regression could actually break.
func TestCarrierAddressDistinguishesPeers(t *testing.T) {
	cfg := Config{Secret: testSecret, Streams: 4, QueueDepth: 8, DialTimeout: 5 * time.Second, KeepAlive: -1}

	ln, err := ListenWithConfig("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("ListenWithConfig: %v", err)
	}
	defer ln.Close()

	// dial writes one datagram per stream so every stream of the dialer has to
	// contribute an address. Each datagram carries a tag identifying its sender.
	dial := func(tag byte) {
		t.Helper()
		c, err := Dial("tcp", ln.Addr().String(), cfg)
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		t.Cleanup(func() { c.Close() })
		for i := 0; i < cfg.Streams*2; i++ {
			if _, err := c.WriteTo([]byte{tag, byte(i)}, c.RemoteAddr()); err != nil {
				t.Fatalf("WriteTo: %v", err)
			}
		}
	}

	dial('A')
	dial('B')

	// Collect the address each datagram arrived from, keyed by the tag its
	// sender wrote, so the two dialers' addresses can be compared.
	seen := map[byte]string{}
	buf := make([]byte, 64)
	for i := 0; i < cfg.Streams*4; i++ {
		ln.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, addr, err := ln.ReadFrom(buf)
		if err != nil {
			t.Fatalf("ReadFrom %d: %v", i, err)
		}
		if n < 2 {
			t.Fatalf("ReadFrom %d: short datagram, %d bytes", i, n)
		}
		tag, got := buf[0], addr.String()
		if prev, ok := seen[tag]; ok && prev != got {
			t.Fatalf("dialer %q reported two addresses (%q then %q): streams of one session must share one",
				tag, prev, got)
		}
		seen[tag] = got
	}

	if len(seen) != 2 {
		t.Fatalf("heard from %d dialer(s), want 2", len(seen))
	}
	if seen['A'] == seen['B'] {
		t.Fatalf("both dialers reported %q: distinct sessions must not collapse into one address", seen['A'])
	}
}

// TestRemoteAddrUsableAsKCPRemote pins that the address the dialing side hands to
// kcp.NewConn4 — its own RemoteAddr — is the value its read loop accepts, which
// is why the client wiring passes pconn.RemoteAddr() rather than the raw string
// it dialed.
func TestRemoteAddrUsableAsKCPRemote(t *testing.T) {
	cfg := Config{Secret: testSecret, Streams: 2, QueueDepth: 8, DialTimeout: 5 * time.Second, KeepAlive: -1}

	ln, err := ListenWithConfig("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("ListenWithConfig: %v", err)
	}
	defer ln.Close()

	c, err := Dial("tcp", ln.Addr().String(), cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	if c.RemoteAddr() == nil {
		t.Fatal("RemoteAddr is nil")
	}
	if got, want := c.RemoteAddr().String(), ln.Addr().String(); got != want {
		t.Fatalf("RemoteAddr = %q, want the dialed address %q", got, want)
	}

	// A datagram written to that address must arrive, which is the property
	// kcp-go's session depends on when it fills in its send queue address.
	if _, err := c.WriteTo([]byte("directed"), c.RemoteAddr()); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	buf := make([]byte, 64)
	ln.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, _, err := ln.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if string(buf[:n]) != "directed" {
		t.Fatalf("read %q", buf[:n])
	}
}

// TestNetPacketConnInterface pins the compile-time contract the wiring relies
// on: both endpoints must satisfy net.PacketConn, because that is the parameter
// type kcp.ServeConn and kcp.NewConn4 accept.
func TestNetPacketConnInterface(t *testing.T) {
	var _ net.PacketConn = (*Conn)(nil)
	var _ net.PacketConn = (*Listener)(nil)
}
