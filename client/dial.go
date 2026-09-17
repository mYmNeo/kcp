// The MIT License (MIT)
//
// # Copyright (c) 2016 xtaci
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package main

import (
	"crypto/rand"
	"encoding/binary"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"
	"github.com/xtaci/kcptun/carrier"
	"github.com/xtaci/kcptun/std"
)

var (
	multiPort           *std.MultiPort
	multiPortParseError error
	multiPortOnce       sync.Once
)

// resolveRemoteAddr validates the configured server address and picks a random
// destination port inside its range. Both transports resolve through it, so
// range validation and the random-port-per-dial semantics stay identical, and
// the multiport definition is parsed once for the lifetime of the process.
func resolveRemoteAddr(config *Config) (string, error) {
	// Parse the multiPort definition only once.
	multiPortOnce.Do(func() {
		multiPort, multiPortParseError = std.ParseMultiPort(config.RemoteAddr)
	})

	// Abort when the multiPort definition is invalid.
	if multiPortParseError != nil {
		return "", multiPortParseError
	}

	// Pick a random destination port within the configured range.
	var randport uint64
	err := binary.Read(rand.Reader, binary.LittleEndian, &randport)
	if err != nil {
		return "", err
	}

	port := uint64(multiPort.MinPort) + randport%uint64(multiPort.MaxPort-multiPort.MinPort+1)
	return net.JoinHostPort(strings.Trim(multiPort.Host, "[]"), strconv.FormatUint(port, 10)), nil
}

// dial establishes a connection to the configured remote endpoint over UDP.
func dial(config *Config, block kcp.BlockCrypt) (*kcp.UDPSession, error) {
	remoteAddr, err := resolveRemoteAddr(config)
	if err != nil {
		return nil, err
	}

	return dialUDP(config, block, remoteAddr)
}

// dialUDP opens a KCP session on a fresh UDP socket.
func dialUDP(config *Config, block kcp.BlockCrypt, remoteAddr string) (*kcp.UDPSession, error) {
	return kcp.DialWithOptions(remoteAddr, block, config.DataShard, config.ParityShard)
}

// dialCarrier opens a KCP session over the TCP carrier.
//
// The carrier is a net.PacketConn just like a UDP socket, so KCP is unaware of
// the transport underneath it: the session is created with the same conversation
// id discipline and FEC parameters as the UDP path.
func dialCarrier(config *Config, block kcp.BlockCrypt) (*kcp.UDPSession, error) {
	remoteAddr, err := resolveRemoteAddr(config)
	if err != nil {
		return nil, err
	}

	var convid uint32
	if err := binary.Read(rand.Reader, binary.LittleEndian, &convid); err != nil {
		return nil, err
	}

	// QueueDepth is left to the carrier's own default, which the server also
	// uses, so both ends of a session apply backpressure at the same point. It
	// is deliberately not tied to --sndwnd: chSend is allocated with this
	// capacity per stream, so letting a KCP window of 1024 set it would multiply
	// memory by 4 across a listener's whole stream budget for no gain in
	// throughput. The flag remains the explicit override.
	//
	// Streams is the striping fan-out: several TCP streams per KCP session, so
	// that one stalled stream cannot freeze the session. It stays at a single
	// stream unless asked for, because striping pays off on lossy or long-haul
	// paths rather than on a healthy one.
	pconn, err := carrier.Dial("tcp", remoteAddr, carrier.Config{
		Secret:     config.CarrierSecret,
		Streams:    config.CarrierStreams,
		QueueDepth: config.CarrierQueueDepth,
		KeepAlive:  time.Duration(config.KeepAlive) * time.Second,
	})
	if err != nil {
		return nil, err
	}

	// The read loop accepts a datagram only when its source matches the address
	// handed to NewConn4, so pass the exact net.Addr carrier.Conn.ReadFrom
	// reports. ownConn releases the carrier's TCP streams when the session
	// closes.
	sess, err := kcp.NewConn4(convid, pconn.RemoteAddr(), block, config.DataShard, config.ParityShard, true, pconn)
	if err != nil {
		pconn.Close()
		return nil, err
	}
	return sess, nil
}
