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
	"crypto/sha256"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/crypto/pbkdf2"

	"github.com/fatih/color"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
	kcp "github.com/xtaci/kcp-go/v5"
	"github.com/xtaci/kcptun/std"
	"github.com/xtaci/kcptun/std/shmmap"
	"github.com/xtaci/smux"
)

var SALT = "kcp-go"

const (
	// maxSmuxVer guards against negotiating unsupported smux protocol versions.
	maxSmuxVer = 2
	// scavengePeriod defines how frequently expired sessions are purged.
	scavengePeriod = 5
)

// VERSION is populated via build flags when packaging official binaries.
var VERSION = "SELFBUILD"

func main() {
	if VERSION == "SELFBUILD" {
		// Enable timestamps + file:line to simplify debugging self-built binaries.
		log.SetFlags(log.LstdFlags | log.Lshortfile)
	}

	myApp := cli.NewApp()
	myApp.Name = "kcptun"
	myApp.Usage = "client(with SMUX)"
	myApp.Version = VERSION
	myApp.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "localaddr,l",
			Value: ":12948",
			Usage: "local listen address",
		},
		cli.StringFlag{
			Name:  "remoteaddr, r",
			Value: "vps:29900",
			Usage: `kcp server address, eg: "IP:29900" a for single port, "IP:minport-maxport" for port range`,
		},
		cli.StringFlag{
			Name:   "key",
			Value:  "it's a secrect",
			Usage:  "pre-shared secret between client and server",
			EnvVar: "KCPTUN_KEY",
		},
		cli.StringFlag{
			Name:  "crypt",
			Value: "aes-128-gcm",
			Usage: "sm4, tea, aes-128, aes-128-gcm, aes-192, blowfish, twofish, cast5, 3des, xtea, salsa20",
		},
		cli.StringFlag{
			Name:  "mode",
			Value: "fast",
			Usage: "profiles: fast3, fast2, fast, normal, manual",
		},
		cli.IntFlag{
			Name:  "conn",
			Value: 1,
			Usage: "set num of UDP connections to server",
		},
		cli.IntFlag{
			Name:  "autoexpire",
			Value: 0,
			Usage: "set auto expiration time(in seconds) for a single UDP connection, 0 to disable",
		},
		cli.IntFlag{
			Name:  "scavengettl",
			Value: 600,
			Usage: "set how long an expired connection can live (in seconds)",
		},
		cli.IntFlag{
			Name:  "mtu",
			Value: 1350,
			Usage: "set maximum transmission unit for UDP packets",
		},
		cli.IntFlag{
			Name:  "ratelimit",
			Value: 0,
			Usage: "set maximum outgoing speed (in bytes per second) for a single KCP connection, 0 to disable. Also known as packet pacing",
		},
		cli.IntFlag{
			Name:  "sndwnd",
			Value: 128,
			Usage: "set send window size(num of packets)",
		},
		cli.IntFlag{
			Name:  "rcvwnd",
			Value: 512,
			Usage: "set receive window size(num of packets)",
		},
		cli.IntFlag{
			Name:  "datashard,ds",
			Value: 10,
			Usage: "set reed-solomon erasure coding - datashard",
		},
		cli.IntFlag{
			Name:  "parityshard,ps",
			Value: 3,
			Usage: "set reed-solomon erasure coding - parityshard",
		},
		cli.IntFlag{
			Name:  "dscp",
			Value: 0,
			Usage: "set DSCP(6bit)",
		},
		cli.BoolFlag{
			Name:  "nocomp",
			Usage: "disable compression",
		},
		cli.BoolFlag{
			Name:   "acknodelay",
			Usage:  "flush ack immediately when a packet is received",
			Hidden: true,
		},
		cli.IntFlag{
			Name:   "nodelay",
			Value:  0,
			Hidden: true,
		},
		cli.IntFlag{
			Name:   "interval",
			Value:  50,
			Hidden: true,
		},
		cli.IntFlag{
			Name:   "resend",
			Value:  0,
			Hidden: true,
		},
		cli.IntFlag{
			Name:   "nc",
			Value:  0,
			Hidden: true,
		},
		cli.IntFlag{
			Name:  "sockbuf",
			Value: 4194304, // default socket buffer size in bytes
			Usage: "per-socket buffer in bytes",
		},
		cli.IntFlag{
			Name:  "smuxver",
			Value: 2,
			Usage: "specify smux version, available 1,2",
		},
		cli.IntFlag{
			Name:  "smuxbuf",
			Value: 4194304,
			Usage: "the overall de-mux buffer in bytes",
		},
		cli.IntFlag{
			Name:  "framesize",
			Value: 8192,
			Usage: "smux max frame size",
		},
		cli.IntFlag{
			Name:  "streambuf",
			Value: 2097152,
			Usage: "per stream receive buffer in bytes, smux v2+",
		},
		cli.IntFlag{
			Name:  "keepalive",
			Value: 10, // NAT keepalive interval in seconds
			Usage: "seconds between heartbeats",
		},
		cli.IntFlag{
			Name:  "closewait",
			Value: 0,
			Usage: "the seconds to wait before tearing down a connection",
		},
		cli.StringFlag{
			Name:  "snmplog",
			Value: "",
			Usage: "collect snmp to file, aware of timeformat in golang, like: ./snmp-20060102.log",
		},
		cli.IntFlag{
			Name:  "snmpperiod",
			Value: 60,
			Usage: "snmp collect period, in seconds",
		},
		cli.StringFlag{
			Name:  "log",
			Value: "",
			Usage: "specify a log file to output, default goes to stderr",
		},
		cli.BoolFlag{
			Name:  "quiet",
			Usage: "to suppress the 'stream open/close' messages",
		},
		cli.StringFlag{
			Name:  "c",
			Value: "", // when set, the referenced JSON file must exist on disk
			Usage: "config from json file, which will override the command from shell",
		},
		cli.BoolFlag{
			Name:  "pprof",
			Usage: "start profiling server on :6060",
		},
	}
	myApp.Action = func(c *cli.Context) error {
		config := Config{}
		config.LocalAddr = c.String("localaddr")
		config.RemoteAddr = c.String("remoteaddr")
		config.Key = c.String("key")
		config.Crypt = c.String("crypt")
		config.Mode = c.String("mode")
		config.Conn = c.Int("conn")
		config.AutoExpire = c.Int("autoexpire")
		config.ScavengeTTL = c.Int("scavengettl")
		config.MTU = c.Int("mtu")
		config.RateLimit = c.Int("ratelimit")
		config.SndWnd = c.Int("sndwnd")
		config.RcvWnd = c.Int("rcvwnd")
		config.DataShard = c.Int("datashard")
		config.ParityShard = c.Int("parityshard")
		config.DSCP = c.Int("dscp")
		config.NoComp = c.Bool("nocomp")
		config.AckNodelay = c.Bool("acknodelay")
		config.NoDelay = c.Int("nodelay")
		config.Interval = c.Int("interval")
		config.Resend = c.Int("resend")
		config.NoCongestion = c.Int("nc")
		config.SockBuf = c.Int("sockbuf")
		config.SmuxBuf = c.Int("smuxbuf")
		config.FrameSize = c.Int("framesize")
		config.StreamBuf = c.Int("streambuf")
		config.SmuxVer = c.Int("smuxver")
		config.KeepAlive = c.Int("keepalive")
		config.Log = c.String("log")
		config.SnmpLog = c.String("snmplog")
		config.SnmpPeriod = c.Int("snmpperiod")
		config.Quiet = c.Bool("quiet")
		config.Pprof = c.Bool("pprof")
		config.CloseWait = c.Int("closewait")

		if c.String("c") != "" {
			err := parseJSONConfig(&config, c.String("c"))
			checkError(err)
		}

		if config.Key == "it's a secrect" {
			log.Fatal("refusing to run with the default pre-shared key; set -key or KCPTUN_KEY to a high-entropy secret")
		}

		if config.Conn <= 0 {
			log.Fatal("conn must be greater than 0")
		}

		if config.RateLimit < 0 {
			log.Printf("ratelimit %d is negative, falling back to 0", config.RateLimit)
			config.RateLimit = 0
		}

		// Redirect logs when the user supplied a dedicated log file.
		if config.Log != "" {
			f, err := os.OpenFile(config.Log, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
			checkError(err)
			defer f.Close()
			log.SetOutput(f)
		}

		// Apply mode presets using the shared configuration helper.
		config.ApplyMode()

		log.Println("version:", VERSION)
		var listener net.Listener
		var isUnix bool
		if _, _, err := net.SplitHostPort(config.LocalAddr); err != nil {
			isUnix = true
		}
		if isUnix {
			addr, err := net.ResolveUnixAddr("unix", config.LocalAddr)
			checkError(err)
			listener, err = net.ListenUnix("unix", addr)
			checkError(err)
		} else {
			addr, err := net.ResolveTCPAddr("tcp", config.LocalAddr)
			checkError(err)
			listener, err = net.ListenTCP("tcp", addr)
			checkError(err)
		}

		log.Println("smux version:", config.SmuxVer)
		log.Println("listening on:", listener.Addr())
		log.Println("encryption:", config.Crypt)
		log.Println("nodelay parameters:", config.NoDelay, config.Interval, config.Resend, config.NoCongestion)
		log.Println("remote address:", config.RemoteAddr)
		log.Println("sndwnd:", config.SndWnd, "rcvwnd:", config.RcvWnd)
		log.Println("compression:", !config.NoComp)
		log.Println("mtu:", config.MTU)
		log.Println("ratelimit:", config.RateLimit)
		log.Println("datashard:", config.DataShard, "parityshard:", config.ParityShard)
		log.Println("acknodelay:", config.AckNodelay)
		log.Println("dscp:", config.DSCP)
		log.Println("sockbuf:", config.SockBuf)
		log.Println("smuxbuf:", config.SmuxBuf)
		log.Println("framesize:", config.FrameSize)
		log.Println("streambuf:", config.StreamBuf)
		log.Println("keepalive:", config.KeepAlive)
		log.Println("conn:", config.Conn)
		log.Println("autoexpire:", config.AutoExpire)
		log.Println("scavengettl:", config.ScavengeTTL)
		log.Println("snmplog:", config.SnmpLog)
		log.Println("snmpperiod:", config.SnmpPeriod)
		log.Println("quiet:", config.Quiet)
		log.Println("pprof:", config.Pprof)
		if config.DNSConfig != nil {
			log.Println("dns config:", config.DNSConfig.LocalInterfaceName)
		}
		if config.ShmMap != "" {
			log.Println("shmmap:", config.ShmMap)
		}

		var ipDomainStore *shmmap.Store
		if config.ShmMap != "" {
			store, err := shmmap.OpenReadOnly(config.ShmMap)
			if err != nil {
				checkError(err)
			}
			ipDomainStore = store
			defer store.Close()
		}

		// Ensure scavenger TTL does not exceed the auto-expire window.
		if config.AutoExpire != 0 && config.ScavengeTTL > config.AutoExpire {
			color.Red("WARNING: scavengettl is bigger than autoexpire, connections may race hard to use bandwidth.")
			color.Red("Try limiting scavengettl to a smaller value.")
		}

		// Guard against negotiating unsupported smux protocol versions.
		if config.SmuxVer > maxSmuxVer {
			log.Fatal("unsupported smux version:", config.SmuxVer)
		}

		// Precompute smux configuration once; it is identical for every session.
		smuxConfig, err := std.BuildSmuxConfig(
			config.SmuxVer, config.SmuxBuf, config.StreamBuf,
			config.FrameSize, config.KeepAlive,
		)
		if err != nil {
			log.Fatal("BuildSmuxConfig:", err)
		}
		config.SmuxConfig = smuxConfig

		// Derive the shared encryption key and prepare the block cipher.
		log.Println("initiating key derivation")
		pass := pbkdf2.Key([]byte(config.Key), []byte(SALT), 600000, 32, sha256.New)
		log.Println("key derivation done")
		block, effectiveCrypt, err := std.SelectBlockCrypt(config.Crypt, pass)
		checkError(err)
		config.Crypt = effectiveCrypt

		// Continuously export SNMP counters when requested.
		go std.SnmpLogger(config.SnmpLog, config.SnmpPeriod)

		// Optionally expose Go's net/http/pprof handlers on :6060.
		if config.Pprof {
			go func() {
				if err := http.ListenAndServe("127.0.0.1:6060", nil); err != nil {
					log.Println("pprof server:", err)
				}
			}()
		}

		// Launch the session scavenger only when auto-expiration is enabled.
		chScavenger := make(chan timedSession, 128)
		if config.AutoExpire > 0 {
			go scavenger(chScavenger, &config)
		}

		// Validate the remote address up front so a malformed --remoteaddr fails
		// fast instead of spinning forever inside waitConn's reconnect loop.
		if _, err := std.ParseMultiPort(config.RemoteAddr); err != nil {
			checkError(err)
		}

		// Accept TCP/UNIX clients and multiplex them across the UDP tunnels.
		numconn := uint16(config.Conn)
		if numconn == 0 {
			numconn = 1
		}
		muxes := make([]timedSession, numconn)
		refreshInflight := make([]bool, numconn)
		var muxMu sync.Mutex

		// ensureSession refreshes slot idx in the background so the accept loop is
		// never blocked by reconnection latency. At most one refresh runs per slot.
		ensureSession := func(idx uint16) {
			muxMu.Lock()
			if refreshInflight[idx] {
				muxMu.Unlock()
				return
			}
			refreshInflight[idx] = true
			muxMu.Unlock()

			go func() {
				session := waitConn(&config, block)
				muxMu.Lock()
				muxes[idx].session = session
				muxes[idx].expiryDate = time.Now().Add(time.Duration(config.AutoExpire) * time.Second)
				refreshInflight[idx] = false
				ts := muxes[idx]
				muxMu.Unlock()
				if config.AutoExpire > 0 {
					select {
					case chScavenger <- ts:
					default:
					}
				}
			}()
		}

		// rr tracks which pre-established session should carry the next client so
		// short-lived TCP dials do not hammer the same UDP tunnel.
		rr := uint16(0)

		// Kick off the first session in the background; until it is ready, clients
		// are rejected rather than blocking the accept loop.
		ensureSession(0)

		// Main accept loop: assign each inbound client to a live smux session and
		// refresh dead sessions in the background so the loop never stalls.
		for {
			p1, err := listener.Accept()
			if err != nil {
				log.Fatalf("%+v", err)
			}

			// Find a live session round-robin, triggering background refreshes for dead slots.
			var sess *smux.Session
			for i := uint16(0); i < numconn; i++ {
				idx := (rr + i) % numconn
				muxMu.Lock()
				s := muxes[idx].session
				dead := s == nil || s.IsClosed() ||
					(config.AutoExpire > 0 && time.Now().After(muxes[idx].expiryDate))
				muxMu.Unlock()
				if dead {
					ensureSession(idx)
					continue
				}
				sess = s
				rr = idx + 1
				break
			}

			if sess == nil {
				// No live session yet; drop this client to keep the accept loop
				// responsive rather than blocking on a reconnect.
				if !config.Quiet {
					log.Println("no live session available, closing client:", p1.RemoteAddr())
				}
				p1.Close()
				continue
			}

			go handleClient(sess, p1, config.Quiet, config.CloseWait, config.UseConntrack, ipDomainStore)
		}
	}
	myApp.Run(os.Args)
}

// createConn establishes a fresh KCP connection with all tunables applied and
// then upgrades it into an smux session ready for multiplexing.
func createConn(config *Config, block kcp.BlockCrypt) (*smux.Session, error) {
	kcpconn, err := dial(config, block)
	if err != nil {
		return nil, errors.Wrap(err, "dial()")
	}
	kcpconn.SetStreamMode(true)
	kcpconn.SetWriteDelay(false)
	kcpconn.SetNoDelay(config.NoDelay, config.Interval, config.Resend, config.NoCongestion)
	kcpconn.SetWindowSize(config.SndWnd, config.RcvWnd)
	kcpconn.SetMtu(config.MTU)
	kcpconn.SetACKNoDelay(config.AckNodelay)
	kcpconn.SetRateLimit(uint32(config.RateLimit))

	if err := kcpconn.SetDSCP(config.DSCP); err != nil {
		log.Println("SetDSCP:", err)
	}
	if err := kcpconn.SetReadBuffer(config.SockBuf); err != nil {
		log.Println("SetReadBuffer:", err)
	}
	if err := kcpconn.SetWriteBuffer(config.SockBuf); err != nil {
		log.Println("SetWriteBuffer:", err)
	}
	log.Println("smux version:", config.SmuxVer, "on connection:", kcpconn.LocalAddr(), "->", kcpconn.RemoteAddr())

	var session *smux.Session
	if config.NoComp {
		session, err = smux.Client(kcpconn, config.SmuxConfig)
	} else {
		session, err = smux.Client(std.NewCompStream(kcpconn), config.SmuxConfig)
	}
	if err != nil {
		kcpconn.Close()
		return nil, errors.Wrap(err, "createConn()")
	}
	return session, nil
}

// waitConn keeps dialing until a healthy smux session becomes available.
func waitConn(config *Config, block kcp.BlockCrypt) *smux.Session {
	for {
		session, err := createConn(config, block)
		if err == nil {
			return session
		}
		log.Println("re-connecting:", err)
		time.Sleep(time.Second)
	}
}

// handleClient tunnels a single accepted TCP/UNIX client through an smux
// stream.
func handleClient(session *smux.Session, p1 net.Conn, quiet bool, closeWait int, useTransparent bool, ipDomainStore *shmmap.Store) {
	logln := func(v ...any) {
		if !quiet {
			log.Println(v...)
		}
	}

	// Transport layer: accept the inbound socket.
	p2, err := session.OpenStream()
	if err != nil {
		logln(err)
		p1.Close()
		return
	}

	streamID := p2.RemoteAddr().String() + "(" + strconv.FormatUint(uint64(p2.ID()), 10) + ")"
	logln("stream opened", "in:", p1.RemoteAddr(), "out:", streamID)
	defer logln("stream closed", "in:", p1.RemoteAddr(), "out:", streamID)

	var s1, s2 io.ReadWriteCloser = p1, p2
	if useTransparent {
		tcpConn, ok := p1.(*net.TCPConn)
		if !ok {
			logln("transparent proxy requires TCP connection")
			p1.Close()
			p2.Close()
			return
		}
		if err := setupTransparentProxy(tcpConn, p2, ipDomainStore, logln); err != nil {
			p1.Close()
			p2.Close()
			return
		}
	}

	// Begin piping data bidirectionally between the socket and the smux stream.
	err1, err2 := std.Pipe(s1, s2, closeWait)

	// Report non-EOF errors so operators can diagnose failing streams.
	if err1 != nil && !errors.Is(err1, io.EOF) {
		logln("pipe:", err1, "in:", p1.RemoteAddr(), "out:", streamID)
	}
	if err2 != nil && !errors.Is(err2, io.EOF) {
		logln("pipe:", err2, "in:", p1.RemoteAddr(), "out:", streamID)
	}
}

// setupTransparentProxy resolves the original destination and performs the
// SOCKS5 handshake when iptables redirected the connection. Direct connections
// without a NAT redirect fall back to normal piping.
func setupTransparentProxy(tcpConn *net.TCPConn, stream io.ReadWriter, ipDomainStore *shmmap.Store, logln func(...any)) error {
	to, err := resolveTransparentDst(tcpConn, logln)
	if err != nil {
		logln("transparent proxy original dst lookup failed", "error", err)
		return err
	}
	if to == nil {
		return nil
	}

	from := tcpConn.RemoteAddr().(*net.TCPAddr)
	logln("transparent proxy original dst", "src-ip", from.IP.String(), "src-port", from.Port, "dst-ip", to.IP.String(), "dst-port", to.Port)

	host := to.IP.String()
	if domain, ok := lookupIPDomain(ipDomainStore, to.IP); ok {
		host = domain
		logln("dns domain lookup", "dst-ip", to.IP.String(), "domain", domain)
	}

	logln("send socks5 connect request", "host", host, "dst-port", to.Port)
	// Bound the SOCKS5 handshake so an unresponsive server cannot pin this
	// goroutine, stream, and connection indefinitely.
	if conn, ok := stream.(net.Conn); ok {
		_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
		defer conn.SetReadDeadline(time.Time{})
	}
	if err := std.SendSocksConnectRequestHost(stream, host, to.Port); err != nil {
		logln("socks5 send handshake", "error", err)
		return err
	}
	if err := std.ReadSocksConnectResponse(stream); err != nil {
		logln("socks5 read handshake", "error", err)
		return err
	}
	logln("sock5 connected", "host", host)
	return nil
}

// resolveTransparentDst returns the pre-NAT destination for redirected traffic.
// A nil destination with no error means the connection should pipe directly.
func resolveTransparentDst(tcpConn *net.TCPConn, logln func(...any)) (*net.TCPAddr, error) {
	from := tcpConn.RemoteAddr().(*net.TCPAddr)

	to, err := GetOriginalDst(tcpConn)
	if err != nil {
		if shouldPassthroughOnOriginalDstFailure(from.IP) {
			logln("transparent proxy: loopback source, direct passthrough", "src-ip", from.IP.String(), "src-port", from.Port, "error", err)
			return nil, nil
		}
		return nil, err
	}
	if originalDstMatchesLocal(tcpConn, to) {
		logln("transparent proxy: direct connection, passthrough", "src-ip", from.IP.String(), "src-port", from.Port)
		return nil, nil
	}
	return to, nil
}

func originalDstMatchesLocal(conn *net.TCPConn, orig *net.TCPAddr) bool {
	local := conn.LocalAddr().(*net.TCPAddr)
	return orig.Port == local.Port && orig.IP.Equal(local.IP)
}

func lookupIPDomain(store *shmmap.Store, ip net.IP) (string, bool) {
	if store == nil {
		return "", false
	}
	return store.Lookup(ip)
}

// shouldPassthroughOnOriginalDstFailure reports whether a failed SO_ORIGINAL_DST
// lookup should fall back to direct piping. Loopback sources are allowed to
// passthrough for direct local connections; redirected loopback traffic should
// succeed via GetOriginalDst and take the SOCKS5 path instead.
func shouldPassthroughOnOriginalDstFailure(from net.IP) bool {
	return from.IsLoopback()
}

// checkError logs the supplied fatal error and terminates the process.
func checkError(err error) {
	if err != nil {
		log.Printf("%+v\n", err)
		os.Exit(-1)
	}
}

// timedSession annotates an smux session with its expiration deadline.
type timedSession struct {
	session    *smux.Session
	expiryDate time.Time
}

// scavenger tracks expiring sessions received on ch and closes them after the
// configured TTL elapses.
func scavenger(ch chan timedSession, config *Config) {
	ticker := time.NewTicker(scavengePeriod * time.Second)
	defer ticker.Stop()
	// Pre-allocate with reasonable capacity to reduce slice growth overhead
	sessionList := make([]timedSession, 0, 16)
	for {
		select {
		case item := <-ch:
			sessionList = append(sessionList, timedSession{
				item.session,
				item.expiryDate.Add(time.Duration(config.ScavengeTTL) * time.Second),
			})
		case <-ticker.C:
			// Reuse slice capacity to avoid allocation
			newList := sessionList[:0]
			for k := range sessionList {
				s := sessionList[k]
				if s.session.IsClosed() {
					log.Println("scavenger: session normally closed:", s.session.LocalAddr())
				} else if time.Now().After(s.expiryDate) {
					s.session.Close()
					log.Println("scavenger: session closed due to ttl:", s.session.LocalAddr())
				} else {
					newList = append(newList, sessionList[k])
				}
			}
			sessionList = newList
		}
	}
}
