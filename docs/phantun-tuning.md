# KCP over Phantun: performance parameters

Phantun is not kernel TCP. It fakes a handshake, sequence numbers, and ACKs so NAT and L3/L4 firewalls let the flow through. After that, lost packets stay lost, there is no kernel congestion control, and datagrams stay datagrams. KCP is still the only reliability, window, and loss-recovery layer. Kernel TCP knobs (`tcp_rmem`, BBR, Fast Open, slow-start) do nothing for the tunnel.

```
app TCP → kcptun (UDP) → phantun TUN (fake TCP) → SNAT/DNAT → NIC
```

On the wire each KCP datagram becomes `IP(20) + TCP(20) + KCP UDP payload`. That is 12 bytes more than native UDP. Phantun never fragments and always sets DF. A packet bigger than the path MTU is dropped with no ICMP recovery.

kcptun `-mtu` is that UDP payload. Internally kcp-go subtracts FEC + crypto (and the AES-GCM tag) so the datagram you asked for is what leaves the UDP socket.

## MTU (do this first)

IPv4:

```
kcptun -mtu = path MTU - 20 (IP) - 20 (TCP) - safety
```

IPv6: subtract 40+20 instead of 20+20.

| Path | `-mtu` |
|---|---|
| Ethernet 1500, IPv4 | **1420–1440** (1460 is the hard ceiling) |
| PPPoE 1492 | **1400** |
| Extra unknown middlebox | keep **1350** |
| IPv6 1500 | **1400** |

Do not set `-mtu 1500`. That becomes a 1540-byte fake TCP frame, which exceeds phantun’s 1500-byte buffer and the TUN MTU, and dies silently.

Set both kcptun ends to the same value. Leave the TUN at 1500; shrinking TUN MTU does not help.

## KCP parameters

### Mode

`fast3` (`nodelay=1, interval=10, resend=2, nc=1`) is the latency default. On a stable “TCP-looking” path, `fast2` (`interval=20`) usually wastes less CPU and false-retransmits less. Both disable KCP congestion control, and phantun has none either, so you must cap send rate yourself.

### FEC

If UDP was blocked but the TCP-looking path is clean, FEC is pure overhead (default 10/3 = +30%). Start with `--datashard 0 --parityshard 0`. If SNMP `LostSegs` or `RetransSegs` climb, use `10/1` or `10/2`, not `10/3`.

### Windows

Throughput ≈ `min(sndwnd, rcvwnd) × mtu / rtt`. Raise client `-rcvwnd` and server `-sndwnd` together.

| RTT × bandwidth | packets to aim for |
|---|---|
| 30 ms × 100 Mbps | ~256–512 |
| 100 ms × 100 Mbps | ~1024 |
| 100 ms × 1 Gbps | ~4096–8192 |

Client default `sndwnd=128, rcvwnd=512` is too small for WAN. Practical start: client `sndwnd 1024 rcvwnd 2048`, server `sndwnd 2048 rcvwnd 2048`. Grow until goodput stops rising or memory/CPU hurts.

### Rate limit (pacing)

`-ratelimit` is **per KCP connection and one-directional**. It is applied independently on the client session and the server session; each limiter only paces that process’s send path. Units are bytes/s. `0` disables it.

Set each peer to:

```
ratelimit ≈ 0.9 × bottleneck_in_this_direction / N
```

`N` is `-conn`. Apply it on **both** client and server.

Examples:

- 100 Mbps symmetric, `-conn 1` → both sides `11796480` (≈ 90 Mbps)
- 100 Mbps symmetric, `-conn 4` → both sides `2949120`
- 100/20 Mbps cable, `-conn 1` → server (bulk download) `11796480`, client (upload) `2359296`

Keep `-conn 1` unless one core is actually maxed. Extra connections only help after pacing is already correct; they do not raise the link, they multiply the senders.

If you omit `-ratelimit`, KCP will burst the TUN/NIC with no kernel TCP congestion control behind it.

### Local UDP buffers

`-sockbuf 16777217` on both kcptun sides. Phantun’s own UDP sockets do **not** set `SO_RCVBUF`, so they follow `net.core.rmem_default` / `wmem_default` — those sysctls matter more than `-sockbuf`.

### Keepalive

Leave `-keepalive 10`. Phantun tears down idle fake-TCP at 180s. 10s stays under typical NAT TCP idle timers.

### Connections

Each extra KCP UDP flow becomes another fake TCP connection, which is how you use more cores (phantun runs one worker set per connection). `2–4` is useful on 1 Gbps+; `1` is enough for a single bulk flow. KCP multiport (`IP:min-max`) does not help: phantun presents one TCP port.

When raising `-conn`, divide `-ratelimit` by the same N (see above).

### Smux

`-smuxver 2 -smuxbuf 8388608 -streambuf 2097152`. Raise `smuxbuf` if many streams HOL-block.

### CPU

`-nocomp` if the payload is already encrypted. `aes-128-gcm` with AES-NI; `salsa20` if not.

### DSCP

`-dscp` only marks the **local** UDP hop to phantun, not the fake TCP on the wire. To mark the tunnel, use iptables/nftables on `FORWARD`/`POSTROUTING` for the TUN or the TCP port.

## Kernel sysctl (both ends)

Required for phantun:

```
net.ipv4.ip_forward = 1
net.ipv6.conf.all.forwarding = 1   # only if you use v6
```

UDP / TUN burst absorption (kcptun already ships the first five in `dist/linux/sysctl_linux`):

```
net.core.rmem_max = 26214400
net.core.wmem_max = 26214400
net.core.rmem_default = 26214400
net.core.wmem_default = 26214400
net.core.netdev_max_backlog = 16384
net.core.optmem_max = 20480
net.core.somaxconn = 4096
```

Conntrack: fake TCP has a hardcoded window `0xffff` and ACKs that are not real flow control. Strict window tracking will mark packets `INVALID` and drop them.

`nf_conntrack_max` is writable. `nf_conntrack_buckets` is **not**; it is fixed when the module loads. Do not put `nf_conntrack_buckets` in sysctl.conf.

At load (or in `/etc/modprobe.d/`):

```
options nf_conntrack hashsize=262144
```

Then, via sysctl:

```
net.netfilter.nf_conntrack_max = 1048576
net.netfilter.nf_conntrack_tcp_be_liberal = 1
net.netfilter.nf_conntrack_tcp_loose = 1
net.netfilter.nf_conntrack_tcp_timeout_established = 86400
```

If the module is already loaded, `sysctl net.netfilter.nf_conntrack_buckets=…` will fail or be ignored. Unload/reload (or reboot) with `hashsize=` set. Rule of thumb: `hashsize ≈ nf_conntrack_max / 4`.

If the module is present, also `nf_conntrack_tcp_no_window_check=1`.

Reverse-path filter will drop TUN traffic if it is strict:

```
net.ipv4.conf.all.rp_filter = 2
net.ipv4.conf.default.rp_filter = 2
```

And the same `= 2` (or `0`) on `tun0` and the physical NIC. `2` is loose mode; `1` is the usual trap.

Do **not** raise `tcp_rmem` / `tcp_wmem` / `tcp_congestion_control` for the tunnel. Those apply to kernel TCP sockets, which this path does not use.

## NIC, qdisc, TUN (often the real limiter)

Disable **receive** coalescing on the physical NIC that carries phantun. Leave TSO/GSO alone.

```
ethtool -K eth0 gro off
ethtool -K eth0 rx-udp-gro-forwarding off   # if the NIC/driver exposes it
```

GRO merges consecutive fake-TCP segments into one skb. Phantun then hands KCP a single “datagram” that is several KCP packets glued together, which looks like loss and checksum failure.

TSO/GSO only fire on skbs that already carry GSO metadata. Packets Phantun injects through TUN are ordinary ≤MTU frames with no that metadata, so turning TSO/GSO off does not protect the tunnel and only slows the host’s real TCP.

Disable nftables flow offload / hardware NAT for that TCP port; offloaded TCP reassembly has the same effect.

Smooth bursts the kernel TCP stack is no longer pacing:

```
net.core.default_qdisc = fq
```

or `cake` / `fq_codel` on the uplink. Raise TUN txqueue:

```
ip link set tun0 txqueuelen 5000
```

`ulimit -n 65535` on both kcptun processes.

NAT stays as phantun documents: client SNAT/MASQUERADE from TUN, server DNAT of the listen port to the TUN peer. Do not `NOTRACK` that port if you still need SNAT/DNAT.

## Practical starting point

For a typical WAN IPv4 1500 path, both kcptun ends:

- `-mode fast3` (or `fast2` if CPU or spurious retries show up)
- `-mtu 1420`
- `-nocomp`
- `-datashard 0 -parityshard 0` until you measure loss
- client `-sndwnd 1024 -rcvwnd 2048`, server `-sndwnd 2048 -rcvwnd 2048`
- `-sockbuf 16777217 -smuxver 2 -smuxbuf 8388608 -streambuf 2097152 -keepalive 10`
- `-ratelimit` ≈ `0.9 × bottleneck_bytes_per_sec / N` on **both** peers (`N` = `-conn`)
- `-conn 1`, bump to 2–4 only if one core is maxed and the NIC is not (then divide ratelimit by N)

Then confirm with kcptun SNMP (`SIGUSR1`): `InCsumErrors` / `KCPInErrors` should stay ~0; `LostSegs` and `RetransSegs` should not grow on a quiet path. If they do, you still have GRO, conntrack window checks, or an MTU that does not fit.
