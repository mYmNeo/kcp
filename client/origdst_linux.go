//go:build linux

package main

import (
	"net"
	"os"
	"syscall"
	"unsafe"
)

// When using transparent proxy mode, add iptables rules to redirect traffic:
// 1. -A PREROUTING -s <src ips optional> -i <in-if> -p tcp -j DNAT --to-destination <kcptun listen address>
// example: -A PREROUTING -s 192.168.1.107/32 -i eth0 -p tcp -j DNAT --to-destination 192.168.1.108:8888
// 2. -A POSTROUTING -s <dhcp-range> -j MASQUERADE
// example: -A POSTROUTING -s 192.168.1.0/24 -j MASQUERADE

const (
	soOriginalDst   = 80
	ip6tOriginalDst = 80
)

func getsockopt(s int, level int, optname int, optval unsafe.Pointer, optlen *uint32) error {
	_, _, e := syscall.Syscall6(
		syscall.SYS_GETSOCKOPT, uintptr(s), uintptr(level), uintptr(optname),
		uintptr(optval), uintptr(unsafe.Pointer(optlen)), 0)
	if e != 0 {
		return e
	}
	return nil
}

func sockaddrPort(port uint16) int {
	pb := *(*[2]byte)(unsafe.Pointer(&port))
	return int(pb[0])*256 + int(pb[1])
}

func parseSockaddrInet4(addr *syscall.RawSockaddrInet4) *net.TCPAddr {
	ip := make([]byte, 4)
	for i, b := range addr.Addr {
		ip[i] = b
	}
	return &net.TCPAddr{
		IP:   ip,
		Port: sockaddrPort(addr.Port),
	}
}

func parseSockaddrInet6(addr *syscall.RawSockaddrInet6) *net.TCPAddr {
	ip := make([]byte, 16)
	for i, b := range addr.Addr {
		ip[i] = b
	}
	return &net.TCPAddr{
		IP:   ip,
		Port: sockaddrPort(addr.Port),
	}
}

// GetOriginalDst retrieves the original destination from an iptables DNAT/REDIRECTed TCP connection.
func GetOriginalDst(conn *net.TCPConn) (*net.TCPAddr, error) {
	f, err := conn.File()
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fd := int(f.Fd())
	if err = syscall.SetNonblock(fd, true); err != nil {
		return nil, os.NewSyscallError("setnonblock", err)
	}

	v6 := conn.LocalAddr().(*net.TCPAddr).IP.To4() == nil
	if v6 {
		var addr syscall.RawSockaddrInet6
		len := uint32(unsafe.Sizeof(addr))
		err = getsockopt(fd, syscall.IPPROTO_IPV6, ip6tOriginalDst, unsafe.Pointer(&addr), &len)
		if err != nil {
			return nil, os.NewSyscallError("getsockopt", err)
		}
		return parseSockaddrInet6(&addr), nil
	}

	var addr syscall.RawSockaddrInet4
	len := uint32(unsafe.Sizeof(addr))
	err = getsockopt(fd, syscall.IPPROTO_IP, soOriginalDst, unsafe.Pointer(&addr), &len)
	if err != nil {
		return nil, os.NewSyscallError("getsockopt", err)
	}
	return parseSockaddrInet4(&addr), nil
}
