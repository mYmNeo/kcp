//go:build linux

package main

import (
	"net"
	"syscall"
	"testing"
)

func TestParseSockaddrInet4(t *testing.T) {
	addr := &syscall.RawSockaddrInet4{
		Addr: [4]byte{192, 168, 1, 100},
		Port: 0x5000, // 80 in network byte order
	}

	got := parseSockaddrInet4(addr)
	want := &net.TCPAddr{IP: net.IPv4(192, 168, 1, 100), Port: 80}
	if got.String() != want.String() {
		t.Fatalf("parseSockaddrInet4() = %v, want %v", got, want)
	}
}

func TestParseSockaddrInet6(t *testing.T) {
	addr := &syscall.RawSockaddrInet6{
		Addr: [16]byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		Port: 0xbb01, // 443 in network byte order
	}

	got := parseSockaddrInet6(addr)
	want := &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}
	if got.String() != want.String() {
		t.Fatalf("parseSockaddrInet6() = %v, want %v", got, want)
	}
}

func TestSockaddrPort(t *testing.T) {
	tests := []struct {
		port uint16
		want int
	}{
		{port: 0x5000, want: 80},
		{port: 0xbb01, want: 443},
		{port: 0x901f, want: 8080},
	}

	for _, tc := range tests {
		if got := sockaddrPort(tc.port); got != tc.want {
			t.Fatalf("sockaddrPort(0x%x) = %d, want %d", tc.port, got, tc.want)
		}
	}
}
