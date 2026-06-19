//go:build !linux

package main

import (
	"errors"
	"net"
)

func GetOriginalDst(conn *net.TCPConn) (*net.TCPAddr, error) {
	return nil, errors.New("GetOriginalDst: not implemented on this platform")
}
