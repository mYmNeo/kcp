//go:build !linux

package shmmap

import (
	"context"
	"fmt"
	"net"

	"github.com/miekg/dns"
)

// Store maps IP addresses to the domain name from forward A/AAAA queries.
type Store struct{}

// Open is not supported on this platform.
func Open(name string, size uint) (*Store, error) {
	return nil, fmt.Errorf("shmmap: not supported on this platform")
}

// OpenReadOnly is not supported on this platform.
func OpenReadOnly(name string) (*Store, error) {
	return nil, fmt.Errorf("shmmap: not supported on this platform")
}

// Put is a no-op on unsupported platforms.
func (s *Store) Put(_ *dns.Msg) {}

// Lookup always returns a miss on unsupported platforms.
func (s *Store) Lookup(_ net.IP) (string, bool) {
	return "", false
}

// StartCleanup is a no-op on unsupported platforms.
func (s *Store) StartCleanup(_ context.Context) {}

// Close is a no-op on unsupported platforms.
func (s *Store) Close() error {
	return nil
}
