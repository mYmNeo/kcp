//go:build linux

package shmmap

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/miekg/dns"
	"golang.org/x/sys/unix"
)

// Store maps IP addresses to the domain name from forward A/AAAA queries.
type Store struct {
	name string
	path string
	data []byte
}

func shmPath(name string) (string, error) {
	if !strings.HasPrefix(name, "/") {
		return "", fmt.Errorf("shmmap: name must start with /")
	}
	return filepath.Join("/dev/shm", strings.TrimPrefix(name, "/")), nil
}

// Open creates or opens a POSIX shared memory region for the IP-to-domain index.
func Open(name string, size uint) (*Store, error) {
	if size == 0 {
		size = defaultSize
	}
	if size < headerSize+slotSize {
		return nil, fmt.Errorf("shmmap: size %d too small", size)
	}

	path, err := shmPath(name)
	if err != nil {
		return nil, err
	}

	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT, 0o666)
	if err != nil {
		return nil, fmt.Errorf("shmmap: open %s: %w", path, err)
	}
	defer unix.Close(fd)

	if err := unix.Ftruncate(fd, int64(size)); err != nil {
		return nil, fmt.Errorf("shmmap: ftruncate: %w", err)
	}

	data, err := unix.Mmap(fd, 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("shmmap: mmap: %w", err)
	}

	s := &Store{name: name, path: path, data: data}
	s.initHeader()
	return s, nil
}

// OpenReadOnly opens an existing shared memory region for read-only lookup.
func OpenReadOnly(name string) (*Store, error) {
	path, err := shmPath(name)
	if err != nil {
		return nil, err
	}

	fd, err := unix.Open(path, unix.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("shmmap: open %s: %w", path, err)
	}
	defer unix.Close(fd)

	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return nil, fmt.Errorf("shmmap: fstat: %w", err)
	}
	size := st.Size
	if size < int64(headerSize+slotSize) {
		return nil, fmt.Errorf("shmmap: region too small")
	}

	data, err := unix.Mmap(fd, 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("shmmap: mmap: %w", err)
	}

	s := &Store{name: name, path: path, data: data}
	if err := s.validateHeader(); err != nil {
		unix.Munmap(data)
		return nil, err
	}
	return s, nil
}

func (s *Store) initHeader() {
	h := s.header()
	if string(h.magic[:8]) != magic {
		copy(h.magic[:], magic)
		h.version = version
		h.slotCount = (uint32(len(s.data)) - headerSize) / slotSize
		h.seq = 0
	}
}

func (s *Store) validateHeader() error {
	h := s.header()
	if string(h.magic[:8]) != magic {
		return fmt.Errorf("shmmap: invalid magic")
	}
	if h.version != version {
		return fmt.Errorf("shmmap: unsupported version %d", h.version)
	}
	if h.slotCount == 0 || headerSize+int(h.slotCount)*slotSize > len(s.data) {
		return fmt.Errorf("shmmap: invalid slot count")
	}
	return nil
}

func (s *Store) header() *header {
	return (*header)(unsafe.Pointer(&s.data[0]))
}

func (s *Store) slotAt(i uint32) *slot {
	offset := headerSize + int(i)*slotSize
	return (*slot)(unsafe.Pointer(&s.data[offset]))
}

func (s *Store) seqBeginWrite() {
	h := s.header()
	atomic.AddUint32(&h.seq, 1)
}

func (s *Store) seqEndWrite() {
	h := s.header()
	atomic.AddUint32(&h.seq, 1)
}

func (s *Store) seqSnapshot() uint32 {
	h := s.header()
	seq := atomic.LoadUint32(&h.seq)
	for seq&1 != 0 {
		time.Sleep(time.Microsecond)
		seq = atomic.LoadUint32(&h.seq)
	}
	return seq
}

func ipHash(ip net.IP) uint32 {
	v4 := ip.To4()
	if v4 != nil {
		return binary.BigEndian.Uint32(v4)
	}
	ip = ip.To16()
	if ip == nil {
		return 0
	}
	return binary.BigEndian.Uint32(ip[0:4]) ^ binary.BigEndian.Uint32(ip[4:8]) ^
		binary.BigEndian.Uint32(ip[8:12]) ^ binary.BigEndian.Uint32(ip[12:16])
}

func writeIP(dst *[16]byte, ip net.IP) uint8 {
	v4 := ip.To4()
	if v4 != nil {
		copy(dst[:4], v4)
		for i := 4; i < 16; i++ {
			dst[i] = 0
		}
		return 4
	}
	ip16 := ip.To16()
	if ip16 == nil {
		return 0
	}
	copy(dst[:], ip16)
	return 6
}

func ipEqual(slotIP *[16]byte, family uint8, ip net.IP) bool {
	if family == 4 {
		v4 := ip.To4()
		if v4 == nil {
			return false
		}
		return string(slotIP[:4]) == string(v4)
	}
	ip16 := ip.To16()
	if ip16 == nil {
		return false
	}
	return string(slotIP[:16]) == string(ip16)
}

func (s *Store) findSlot(ip net.IP, forWrite bool) *slot {
	h := s.header()
	count := h.slotCount
	if count == 0 {
		return nil
	}
	hash := ipHash(ip)
	start := hash % count
	var empty *slot
	for i := uint32(0); i < maxProbe && i < count; i++ {
		idx := (start + i) % count
		sl := s.slotAt(idx)
		if sl.family == 0 {
			if forWrite && empty == nil {
				empty = sl
			}
			continue
		}
		if ipEqual(&sl.ip, sl.family, ip) {
			return sl
		}
	}
	if forWrite {
		return empty
	}
	return nil
}

// Put indexes A/AAAA answer IPs from a successful DNS response.
func (s *Store) Put(msg *dns.Msg) {
	if msg == nil || msg.Rcode != dns.RcodeSuccess || len(msg.Question) != 1 || len(msg.Answer) == 0 {
		return
	}
	q := msg.Question[0]
	if q.Qtype != dns.TypeA && q.Qtype != dns.TypeAAAA {
		return
	}

	var minTTL uint32
	for i, rr := range msg.Answer {
		ttl := rr.Header().Ttl
		if i == 0 || ttl < minTTL {
			minTTL = ttl
		}
	}
	if minTTL == 0 {
		return
	}

	domain := strings.ToLower(strings.TrimSuffix(q.Name, "."))
	if len(domain) >= domainMax {
		domain = domain[:domainMax-1]
	}
	expiresAt := uint64(time.Now().Unix()) + uint64(minTTL)

	var ips []net.IP
	for _, rr := range msg.Answer {
		switch v := rr.(type) {
		case *dns.A:
			ips = append(ips, v.A)
		case *dns.AAAA:
			ips = append(ips, v.AAAA)
		}
	}
	if len(ips) == 0 {
		return
	}

	s.seqBeginWrite()
	for _, ip := range ips {
		sl := s.findSlot(ip, true)
		if sl == nil {
			continue
		}
		sl.family = writeIP(&sl.ip, ip)
		sl.qtype = q.Qtype
		sl.expiresAt = expiresAt
		n := copy(sl.domain[:], domain)
		sl.domain[n] = 0
	}
	s.seqEndWrite()
}

// Lookup returns the domain name for an IP address, or empty string if not found.
func (s *Store) Lookup(ip net.IP) (string, bool) {
	if ip == nil {
		return "", false
	}
	now := uint64(time.Now().Unix())

	for attempt := 0; attempt < 3; attempt++ {
		seq1 := s.seqSnapshot()
		sl := s.findSlot(ip, false)
		if sl == nil {
			return "", false
		}
		if sl.expiresAt <= now {
			return "", false
		}
		domain := stringFromNull(sl.domain[:])
		seq2 := atomic.LoadUint32(&s.header().seq)
		if seq1 == seq2 && seq2&1 == 0 {
			return domain, domain != ""
		}
	}
	return "", false
}

func stringFromNull(b []byte) string {
	for i, c := range b {
		if c == 0 {
			return string(b[:i])
		}
	}
	return string(b)
}

// StartCleanup runs a background goroutine that clears expired slots.
func (s *Store) StartCleanup(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.cleanup()
			}
		}
	}()
}

func (s *Store) cleanup() {
	now := uint64(time.Now().Unix())
	h := s.header()
	count := h.slotCount

	s.seqBeginWrite()
	for i := uint32(0); i < count; i++ {
		sl := s.slotAt(i)
		if sl.family != 0 && sl.expiresAt <= now {
			sl.family = 0
			sl.expiresAt = 0
			sl.domain[0] = 0
		}
	}
	s.seqEndWrite()
}

// Close unmaps the shared memory region.
func (s *Store) Close() error {
	if s.data == nil {
		return nil
	}
	err := unix.Munmap(s.data)
	s.data = nil
	return err
}
