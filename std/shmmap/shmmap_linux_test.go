//go:build linux

package shmmap

import (
	"net"
	"os"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/miekg/dns"
)

func TestLayoutSize(t *testing.T) {
	if unsafe.Sizeof(header{}) != headerSize {
		t.Fatalf("header size = %d, want %d", unsafe.Sizeof(header{}), headerSize)
	}
	if unsafe.Sizeof(slot{}) != slotSize {
		t.Fatalf("slot size = %d, want %d", unsafe.Sizeof(slot{}), slotSize)
	}
}

func TestPutAndLookup(t *testing.T) {
	name := "/doh-shm-test-" + t.Name()
	size := uint(headerSize + slotSize*64)

	store, err := Open(name, size)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	defer unixUnlink(name)

	msg := newTestMsg("www.example.com.", dns.TypeA, []string{"93.184.216.34"})
	store.Put(msg)

	domain, ok := store.Lookup(parseIP("93.184.216.34"))
	if !ok || domain != "www.example.com" {
		t.Fatalf("Lookup = %q, ok = %v", domain, ok)
	}
}

func TestPutAAAA(t *testing.T) {
	name := "/doh-shm-test-aaaa"
	size := uint(headerSize + slotSize*64)

	store, err := Open(name, size)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	defer unixUnlink(name)

	msg := newTestMsg("ipv6.example.com.", dns.TypeAAAA, []string{"2001:db8::1"})
	store.Put(msg)

	domain, ok := store.Lookup(parseIP("2001:db8::1"))
	if !ok || domain != "ipv6.example.com" {
		t.Fatalf("Lookup = %q, ok = %v", domain, ok)
	}
}

func TestOverwriteIP(t *testing.T) {
	name := "/doh-shm-test-overwrite"
	size := uint(headerSize + slotSize*64)

	store, err := Open(name, size)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	defer unixUnlink(name)

	ip := "10.0.0.1"
	store.Put(newTestMsg("first.example.com.", dns.TypeA, []string{ip}))
	store.Put(newTestMsg("second.example.com.", dns.TypeA, []string{ip}))

	domain, ok := store.Lookup(parseIP(ip))
	if !ok || domain != "second.example.com" {
		t.Fatalf("Lookup = %q, ok = %v", domain, ok)
	}
}

func TestExpiry(t *testing.T) {
	name := "/doh-shm-test-expiry"
	size := uint(headerSize + slotSize*64)

	store, err := Open(name, size)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	defer unixUnlink(name)

	msg := newTestMsg("short.example.com.", dns.TypeA, []string{"10.0.0.2"})
	msg.Answer[0].Header().Ttl = 1
	store.Put(msg)

	time.Sleep(1100 * time.Millisecond)

	if _, ok := store.Lookup(parseIP("10.0.0.2")); ok {
		t.Fatal("expected expired entry to miss")
	}
}

func TestOpenReadOnly(t *testing.T) {
	name := "/doh-shm-test-readonly"
	size := uint(headerSize + slotSize*64)

	writer, err := Open(name, size)
	if err != nil {
		t.Fatal(err)
	}
	writer.Put(newTestMsg("readonly.example.com.", dns.TypeA, []string{"10.0.0.3"}))
	writer.Close()

	reader, err := OpenReadOnly(name)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer unixUnlink(name)

	domain, ok := reader.Lookup(parseIP("10.0.0.3"))
	if !ok || domain != "readonly.example.com" {
		t.Fatalf("Lookup = %q, ok = %v", domain, ok)
	}
}

func TestLongDomain(t *testing.T) {
	name := "/doh-shm-test-long-domain"
	size := uint(headerSize + slotSize*64)

	store, err := Open(name, size)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	defer unixUnlink(name)

	longName := strings.Repeat("a", 300) + ".example.com"
	msg := newTestMsg(longName+".", dns.TypeA, []string{"10.0.0.4"})
	store.Put(msg)

	domain, ok := store.Lookup(parseIP("10.0.0.4"))
	if !ok {
		t.Fatal("expected lookup hit")
	}
	if len(domain) >= domainMax {
		t.Fatalf("domain length = %d, want <%d", len(domain), domainMax)
	}
}

// TestReopenSmallerSizeRecomputesSlotCount guards against a stale slotCount
// left in the header by a previous run with a larger DNSShmSize. The POSIX
// shm file persists across restarts, and initHeader must resync slotCount
// from the actual mapping size on every open — otherwise findSlot/Put/cleanup
// index past the (smaller) buffer and panic with "index out of range".
func TestReopenSmallerSizeRecomputesSlotCount(t *testing.T) {
	name := "/doh-shm-test-resize"

	// First open with a LARGER mapping to plant a large slotCount in the header.
	const bigSlots = 1024
	big, err := Open(name, uint(headerSize+slotSize*bigSlots))
	if err != nil {
		t.Fatal(err)
	}
	big.Put(newTestMsg("big.example.com.", dns.TypeA, []string{"10.0.0.5"}))
	big.Close()

	// Reopen with a much SMALLER mapping. The stale large slotCount must be
	// recomputed from the small buffer, or cleanup() (which visits every slot)
	// indexes past the buffer and panics.
	const smallSlots = 16
	small, err := Open(name, uint(headerSize+slotSize*smallSlots))
	if err != nil {
		t.Fatal(err)
	}
	defer small.Close()
	defer unixUnlink(name)

	// Must not panic with "index out of range".
	small.cleanup()

	if h := small.header(); h.slotCount != smallSlots {
		t.Fatalf("slotCount = %d, want %d (it must track the actual mapping size)", h.slotCount, smallSlots)
	}
}

func newTestMsg(name string, qtype uint16, ips []string) *dns.Msg {
	msg := new(dns.Msg)
	msg.Rcode = dns.RcodeSuccess
	msg.Question = []dns.Question{{Name: name, Qtype: qtype, Qclass: dns.ClassINET}}
	for _, ipStr := range ips {
		if qtype == dns.TypeA {
			msg.Answer = append(msg.Answer, &dns.A{
				Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 300},
				A:   parseIP(ipStr).To4(),
			})
		} else {
			msg.Answer = append(msg.Answer, &dns.AAAA{
				Hdr:  dns.RR_Header{Name: name, Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: 300},
				AAAA: parseIP(ipStr),
			})
		}
	}
	return msg
}

func parseIP(s string) net.IP {
	return net.ParseIP(s)
}

func unixUnlink(name string) {
	path, err := shmPath(name)
	if err != nil {
		return
	}
	_ = os.Remove(path)
}
