/*
   DNS-over-HTTPS
   Copyright (C) 2017-2018 Star Brilliant <m13253@hotmail.com>

   Shared memory layout for IP-to-domain reverse lookup index.
   Other processes may read /dev/shm/<name> directly using this layout,
   or use the doh-ip-lookup CLI.

   Header (64 bytes):
     magic     [8]  "DOHSHM01"
     version   u32  1
     slotCount u32  number of hash slots
     seq       u32  seqlock (even = stable)

   Slot (272 bytes each):
     ip        [16]  IPv4 BE in first 4 bytes; full IPv6 otherwise
     family    u8    4 or 6
     qtype     u16   dns.TypeA or dns.TypeAAAA
     expiresAt u64   unix seconds
     domain    [253] null-terminated FQDN
*/

package shmmap

const (
	magic       = "DOHSHM01"
	version     = 1
	headerSize  = 64
	slotSize    = 272
	domainMax   = 240 // fits in 272-byte slot with aligned fields
	maxProbe    = 32
	defaultSize = 4 * 1024 * 1024
)

type header struct {
	magic     [8]byte
	version   uint32
	slotCount uint32
	seq       uint32
	_         [44]byte // pad to 64 bytes
}

type slot struct {
	ip        [16]byte
	family    uint8
	_         [1]byte // align qtype to offset 18
	qtype     uint16
	_         [4]byte // align expiresAt to offset 24
	expiresAt uint64
	domain    [domainMax]byte
}
