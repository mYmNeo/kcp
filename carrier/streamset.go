package carrier

import (
	"sync"
	"sync/atomic"
)

// streamSet is a concurrently readable collection of TCP streams forming one
// logical peer.
//
// KCP sends one datagram per call on the hot path, so reads must not take a
// lock or allocate: the live slice is published behind an atomic pointer and
// readers grab it as-is. Mutations are rare — a stream appears when a peer
// connects and disappears when it breaks — so they copy the slice under a lock,
// then publish the replacement. A reader holding the old snapshot simply sees a
// dead stream and skips it, which is exactly the desired behavior.
type streamSet struct {
	mu      sync.Mutex
	streams atomic.Pointer[[]*stream]

	// onEmpty is called once, when the last stream of the set goes away. It
	// lets a dialing connection fail permanently rather than retransmitting
	// into a set with nowhere to send.
	onEmpty func()

	emptyOnce sync.Once
}

// newStreamSet returns an empty set. onEmpty may be nil.
func newStreamSet(onEmpty func()) *streamSet {
	s := &streamSet{onEmpty: onEmpty}
	empty := make([]*stream, 0)
	s.streams.Store(&empty)
	return s
}

// add publishes a new stream into the set.
func (s *streamSet) add(st *stream) {
	s.mu.Lock()
	cur := *s.streams.Load()
	next := make([]*stream, len(cur), len(cur)+1)
	copy(next, cur)
	next = append(next, st)
	s.streams.Store(&next)
	s.mu.Unlock()
}

// remove drops a stream. When the set becomes empty, onEmpty fires exactly once.
func (s *streamSet) remove(st *stream) {
	s.mu.Lock()
	cur := *s.streams.Load()
	next := make([]*stream, 0, len(cur))
	for _, existing := range cur {
		if existing != st {
			next = append(next, existing)
		}
	}
	s.streams.Store(&next)
	empty := len(next) == 0
	s.mu.Unlock()

	if empty && s.onEmpty != nil {
		s.emptyOnce.Do(s.onEmpty)
	}
}

// pick returns the live stream with the shortest send queue, so the fastest
// stream carries the most traffic. It returns nil when every stream has failed.
func (s *streamSet) pick() *stream {
	return pickStream(*s.streams.Load())
}

// len reports how many streams the set currently holds.
func (s *streamSet) len() int {
	return len(*s.streams.Load())
}

// each calls fn for every stream in the set.
func (s *streamSet) each(fn func(*stream)) {
	for _, st := range *s.streams.Load() {
		fn(st)
	}
}

// closeAll shuts down every stream and waits for their goroutines.
func (s *streamSet) closeAll() {
	for _, st := range *s.streams.Load() {
		st.close()
	}
}

// pickStream chooses the usable stream with the shortest send queue. It returns
// nil when every stream has already failed.
func pickStream(streams []*stream) *stream {
	var best *stream
	for _, s := range streams {
		if s.closed() {
			continue
		}
		if best == nil || s.depth() < best.depth() {
			best = s
		}
	}
	return best
}
