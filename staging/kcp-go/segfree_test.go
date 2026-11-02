package kcp

import "testing"

func TestSegFreeTracksSendWindowAndClose(t *testing.T) {
	s := newNonceSession(t, nil)
	if cap(s.kcp.segFree) != int(s.kcp.snd_wnd) {
		t.Fatalf("cap %d, send window %d", cap(s.kcp.segFree), s.kcp.snd_wnd)
	}

	const wnd = 4
	s.SetWindowSize(wnd, wnd)
	s.mu.Lock()
	if cap(s.kcp.segFree) != wnd {
		t.Fatalf("cap %d, want %d", cap(s.kcp.segFree), wnd)
	}
	segs := make([]segment, wnd*2)
	for i := range segs {
		segs[i] = s.kcp.newSegment(32)
	}
	for i := range segs {
		s.kcp.recycleSegment(&segs[i])
	}
	got := len(s.kcp.segFree)
	gotCap := cap(s.kcp.segFree)
	s.mu.Unlock()
	if got != wnd || gotCap != wnd {
		t.Fatalf("held %d buffers (cap %d), want %d", got, gotCap, wnd)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	if s.kcp.segFree != nil {
		t.Fatalf("close left %d cached buffers", len(s.kcp.segFree))
	}
	s.mu.Unlock()
}
