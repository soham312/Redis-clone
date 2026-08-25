package store

import (
	"testing"
	"time"
)

// newTestStore returns a Store whose background sweeper effectively never
// fires during a test (a long interval), so tests that want to check
// *lazy* expiration in isolation aren't racing the active sweeper. Tests
// that specifically want the active sweeper use NewWithSweepInterval with
// a short interval instead.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	s := NewWithSweepInterval(time.Hour)
	t.Cleanup(s.Close)
	return s
}

func TestTTLNoExpirySentinel(t *testing.T) {
	s := newTestStore(t)
	s.Set("k", "v")

	if got := s.TTL("k"); got != TTLNoExpiry {
		t.Fatalf("TTL(k) = %d; want TTLNoExpiry (%d)", got, TTLNoExpiry)
	}
}

func TestTTLKeyNotFoundSentinel(t *testing.T) {
	s := newTestStore(t)
	if got := s.TTL("missing"); got != TTLKeyNotFound {
		t.Fatalf("TTL(missing) = %d; want TTLKeyNotFound (%d)", got, TTLKeyNotFound)
	}
}

func TestExpireOnMissingKeyReturnsFalse(t *testing.T) {
	s := newTestStore(t)
	if s.Expire("missing", time.Minute) {
		t.Fatalf("Expire(missing) = true; want false")
	}
}

func TestExpireSetsRoundTrippableTTL(t *testing.T) {
	s := newTestStore(t)
	s.Set("k", "v")

	if !s.Expire("k", 10*time.Second) {
		t.Fatalf("Expire(k, 10s) = false; want true")
	}
	got := s.TTL("k")
	if got < 9 || got > 10 {
		t.Fatalf("TTL(k) = %d; want ~10", got)
	}
}

func TestGetOnLazilyExpiredKeyReportsNotFound(t *testing.T) {
	s := newTestStore(t)
	s.Set("k", "v")
	s.Expire("k", 10*time.Millisecond)

	time.Sleep(30 * time.Millisecond) // past TTL; sweeper interval is 1h, so this is purely the lazy path

	if _, found, _ := s.Get("k"); found {
		t.Fatalf("Get(k) found a key past its TTL (lazy expiration didn't hide it)")
	}
	if n := s.Exists("k"); n != 0 {
		t.Fatalf("Exists(k) = %d after TTL passed; want 0", n)
	}
	for _, k := range s.Keys() {
		if k == "k" {
			t.Fatalf("Keys() included an expired key")
		}
	}

	// Lazy expiration only hides the key; it must NOT have physically
	// deleted it (that's the active sweeper's job, and this store's
	// sweeper interval is 1h) — whitebox-check s.data directly.
	if _, ok := s.data.Get("k"); !ok {
		t.Fatalf("lazily-expired key was physically removed from s.data — lazy expiration should only affect visibility")
	}
}

func TestActiveSweepPhysicallyRemovesExpiredKeys(t *testing.T) {
	s := NewWithSweepInterval(10 * time.Millisecond)
	t.Cleanup(s.Close)

	s.Set("k", "v")
	s.Expire("k", 5*time.Millisecond)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		s.mu.RLock()
		_, stillPresent := s.data.Get("k")
		s.mu.RUnlock()
		if !stillPresent {
			return // swept — test passes
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("active sweeper never physically removed an expired key within 500ms")
}

func TestSetClearsExistingTTL(t *testing.T) {
	s := newTestStore(t)
	s.Set("k", "v1")
	s.Expire("k", time.Minute)

	s.Set("k", "v2") // plain SET should clear the prior TTL, like Redis

	if got := s.TTL("k"); got != TTLNoExpiry {
		t.Fatalf("TTL(k) after re-SET = %d; want TTLNoExpiry (%d)", got, TTLNoExpiry)
	}
}

func TestDelCleansUpExpiresIndex(t *testing.T) {
	s := newTestStore(t)
	s.Set("k", "v")
	s.Expire("k", time.Minute)

	s.Del("k")

	if _, ok := s.expires.Get("k"); ok {
		t.Fatalf("s.expires still has an entry for a deleted key (leak)")
	}
}

func TestFlushAllClearsExpiresIndex(t *testing.T) {
	s := newTestStore(t)
	s.Set("k", "v")
	s.Expire("k", time.Minute)

	s.FlushAll()

	if s.expires.Len() != 0 {
		t.Fatalf("s.expires.Len() = %d after FlushAll(); want 0", s.expires.Len())
	}
}

func TestExpireOnAlreadyLazilyExpiredKeyReturnsFalseAndReclaims(t *testing.T) {
	s := newTestStore(t)
	s.Set("k", "v")
	s.Expire("k", 5*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	if s.Expire("k", time.Minute) {
		t.Fatalf("Expire on an already-expired key returned true; want false")
	}
	if _, ok := s.data.Get("k"); ok {
		t.Fatalf("Expire on an already-expired key should opportunistically reclaim it from s.data")
	}
}
