package store

import (
	"fmt"
	"testing"
	"time"
)

func newTestStoreWithConfig(t *testing.T, cfg Config) *Store {
	t.Helper()
	s := NewWithOptions(cfg, time.Hour)
	t.Cleanup(s.Close)
	return s
}

func TestStoreMaxKeysTriggersLRUEviction(t *testing.T) {
	s := newTestStoreWithConfig(t, Config{MaxKeys: 3, EvictionPolicy: EvictionLRU})

	s.Set("a", "1")
	s.Set("b", "2")
	s.Set("c", "3")
	s.Get("a")      // touch a: recency order (MRU->LRU) becomes a, c, b
	s.Set("d", "4") // over MaxKeys; evicts the LRU key, which is now b

	if n := s.Exists("d", "a", "c"); n != 3 {
		t.Fatalf("expected d, a, c to survive eviction; Exists = %d, keys = %v", n, s.Keys())
	}
	if n := s.Exists("b"); n != 0 {
		t.Fatalf("expected b (least recently used) to be evicted; still exists")
	}
	if len(s.Keys()) != 3 {
		t.Fatalf("Keys() = %v; want exactly 3 keys (MaxKeys limit)", s.Keys())
	}
}

func TestStoreMaxKeysTriggersRandomEvictionStaysWithinLimit(t *testing.T) {
	s := newTestStoreWithConfig(t, Config{MaxKeys: 5, EvictionPolicy: EvictionRandom})

	for i := 0; i < 200; i++ {
		s.Set(fmt.Sprintf("k%d", i), "v")
	}

	if n := len(s.Keys()); n > 5 {
		t.Fatalf("len(Keys()) = %d; want <= 5 (MaxKeys)", n)
	}
}

func TestStoreMaxMemoryBytesTriggersEviction(t *testing.T) {
	// "k1"+"0123456789" = 2 + 10 = 12 bytes per entry; cap at 20 bytes
	// means at most one such entry can coexist.
	s := newTestStoreWithConfig(t, Config{MaxMemoryBytes: 20, EvictionPolicy: EvictionLRU})

	s.Set("k1", "0123456789")
	s.Set("k2", "0123456789")

	keys := s.Keys()
	if len(keys) != 1 || keys[0] != "k2" {
		t.Fatalf("Keys() = %v; want exactly [k2] (k1 evicted to stay under MaxMemoryBytes)", keys)
	}
}

func TestStoreFlushAllResetsEvictionTracking(t *testing.T) {
	s := newTestStoreWithConfig(t, Config{MaxKeys: 2, EvictionPolicy: EvictionLRU})

	s.Set("a", "1")
	s.Set("b", "2")
	s.FlushAll()

	// If eviction tracking still held stale entries for a/b, adding two
	// fresh keys might spuriously evict one of them as if it were "old."
	s.Set("c", "3")
	s.Set("d", "4")

	if n := s.Exists("c", "d"); n != 2 {
		t.Fatalf("expected both post-flush keys to survive; Exists = %d, keys = %v", n, s.Keys())
	}
}

func TestStoreDefaultConfigHasNoLimit(t *testing.T) {
	s := newTestStore(t)
	for i := 0; i < 1000; i++ {
		s.Set(fmt.Sprintf("k%d", i), "v")
	}
	if n := len(s.Keys()); n != 1000 {
		t.Fatalf("len(Keys()) = %d with default (unlimited) config; want 1000, no eviction", n)
	}
}

func TestStoreExpiredKeyDoesNotCountAgainstMaxKeysForever(t *testing.T) {
	s := NewWithOptions(Config{MaxKeys: 2, EvictionPolicy: EvictionLRU}, 10*time.Millisecond)
	t.Cleanup(s.Close)

	s.Set("x", "1")
	s.Expire("x", 5*time.Millisecond)
	time.Sleep(100 * time.Millisecond) // let the active sweeper reclaim x

	s.Set("y", "1")
	s.Set("z", "1")

	if n := s.Exists("y", "z"); n != 2 {
		t.Fatalf("expected y and z to both survive (x already swept, not competing for the limit); keys = %v", s.Keys())
	}
}
