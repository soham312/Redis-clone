package store

import (
	"sort"
	"testing"
	"time"
)

func TestSnapshotRoundTrip(t *testing.T) {
	s := newTestStore(t)
	s.Set("str", "hello")
	s.RPush("list", "a", "b", "c")
	s.Expire("str", time.Hour)

	entries := s.Snapshot()

	restored := newTestStore(t)
	restored.LoadSnapshot(entries)

	v, found, err := restored.Get("str")
	if err != nil || !found || v != "hello" {
		t.Fatalf("Get(str) after restore = %q, %v, %v; want hello, true, nil", v, found, err)
	}
	lr, err := restored.LRange("list", 0, -1)
	if err != nil {
		t.Fatalf("LRange(list): %v", err)
	}
	if !equalStrings(lr, []string{"a", "b", "c"}) {
		t.Fatalf("LRange(list) after restore = %v; want [a b c]", lr)
	}

	ttl := restored.TTL("str")
	if ttl < 3599 || ttl > 3600 {
		t.Fatalf("TTL(str) after restore = %d; want ~3600", ttl)
	}
}

func TestSnapshotExcludesExpiredKeys(t *testing.T) {
	s := newTestStore(t)
	s.Set("live", "1")
	s.Set("dying", "2")
	s.Expire("dying", 5*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	entries := s.Snapshot()

	keys := make([]string, 0, len(entries))
	for _, e := range entries {
		keys = append(keys, e.Key)
	}
	sort.Strings(keys)
	if !equalStrings(keys, []string{"live"}) {
		t.Fatalf("Snapshot() keys = %v; want only [live] (dying already expired)", keys)
	}
}

func TestLoadSnapshotSkipsAlreadyExpiredEntries(t *testing.T) {
	entries := []Entry{
		{Key: "expired", Type: TypeString, Str: "old", HasTTL: true, ExpireAt: time.Now().Add(-time.Hour).UnixNano()},
		{Key: "fresh", Type: TypeString, Str: "new"},
	}

	s := newTestStore(t)
	s.LoadSnapshot(entries)

	if _, found, _ := s.Get("expired"); found {
		t.Fatalf("Get(expired) found a key whose ExpireAt was already in the past")
	}
	if v, found, _ := s.Get("fresh"); !found || v != "new" {
		t.Fatalf("Get(fresh) = %q, %v; want new, true", v, found)
	}
}

func TestLoadSnapshotEnforcesConfiguredLimit(t *testing.T) {
	entries := make([]Entry, 0, 10)
	for i := 0; i < 10; i++ {
		entries = append(entries, Entry{Key: string(rune('a' + i)), Type: TypeString, Str: "v"})
	}

	s := newTestStoreWithConfig(t, Config{MaxKeys: 3, EvictionPolicy: EvictionLRU})
	s.LoadSnapshot(entries)

	if n := len(s.Keys()); n > 3 {
		t.Fatalf("len(Keys()) after LoadSnapshot = %d; want <= 3 (MaxKeys should apply on load too)", n)
	}
}
