package store

import "time"

// Entry is a flat, encoder-agnostic view of one key's complete state
// (value + TTL), used for BGSAVE-style full snapshots. It's plain data —
// no methods, no pointers into Store's internals — specifically so it's
// safe to hand to encoding/gob (or any other encoder) without exposing
// live store state to something outside Store's own locking.
type Entry struct {
	Key      string
	Type     ValueType
	Str      string
	List     []string
	HasTTL   bool
	ExpireAt int64 // absolute Unix nanoseconds; meaningful only if HasTTL
}

// Snapshot returns a point-in-time copy of every live (non-expired) key.
// This is the data BGSAVE persists to disk; see Store.LoadSnapshot for the
// inverse operation used at startup.
func (s *Store) Snapshot() []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := make([]Entry, 0, s.data.Len())
	s.data.ForEach(func(key string, v *Value) {
		if s.isExpired(key) {
			return
		}
		e := Entry{Key: key, Type: v.Type, Str: v.Str}
		if v.Type == TypeList {
			e.List = append([]string(nil), v.List...) // defensive copy
		}
		if expireAt, ok := s.expires.Get(key); ok {
			e.HasTTL = true
			e.ExpireAt = expireAt
		}
		entries = append(entries, e)
	})
	return entries
}

// LoadSnapshot replaces the store's entire contents with entries. Used at
// startup to restore from a previously-saved BGSAVE file. Entries whose
// TTL has already passed (the snapshot was taken a while ago) are skipped
// rather than loaded and then immediately swept by the active expirer.
func (s *Store) LoadSnapshot(entries []Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data.Clear()
	s.expires.Clear()
	s.policy.Clear()
	s.usedBytes = 0

	now := time.Now().UnixNano()
	for _, e := range entries {
		if e.HasTTL && now >= e.ExpireAt {
			continue
		}
		s.replace(e.Key, &Value{Type: e.Type, Str: e.Str, List: e.List})
		if e.HasTTL {
			s.expires.Set(e.Key, e.ExpireAt)
		}
		s.policy.Touch(e.Key)
	}

	// A snapshot taken under a looser (or no) limit and restored into a
	// store configured with a tighter one shouldn't silently ignore that
	// limit — trim down to it immediately rather than waiting for the
	// next write to notice.
	s.enforceLimits()
}
