package store

import "sync"

// Store is the concurrency-safe, typed API on top of the raw HashTable.
// This is the layer commands (and eventually the TCP server) talk to.
//
// A single sync.RWMutex guards the data table for every operation. This is
// a deliberate, coarse-grained choice over the alternatives:
//   - sync.Map was considered, but it's optimized for the "many goroutines,
//     mostly-disjoint keys, rare writes" access pattern (its own docs say
//     so). A key-value store gets read *and* written to the same hot keys
//     constantly, which is exactly the case sync.Map's docs warn performs
//     worse than a plain map+Mutex.
//   - Per-bucket / striped locking (sharding the lock, like Java's
//     ConcurrentHashMap) would allow more parallelism, but it's much
//     harder to reason about correctness for operations that need a
//     consistent view across the whole table (KEYS, FLUSHALL, resize,
//     snapshotting for persistence) — those would need to acquire every
//     shard's lock anyway, which erases most of the benefit while adding
//     real complexity.
// A single RWMutex is the simplest thing that is obviously correct, and
// RWMutex specifically (not a plain Mutex) matters because GET is expected
// to vastly outnumber SET/DEL in a typical workload: RWMutex lets any
// number of concurrent readers (goroutines running GET/EXISTS/KEYS) proceed
// in parallel, and only blocks readers out while a writer holds the
// exclusive lock. (Eviction bookkeeping has its own, separate lock — see
// the EvictionPolicy doc comment in eviction.go for why.)
type Store struct {
	mu   sync.RWMutex
	data *HashTable[*Value]

	// expires holds the absolute expiry time (Unix nanoseconds) for every
	// key that has a TTL set. Keys with no TTL simply have no entry here —
	// see expiry.go for the full rationale, but in short: this mirrors
	// Redis's own design of keeping a separate "expires" dictionary rather
	// than tagging every value with an (almost-always-unused) expiry
	// field, so the active-expiry sweep only ever has to walk keys that
	// can actually expire.
	expires *HashTable[int64]

	cfg    Config
	policy EvictionPolicy
	// usedBytes is a running, approximate total of key+value content
	// bytes, maintained incrementally on every mutation rather than
	// recomputed by walking the table. See Config.MaxMemoryBytes for why
	// it's approximate.
	usedBytes int64

	stopSweep chan struct{} // closed by Close() to signal the sweeper to exit.
	sweepDone chan struct{} // closed by the sweeper goroutine once it has exited.
}

// Set stores key as a string value, overwriting whatever was there before
// (including a different type — like Redis, SET always succeeds and
// replaces the key wholesale). Like Redis's plain SET, this also clears any
// TTL that was previously set on the key: a fresh value with no explicit
// expiry is a fresh key, not a continuation of the old one's lifetime.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.replace(key, &Value{Type: TypeString, Str: value})
	s.expires.Delete(key)
	s.policy.Touch(key)
	s.enforceLimits()
}

// Get returns the string value for key. found is false if the key doesn't
// exist (or has lazily expired); err is ErrWrongType if the key holds a
// non-string value.
func (s *Store) Get(key string) (value string, found bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isExpired(key) {
		return "", false, nil
	}
	v, ok := s.data.Get(key)
	if !ok {
		return "", false, nil
	}
	if v.Type != TypeString {
		return "", false, ErrWrongType
	}

	// Recording recency here — while only holding the *read* lock on the
	// data table — is exactly why EvictionPolicy implementations manage
	// their own internal mutex instead of sharing s.mu: this call mutates
	// the LRU list, but does so under lruPolicy's own lock, so it doesn't
	// need (and mustn't take) s.mu for writing.
	s.policy.Touch(key)
	return v.Str, true, nil
}

// Del removes one or more keys and returns how many were actually present
// (matching Redis's DEL semantics, which is variadic and returns a count).
// A logically-expired-but-not-yet-swept key counts as absent, matching
// what a client would observe from GET/EXISTS on that key.
func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for _, key := range keys {
		expired := s.isExpired(key)
		s.expires.Delete(key)
		if s.removeFromData(key) && !expired {
			removed++
		}
	}
	return removed
}

// Exists returns how many of the given keys are present. Like Del, this
// matches Redis's variadic EXISTS, which counts rather than returning a
// bool, so callers can pass duplicate keys and get a duplicate-aware count.
func (s *Store) Exists(keys ...string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for _, key := range keys {
		if s.isExpired(key) {
			continue
		}
		if _, ok := s.data.Get(key); ok {
			count++
		}
	}
	return count
}

// Keys returns a snapshot of every key currently in the store, excluding
// keys that have lazily expired but haven't been physically swept yet.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	all := s.data.Keys()
	live := make([]string, 0, len(all))
	for _, k := range all {
		if !s.isExpired(k) {
			live = append(live, k)
		}
	}
	return live
}

// FlushAll removes every key, resetting the store to empty.
func (s *Store) FlushAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data.Clear()
	s.expires.Clear()
	s.policy.Clear()
	s.usedBytes = 0
}

// LPush prepends one or more values to the list at key (creating the list
// if key doesn't exist) and returns the resulting list length. Values are
// pushed one at a time in the order given, matching Redis's LPUSH, which
// means the *last* argument ends up at the head of the list.
func (s *Store) LPush(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	list, err := s.getOrCreateList(key)
	if err != nil {
		return 0, err
	}
	for _, v := range values {
		list.List = append([]string{v}, list.List...)
	}
	s.replace(key, list)
	s.policy.Touch(key)
	s.enforceLimits()
	return len(list.List), nil
}

// RPush appends one or more values to the list at key (creating the list
// if key doesn't exist) and returns the resulting list length.
func (s *Store) RPush(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	list, err := s.getOrCreateList(key)
	if err != nil {
		return 0, err
	}
	list.List = append(list.List, values...)
	s.replace(key, list)
	s.policy.Touch(key)
	s.enforceLimits()
	return len(list.List), nil
}

// LLen returns the length of the list at key (0 if the key doesn't exist).
func (s *Store) LLen(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isExpired(key) {
		return 0, nil
	}
	v, ok := s.data.Get(key)
	if !ok {
		return 0, nil
	}
	if v.Type != TypeList {
		return 0, ErrWrongType
	}
	s.policy.Touch(key)
	return len(v.List), nil
}

// LRange returns the elements of the list at key between start and stop
// (inclusive), supporting Redis-style negative indices meaning "from the
// end" (-1 is the last element). An empty slice is returned for a missing
// key rather than an error, matching Redis's LRANGE semantics.
func (s *Store) LRange(key string, start, stop int) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isExpired(key) {
		return []string{}, nil
	}
	v, ok := s.data.Get(key)
	if !ok {
		return []string{}, nil
	}
	if v.Type != TypeList {
		return nil, ErrWrongType
	}
	s.policy.Touch(key)

	n := len(v.List)
	start = normalizeIndex(start, n)
	stop = normalizeIndex(stop, n)
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || n == 0 {
		return []string{}, nil
	}

	// Copy rather than slice the underlying array directly: callers get
	// their own slice, so they can't accidentally mutate list state stored
	// in the table out from under the lock we've already released.
	out := make([]string, stop-start+1)
	copy(out, v.List[start:stop+1])
	return out, nil
}

// getOrCreateList fetches key's list Value, creating a new empty one if the
// key is absent (or has lazily expired). Caller must hold s.mu (write
// lock).
func (s *Store) getOrCreateList(key string) (*Value, error) {
	if s.isExpired(key) {
		// We already hold the write lock for this key, so this is a good
		// opportunity to physically reclaim the stale entry rather than
		// waiting on the next active-sweep tick.
		s.removeFromData(key)
		s.expires.Delete(key)
		s.policy.RemoveKey(key)
		return &Value{Type: TypeList}, nil
	}

	v, ok := s.data.Get(key)
	if !ok {
		return &Value{Type: TypeList}, nil
	}
	if v.Type != TypeList {
		return nil, ErrWrongType
	}
	return v, nil
}

// entrySize is the approximate byte cost attributed to one key/value pair
// for MaxMemoryBytes accounting — see Config.MaxMemoryBytes for what this
// deliberately does and doesn't count.
func entrySize(key string, v *Value) int64 {
	size := int64(len(key))
	switch v.Type {
	case TypeString:
		size += int64(len(v.Str))
	case TypeList:
		for _, e := range v.List {
			size += int64(len(e))
		}
	}
	return size
}

// replace writes newValue for key, keeping usedBytes correct by first
// subtracting whatever key previously cost (if anything). Caller must hold
// s.mu (write lock).
func (s *Store) replace(key string, newValue *Value) {
	if old, ok := s.data.Get(key); ok {
		s.usedBytes -= entrySize(key, old)
	}
	s.data.Set(key, newValue)
	s.usedBytes += entrySize(key, newValue)
}

// removeFromData deletes key from the data table (only — callers are
// responsible for also cleaning up s.expires / s.policy as appropriate)
// and keeps usedBytes correct. Reports whether key was actually present.
// Caller must hold s.mu (write lock).
func (s *Store) removeFromData(key string) bool {
	v, ok := s.data.Get(key)
	if !ok {
		return false
	}
	s.usedBytes -= entrySize(key, v)
	s.data.Delete(key)
	return true
}

// overLimit reports whether the store currently exceeds a configured
// resource limit. Caller must hold s.mu (write lock).
func (s *Store) overLimit() bool {
	if s.cfg.MaxKeys > 0 && s.data.Len() > s.cfg.MaxKeys {
		return true
	}
	if s.cfg.MaxMemoryBytes > 0 && s.usedBytes > s.cfg.MaxMemoryBytes {
		return true
	}
	return false
}

// enforceLimits evicts keys, one at a time via the configured
// EvictionPolicy, until the store is back within its configured limits (or
// the policy runs out of keys to suggest, which shouldn't happen in
// practice: every key Set/LPush/RPush touches is also handed to
// s.policy.Touch in the same critical section, so the policy's tracked set
// and s.data's key set never drift apart under normal operation).
//
// Called at the end of every write path that can grow the store
// (Set/LPush/RPush), while still holding s.mu — eviction must be atomic
// with the write that triggered it, or a burst of concurrent writers could
// all observe "over limit" and each evict, overshooting well below the
// configured limit.
func (s *Store) enforceLimits() {
	for s.overLimit() {
		key, ok := s.policy.Evict()
		if !ok {
			return
		}
		// The policy is a best-effort suggestion, not the source of
		// truth — always go through removeFromData so usedBytes/data.Len
		// stay correct even if the key had already been removed some
		// other way.
		s.removeFromData(key)
		s.expires.Delete(key)
	}
}

// normalizeIndex converts a possibly-negative Redis-style index (-1 = last
// element) into a plain non-negative index into a slice of length n.
func normalizeIndex(index, n int) int {
	if index < 0 {
		index = n + index
	}
	return index
}
