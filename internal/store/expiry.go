package store

import "time"

// defaultSweepInterval is how often the background active-expiry goroutine
// wakes up to look for expired keys. 100ms mirrors Redis's own default
// server "cron" cadence (it runs housekeeping, including a slice of active
// expiry, ~10 times a second) — frequent enough that memory used by expired
// keys is reclaimed promptly, infrequent enough that the periodic full
// lock+scan is negligible overhead.
const defaultSweepInterval = 100 * time.Millisecond

// NewWithSweepInterval creates a Store and immediately starts its
// background active-expiry goroutine at the given interval. Exposed
// separately from New() so tests can use a much shorter interval instead
// of sleeping for 100ms+ per assertion.
func NewWithSweepInterval(interval time.Duration) *Store {
	s := &Store{
		data:      NewHashTable[*Value](),
		expires:   NewHashTable[int64](),
		stopSweep: make(chan struct{}),
		sweepDone: make(chan struct{}),
	}
	go s.runActiveExpiry(interval)
	return s
}

// Close stops the background active-expiry goroutine and waits for it to
// actually exit before returning. This matters for graceful shutdown (the
// server, and tests) and to avoid leaking a goroutine that outlives its
// Store — a `go vet`/leak-detector-visible bug that's easy to introduce by
// forgetting to ever signal the goroutine to stop.
//
// stopSweep/sweepDone is the "channel as shutdown signal" pattern: closing
// stopSweep (rather than sending a value) lets a single close() wake the
// goroutine regardless of timing, and sweepDone lets Close() block until
// the goroutine has actually observed that signal and returned, rather
// than just assuming it will.
func (s *Store) Close() {
	close(s.stopSweep)
	<-s.sweepDone
}

// runActiveExpiry is the body of the background sweeper goroutine. It's
// the "active expiration" half of TTL support (the other half, lazy
// expiration, lives in isExpired and is checked inline by Get/Exists/etc).
// Active expiration exists because lazy expiration alone would let an
// expired key that nobody ever reads again sit in memory forever — a slow
// memory leak in any workload with write-once, read-rarely keys.
//
// A select over the ticker channel and the stop channel is the idiomatic
// Go way to run "do X periodically, but stop cleanly when told to" without
// resorting to a shared boolean flag (which would need its own
// synchronization to be read safely from another goroutine anyway).
func (s *Store) runActiveExpiry(interval time.Duration) {
	defer close(s.sweepDone)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.sweepExpiredKeys()
		case <-s.stopSweep:
			return
		}
	}
}

// sweepExpiredKeys scans the expires index and physically removes any key
// whose TTL has passed.
//
// This walks only s.expires (keys that actually have a TTL set), not the
// whole data table — in a store where most keys never get an EXPIRE call,
// that's the difference between an O(keys-with-ttl) sweep and an
// O(all-keys) one. Real Redis takes this further with random sampling
// (check ~20 random keys with TTLs, and keep sampling if more than 25% of
// the sample was expired) so a single sweep tick stays roughly constant
// time even with millions of keys carrying a TTL; a full scan of the
// expires index was chosen here instead purely for clarity of an already
// clearly-scoped portfolio piece — the sampling approach would be a
// one-function change on top of this same expires index if this needed to
// scale further.
//
// Expired keys are collected into a slice during ForEach and deleted only
// after ForEach returns. Deleting a *different* bucket-chain entry than the
// one currently being visited while ForEach is mid-traversal would corrupt
// the chain out from under the iterator, so the two-phase
// "gather, then mutate" approach sidesteps that entirely (see the
// ForEach doc comment in hashtable.go).
func (s *Store) sweepExpiredKeys() {
	now := time.Now().UnixNano()

	s.mu.Lock()
	defer s.mu.Unlock()

	var expiredKeys []string
	s.expires.ForEach(func(key string, expireAt int64) {
		if now >= expireAt {
			expiredKeys = append(expiredKeys, key)
		}
	})

	for _, key := range expiredKeys {
		s.data.Delete(key)
		s.expires.Delete(key)
	}
}

// isExpired reports whether key has a TTL that has passed — i.e. whether
// it should be *treated* as absent by the caller, regardless of whether
// it's still physically present in s.data.
//
// This is the lazy-expiration half of TTL support: it only ever reads
// s.expires and never deletes anything, which means it's safe to call
// under s.mu.RLock(). Read-only commands (Get, Exists, Keys, LLen, LRange)
// call this and simply treat a "yes, expired" answer as "not found" —
// they do NOT upgrade to a write lock to physically delete the key inline.
//
// That's a deliberate simplification: reclaiming the memory is left to
// either the active sweeper (within one sweep interval) or to the next
// write-lock-holding command that happens to touch the same key (several
// of which opportunistically clean up while they're already holding the
// lock, e.g. getOrCreateList and Expire). The alternative — upgrading a
// read lock to a write lock inline whenever an expired key is observed —
// would force every GET on an expired key onto the exclusive-lock path,
// which is exactly the kind of read/write contention RWMutex is meant to
// avoid. Decoupling "is this key visible" from "has this key's memory been
// reclaimed" is the same idea behind tombstones/MVCC visibility in real
// databases, just applied at a much smaller scale here.
//
// Caller must hold s.mu (either lock).
func (s *Store) isExpired(key string) bool {
	expireAt, hasTTL := s.expires.Get(key)
	if !hasTTL {
		return false
	}
	return time.Now().UnixNano() >= expireAt
}

// Expire sets key to expire after ttl elapses, returning true if the key
// existed (and so the TTL was actually set) or false if it didn't — this
// mirrors Redis's EXPIRE, which returns 1/0 for the same distinction.
func (s *Store) Expire(key string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isExpired(key) {
		// Already logically gone; opportunistically reclaim it now that
		// we hold the write lock, and report "key doesn't exist" like
		// Redis does for EXPIRE on an already-expired key.
		s.data.Delete(key)
		s.expires.Delete(key)
		return false
	}

	if _, ok := s.data.Get(key); !ok {
		return false
	}

	s.expires.Set(key, time.Now().Add(ttl).UnixNano())
	return true
}

// Sentinel return values for TTL, matching Redis's TTL command exactly:
// -2 means the key doesn't exist, -1 means it exists but has no TTL set.
const (
	TTLKeyNotFound int64 = -2
	TTLNoExpiry    int64 = -1
)

// TTL returns the remaining time to live for key, in whole seconds
// (rounded to the nearest second), or one of the TTLKeyNotFound /
// TTLNoExpiry sentinels.
func (s *Store) TTL(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isExpired(key) {
		return TTLKeyNotFound
	}
	if _, ok := s.data.Get(key); !ok {
		return TTLKeyNotFound
	}

	expireAt, hasTTL := s.expires.Get(key)
	if !hasTTL {
		return TTLNoExpiry
	}

	remaining := time.Duration(expireAt - time.Now().UnixNano())
	if remaining <= 0 {
		return TTLKeyNotFound
	}
	return int64(remaining.Round(time.Second) / time.Second)
}
