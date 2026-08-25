package store

// EvictionPolicyType selects which strategy Store uses to pick a victim
// key once a configured resource limit (MaxKeys / MaxMemoryBytes) is
// exceeded.
type EvictionPolicyType int

const (
	EvictionLRU EvictionPolicyType = iota
	EvictionRandom
)

func (t EvictionPolicyType) String() string {
	switch t {
	case EvictionLRU:
		return "lru"
	case EvictionRandom:
		return "random"
	default:
		return "unknown"
	}
}

// Config controls the store's resource limits and eviction behavior.
type Config struct {
	// MaxKeys caps the number of keys the store will hold; 0 disables the
	// check.
	MaxKeys int

	// MaxMemoryBytes caps an *approximate* running total of key+value
	// content bytes (see entrySize in store.go); 0 disables the check.
	// This is intentionally approximate: it counts string/list content
	// bytes, not actual Go runtime memory (struct headers, pointer
	// overhead, the hash table's own bucket/entry allocations, GC
	// overhead, map/slice growth slack, etc). Byte-exact accounting would
	// mean hooking the allocator or sampling runtime.MemStats, both
	// expensive to do per-write and famously imprecise for attributing
	// "how much did THIS key cost." An incrementally-maintained
	// approximate counter is the same trade-off most real LRU-cache
	// libraries make.
	//
	// If both MaxKeys and MaxMemoryBytes are set, eviction runs whenever
	// *either* limit is exceeded.
	MaxMemoryBytes int64

	EvictionPolicy EvictionPolicyType
}

// DefaultConfig returns a Store configuration with no resource limits (so
// eviction never triggers) and LRU selected as the policy that would be
// used if a limit were configured.
func DefaultConfig() Config {
	return Config{EvictionPolicy: EvictionLRU}
}

// EvictionPolicy decides which key to remove when the store is over its
// configured limits.
//
// Store is always the source of truth for which keys actually exist; a
// policy is just bookkeeping for "which key should go next," and it's
// deliberately allowed to be momentarily stale relative to Store (e.g. a
// key the policy would suggest evicting might already have been deleted or
// expired) because every caller re-checks existence in s.data before
// actually removing anything a policy names — see enforceLimits in
// store.go.
//
// Implementations own their own internal locking (see lruPolicy /
// randomPolicy) instead of relying on Store's mutex. That split matters
// for read concurrency: Store.Get only needs a read lock (s.mu.RLock) to
// read a value, but recording LRU recency is a *mutation* of the policy's
// internal linked list. If that mutation were guarded by s.mu, every GET
// would need the exclusive write lock just to update recency — exactly
// the read/write contention RWMutex exists to avoid. Giving each policy
// its own small internal mutex means GET can hold Store's RLock (for safe
// concurrent reads of the data table) while separately, briefly, taking
// the policy's own lock to record the touch — a short, independent
// critical section instead of serializing all reads store-wide. Lock
// ordering is one-directional (Store may call into a policy while holding
// s.mu, a policy never calls back into Store), so this can't deadlock.
type EvictionPolicy interface {
	// Touch records that key was just accessed (read or written).
	Touch(key string)
	// RemoveKey stops tracking key, e.g. because it was deleted or expired.
	RemoveKey(key string)
	// Evict picks a victim key, stops tracking it, and returns it. ok is
	// false if the policy has nothing left to track.
	Evict() (key string, ok bool)
	// Clear resets the policy to empty (e.g. for FLUSHALL).
	Clear()
}

func newEvictionPolicy(t EvictionPolicyType) EvictionPolicy {
	if t == EvictionRandom {
		return newRandomPolicy()
	}
	return newLRUPolicy()
}
