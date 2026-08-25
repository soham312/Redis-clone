// Package store implements the in-memory key-value engine, starting with a
// hand-rolled hash table. Go's builtin map is intentionally not used here:
// the point of this package is to demonstrate (and be able to explain in an
// interview) exactly how a hash table works under the hood.
package store

// entry is one key/value pair inside a bucket's collision chain.
//
// Collisions are resolved with separate chaining (each bucket holds a
// singly linked list of entries) rather than open addressing. Chaining was
// chosen over open addressing for three reasons that matter in an
// interview:
//  1. Deletion is trivial (unlink a node) — open addressing needs
//     tombstones or backward-shift deletion to avoid breaking probe
//     sequences, which is a much easier place to introduce a bug.
//  2. Load factor can safely exceed 1.0 before it matters — chains just get
//     a bit longer — whereas open addressing degrades sharply as the table
//     fills up and *must* stay well under 1.0.
//  3. It mirrors how Go's own runtime map is implemented (buckets of
//     entries), so the mental model transfers directly.
// The tradeoff is pointer chasing (cache-unfriendly) and one extra word of
// memory per entry for the `next` pointer — open addressing would win on
// raw cache locality for small/simple value types.
type entry[V any] struct {
	key   string
	value V
	next  *entry[V]
}

// HashTable is a hand-rolled hash table using separate chaining.
//
// It's parameterized over the value type (V) with Go generics rather than
// hard-coded to *Value. That's not speculative "future-proofing" — it's
// needed as of this stage: TTL tracking (see expiry.go) reuses this exact
// same implementation for a second table mapping key -> expiry timestamp
// (int64), instead of either duplicating the whole chaining/resizing
// implementation or reaching for a builtin map. One well-tested hash table,
// two instantiations: HashTable[*Value] for the main data table and
// HashTable[int64] for the expiry index.
//
// IMPORTANT: HashTable itself is NOT safe for concurrent use. Thread safety
// is deliberately layered on top by Store (internal/store/store.go) using a
// single sync.RWMutex around whole operations. Two reasons for that split:
//   - Single Responsibility: the hash table's job is correct hashing,
//     chaining and resizing; concurrency control is a separate concern with
//     its own tradeoffs (e.g. read/write fairness, lock granularity).
//   - Coarse-grained locking here is actually the *correct* choice: a
//     resize (rehashing every entry into a new bucket array) must be
//     atomic with respect to every other operation, so a single RWMutex at
//     the Store layer is simpler and less bug-prone than trying to make
//     the table lock-free or fine-grained internally. This is the same
//     pattern Go's own `map` uses: the map has no internal lock at all, and
//     callers are expected to add one (or use sync.Map) if they need
//     concurrent access.
type HashTable[V any] struct {
	buckets    []*entry[V]
	numEntries int
}

// Design constants for the resize policy.
const (
	initialCapacity  = 16   // must stay a power of two, see hashToIndex.
	maxLoadFactor    = 0.75 // grow when numEntries/capacity exceeds this.
	growthMultiplier = 2
)

// NewHashTable creates an empty hash table with a small power-of-two
// starting capacity. Starting small keeps memory low for short-lived or
// small stores; growth doubles capacity on demand (amortized O(1) insert).
func NewHashTable[V any]() *HashTable[V] {
	return &HashTable[V]{
		buckets: make([]*entry[V], initialCapacity),
	}
}

// hashKey computes a 64-bit FNV-1a hash of the key.
//
// FNV-1a was implemented by hand (rather than reaching for hash/fnv) so the
// full algorithm is visible and explainable: start from an offset basis,
// and for every byte, XOR it into the running hash and then multiply by a
// prime. The "a" variant (XOR before multiply, rather than after) gives
// slightly better avalanche behavior for short keys, which matters because
// most command keys in a Redis-like store are short strings.
//
// FNV-1a is not cryptographically secure (a malicious client could craft
// keys that collide, causing a hash-flooding DoS), but that's an accepted
// tradeoff for an educational/portfolio store — production Redis uses
// SipHash for exactly this reason. Worth mentioning proactively in an
// interview as a "what I'd change for production" point.
func hashKey(key string) uint64 {
	const offsetBasis64 uint64 = 14695981039346656037
	const prime64 uint64 = 1099511628211

	hash := offsetBasis64
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= prime64
	}
	return hash
}

// hashToIndex maps a hash to a bucket index.
//
// Capacity is always kept a power of two specifically so this can be
// `hash & (capacity-1)` instead of `hash % capacity`. Bitwise AND is a
// single fast instruction, while integer modulo (especially of a uint64)
// is comparatively expensive — a classic hash table micro-optimization
// that's easy to explain and easy to get wrong if capacity isn't a power
// of two (the mask trick silently produces incorrect results otherwise).
func hashToIndex(hash uint64, capacity int) int {
	return int(hash) & (capacity - 1)
}

// Set inserts or updates the value for key. Returns true if this was an
// update to an existing key (false if it was a new insertion), which lets
// callers (Store) track things like "was this a new key" without a second
// lookup.
func (h *HashTable[V]) Set(key string, value V) bool {
	index := hashToIndex(hashKey(key), len(h.buckets))

	// Walk the chain first: if the key already exists, update in place
	// rather than appending a duplicate node.
	for e := h.buckets[index]; e != nil; e = e.next {
		if e.key == key {
			e.value = value
			return true
		}
	}

	// New key: prepend to the chain. Prepending (vs appending) is O(1)
	// because we don't need to walk to the end of the list — order within
	// a bucket has no semantic meaning, so insertion position doesn't
	// matter.
	h.buckets[index] = &entry[V]{key: key, value: value, next: h.buckets[index]}
	h.numEntries++

	// Resize check happens *after* insertion so the new entry is already
	// counted in numEntries when deciding the load factor.
	if h.loadFactor() > maxLoadFactor {
		h.resize()
	}
	return false
}

// Get looks up key and reports whether it was found.
func (h *HashTable[V]) Get(key string) (V, bool) {
	index := hashToIndex(hashKey(key), len(h.buckets))
	for e := h.buckets[index]; e != nil; e = e.next {
		if e.key == key {
			return e.value, true
		}
	}
	var zero V
	return zero, false
}

// Delete removes key if present and reports whether anything was removed.
func (h *HashTable[V]) Delete(key string) bool {
	index := hashToIndex(hashKey(key), len(h.buckets))

	// Standard singly-linked-list deletion: track the previous node so we
	// can splice the target out. Using a `prev` pointer here rather than a
	// pointer-to-pointer keeps the logic readable, at the minor cost of one
	// extra branch for the head-of-chain case.
	var prev *entry[V]
	for e := h.buckets[index]; e != nil; e = e.next {
		if e.key == key {
			if prev == nil {
				h.buckets[index] = e.next
			} else {
				prev.next = e.next
			}
			h.numEntries--
			return true
		}
		prev = e
	}
	return false
}

// Len returns the number of keys currently stored.
func (h *HashTable[V]) Len() int {
	return h.numEntries
}

// Keys returns a snapshot slice of every key in the table. Order is
// unspecified (it depends on bucket layout), matching the semantics of
// Go's own map iteration.
func (h *HashTable[V]) Keys() []string {
	keys := make([]string, 0, h.numEntries)
	for _, head := range h.buckets {
		for e := head; e != nil; e = e.next {
			keys = append(keys, e.key)
		}
	}
	return keys
}

// ForEach calls fn for every key/value pair. Used by callers (e.g. active
// expiry sweeps, snapshotting) that need to walk the whole table without
// allocating an intermediate slice via Keys(). fn must not mutate the table
// (it's called while walking bucket chains directly; inserting or deleting
// from within fn can corrupt the chain being walked — callers that need to
// delete based on what they see should collect keys during ForEach and
// delete them afterwards, which is exactly what the active expiry sweep
// does).
func (h *HashTable[V]) ForEach(fn func(key string, value V)) {
	for _, head := range h.buckets {
		for e := head; e != nil; e = e.next {
			fn(e.key, e.value)
		}
	}
}

// Clear resets the table to a fresh, empty state.
//
// This reallocates the bucket array at initialCapacity rather than looping
// over every bucket and setting it to nil. For a FLUSHALL-style "drop
// everything" operation that's both simpler and faster: a large table that
// grew to hold millions of keys shouldn't stay that large (and get walked
// bucket-by-bucket) just to become empty again.
func (h *HashTable[V]) Clear() {
	h.buckets = make([]*entry[V], initialCapacity)
	h.numEntries = 0
}

func (h *HashTable[V]) loadFactor() float64 {
	return float64(h.numEntries) / float64(len(h.buckets))
}

// resize doubles bucket capacity and rehashes every existing entry into the
// new bucket array.
//
// This is the expensive part of the "amortized O(1) insert" story: any
// single Set() can trigger an O(n) rehash, but because capacity doubles
// each time, the total cost of all resizes across n insertions is O(n),
// so the *average* cost per insertion stays O(1). This is the same
// argument used to justify amortized-O(1) append on a Go slice.
//
// Note that hashes are recomputed rather than cached and reused, trading a
// bit of CPU for simplicity — caching hashes alongside each entry would
// avoid recomputation but add memory overhead and complexity that isn't
// worth it at this scale.
func (h *HashTable[V]) resize() {
	newCapacity := len(h.buckets) * growthMultiplier
	newBuckets := make([]*entry[V], newCapacity)

	for _, head := range h.buckets {
		for e := head; e != nil; {
			next := e.next // save before we mutate e.next below
			index := hashToIndex(hashKey(e.key), newCapacity)
			e.next = newBuckets[index]
			newBuckets[index] = e
			e = next
		}
	}

	h.buckets = newBuckets
}
