package store

import (
	"math/rand"
	"sync"
	"time"
)

// randomPolicy implements EvictionPolicy by evicting a uniformly random
// tracked key, with zero regard for access recency. It's the baseline the
// LRU policy is meant to be compared against:
//   - Cost per access: Touch is O(1) either way, but random eviction's
//     Touch does less work (no relinking on every single access — a
//     key already being tracked is just a no-op), so under very high
//     read throughput random has a lower constant-factor overhead.
//   - Hit rate: LRU actively protects hot keys from eviction; random
//     eviction can — and eventually will — evict a key that was just
//     accessed a moment ago, right before evicting one that hasn't been
//     touched in hours. For a workload with locality (some keys hot,
//     most cold — the common case), that means a measurably worse cache
//     hit rate.
// Random eviction genuinely wins when access patterns have *no* locality
// (uniformly random key access), where LRU's bookkeeping buys nothing and
// its extra per-access work is pure overhead — an edge case worth naming
// explicitly rather than implying LRU is strictly better.
type randomPolicy struct {
	mu sync.Mutex
	// keys holds every currently-tracked key in no particular order.
	keys []string
	// indexOf maps key -> its position in keys, so Remove/Evict can find
	// and delete a key in O(1) instead of scanning keys linearly. Backed
	// by our own HashTable, not Go's builtin map.
	indexOf *HashTable[int]
	rng     *rand.Rand
}

func newRandomPolicy() *randomPolicy {
	return &randomPolicy{
		indexOf: NewHashTable[int](),
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (p *randomPolicy) Touch(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.indexOf.Get(key); ok {
		return // already tracked; random eviction doesn't care about recency
	}
	p.indexOf.Set(key, len(p.keys))
	p.keys = append(p.keys, key)
}

func (p *randomPolicy) RemoveKey(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.removeTracked(key)
}

// removeTracked deletes key from the tracking slice in O(1) using the
// classic "swap with the last element, then shrink" trick: since keys is
// unordered (eviction picks a random index anyway), there's no need to
// preserve position, so a removal never has to shift every following
// element down the way a naive slice delete would.
//
// Caller must hold p.mu.
func (p *randomPolicy) removeTracked(key string) {
	idx, ok := p.indexOf.Get(key)
	if !ok {
		return
	}

	lastIdx := len(p.keys) - 1
	lastKey := p.keys[lastIdx]

	p.keys[idx] = lastKey
	p.indexOf.Set(lastKey, idx) // fine even when idx == lastIdx (self-assign)
	p.keys = p.keys[:lastIdx]
	p.indexOf.Delete(key)
}

func (p *randomPolicy) Evict() (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.keys) == 0 {
		return "", false
	}
	key := p.keys[p.rng.Intn(len(p.keys))]
	p.removeTracked(key)
	return key, true
}

func (p *randomPolicy) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.keys = nil
	p.indexOf = NewHashTable[int]()
}
