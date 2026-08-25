package store

import (
	"fmt"
	"testing"
)

// benchValue is a fixed, representative value size (64 bytes) used across
// every benchmark below, so dataset-size and concurrency comparisons
// aren't also secretly comparing different value sizes.
const benchValue = "0123456789012345678901234567890123456789012345678901234567890x"

// benchKeys pre-generates n distinct keys ONCE, outside any timed region.
// Benchmarks index into this slice rather than calling fmt.Sprintf inside
// the timed loop — string formatting cost would otherwise be measured as
// if it were store overhead, which is exactly the kind of noise a
// microbenchmark needs to avoid.
func benchKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	return keys
}

// datasetSizes are the "different data sizes" the SET/GET benchmarks run
// at. The interesting claim being tested here is that a hash table's core
// operations are O(1) regardless of how many keys it already holds — so
// ns/op for a lookup in a 1,000-key table and a 1,000,000-key table should
// land in the same ballpark, not scale with table size the way a linear
// structure would.
var datasetSizes = []int{1_000, 100_000, 1_000_000}

func newPopulatedStore(keys []string) *Store {
	s := New()
	for _, k := range keys {
		s.Set(k, benchValue)
	}
	return s
}

// BenchmarkSet measures single-goroutine SET throughput. Because the store
// is pre-populated with exactly the keys being set, every operation in the
// timed loop is an in-place update (see HashTable.Set), not a fresh
// insertion — deliberately so the timing reflects steady-state SET cost,
// not occasionally-amortized resize cost, which would make results noisy
// and harder to compare across runs.
func BenchmarkSet(b *testing.B) {
	for _, n := range datasetSizes {
		b.Run(fmt.Sprintf("dataset=%d", n), func(b *testing.B) {
			keys := benchKeys(n)
			s := newPopulatedStore(keys)
			defer s.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Set(keys[i%n], benchValue)
			}
		})
	}
}

// BenchmarkGet measures single-goroutine GET throughput at each dataset
// size, same rationale as BenchmarkSet.
func BenchmarkGet(b *testing.B) {
	for _, n := range datasetSizes {
		b.Run(fmt.Sprintf("dataset=%d", n), func(b *testing.B) {
			keys := benchKeys(n)
			s := newPopulatedStore(keys)
			defer s.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Get(keys[i%n])
			}
		})
	}
}

// BenchmarkSetParallel measures SET throughput under concurrent access —
// run with `go test -bench BenchmarkSetParallel -cpu 1,2,4,8` to sweep
// concurrency levels. Because Store.Set takes Store's exclusive write
// lock, this is expected to show little to no improvement (and likely
// some regression from lock contention/goroutine scheduling overhead) as
// -cpu increases: SET operations are fundamentally serialized against
// each other regardless of how many goroutines are issuing them. That's
// the whole point of measuring it — it's the concrete, benchmarked
// evidence for "writes don't parallelize under a single RWMutex," not
// just an assertion in a comment.
func BenchmarkSetParallel(b *testing.B) {
	for _, n := range datasetSizes {
		b.Run(fmt.Sprintf("dataset=%d", n), func(b *testing.B) {
			keys := benchKeys(n)
			s := newPopulatedStore(keys)
			defer s.Close()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					s.Set(keys[i%n], benchValue)
					i++
				}
			})
		})
	}
}

// BenchmarkGetParallel measures GET throughput under concurrent access —
// same -cpu sweep as BenchmarkSetParallel. Unlike SET, GET only takes
// Store's RLock, so multiple goroutines can proceed without waiting on
// each other for the data itself. Whether that translates into ns/op
// actually improving as -cpu increases is exactly the thing worth
// measuring rather than assuming: RWMutex still does atomic bookkeeping
// on its internal reader count on every RLock/RUnlock, and that counter
// is itself a shared cache line all readers contend on — so read
// concurrency can still show real overhead under heavy contention, just a
// different (and cheaper) kind than SET's full mutual exclusion. See the
// benchmark results table in the README for what this actually measured.
func BenchmarkGetParallel(b *testing.B) {
	for _, n := range datasetSizes {
		b.Run(fmt.Sprintf("dataset=%d", n), func(b *testing.B) {
			keys := benchKeys(n)
			s := newPopulatedStore(keys)
			defer s.Close()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					s.Get(keys[i%n])
					i++
				}
			})
		})
	}
}
