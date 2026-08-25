package store

import (
	"fmt"
	"sort"
	"testing"
)

func TestHashTableSetGetBasic(t *testing.T) {
	h := NewHashTable[string]()

	if _, ok := h.Get("missing"); ok {
		t.Fatalf("Get on empty table should report not found")
	}

	h.Set("a", "1")
	h.Set("b", "2")

	if v, ok := h.Get("a"); !ok || v != "1" {
		t.Fatalf("Get(a) = %q, %v; want 1, true", v, ok)
	}
	if v, ok := h.Get("b"); !ok || v != "2" {
		t.Fatalf("Get(b) = %q, %v; want 2, true", v, ok)
	}
	if h.Len() != 2 {
		t.Fatalf("Len() = %d; want 2", h.Len())
	}
}

func TestHashTableSetOverwritesExistingKey(t *testing.T) {
	h := NewHashTable[string]()

	if updated := h.Set("a", "1"); updated {
		t.Fatalf("Set on a new key reported an update")
	}
	if updated := h.Set("a", "2"); !updated {
		t.Fatalf("Set on an existing key reported a new insertion")
	}

	if h.Len() != 1 {
		t.Fatalf("Len() = %d; want 1 (overwrite must not grow the table)", h.Len())
	}
	if v, _ := h.Get("a"); v != "2" {
		t.Fatalf("Get(a) = %q; want 2", v)
	}
}

func TestHashTableDelete(t *testing.T) {
	h := NewHashTable[string]()
	h.Set("a", "1")

	if h.Delete("missing") {
		t.Fatalf("Delete on a missing key returned true")
	}
	if !h.Delete("a") {
		t.Fatalf("Delete on an existing key returned false")
	}
	if h.Len() != 0 {
		t.Fatalf("Len() = %d after deleting the only key; want 0", h.Len())
	}
	if _, ok := h.Get("a"); ok {
		t.Fatalf("Get(a) found a key after it was deleted")
	}
}

// findColliding searches for n distinct keys that land in the same bucket
// at the given capacity — i.e. genuine hash collisions under our real
// hashKey/hashToIndex, not synthetic ones. A plain builtin map is used
// here purely as test scaffolding to group candidates by bucket index;
// this isn't the storage engine under test, just bookkeeping to find
// inputs that exercise it.
func findColliding(t *testing.T, n int, capacity int) []string {
	t.Helper()
	buckets := make(map[int][]string)
	for i := 0; ; i++ {
		k := fmt.Sprintf("key-%d", i)
		idx := hashToIndex(hashKey(k), capacity)
		buckets[idx] = append(buckets[idx], k)
		if len(buckets[idx]) == n {
			return buckets[idx]
		}
		if i > 1_000_000 {
			t.Fatalf("couldn't find %d colliding keys after 1,000,000 tries", n)
		}
	}
}

// TestHashTableCollisionChaining exercises the separate-chaining collision
// path directly: three keys that hash to the exact same bucket at the
// table's starting capacity must all still be independently gettable, and
// deleting any one of them (head, middle, or tail of the chain) must leave
// the other two intact.
func TestHashTableCollisionChaining(t *testing.T) {
	keys := findColliding(t, 3, initialCapacity)

	h := NewHashTable[string]()
	for i, k := range keys {
		h.Set(k, fmt.Sprintf("v%d", i))
	}
	if h.Len() != 3 {
		t.Fatalf("Len() = %d; want 3", h.Len())
	}
	for i, k := range keys {
		want := fmt.Sprintf("v%d", i)
		if v, ok := h.Get(k); !ok || v != want {
			t.Fatalf("Get(%q) = %q, %v; want %q, true", k, v, ok, want)
		}
	}

	// Keys were inserted in order keys[0], keys[1], keys[2], and Set
	// prepends new entries, so the chain is keys[2] -> keys[1] -> keys[0]
	// (head to tail). Delete the middle of the chain and confirm the
	// other two survive with the chain still correctly linked.
	if !h.Delete(keys[1]) {
		t.Fatalf("Delete(%q) (middle of chain) returned false", keys[1])
	}
	if _, ok := h.Get(keys[1]); ok {
		t.Fatalf("Get(%q) found a key after deleting it", keys[1])
	}
	if v, ok := h.Get(keys[0]); !ok || v != "v0" {
		t.Fatalf("Get(%q) after deleting chain middle = %q, %v; want v0, true", keys[0], v, ok)
	}
	if v, ok := h.Get(keys[2]); !ok || v != "v2" {
		t.Fatalf("Get(%q) after deleting chain middle = %q, %v; want v2, true", keys[2], v, ok)
	}

	// Now delete the head, then the tail, of what remains.
	if !h.Delete(keys[2]) {
		t.Fatalf("Delete(%q) (head of chain) returned false", keys[2])
	}
	if !h.Delete(keys[0]) {
		t.Fatalf("Delete(%q) (last remaining entry) returned false", keys[0])
	}
	if h.Len() != 0 {
		t.Fatalf("Len() = %d after deleting all three colliding keys; want 0", h.Len())
	}
}

// TestHashTableResizeGrowsAndPreservesEntries inserts enough entries to
// force several resizes (capacity doubling past the 0.75 load factor) and
// verifies every entry is still correctly retrievable afterward — the
// resize rehashes into new buckets, so this is really testing that
// resize's rehash-and-relink logic doesn't lose or corrupt any entry.
func TestHashTableResizeGrowsAndPreservesEntries(t *testing.T) {
	h := NewHashTable[int]()

	const n = 5000
	for i := 0; i < n; i++ {
		h.Set(fmt.Sprintf("key-%d", i), i)
	}

	if h.Len() != n {
		t.Fatalf("Len() = %d; want %d", h.Len(), n)
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%d", i)
		v, ok := h.Get(k)
		if !ok || v != i {
			t.Fatalf("Get(%q) = %d, %v; want %d, true", k, v, ok, i)
		}
	}
}

func TestHashTableKeysReturnsEveryKeyExactlyOnce(t *testing.T) {
	h := NewHashTable[int]()
	want := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key-%d", i)
		h.Set(k, i)
		want = append(want, k)
	}

	got := h.Keys()
	sort.Strings(got)
	sort.Strings(want)

	if len(got) != len(want) {
		t.Fatalf("Keys() returned %d keys; want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Keys()[%d] = %q; want %q", i, got[i], want[i])
		}
	}
}

func TestHashTableForEachVisitsEveryEntryExactlyOnce(t *testing.T) {
	h := NewHashTable[int]()
	for i := 0; i < 50; i++ {
		h.Set(fmt.Sprintf("key-%d", i), i)
	}

	seen := make(map[string]int)
	h.ForEach(func(key string, value int) {
		seen[key]++
	})

	if len(seen) != 50 {
		t.Fatalf("ForEach visited %d distinct keys; want 50", len(seen))
	}
	for k, count := range seen {
		if count != 1 {
			t.Fatalf("ForEach visited %q %d times; want exactly once", k, count)
		}
	}
}

func TestHashTableClearResetsToEmpty(t *testing.T) {
	h := NewHashTable[int]()
	for i := 0; i < 100; i++ {
		h.Set(fmt.Sprintf("key-%d", i), i)
	}

	h.Clear()

	if h.Len() != 0 {
		t.Fatalf("Len() = %d after Clear(); want 0", h.Len())
	}
	if len(h.buckets) != initialCapacity {
		t.Fatalf("bucket count = %d after Clear(); want back to initialCapacity %d", len(h.buckets), initialCapacity)
	}
	if _, ok := h.Get("key-0"); ok {
		t.Fatalf("Get found a key after Clear()")
	}

	// The table must still be fully usable after Clear(), not just empty.
	h.Set("fresh", 1)
	if v, ok := h.Get("fresh"); !ok || v != 1 {
		t.Fatalf("table unusable after Clear(): Get(fresh) = %d, %v", v, ok)
	}
}

// TestHashTableGenericOverPointerType is a light check that the generic
// HashTable works as intended for its other real instantiation in this
// codebase: key -> *Value (see store.go), not just plain value types.
func TestHashTableGenericOverPointerType(t *testing.T) {
	h := NewHashTable[*Value]()
	v := &Value{Type: TypeString, Str: "hello"}
	h.Set("k", v)

	got, ok := h.Get("k")
	if !ok || got != v {
		t.Fatalf("Get(k) = %v, %v; want the same pointer back, true", got, ok)
	}
}
