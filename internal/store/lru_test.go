package store

import "testing"

func TestLRUPolicyEvictsLeastRecentlyUsed(t *testing.T) {
	p := newLRUPolicy()

	p.Touch("a")
	p.Touch("b")
	p.Touch("c")
	// Recency order, most- to least-recent: c, b, a.

	key, ok := p.Evict()
	if !ok || key != "a" {
		t.Fatalf("Evict() = %q, %v; want a, true (a was touched longest ago)", key, ok)
	}
}

func TestLRUPolicyTouchMovesKeyToMostRecent(t *testing.T) {
	p := newLRUPolicy()

	p.Touch("a")
	p.Touch("b")
	p.Touch("c")
	p.Touch("a") // re-touch: a is now most-recently-used, b becomes LRU
	// Recency order, most- to least-recent: a, c, b.

	key, ok := p.Evict()
	if !ok || key != "b" {
		t.Fatalf("Evict() = %q, %v; want b, true (re-touching a should protect it)", key, ok)
	}
	key, ok = p.Evict()
	if !ok || key != "c" {
		t.Fatalf("Evict() = %q, %v; want c, true", key, ok)
	}
	key, ok = p.Evict()
	if !ok || key != "a" {
		t.Fatalf("Evict() = %q, %v; want a, true (touched most recently, evicted last)", key, ok)
	}
}

func TestLRUPolicyEvictOnEmptyReturnsFalse(t *testing.T) {
	p := newLRUPolicy()
	if _, ok := p.Evict(); ok {
		t.Fatalf("Evict() on an empty policy returned ok=true")
	}
}

func TestLRUPolicyRemoveKeyStopsTracking(t *testing.T) {
	p := newLRUPolicy()
	p.Touch("a")
	p.Touch("b")

	p.RemoveKey("a")

	key, ok := p.Evict()
	if !ok || key != "b" {
		t.Fatalf("Evict() after RemoveKey(a) = %q, %v; want b, true", key, ok)
	}
	if _, ok := p.Evict(); ok {
		t.Fatalf("Evict() should be empty after removing a and evicting b")
	}
}

func TestLRUPolicyRemoveKeyOnUntrackedKeyIsNoop(t *testing.T) {
	p := newLRUPolicy()
	p.Touch("a")
	p.RemoveKey("never-tracked") // must not panic or corrupt state

	key, ok := p.Evict()
	if !ok || key != "a" {
		t.Fatalf("Evict() = %q, %v; want a, true", key, ok)
	}
}

func TestLRUPolicyClearResetsState(t *testing.T) {
	p := newLRUPolicy()
	p.Touch("a")
	p.Touch("b")

	p.Clear()

	if _, ok := p.Evict(); ok {
		t.Fatalf("Evict() after Clear() should find nothing")
	}

	// Must remain usable after Clear(), and the sentinel-linked list must
	// still be correctly wired (head <-> tail) rather than left dangling.
	p.Touch("c")
	key, ok := p.Evict()
	if !ok || key != "c" {
		t.Fatalf("Evict() after Clear()+Touch(c) = %q, %v; want c, true", key, ok)
	}
}

// TestLRUPolicyEvictUnlinksSentinelsCorrectly drives the list down to
// empty and back up repeatedly, which would surface a sentinel-linking bug
// (e.g. head.next/tail.prev left pointing at a freed node) as either a
// panic or a wrong eviction order.
func TestLRUPolicyRepeatedEmptyRefill(t *testing.T) {
	p := newLRUPolicy()
	for round := 0; round < 5; round++ {
		p.Touch("x")
		p.Touch("y")
		if k, ok := p.Evict(); !ok || k != "x" {
			t.Fatalf("round %d: Evict() = %q, %v; want x, true", round, k, ok)
		}
		if k, ok := p.Evict(); !ok || k != "y" {
			t.Fatalf("round %d: Evict() = %q, %v; want y, true", round, k, ok)
		}
		if _, ok := p.Evict(); ok {
			t.Fatalf("round %d: policy should be empty", round)
		}
	}
}
