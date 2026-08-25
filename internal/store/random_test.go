package store

import (
	"fmt"
	"testing"
)

func TestRandomPolicyOnlyEvictsTrackedKeys(t *testing.T) {
	p := newRandomPolicy()
	tracked := map[string]bool{"a": true, "b": true, "c": true}
	for k := range tracked {
		p.Touch(k)
	}

	for i := 0; i < 3; i++ {
		key, ok := p.Evict()
		if !ok {
			t.Fatalf("Evict() #%d: ok=false with keys still tracked", i)
		}
		if !tracked[key] {
			t.Fatalf("Evict() #%d returned %q, which was never tracked", i, key)
		}
		delete(tracked, key)
	}
	if len(tracked) != 0 {
		t.Fatalf("%d tracked keys never evicted: %v", len(tracked), tracked)
	}
	if _, ok := p.Evict(); ok {
		t.Fatalf("Evict() after exhausting all tracked keys should return ok=false")
	}
}

func TestRandomPolicyEvictNeverReturnsSameKeyTwice(t *testing.T) {
	p := newRandomPolicy()
	for i := 0; i < 100; i++ {
		p.Touch(fmt.Sprintf("key-%d", i))
	}

	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		key, ok := p.Evict()
		if !ok {
			t.Fatalf("Evict() #%d: ok=false too early", i)
		}
		if seen[key] {
			t.Fatalf("Evict() returned %q twice", key)
		}
		seen[key] = true
	}
}

func TestRandomPolicyRemoveKey(t *testing.T) {
	p := newRandomPolicy()
	p.Touch("a")
	p.Touch("b")
	p.Touch("c")

	p.RemoveKey("b")

	for i := 0; i < 2; i++ {
		key, ok := p.Evict()
		if !ok {
			t.Fatalf("Evict() #%d: ok=false", i)
		}
		if key == "b" {
			t.Fatalf("Evict() returned %q after it was removed", key)
		}
	}
	if _, ok := p.Evict(); ok {
		t.Fatalf("Evict() should be empty after removing b and evicting the other two")
	}
}

// TestRandomPolicyRemoveKeyLastElement exercises the swap-with-last-then-
// shrink removal path specifically when the key being removed already IS
// the last element (idx == lastIdx) — the self-assignment edge case in
// removeTracked.
func TestRandomPolicyRemoveKeyLastElement(t *testing.T) {
	p := newRandomPolicy()
	p.Touch("a")
	p.Touch("b")

	p.RemoveKey("b") // b was inserted last, so it's at the tail of the slice

	key, ok := p.Evict()
	if !ok || key != "a" {
		t.Fatalf("Evict() = %q, %v; want a, true", key, ok)
	}
	if _, ok := p.Evict(); ok {
		t.Fatalf("policy should be empty")
	}
}

func TestRandomPolicyTouchIgnoresAlreadyTrackedKey(t *testing.T) {
	p := newRandomPolicy()
	p.Touch("a")
	p.Touch("a") // must not double-track

	count := 0
	for {
		if _, ok := p.Evict(); !ok {
			break
		}
		count++
	}
	if count != 1 {
		t.Fatalf("evicted %d times for a single key touched twice; want 1", count)
	}
}

func TestRandomPolicyClearResetsState(t *testing.T) {
	p := newRandomPolicy()
	p.Touch("a")
	p.Touch("b")

	p.Clear()

	if _, ok := p.Evict(); ok {
		t.Fatalf("Evict() after Clear() should find nothing")
	}
	p.Touch("c")
	key, ok := p.Evict()
	if !ok || key != "c" {
		t.Fatalf("Evict() after Clear()+Touch(c) = %q, %v; want c, true", key, ok)
	}
}
