package store

import (
	"errors"
	"sort"
	"testing"
)

func TestStoreSetGet(t *testing.T) {
	s := newTestStore(t)
	s.Set("k", "v")

	v, found, err := s.Get("k")
	if err != nil || !found || v != "v" {
		t.Fatalf("Get(k) = %q, %v, %v; want v, true, nil", v, found, err)
	}
}

func TestStoreGetMissingKey(t *testing.T) {
	s := newTestStore(t)
	v, found, err := s.Get("missing")
	if err != nil || found || v != "" {
		t.Fatalf("Get(missing) = %q, %v, %v; want \"\", false, nil", v, found, err)
	}
}

func TestStoreGetWrongType(t *testing.T) {
	s := newTestStore(t)
	s.LPush("l", "a")

	_, _, err := s.Get("l")
	if !errors.Is(err, ErrWrongType) {
		t.Fatalf("Get on a list key: err = %v; want ErrWrongType", err)
	}
}

func TestStoreDelVariadicCountsOnlyPresentKeys(t *testing.T) {
	s := newTestStore(t)
	s.Set("a", "1")
	s.Set("b", "2")

	n := s.Del("a", "b", "missing")
	if n != 2 {
		t.Fatalf("Del(a, b, missing) = %d; want 2", n)
	}
	if n := s.Exists("a", "b"); n != 0 {
		t.Fatalf("Exists(a, b) after Del = %d; want 0", n)
	}
}

func TestStoreExistsCountsDuplicates(t *testing.T) {
	s := newTestStore(t)
	s.Set("a", "1")

	if n := s.Exists("a", "a", "missing"); n != 2 {
		t.Fatalf("Exists(a, a, missing) = %d; want 2 (duplicates counted)", n)
	}
}

func TestStoreKeys(t *testing.T) {
	s := newTestStore(t)
	s.Set("a", "1")
	s.Set("b", "2")
	s.Set("c", "3")

	got := s.Keys()
	sort.Strings(got)
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("Keys() = %v; want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Keys() = %v; want %v", got, want)
		}
	}
}

func TestStoreFlushAll(t *testing.T) {
	s := newTestStore(t)
	s.Set("a", "1")
	s.LPush("l", "x")

	s.FlushAll()

	if len(s.Keys()) != 0 {
		t.Fatalf("Keys() after FlushAll() is non-empty: %v", s.Keys())
	}
	if _, found, _ := s.Get("a"); found {
		t.Fatalf("a still found after FlushAll()")
	}
}

func TestStoreSetOverwritesDifferentType(t *testing.T) {
	s := newTestStore(t)
	s.LPush("k", "a", "b")

	s.Set("k", "now a string")

	v, found, err := s.Get("k")
	if err != nil || !found || v != "now a string" {
		t.Fatalf("Get(k) after overwriting a list with SET = %q, %v, %v", v, found, err)
	}
}

func TestStoreLPushOrderAndLength(t *testing.T) {
	s := newTestStore(t)
	n, err := s.LPush("l", "a", "b", "c")
	if err != nil {
		t.Fatalf("LPush: %v", err)
	}
	if n != 3 {
		t.Fatalf("LPush returned length %d; want 3", n)
	}

	// LPUSH pushes args one at a time, so the LAST arg ends up at the head.
	got, err := s.LRange("l", 0, -1)
	if err != nil {
		t.Fatalf("LRange: %v", err)
	}
	want := []string{"c", "b", "a"}
	if !equalStrings(got, want) {
		t.Fatalf("LRange after LPush(a,b,c) = %v; want %v", got, want)
	}
}

func TestStoreRPushOrderAndLength(t *testing.T) {
	s := newTestStore(t)
	n, err := s.RPush("l", "a", "b", "c")
	if err != nil {
		t.Fatalf("RPush: %v", err)
	}
	if n != 3 {
		t.Fatalf("RPush returned length %d; want 3", n)
	}

	got, _ := s.LRange("l", 0, -1)
	want := []string{"a", "b", "c"}
	if !equalStrings(got, want) {
		t.Fatalf("LRange after RPush(a,b,c) = %v; want %v", got, want)
	}
}

func TestStoreLLenOnMissingKeyIsZero(t *testing.T) {
	s := newTestStore(t)
	n, err := s.LLen("missing")
	if err != nil || n != 0 {
		t.Fatalf("LLen(missing) = %d, %v; want 0, nil", n, err)
	}
}

func TestStoreLRangeNegativeIndices(t *testing.T) {
	s := newTestStore(t)
	s.RPush("l", "a", "b", "c", "d", "e")

	got, err := s.LRange("l", -2, -1)
	if err != nil {
		t.Fatalf("LRange: %v", err)
	}
	want := []string{"d", "e"}
	if !equalStrings(got, want) {
		t.Fatalf("LRange(l, -2, -1) = %v; want %v", got, want)
	}
}

func TestStoreLRangeOutOfBoundsClamps(t *testing.T) {
	s := newTestStore(t)
	s.RPush("l", "a", "b", "c")

	got, err := s.LRange("l", 0, 100)
	if err != nil {
		t.Fatalf("LRange: %v", err)
	}
	want := []string{"a", "b", "c"}
	if !equalStrings(got, want) {
		t.Fatalf("LRange(l, 0, 100) = %v; want %v", got, want)
	}
}

func TestStoreLRangeEmptyWhenStartAfterStop(t *testing.T) {
	s := newTestStore(t)
	s.RPush("l", "a", "b", "c")

	got, err := s.LRange("l", 2, 1)
	if err != nil {
		t.Fatalf("LRange: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("LRange(l, 2, 1) = %v; want empty", got)
	}
}

func TestStoreListWrongTypeOnStringKey(t *testing.T) {
	s := newTestStore(t)
	s.Set("k", "v")

	if _, err := s.LPush("k", "x"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("LPush on a string key: err = %v; want ErrWrongType", err)
	}
	if _, err := s.RPush("k", "x"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("RPush on a string key: err = %v; want ErrWrongType", err)
	}
	if _, err := s.LLen("k"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("LLen on a string key: err = %v; want ErrWrongType", err)
	}
	if _, err := s.LRange("k", 0, -1); !errors.Is(err, ErrWrongType) {
		t.Fatalf("LRange on a string key: err = %v; want ErrWrongType", err)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
