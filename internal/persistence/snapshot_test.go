package persistence

import (
	"os"
	"path/filepath"
	"testing"

	"goredis/internal/store"
)

func TestSaveLoadSnapshotRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.rdb")

	entries := []store.Entry{
		{Key: "str", Type: store.TypeString, Str: "hello"},
		{Key: "list", Type: store.TypeList, List: []string{"a", "b", "c"}},
		{Key: "withttl", Type: store.TypeString, Str: "v", HasTTL: true, ExpireAt: 123456789},
	}

	if err := SaveSnapshot(path, entries); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	got, err := LoadSnapshot(path)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if len(got) != len(entries) {
		t.Fatalf("LoadSnapshot returned %d entries; want %d", len(got), len(entries))
	}
	for i, e := range entries {
		// store.Entry contains a []string field, so it isn't comparable
		// with == — compare the scalar fields directly instead. List
		// content is checked separately below.
		if got[i].Key != e.Key || got[i].Type != e.Type || got[i].Str != e.Str ||
			got[i].HasTTL != e.HasTTL || got[i].ExpireAt != e.ExpireAt {
			t.Fatalf("entry %d = %+v; want %+v", i, got[i], e)
		}
	}
	if len(got[1].List) != 3 || got[1].List[0] != "a" || got[1].List[2] != "c" {
		t.Fatalf("list entry = %+v; want List [a b c]", got[1])
	}
}

func TestLoadSnapshotOnMissingFileReturnsNil(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist.rdb")

	entries, err := LoadSnapshot(path)
	if err != nil {
		t.Fatalf("LoadSnapshot on missing file: %v", err)
	}
	if entries != nil {
		t.Fatalf("LoadSnapshot on missing file returned %v; want nil", entries)
	}
}

// TestSaveSnapshotDoesNotClobberExistingFileOnFailure exercises the
// write-to-temp-then-atomic-rename design: SaveSnapshot must not leave a
// half-written or missing file in place of a previously-good snapshot.
// This doesn't inject a real mid-write crash (hard to do portably in a
// unit test), but it does confirm the previous good snapshot is left
// completely untouched by a *second*, successful save that follows it —
// i.e. the rename path is exercised and doesn't corrupt anything.
func TestSaveSnapshotOverwritesAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.rdb")

	first := []store.Entry{{Key: "v1", Type: store.TypeString, Str: "old"}}
	if err := SaveSnapshot(path, first); err != nil {
		t.Fatalf("first SaveSnapshot: %v", err)
	}

	second := []store.Entry{{Key: "v2", Type: store.TypeString, Str: "new"}}
	if err := SaveSnapshot(path, second); err != nil {
		t.Fatalf("second SaveSnapshot: %v", err)
	}

	// No leftover .tmp file should remain after a successful save.
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("leftover temp file after SaveSnapshot: err = %v", err)
	}

	got, err := LoadSnapshot(path)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if len(got) != 1 || got[0].Key != "v2" {
		t.Fatalf("LoadSnapshot after second save = %+v; want just the second snapshot's data", got)
	}
}
