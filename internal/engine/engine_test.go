package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"goredis/internal/persistence"
	"goredis/internal/protocol"
	"goredis/internal/store"
)

func newTestEngine(t *testing.T, dir string) (*Engine, string, string) {
	t.Helper()
	aofPath := filepath.Join(dir, "appendonly.aof")
	snapPath := filepath.Join(dir, "dump.rdb")
	aof, err := persistence.OpenAOF(aofPath)
	if err != nil {
		t.Fatalf("OpenAOF: %v", err)
	}
	s := store.New()
	e := New(s, aof, snapPath)
	t.Cleanup(func() { e.Close() })
	return e, aofPath, snapPath
}

func TestEngineExecuteAppliesToStore(t *testing.T) {
	e, _, _ := newTestEngine(t, t.TempDir())

	if _, err := e.Execute(protocol.Command{Op: "SET", Args: []string{"k", "v"}}); err != nil {
		t.Fatalf("Execute SET: %v", err)
	}
	v, found, _ := e.Store.Get("k")
	if !found || v != "v" {
		t.Fatalf("Store.Get(k) = %q, %v; want v, true", v, found)
	}
}

func TestEngineExecuteLogsWriteCommandsOnly(t *testing.T) {
	dir := t.TempDir()
	e, aofPath, _ := newTestEngine(t, dir)

	if _, err := e.Execute(protocol.Command{Op: "SET", Args: []string{"k", "v"}}); err != nil {
		t.Fatalf("Execute SET: %v", err)
	}
	if _, err := e.Execute(protocol.Command{Op: "GET", Args: []string{"k"}}); err != nil {
		t.Fatalf("Execute GET: %v", err)
	}
	if _, err := e.Execute(protocol.Command{Op: "KEYS"}); err != nil {
		t.Fatalf("Execute KEYS: %v", err)
	}

	var logged []protocol.Command
	aof, err := persistence.OpenAOF(aofPath)
	if err != nil {
		t.Fatalf("OpenAOF: %v", err)
	}
	defer aof.Close()
	if err := aof.Replay(func(c protocol.Command) error {
		logged = append(logged, c)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(logged) != 1 || logged[0].Op != "SET" {
		t.Fatalf("logged commands = %+v; want exactly one SET (GET/KEYS must not be logged)", logged)
	}
}

func TestEngineExecuteFailedApplyIsNotLogged(t *testing.T) {
	dir := t.TempDir()
	e, aofPath, _ := newTestEngine(t, dir)

	// LPUSH on a key that already holds a string is a WRONGTYPE error —
	// Apply fails, so Execute must not append it to the AOF even though
	// LPUSH is a write op.
	if _, err := e.Execute(protocol.Command{Op: "SET", Args: []string{"k", "v"}}); err != nil {
		t.Fatalf("Execute SET: %v", err)
	}
	if _, err := e.Execute(protocol.Command{Op: "LPUSH", Args: []string{"k", "x"}}); err == nil {
		t.Fatalf("Execute LPUSH on a string key should have errored")
	}

	var logged []protocol.Command
	aof, err := persistence.OpenAOF(aofPath)
	if err != nil {
		t.Fatalf("OpenAOF: %v", err)
	}
	defer aof.Close()
	aof.Replay(func(c protocol.Command) error {
		logged = append(logged, c)
		return nil
	})
	if len(logged) != 1 {
		t.Fatalf("logged = %+v; want only the successful SET, not the failed LPUSH", logged)
	}
}

func TestEngineLoadFromDiskReplaysAOF(t *testing.T) {
	dir := t.TempDir()
	e1, _, _ := newTestEngine(t, dir)

	mustExecute(t, e1, "SET", "a", "1")
	mustExecute(t, e1, "SET", "b", "2")
	mustExecute(t, e1, "DEL", "a")

	e2, _, _ := newTestEngine(t, dir)
	if err := e2.LoadFromDisk(); err != nil {
		t.Fatalf("LoadFromDisk: %v", err)
	}

	if _, found, _ := e2.Store.Get("a"); found {
		t.Fatalf("a should have been deleted before replay finished")
	}
	if v, found, _ := e2.Store.Get("b"); !found || v != "2" {
		t.Fatalf("Get(b) after replay = %q, %v; want 2, true", v, found)
	}
}

func TestEngineBGSaveSnapshotsAndTruncatesAOF(t *testing.T) {
	dir := t.TempDir()
	e, aofPath, snapPath := newTestEngine(t, dir)

	mustExecute(t, e, "SET", "x", "1")
	mustExecute(t, e, "SET", "y", "2")

	if _, err := e.Execute(protocol.Command{Op: "BGSAVE"}); err != nil {
		t.Fatalf("Execute BGSAVE: %v", err)
	}

	info, err := os.Stat(snapPath)
	if err != nil || info.Size() == 0 {
		t.Fatalf("snapshot file missing or empty after BGSAVE: %v", err)
	}
	aofInfo, err := os.Stat(aofPath)
	if err != nil {
		t.Fatalf("stat AOF: %v", err)
	}
	if aofInfo.Size() != 0 {
		t.Fatalf("AOF size after BGSAVE = %d; want 0 (truncated)", aofInfo.Size())
	}

	// A write after BGSAVE should still be logged to the (now-empty) AOF.
	mustExecute(t, e, "SET", "z", "3")

	e2, _, _ := newTestEngine(t, dir)
	if err := e2.LoadFromDisk(); err != nil {
		t.Fatalf("LoadFromDisk: %v", err)
	}
	keys := e2.Store.Keys()
	sort.Strings(keys)
	if len(keys) != 3 || keys[0] != "x" || keys[1] != "y" || keys[2] != "z" {
		t.Fatalf("keys after snapshot+tail reload = %v; want [x y z]", keys)
	}
}

// TestEngineConcurrentWritesKeepAOFOrderMatchingApplyOrder is the
// regression test for the exact bug writeMu exists to prevent: without it,
// concurrent writers could apply to the store in one order but log to the
// AOF in a different order, so a replay could reconstruct different final
// state than the live store actually held. This drives many goroutines
// issuing SET on a shared set of keys concurrently, then replays the AOF
// into a fresh store and asserts every key matches what the live store
// ended up holding.
func TestEngineConcurrentWritesKeepAOFOrderMatchingApplyOrder(t *testing.T) {
	dir := t.TempDir()
	e, aofPath, _ := newTestEngine(t, dir)

	const goroutines = 30
	const writesPerGoroutine = 50
	const sharedKeys = 30

	// t.Fatal/t.Fatalf must only be called from the test's own goroutine
	// (per the testing package's rules), so errors from these worker
	// goroutines are collected on a channel and reported after wg.Wait(),
	// rather than calling mustExecute (which calls t.Fatalf) from inside
	// each goroutine.
	errs := make(chan error, goroutines*writesPerGoroutine)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", g%sharedKeys)
			for i := 0; i < writesPerGoroutine; i++ {
				_, err := e.Execute(protocol.Command{Op: "SET", Args: []string{key, fmt.Sprintf("g%d-i%d", g, i)}})
				if err != nil {
					errs <- err
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Execute: %v", err)
	}

	live := map[string]string{}
	for _, k := range e.Store.Keys() {
		v, _, _ := e.Store.Get(k)
		live[k] = v
	}

	replayedStore := store.New()
	defer replayedStore.Close()
	aof, err := persistence.OpenAOF(aofPath)
	if err != nil {
		t.Fatalf("OpenAOF: %v", err)
	}
	defer aof.Close()
	if err := aof.Replay(func(c protocol.Command) error {
		_, err := Apply(replayedStore, c)
		return err
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	for k, wantV := range live {
		gotV, found, _ := replayedStore.Get(k)
		if !found || gotV != wantV {
			t.Fatalf("key %q: live store has %q but AOF replay produced %q (found=%v) — AOF order diverged from apply order", k, wantV, gotV, found)
		}
	}
}

func mustExecute(t *testing.T, e *Engine, op string, args ...string) {
	t.Helper()
	if _, err := e.Execute(protocol.Command{Op: op, Args: args}); err != nil {
		t.Fatalf("Execute(%s %v): %v", op, args, err)
	}
}
