package persistence

import (
	"os"
	"path/filepath"
	"testing"

	"goredis/internal/protocol"
)

func TestAOFAppendAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")

	aof, err := OpenAOF(path)
	if err != nil {
		t.Fatalf("OpenAOF: %v", err)
	}
	cmds := []protocol.Command{
		{Op: "SET", Args: []string{"a", "1"}},
		{Op: "SET", Args: []string{"b", "value with spaces"}},
		{Op: "DEL", Args: []string{"a"}},
	}
	for _, c := range cmds {
		if err := aof.Append(c); err != nil {
			t.Fatalf("Append(%+v): %v", c, err)
		}
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenAOF(path)
	if err != nil {
		t.Fatalf("re-OpenAOF: %v", err)
	}
	defer reopened.Close()

	var replayed []protocol.Command
	if err := reopened.Replay(func(c protocol.Command) error {
		replayed = append(replayed, c)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(replayed) != len(cmds) {
		t.Fatalf("Replay produced %d commands; want %d", len(replayed), len(cmds))
	}
	for i, c := range cmds {
		if replayed[i].Op != c.Op || len(replayed[i].Args) != len(c.Args) {
			t.Fatalf("replayed[%d] = %+v; want %+v", i, replayed[i], c)
		}
		for j := range c.Args {
			if replayed[i].Args[j] != c.Args[j] {
				t.Fatalf("replayed[%d].Args[%d] = %q; want %q", i, j, replayed[i].Args[j], c.Args[j])
			}
		}
	}
}

func TestAOFReplayOnMissingFileIsNoop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist.aof")
	aof := &AOF{path: path} // never opened for writing — Replay must tolerate a missing file

	called := false
	if err := aof.Replay(func(protocol.Command) error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("Replay on missing file: %v", err)
	}
	if called {
		t.Fatalf("apply callback was invoked for a nonexistent AOF")
	}
}

// TestAOFReplayToleratesTruncatedTail simulates the on-disk signature of a
// crash mid-Append: a well-formed command followed by a partial fragment
// of a second one. Replay should apply the first command and stop
// cleanly, not error out and lose everything before the crash.
func TestAOFReplayToleratesTruncatedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")

	aof, err := OpenAOF(path)
	if err != nil {
		t.Fatalf("OpenAOF: %v", err)
	}
	if err := aof.Append(protocol.Command{Op: "SET", Args: []string{"good", "1"}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a crash mid-write: append a fragment that isn't a complete
	// command (no closing \r\n on the bulk payload).
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	if _, err := f.Write([]byte("*3\r\n$3\r\nSET\r\n$5\r\ntrunc")); err != nil {
		t.Fatalf("write fragment: %v", err)
	}
	f.Close()

	reopened, err := OpenAOF(path)
	if err != nil {
		t.Fatalf("re-OpenAOF: %v", err)
	}
	defer reopened.Close()

	var replayed []protocol.Command
	if err := reopened.Replay(func(c protocol.Command) error {
		replayed = append(replayed, c)
		return nil
	}); err != nil {
		t.Fatalf("Replay with truncated tail returned an error (should tolerate it): %v", err)
	}
	if len(replayed) != 1 || replayed[0].Op != "SET" || replayed[0].Args[0] != "good" {
		t.Fatalf("replayed = %+v; want exactly the one well-formed command before the truncated tail", replayed)
	}
}

func TestAOFTruncateEmptiesTheLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")

	aof, err := OpenAOF(path)
	if err != nil {
		t.Fatalf("OpenAOF: %v", err)
	}
	defer aof.Close()

	if err := aof.Append(protocol.Command{Op: "SET", Args: []string{"a", "1"}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := aof.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("file size after Truncate = %d; want 0", info.Size())
	}

	// Must still be writable/appendable after Truncate.
	if err := aof.Append(protocol.Command{Op: "SET", Args: []string{"b", "2"}}); err != nil {
		t.Fatalf("Append after Truncate: %v", err)
	}
	var replayed []protocol.Command
	if err := aof.Replay(func(c protocol.Command) error {
		replayed = append(replayed, c)
		return nil
	}); err != nil {
		t.Fatalf("Replay after Truncate+Append: %v", err)
	}
	if len(replayed) != 1 || replayed[0].Args[0] != "b" {
		t.Fatalf("replayed after Truncate+Append = %+v; want just the b command", replayed)
	}
}
