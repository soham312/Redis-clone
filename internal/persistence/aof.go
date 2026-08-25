// Package persistence handles getting Store state to and from disk: an
// append-only command log (AOF) for durability of every write, and a
// compact full-state snapshot (BGSAVE) used to bound how much of that log
// ever needs to be replayed.
//
// This package deliberately knows nothing about *how* a command mutates a
// Store — it only knows how to encode/decode/write/read them. Applying a
// decoded command back to a Store is the caller's job (see
// internal/engine), passed in here as a plain function value. That keeps
// this package a leaf: it depends on internal/store (for the Entry type
// snapshots are made of) and internal/protocol (for command encoding), but
// nothing depends on it in a way that could create an import cycle with
// the package that actually interprets commands.
package persistence

import (
	"bufio"
	"errors"
	"io"
	"os"
	"sync"

	"goredis/internal/protocol"
)

// AOF is an append-only log of write commands, plus the ability to replay
// them and to truncate the log (used after a BGSAVE snapshot makes the
// existing log redundant).
type AOF struct {
	// mu serializes Append/Truncate/Close against each other. It does NOT
	// by itself guarantee AOF order matches Store-apply order across
	// concurrent writers — that ordering guarantee is the caller's
	// responsibility (see Engine.Execute's writeMu in internal/engine),
	// since "apply to the store" and "append to the log" are two separate
	// calls this package has no visibility into as a single unit.
	mu   sync.Mutex
	path string
	file *os.File
}

// OpenAOF opens (creating if necessary) the AOF file at path for
// appending.
func OpenAOF(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &AOF{path: path, file: f}, nil
}

// Append writes cmd to the log and fsyncs before returning.
//
// Fsyncing on every single write is the "always" durability mode in real
// Redis's appendfsync setting: it guarantees a command that Append
// returned success for will survive a crash, at the cost of one disk
// flush per write — a real throughput ceiling under heavy write load.
// Redis's default is actually "everysec" (fsync once a second in a
// background thread, batching writes in between) precisely to trade a
// tiny, bounded durability window for much better throughput. "Always" was
// chosen here because it's the simplest policy to reason about and to
// verify is actually correct (no risk of losing the last second of writes
// in a demo), with the throughput tradeoff called out explicitly as the
// thing I'd change first for a write-heavy production workload.
func (a *AOF) Append(cmd protocol.Command) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, err := a.file.Write(protocol.Encode(cmd)); err != nil {
		return err
	}
	return a.file.Sync()
}

// Replay reads every command from the beginning of the log (via a
// separate read handle from the append-only write handle) and calls apply
// for each one, in order. A truncated final command — the on-disk
// signature of a crash mid-Append — stops replay without returning an
// error, so that everything before the crash still loads; any other
// decode error is treated as genuine corruption and returned.
func (a *AOF) Replay(apply func(protocol.Command) error) error {
	f, err := os.Open(a.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		cmd, err := protocol.Decode(r)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		if err := apply(cmd); err != nil {
			return err
		}
	}
}

// Truncate discards everything currently in the log, leaving it empty.
// Used after a BGSAVE snapshot has captured full state: the log only
// needs to hold commands since the last snapshot, so once the snapshot on
// disk is up to date, the commands that produced that state no longer
// need to be replayed from the log too.
func (a *AOF) Truncate() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err := a.file.Close(); err != nil {
		return err
	}
	f, err := os.OpenFile(a.path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	a.file = f
	return nil
}

func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.file.Close()
}
