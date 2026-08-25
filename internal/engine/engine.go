package engine

import (
	"fmt"
	"sync"

	"goredis/internal/persistence"
	"goredis/internal/protocol"
	"goredis/internal/store"
)

// Engine ties a Store to its persistence: every write command that
// succeeds against the Store is durably appended to an AOF, and BGSAVE
// compacts the Store's current state into a snapshot file, letting the AOF
// be truncated.
//
// aof and snapshotPath are both optional (nil / ""): an Engine can run
// purely in-memory with no persistence at all, which is what the unit
// tests for command dispatch use to avoid touching disk.
type Engine struct {
	Store        *store.Store
	aof          *persistence.AOF
	snapshotPath string

	// writeMu makes "apply to the store" and "append to the AOF" a single
	// atomic step for write commands — see Execute for why that matters,
	// and BGSave for why it also needs this same lock.
	writeMu sync.Mutex
}

// New creates an Engine around an existing Store. aof and snapshotPath may
// be nil / empty to disable that piece of persistence.
func New(s *store.Store, aof *persistence.AOF, snapshotPath string) *Engine {
	return &Engine{Store: s, aof: aof, snapshotPath: snapshotPath}
}

// Execute applies cmd to the store and, for write commands, durably logs
// it. BGSAVE is handled specially — it's a persistence operation, not a
// store mutation, so it never goes through Apply/the store at all.
func (e *Engine) Execute(cmd protocol.Command) (Result, error) {
	if cmd.Op == "BGSAVE" {
		if len(cmd.Args) != 0 {
			return Result{}, wrongArgs("bgsave")
		}
		if err := e.BGSave(); err != nil {
			return Result{}, err
		}
		return Result{Kind: KindOK}, nil
	}

	if !IsWriteOp(cmd.Op) {
		// Read commands never touch the AOF and don't need writeMu:
		// Store's own RWMutex already gives them safe, concurrent access.
		return Apply(e.Store, cmd)
	}

	// Write commands: apply-then-log as one atomic unit under writeMu.
	//
	// Store's methods already fully serialize writes to the data
	// structure itself (Set/Del/etc. take Store's own exclusive lock), so
	// this isn't introducing contention among writers that wasn't already
	// there. What it closes is a narrower gap: without a lock spanning
	// both steps, two concurrent writers could apply to the Store in one
	// order (decided by Store's internal mutex) but append to the AOF in
	// the *other* order (decided by whichever goroutine happens to reach
	// aof.Append first) — e.g. two concurrent `SET k v1` / `SET k v2`
	// where v2 "wins" in the live store but v1 is logged last. A replay
	// after a crash would then reconstruct v1, silently diverging from
	// what the live store actually held right before the crash. Holding
	// writeMu across both steps guarantees AOF order always matches
	// Store-apply order.
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	result, err := Apply(e.Store, cmd)
	if err != nil {
		return result, err
	}
	if e.aof != nil {
		if logErr := e.aof.Append(cmd); logErr != nil {
			return result, fmt.Errorf("applied but failed to persist to AOF: %w", logErr)
		}
	}
	return result, nil
}

// LoadFromDisk restores state at startup: load the last snapshot (if any)
// as a base, then replay the AOF on top of it to pick up whatever writes
// happened after that snapshot was taken. This mirrors the order real
// Redis uses when both an RDB-style snapshot and an AOF are present — the
// snapshot is cheap to load in bulk, and only the (typically much
// shorter) tail of commands since then needs to be replayed one at a time.
func (e *Engine) LoadFromDisk() error {
	if e.snapshotPath != "" {
		entries, err := persistence.LoadSnapshot(e.snapshotPath)
		if err != nil {
			return fmt.Errorf("loading snapshot: %w", err)
		}
		if entries != nil {
			e.Store.LoadSnapshot(entries)
		}
	}

	if e.aof != nil {
		err := e.aof.Replay(func(cmd protocol.Command) error {
			// Apply directly against the store — deliberately bypassing
			// Execute/writeMu/aof.Append. Replaying is reconstructing
			// state *from* the AOF; re-appending each replayed command
			// back into the same file would both be redundant (it's
			// already there) and, worse, would duplicate every command on
			// every restart, growing the log forever.
			_, err := Apply(e.Store, cmd)
			return err
		})
		if err != nil {
			return fmt.Errorf("replaying AOF: %w", err)
		}
	}
	return nil
}

// BGSave is the "AOF rewrite" story: it compacts the store's entire
// current state into a snapshot file, then truncates the AOF, because
// every write that produced the current state is now redundant with the
// snapshot that just captured it — a fresh restart only needs the
// snapshot plus whatever gets written *after* this point.
//
// This holds writeMu for the duration of the snapshot + truncate, which
// blocks new writes from being applied+logged while it runs. Real Redis's
// BGSAVE forks a child process instead, so the parent keeps serving writes
// uninterrupted via copy-on-write memory pages — Go has no fork(), and
// goroutines share memory, so there's no equivalent trick available here.
// Blocking writes for the duration of an in-memory snapshot + a file
// write is the honest, simple alternative; it's a real tradeoff (a brief
// write-pause proportional to store size) worth naming directly rather
// than glossing over, and is the main thing a production rewrite of this
// project would need a different approach for (e.g. snapshotting from a
// copy of the data, or a copy-on-write-friendly structure).
func (e *Engine) BGSave() error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	entries := e.Store.Snapshot()

	if e.snapshotPath != "" {
		if err := persistence.SaveSnapshot(e.snapshotPath, entries); err != nil {
			return fmt.Errorf("saving snapshot: %w", err)
		}
	}
	if e.aof != nil {
		if err := e.aof.Truncate(); err != nil {
			return fmt.Errorf("truncating AOF: %w", err)
		}
	}
	return nil
}

// Close releases the Engine's resources (the store's background expiry
// goroutine and the AOF file handle).
func (e *Engine) Close() error {
	e.Store.Close()
	if e.aof != nil {
		return e.aof.Close()
	}
	return nil
}
