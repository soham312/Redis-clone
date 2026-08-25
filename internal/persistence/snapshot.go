package persistence

import (
	"bufio"
	"encoding/gob"
	"os"

	"goredis/internal/store"
)

// SaveSnapshot writes entries to path using encoding/gob — a compact
// binary encoding of Go's own type system, chosen over something like JSON
// specifically because there's no cross-language or human-readability
// requirement here (this file is only ever read back by this same Go
// program), so gob's smaller output and faster encode/decode are pure
// upside.
//
// The write goes to a temp file in the same directory, which is fsynced
// and only then atomically renamed onto the real path. os.Rename within
// the same filesystem is atomic, so a crash at any point before the rename
// leaves the previous snapshot (if any) completely untouched — there's no
// window where the snapshot file exists but is half-written or truncated.
// Writing directly to path and getting interrupted partway through would
// leave a corrupt snapshot in place of a previously-good one, which is
// exactly the failure mode this sidesteps.
func SaveSnapshot(path string, entries []store.Entry) error {
	tmpPath := path + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)
	if err := gob.NewEncoder(w).Encode(entries); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	return os.Rename(tmpPath, path)
}

// LoadSnapshot reads back what SaveSnapshot wrote. A missing file (no
// snapshot has ever been taken) is not an error — it returns a nil slice.
func LoadSnapshot(path string) ([]store.Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var entries []store.Entry
	if err := gob.NewDecoder(bufio.NewReader(f)).Decode(&entries); err != nil {
		return nil, err
	}
	return entries, nil
}
