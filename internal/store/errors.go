package store

import "errors"

// ErrWrongType mirrors Redis's WRONGTYPE error: it's returned when a
// command for one type (e.g. GET, a string command) is run against a key
// holding a different type (e.g. a list). Surfacing this as a distinct
// sentinel error (rather than e.g. a generic "invalid" error or a panic)
// lets the future command layer map it to a specific wire-protocol error
// response.
var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
