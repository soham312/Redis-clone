package engine

// ResultKind tags which field of Result is meaningful, so a command's
// return value can travel as plain data from Apply all the way out to
// whatever eventually renders it (a wire reply, a CLI print, a test
// assertion) without that caller needing to know which store method
// produced it.
type ResultKind int

const (
	KindNil     ResultKind = iota // no value, e.g. GET on a missing key
	KindOK                        // simple success, e.g. SET, FLUSHALL
	KindString                    // e.g. GET
	KindInt                       // e.g. DEL, EXISTS, EXPIRE, TTL, LLEN, LPUSH, RPUSH
	KindStrings                   // e.g. KEYS, LRANGE
)

type Result struct {
	Kind ResultKind
	Str  string
	Int  int64
	Strs []string
}
