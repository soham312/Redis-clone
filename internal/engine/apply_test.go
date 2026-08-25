package engine

import (
	"testing"
	"time"

	"goredis/internal/protocol"
	"goredis/internal/store"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	s := store.New()
	t.Cleanup(s.Close)
	return s
}

func mustApply(t *testing.T, s *store.Store, op string, args ...string) Result {
	t.Helper()
	r, err := Apply(s, protocol.Command{Op: op, Args: args})
	if err != nil {
		t.Fatalf("Apply(%s %v): unexpected error: %v", op, args, err)
	}
	return r
}

func TestApplySetGet(t *testing.T) {
	s := newTestStore(t)

	r := mustApply(t, s, "SET", "k", "v")
	if r.Kind != KindOK {
		t.Fatalf("SET result kind = %v; want KindOK", r.Kind)
	}

	r = mustApply(t, s, "GET", "k")
	if r.Kind != KindString || r.Str != "v" {
		t.Fatalf("GET result = %+v; want string v", r)
	}
}

func TestApplyGetMissingReturnsNil(t *testing.T) {
	s := newTestStore(t)
	r := mustApply(t, s, "GET", "missing")
	if r.Kind != KindNil {
		t.Fatalf("GET missing result kind = %v; want KindNil", r.Kind)
	}
}

func TestApplyDelExistsKeysFlushall(t *testing.T) {
	s := newTestStore(t)
	mustApply(t, s, "SET", "a", "1")
	mustApply(t, s, "SET", "b", "2")

	if r := mustApply(t, s, "EXISTS", "a", "b", "missing"); r.Int != 2 {
		t.Fatalf("EXISTS = %d; want 2", r.Int)
	}
	if r := mustApply(t, s, "DEL", "a", "missing"); r.Int != 1 {
		t.Fatalf("DEL = %d; want 1", r.Int)
	}
	if r := mustApply(t, s, "KEYS"); r.Kind != KindStrings || len(r.Strs) != 1 || r.Strs[0] != "b" {
		t.Fatalf("KEYS = %+v; want just [b]", r)
	}
	if r := mustApply(t, s, "FLUSHALL"); r.Kind != KindOK {
		t.Fatalf("FLUSHALL result kind = %v; want KindOK", r.Kind)
	}
	if r := mustApply(t, s, "KEYS"); len(r.Strs) != 0 {
		t.Fatalf("KEYS after FLUSHALL = %v; want empty", r.Strs)
	}
}

func TestApplyExpireAndTTL(t *testing.T) {
	s := newTestStore(t)
	mustApply(t, s, "SET", "k", "v")

	r := mustApply(t, s, "EXPIRE", "k", "100")
	if r.Int != 1 {
		t.Fatalf("EXPIRE = %d; want 1", r.Int)
	}
	r = mustApply(t, s, "TTL", "k")
	if r.Int < 99 || r.Int > 100 {
		t.Fatalf("TTL = %d; want ~100", r.Int)
	}
}

func TestApplyExpireBadSecondsIsError(t *testing.T) {
	s := newTestStore(t)
	mustApply(t, s, "SET", "k", "v")
	if _, err := Apply(s, protocol.Command{Op: "EXPIRE", Args: []string{"k", "not-a-number"}}); err == nil {
		t.Fatalf("EXPIRE with non-numeric seconds should error")
	}
}

func TestApplyListCommands(t *testing.T) {
	s := newTestStore(t)

	if r := mustApply(t, s, "RPUSH", "l", "a", "b", "c"); r.Int != 3 {
		t.Fatalf("RPUSH = %d; want 3", r.Int)
	}
	if r := mustApply(t, s, "LLEN", "l"); r.Int != 3 {
		t.Fatalf("LLEN = %d; want 3", r.Int)
	}
	r := mustApply(t, s, "LRANGE", "l", "0", "-1")
	if r.Kind != KindStrings || len(r.Strs) != 3 {
		t.Fatalf("LRANGE = %+v; want 3 items", r)
	}
}

func TestApplyWrongNumberOfArgs(t *testing.T) {
	s := newTestStore(t)
	cases := []protocol.Command{
		{Op: "SET", Args: []string{"onlyone"}},
		{Op: "GET", Args: []string{}},
		{Op: "DEL", Args: []string{}},
		{Op: "KEYS", Args: []string{"unexpected"}},
		{Op: "EXPIRE", Args: []string{"k"}},
		{Op: "LPUSH", Args: []string{"onlykey"}},
		{Op: "LRANGE", Args: []string{"k", "0"}},
	}
	for _, c := range cases {
		if _, err := Apply(s, c); err == nil {
			t.Fatalf("Apply(%+v) should have errored on wrong arg count", c)
		}
	}
}

func TestApplyUnknownCommand(t *testing.T) {
	s := newTestStore(t)
	if _, err := Apply(s, protocol.Command{Op: "NOSUCHCOMMAND"}); err == nil {
		t.Fatalf("Apply on an unknown command should error")
	}
}

func TestApplyWrongType(t *testing.T) {
	s := newTestStore(t)
	mustApply(t, s, "RPUSH", "l", "x")

	if _, err := Apply(s, protocol.Command{Op: "GET", Args: []string{"l"}}); err == nil {
		t.Fatalf("GET on a list key should error")
	}
}

func TestApplyDoesNotTimeoutOnExpireEdge(t *testing.T) {
	// Sanity check that EXPIRE with 0 seconds is accepted and the key
	// becomes unreachable almost immediately (regression guard for the
	// seconds-parsing path, not a timing-sensitive assertion).
	s := newTestStore(t)
	mustApply(t, s, "SET", "k", "v")
	mustApply(t, s, "EXPIRE", "k", "0")
	time.Sleep(5 * time.Millisecond)
	r := mustApply(t, s, "GET", "k")
	if r.Kind != KindNil {
		t.Fatalf("GET after EXPIRE 0 = %+v; want nil", r)
	}
}
