package engine

import (
	"fmt"
	"strconv"
	"time"

	"goredis/internal/protocol"
	"goredis/internal/store"
)

// writeOps is the set of commands that mutate store state and therefore
// need to be durably logged to the AOF. Every other command is read-only.
//
// Implemented as a call to our own HashTable rather than Go's builtin map
// would be overkill here (this is a small, fixed, compile-time-known set
// looked up by command name, not part of the store's core data path) —
// this is exactly the kind of incidental, non-domain bookkeeping the
// project's "no builtin map for the storage engine" constraint isn't
// aimed at, so a plain map is the right, boring tool for it.
var writeOps = map[string]bool{
	"SET":      true,
	"DEL":      true,
	"EXPIRE":   true,
	"LPUSH":    true,
	"RPUSH":    true,
	"FLUSHALL": true,
}

// IsWriteOp reports whether op mutates store state.
func IsWriteOp(op string) bool {
	return writeOps[op]
}

// Apply is the single place a Command is interpreted against a Store. It
// has no knowledge of persistence or networking — just "given this parsed
// command, what does the store do, and what comes back." That
// single-source-of-truth property is what lets both AOF replay
// (internal/persistence, via internal/engine.Engine.LoadFromDisk) and live
// client traffic (the TCP server) share identical command semantics
// without duplicating a second copy of this switch statement somewhere
// else.
func Apply(s *store.Store, cmd protocol.Command) (Result, error) {
	switch cmd.Op {
	case "SET":
		if len(cmd.Args) != 2 {
			return Result{}, wrongArgs("set")
		}
		s.Set(cmd.Args[0], cmd.Args[1])
		return Result{Kind: KindOK}, nil

	case "GET":
		if len(cmd.Args) != 1 {
			return Result{}, wrongArgs("get")
		}
		val, found, err := s.Get(cmd.Args[0])
		if err != nil {
			return Result{}, err
		}
		if !found {
			return Result{Kind: KindNil}, nil
		}
		return Result{Kind: KindString, Str: val}, nil

	case "DEL":
		if len(cmd.Args) < 1 {
			return Result{}, wrongArgs("del")
		}
		return Result{Kind: KindInt, Int: int64(s.Del(cmd.Args...))}, nil

	case "EXISTS":
		if len(cmd.Args) < 1 {
			return Result{}, wrongArgs("exists")
		}
		return Result{Kind: KindInt, Int: int64(s.Exists(cmd.Args...))}, nil

	case "KEYS":
		if len(cmd.Args) != 0 {
			return Result{}, wrongArgs("keys")
		}
		return Result{Kind: KindStrings, Strs: s.Keys()}, nil

	case "FLUSHALL":
		if len(cmd.Args) != 0 {
			return Result{}, wrongArgs("flushall")
		}
		s.FlushAll()
		return Result{Kind: KindOK}, nil

	case "EXPIRE":
		if len(cmd.Args) != 2 {
			return Result{}, wrongArgs("expire")
		}
		seconds, err := strconv.ParseInt(cmd.Args[1], 10, 64)
		if err != nil {
			return Result{}, fmt.Errorf("ERR value is not an integer or out of range")
		}
		ok := s.Expire(cmd.Args[0], time.Duration(seconds)*time.Second)
		return Result{Kind: KindInt, Int: boolToInt(ok)}, nil

	case "TTL":
		if len(cmd.Args) != 1 {
			return Result{}, wrongArgs("ttl")
		}
		return Result{Kind: KindInt, Int: s.TTL(cmd.Args[0])}, nil

	case "LPUSH":
		if len(cmd.Args) < 2 {
			return Result{}, wrongArgs("lpush")
		}
		n, err := s.LPush(cmd.Args[0], cmd.Args[1:]...)
		if err != nil {
			return Result{}, err
		}
		return Result{Kind: KindInt, Int: int64(n)}, nil

	case "RPUSH":
		if len(cmd.Args) < 2 {
			return Result{}, wrongArgs("rpush")
		}
		n, err := s.RPush(cmd.Args[0], cmd.Args[1:]...)
		if err != nil {
			return Result{}, err
		}
		return Result{Kind: KindInt, Int: int64(n)}, nil

	case "LLEN":
		if len(cmd.Args) != 1 {
			return Result{}, wrongArgs("llen")
		}
		n, err := s.LLen(cmd.Args[0])
		if err != nil {
			return Result{}, err
		}
		return Result{Kind: KindInt, Int: int64(n)}, nil

	case "LRANGE":
		if len(cmd.Args) != 3 {
			return Result{}, wrongArgs("lrange")
		}
		start, err1 := strconv.Atoi(cmd.Args[1])
		stop, err2 := strconv.Atoi(cmd.Args[2])
		if err1 != nil || err2 != nil {
			return Result{}, fmt.Errorf("ERR value is not an integer or out of range")
		}
		vals, err := s.LRange(cmd.Args[0], start, stop)
		if err != nil {
			return Result{}, err
		}
		return Result{Kind: KindStrings, Strs: vals}, nil

	default:
		return Result{}, fmt.Errorf("ERR unknown command %q", cmd.Op)
	}
}

func wrongArgs(op string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", op)
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
