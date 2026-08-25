// Package protocol defines the wire format shared by two very different
// consumers: the AOF (internal/persistence), which needs to write commands
// to disk in a way that survives being read back byte-for-byte, and the
// TCP server (added in a later stage), which needs to read commands off a
// live socket. Defining the format once here — independent of both — means
// "how a command is spelled on the wire" and "what a command does" are
// separate concerns, and the AOF never has to duplicate parsing logic that
// the network layer also needs.
package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Command is a parsed request: a command name plus its arguments, e.g.
// {Op: "SET", Args: []string{"foo", "bar"}}.
type Command struct {
	Op   string
	Args []string
}

// Encode serializes cmd as a RESP-lite array of bulk strings:
//
//	*<argc>\r\n
//	$<len>\r\n<bytes>\r\n   (repeated once per token: Op, then each Arg)
//
// This is the same shape real Redis uses for client requests (RESP array
// of bulk strings), scaled down to just what's needed here. The key
// property it buys over a naive "space-separated line" format is binary
// safety: each token is prefixed with its exact byte length, so a value
// that itself contains spaces, newlines, or arbitrary bytes round-trips
// correctly. That matters a lot for the AOF specifically — it must persist
// whatever a client actually SET, not a version that's been mangled by
// splitting on whitespace.
func Encode(cmd Command) []byte {
	tokens := make([]string, 0, 1+len(cmd.Args))
	tokens = append(tokens, cmd.Op)
	tokens = append(tokens, cmd.Args...)

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "*%d\r\n", len(tokens))
	for _, t := range tokens {
		fmt.Fprintf(&buf, "$%d\r\n%s\r\n", len(t), t)
	}
	return buf.Bytes()
}

// Decode reads exactly one command from r.
//
// Two distinct "no more data" outcomes are surfaced, and callers (notably
// AOF replay) care about the difference:
//   - io.EOF: the stream ended cleanly, exactly at a command boundary.
//     This is the normal, expected way a read loop ends.
//   - io.ErrUnexpectedEOF: the stream ended in the *middle* of a command
//     — e.g. the process crashed mid-write to the AOF, so the last
//     command on disk is a truncated fragment. AOF replay treats this as
//     "stop here, keep everything decoded so far" rather than a fatal
//     corruption error, which mirrors how real Redis tolerates a
//     truncated tail of its AOF after an unclean shutdown.
func Decode(r *bufio.Reader) (Command, error) {
	header, err := readLine(r)
	if err != nil {
		return Command{}, err
	}
	if len(header) == 0 || header[0] != '*' {
		return Command{}, fmt.Errorf("protocol error: expected array header, got %q", header)
	}
	argc, err := strconv.Atoi(header[1:])
	if err != nil || argc <= 0 {
		return Command{}, fmt.Errorf("protocol error: invalid array length %q", header)
	}

	tokens := make([]string, 0, argc)
	for i := 0; i < argc; i++ {
		lenLine, err := readLine(r)
		if err != nil {
			return Command{}, io.ErrUnexpectedEOF
		}
		if len(lenLine) == 0 || lenLine[0] != '$' {
			return Command{}, fmt.Errorf("protocol error: expected bulk header, got %q", lenLine)
		}
		length, err := strconv.Atoi(lenLine[1:])
		if err != nil || length < 0 {
			return Command{}, fmt.Errorf("protocol error: invalid bulk length %q", lenLine)
		}

		data := make([]byte, length)
		if _, err := io.ReadFull(r, data); err != nil {
			return Command{}, io.ErrUnexpectedEOF
		}
		if _, err := readLine(r); err != nil { // trailing \r\n after the bulk payload
			return Command{}, io.ErrUnexpectedEOF
		}
		tokens = append(tokens, string(data))
	}

	return Command{Op: strings.ToUpper(tokens[0]), Args: tokens[1:]}, nil
}

// readLine reads up to and including the next '\n', then strips the
// trailing "\r\n" (or bare "\n"). It distinguishes a clean end-of-stream
// (nothing at all was read) from a partial line cut off by EOF, which is
// what lets Decode tell "no more commands" apart from "a command got
// truncated mid-write": bufio.Reader.ReadString returns whatever partial
// data it managed to read alongside the io.EOF error, so an empty result
// means the stream ended exactly on a boundary, while a non-empty partial
// line means it didn't.
func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		if err == io.EOF && line == "" {
			return "", io.EOF
		}
		return "", io.ErrUnexpectedEOF
	}
	return strings.TrimRight(line, "\r\n"), nil
}
