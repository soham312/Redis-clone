package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cmd := Command{Op: "SET", Args: []string{"foo", "bar"}}

	encoded := Encode(cmd)
	r := bufio.NewReader(bytes.NewReader(encoded))
	got, err := Decode(r)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Op != cmd.Op || len(got.Args) != len(cmd.Args) || got.Args[0] != cmd.Args[0] || got.Args[1] != cmd.Args[1] {
		t.Fatalf("Decode(Encode(%+v)) = %+v", cmd, got)
	}
}

// TestEncodeDecodeBinarySafety is the whole reason bulk strings are
// length-prefixed rather than space/newline-delimited: a value containing
// spaces, \r, or \n must still round-trip exactly.
func TestEncodeDecodeBinarySafety(t *testing.T) {
	tricky := "hello world\r\nwith embedded \"quotes\" and\ttabs"
	cmd := Command{Op: "SET", Args: []string{"key", tricky}}

	r := bufio.NewReader(bytes.NewReader(Encode(cmd)))
	got, err := Decode(r)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Args[1] != tricky {
		t.Fatalf("Decode round-trip mangled a binary-unsafe value: got %q, want %q", got.Args[1], tricky)
	}
}

func TestDecodeUppercasesOpOnly(t *testing.T) {
	cmd := Command{Op: "get", Args: []string{"MixedCaseValue"}}
	r := bufio.NewReader(bytes.NewReader(Encode(cmd)))
	got, err := Decode(r)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Op != "GET" {
		t.Fatalf("Op = %q; want GET (uppercased)", got.Op)
	}
	if got.Args[0] != "MixedCaseValue" {
		t.Fatalf("Args[0] = %q; want MixedCaseValue (args must not be case-normalized)", got.Args[0])
	}
}

func TestDecodeMultipleCommandsFromOneStream(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(Encode(Command{Op: "SET", Args: []string{"a", "1"}}))
	buf.Write(Encode(Command{Op: "GET", Args: []string{"a"}}))

	r := bufio.NewReader(&buf)
	first, err := Decode(r)
	if err != nil || first.Op != "SET" {
		t.Fatalf("first Decode = %+v, %v; want SET", first, err)
	}
	second, err := Decode(r)
	if err != nil || second.Op != "GET" {
		t.Fatalf("second Decode = %+v, %v; want GET", second, err)
	}
	if _, err := Decode(r); !errors.Is(err, io.EOF) {
		t.Fatalf("third Decode error = %v; want io.EOF (clean end of stream)", err)
	}
}

func TestDecodeEmptyStreamReturnsCleanEOF(t *testing.T) {
	r := bufio.NewReader(bytes.NewReader(nil))
	if _, err := Decode(r); !errors.Is(err, io.EOF) {
		t.Fatalf("Decode on empty stream: err = %v; want io.EOF", err)
	}
}

// TestDecodeTruncatedCommandReturnsUnexpectedEOF is what lets AOF replay
// distinguish "the file legitimately ends here" from "the process crashed
// mid-write and this last command is a corrupt fragment."
func TestDecodeTruncatedCommandReturnsUnexpectedEOF(t *testing.T) {
	full := Encode(Command{Op: "SET", Args: []string{"key", "value"}})

	// Cut the encoded command off at every possible byte boundary before
	// its end and confirm none of them are misread as a valid, different
	// command or as a clean end-of-stream.
	for cut := 1; cut < len(full); cut++ {
		r := bufio.NewReader(bytes.NewReader(full[:cut]))
		_, err := Decode(r)
		if err == nil {
			t.Fatalf("cut=%d: Decode succeeded on a truncated command", cut)
		}
		if errors.Is(err, io.EOF) {
			t.Fatalf("cut=%d: Decode returned clean io.EOF for a truncated command; want io.ErrUnexpectedEOF", cut)
		}
	}
}

func TestDecodeRejectsMalformedHeader(t *testing.T) {
	r := bufio.NewReader(bytes.NewReader([]byte("not-a-valid-header\r\n")))
	if _, err := Decode(r); err == nil {
		t.Fatalf("Decode accepted a stream not starting with '*'")
	}
}

func TestReplyEncodeDecodeRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		write func(*bytes.Buffer)
		want  Reply
	}{
		{"OK", func(b *bytes.Buffer) { WriteOK(b) }, Reply{Type: ReplySimple, Str: "OK"}},
		{"Error", func(b *bytes.Buffer) { WriteError(b, "ERR boom") }, Reply{Type: ReplyError, Str: "ERR boom"}},
		{"Int", func(b *bytes.Buffer) { WriteInt(b, 42) }, Reply{Type: ReplyInt, Int: 42}},
		{"NegativeInt", func(b *bytes.Buffer) { WriteInt(b, -2) }, Reply{Type: ReplyInt, Int: -2}},
		{"Nil", func(b *bytes.Buffer) { WriteNil(b) }, Reply{Type: ReplyNil}},
		{"Bulk", func(b *bytes.Buffer) { WriteBulkString(b, "hello world\r\nwith newline") }, Reply{Type: ReplyBulk, Str: "hello world\r\nwith newline"}},
		{"EmptyBulk", func(b *bytes.Buffer) { WriteBulkString(b, "") }, Reply{Type: ReplyBulk, Str: ""}},
		{"Array", func(b *bytes.Buffer) { WriteArray(b, []string{"a", "b", "c"}) }, Reply{Type: ReplyArray, Items: []string{"a", "b", "c"}}},
		{"EmptyArray", func(b *bytes.Buffer) { WriteArray(b, []string{}) }, Reply{Type: ReplyArray, Items: []string{}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			tt.write(&buf)

			got, err := ReadReply(bufio.NewReader(&buf))
			if err != nil {
				t.Fatalf("ReadReply: %v", err)
			}
			if got.Type != tt.want.Type || got.Str != tt.want.Str || got.Int != tt.want.Int || !stringSlicesEqual(got.Items, tt.want.Items) {
				t.Fatalf("ReadReply = %+v; want %+v", got, tt.want)
			}
		})
	}
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
