package protocol

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// Reply-side of the wire protocol: the request side (Command, Encode,
// Decode in protocol.go) is a RESP-lite array-of-bulk-strings; replies use
// the small set of scalar RESP reply types real Redis uses, since a
// command's result is naturally one of a few simple shapes rather than
// always "an array of strings":
//
//	+OK\r\n                     simple string   (e.g. SET, FLUSHALL, BGSAVE)
//	-ERR message\r\n            error
//	:123\r\n                    integer         (e.g. DEL, EXISTS, TTL)
//	$3\r\nfoo\r\n               bulk string     (e.g. GET)
//	$-1\r\n                     nil bulk string (e.g. GET on a missing key)
//	*2\r\n$1\r\na\r\n$1\r\nb\r\n  array of bulk strings (e.g. KEYS, LRANGE)
//
// Writers here take a plain io.Writer (in practice a bufio.Writer the
// server flushes once per command) rather than owning any buffering
// themselves — that's the caller's concern, not the wire format's.

// ReplyType tags which field of a decoded Reply is meaningful.
type ReplyType int

const (
	ReplySimple ReplyType = iota
	ReplyError
	ReplyInt
	ReplyBulk
	ReplyNil
	ReplyArray
)

// Reply is a decoded server response, used by the CLI client.
type Reply struct {
	Type  ReplyType
	Str   string
	Int   int64
	Items []string
}

func WriteOK(w io.Writer) error {
	_, err := io.WriteString(w, "+OK\r\n")
	return err
}

// WriteError writes msg as a RESP error reply. All error messages this
// project produces are built by our own code (command names are always
// %q-escaped before being embedded — see internal/engine/apply.go), never
// raw client bytes echoed verbatim, so there's no risk of a client-supplied
// \r\n breaking the simple-string framing here.
func WriteError(w io.Writer, msg string) error {
	_, err := fmt.Fprintf(w, "-%s\r\n", msg)
	return err
}

func WriteInt(w io.Writer, n int64) error {
	_, err := fmt.Fprintf(w, ":%d\r\n", n)
	return err
}

// WriteNil writes RESP's nil bulk string, used for e.g. GET on a key that
// doesn't exist — distinct from an empty string, which is a valid value.
func WriteNil(w io.Writer) error {
	_, err := io.WriteString(w, "$-1\r\n")
	return err
}

// WriteBulkString writes s length-prefixed, for the same binary-safety
// reason request bulk strings are (see Encode in protocol.go): a value
// containing \r\n must still round-trip correctly.
func WriteBulkString(w io.Writer, s string) error {
	_, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(s), s)
	return err
}

// WriteArray writes items as an array of bulk strings.
func WriteArray(w io.Writer, items []string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(items)); err != nil {
		return err
	}
	for _, item := range items {
		if err := WriteBulkString(w, item); err != nil {
			return err
		}
	}
	return nil
}

// ReadReply parses one reply from r. Used by the CLI client to decode what
// the server sends back.
func ReadReply(r *bufio.Reader) (Reply, error) {
	line, err := readLine(r)
	if err != nil {
		return Reply{}, err
	}
	if len(line) == 0 {
		return Reply{}, fmt.Errorf("protocol error: empty reply line")
	}

	switch line[0] {
	case '+':
		return Reply{Type: ReplySimple, Str: line[1:]}, nil

	case '-':
		return Reply{Type: ReplyError, Str: line[1:]}, nil

	case ':':
		n, err := strconv.ParseInt(line[1:], 10, 64)
		if err != nil {
			return Reply{}, fmt.Errorf("protocol error: invalid integer reply %q", line)
		}
		return Reply{Type: ReplyInt, Int: n}, nil

	case '$':
		length, err := strconv.Atoi(line[1:])
		if err != nil {
			return Reply{}, fmt.Errorf("protocol error: invalid bulk length %q", line)
		}
		if length == -1 {
			return Reply{Type: ReplyNil}, nil
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(r, data); err != nil {
			return Reply{}, err
		}
		if _, err := readLine(r); err != nil { // trailing \r\n
			return Reply{}, err
		}
		return Reply{Type: ReplyBulk, Str: string(data)}, nil

	case '*':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return Reply{}, fmt.Errorf("protocol error: invalid array length %q", line)
		}
		items := make([]string, 0, n)
		for i := 0; i < n; i++ {
			// Every array reply this server sends is an array of bulk
			// strings (never nested arrays), so recursing and taking
			// .Str is sufficient rather than needing a general nested
			// reply tree.
			elem, err := ReadReply(r)
			if err != nil {
				return Reply{}, err
			}
			items = append(items, elem.Str)
		}
		return Reply{Type: ReplyArray, Items: items}, nil

	default:
		return Reply{}, fmt.Errorf("protocol error: unknown reply type %q", line[0])
	}
}
