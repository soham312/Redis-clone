// Command client is a minimal redis-cli-style REPL: connect to a goredis
// server over TCP, read one line at a time from stdin, send it as a
// command, and print the reply.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"

	"goredis/internal/protocol"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6380", "server address")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("connected to %s. Commands: SET GET DEL EXISTS KEYS FLUSHALL EXPIRE TTL LPUSH RPUSH LLEN LRANGE BGSAVE. Type QUIT to exit.\n", *addr)

	reader := bufio.NewReader(conn)
	stdin := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("goredis> ")
		if !stdin.Scan() {
			return // EOF on stdin (e.g. piped input ran out, or Ctrl-D)
		}
		line := strings.TrimSpace(stdin.Text())
		if line == "" {
			continue
		}
		if strings.EqualFold(line, "quit") || strings.EqualFold(line, "exit") {
			return
		}

		tokens, err := tokenize(line)
		if err != nil {
			fmt.Println("(error)", err)
			continue
		}
		if len(tokens) == 0 {
			continue
		}

		cmd := protocol.Command{Op: strings.ToUpper(tokens[0]), Args: tokens[1:]}
		if _, err := conn.Write(protocol.Encode(cmd)); err != nil {
			fmt.Fprintln(os.Stderr, "write:", err)
			return
		}

		reply, err := protocol.ReadReply(reader)
		if err != nil {
			fmt.Fprintln(os.Stderr, "read:", err)
			return
		}
		printReply(reply)
	}
}

func printReply(r protocol.Reply) {
	switch r.Type {
	case protocol.ReplySimple:
		fmt.Println(r.Str)
	case protocol.ReplyError:
		fmt.Println("(error)", r.Str)
	case protocol.ReplyInt:
		fmt.Println("(integer)", r.Int)
	case protocol.ReplyNil:
		fmt.Println("(nil)")
	case protocol.ReplyBulk:
		fmt.Printf("%q\n", r.Str)
	case protocol.ReplyArray:
		if len(r.Items) == 0 {
			fmt.Println("(empty array)")
			return
		}
		for i, item := range r.Items {
			fmt.Printf("%d) %q\n", i+1, item)
		}
	}
}

// tokenize is a small, deliberately simple shell-like splitter: whitespace
// separates arguments, and double quotes let an argument contain
// whitespace (e.g. `SET greeting "hello world"`). It doesn't support
// escaped quotes or nesting — a real shell-lexer library would be
// overkill for a demo REPL, and this covers the one case that actually
// matters here (values with spaces).
func tokenize(line string) ([]string, error) {
	var tokens []string
	var cur strings.Builder
	inQuotes := false
	hasToken := false

	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case c == '"':
			inQuotes = !inQuotes
			hasToken = true
		case c == ' ' && !inQuotes:
			if hasToken {
				tokens = append(tokens, cur.String())
				cur.Reset()
				hasToken = false
			}
		default:
			cur.WriteByte(c)
			hasToken = true
		}
	}
	if inQuotes {
		return nil, fmt.Errorf("unclosed quote")
	}
	if hasToken {
		tokens = append(tokens, cur.String())
	}
	return tokens, nil
}
