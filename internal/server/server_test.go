package server

import (
	"bufio"
	"fmt"
	"net"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"goredis/internal/engine"
	"goredis/internal/persistence"
	"goredis/internal/protocol"
	"goredis/internal/store"
)

// testClient is a thin wrapper around a real TCP connection to a real,
// running Server — it sends commands and reads replies exactly the way
// cmd/client does, just without the interactive REPL loop, so this
// integration test is exercising the genuine wire path end to end (TCP
// socket -> protocol.Decode -> engine.Execute -> protocol reply encoding
// -> TCP socket -> protocol.ReadReply), not calling into Store or Engine
// directly.
type testClient struct {
	conn net.Conn
	r    *bufio.Reader
}

func dial(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &testClient{conn: conn, r: bufio.NewReader(conn)}
}

func (c *testClient) send(t *testing.T, op string, args ...string) protocol.Reply {
	t.Helper()
	if _, err := c.conn.Write(protocol.Encode(protocol.Command{Op: op, Args: args})); err != nil {
		t.Fatalf("write %s %v: %v", op, args, err)
	}
	reply, err := protocol.ReadReply(c.r)
	if err != nil {
		t.Fatalf("read reply for %s %v: %v", op, args, err)
	}
	return reply
}

// startTestServer starts a real Server (backed by a real Store and Engine
// with AOF persistence in a temp dir) listening on an OS-assigned loopback
// port, and registers cleanup to shut it down.
func startTestServer(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	aof, err := persistence.OpenAOF(filepath.Join(dir, "appendonly.aof"))
	if err != nil {
		t.Fatalf("OpenAOF: %v", err)
	}
	s := store.New()
	eng := engine.New(s, aof, filepath.Join(dir, "dump.rdb"))

	srv := New("127.0.0.1:0", eng)

	serveErrCh := make(chan error, 1)
	go func() { serveErrCh <- srv.ListenAndServe() }()

	// ListenAndServe binds the listener synchronously before entering its
	// Accept loop, but that bind happens on the goroutine above, so poll
	// briefly for Addr() to become non-nil rather than assuming it's
	// ready immediately after go srv.ListenAndServe().
	deadline := time.Now().Add(2 * time.Second)
	var addr net.Addr
	for time.Now().Before(deadline) {
		if addr = srv.Addr(); addr != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if addr == nil {
		t.Fatalf("server never bound a listener")
	}

	t.Cleanup(func() {
		srv.Shutdown()
		if err := <-serveErrCh; err != nil {
			t.Errorf("ListenAndServe returned an error after Shutdown: %v", err)
		}
		eng.Close()
	})

	return addr.String()
}

// TestIntegrationCommandSequence starts a real server and drives a real
// client through a realistic sequence of commands over an actual TCP
// connection, asserting on the wire replies at each step.
func TestIntegrationCommandSequence(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	if r := c.send(t, "SET", "foo", "bar"); r.Type != protocol.ReplySimple || r.Str != "OK" {
		t.Fatalf("SET foo bar = %+v; want simple OK", r)
	}
	if r := c.send(t, "GET", "foo"); r.Type != protocol.ReplyBulk || r.Str != "bar" {
		t.Fatalf("GET foo = %+v; want bulk bar", r)
	}
	if r := c.send(t, "GET", "missing"); r.Type != protocol.ReplyNil {
		t.Fatalf("GET missing = %+v; want nil", r)
	}
	if r := c.send(t, "EXISTS", "foo", "missing"); r.Type != protocol.ReplyInt || r.Int != 1 {
		t.Fatalf("EXISTS foo missing = %+v; want int 1", r)
	}

	c.send(t, "SET", "a", "1")
	c.send(t, "SET", "b", "2")
	r := c.send(t, "KEYS")
	if r.Type != protocol.ReplyArray {
		t.Fatalf("KEYS = %+v; want array", r)
	}
	sort.Strings(r.Items)
	want := []string{"a", "b", "foo"}
	if len(r.Items) != len(want) {
		t.Fatalf("KEYS = %v; want %v", r.Items, want)
	}
	for i := range want {
		if r.Items[i] != want[i] {
			t.Fatalf("KEYS = %v; want %v", r.Items, want)
		}
	}

	if r := c.send(t, "DEL", "a", "nope"); r.Int != 1 {
		t.Fatalf("DEL a nope = %+v; want int 1", r)
	}

	if r := c.send(t, "EXPIRE", "foo", "100"); r.Int != 1 {
		t.Fatalf("EXPIRE foo 100 = %+v; want int 1", r)
	}
	r = c.send(t, "TTL", "foo")
	if r.Type != protocol.ReplyInt || r.Int < 99 || r.Int > 100 {
		t.Fatalf("TTL foo = %+v; want ~100", r)
	}

	if r := c.send(t, "RPUSH", "l", "x", "y", "z"); r.Int != 3 {
		t.Fatalf("RPUSH l x y z = %+v; want int 3", r)
	}
	r = c.send(t, "LRANGE", "l", "0", "-1")
	if r.Type != protocol.ReplyArray || len(r.Items) != 3 || r.Items[0] != "x" || r.Items[2] != "z" {
		t.Fatalf("LRANGE l 0 -1 = %+v; want [x y z]", r)
	}

	if r := c.send(t, "GET", "l"); r.Type != protocol.ReplyError {
		t.Fatalf("GET on a list key = %+v; want an error reply (WRONGTYPE)", r)
	}

	if r := c.send(t, "BGSAVE"); r.Type != protocol.ReplySimple || r.Str != "OK" {
		t.Fatalf("BGSAVE = %+v; want simple OK", r)
	}

	if r := c.send(t, "FLUSHALL"); r.Type != protocol.ReplySimple || r.Str != "OK" {
		t.Fatalf("FLUSHALL = %+v; want simple OK", r)
	}
	if r := c.send(t, "KEYS"); len(r.Items) != 0 {
		t.Fatalf("KEYS after FLUSHALL = %v; want empty", r.Items)
	}
}

func TestIntegrationUnknownCommandReturnsErrorReplyNotDisconnect(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	r := c.send(t, "NOSUCHCOMMAND")
	if r.Type != protocol.ReplyError {
		t.Fatalf("unknown command reply = %+v; want an error reply", r)
	}

	// The connection must still be usable after a well-formed-but-unknown
	// command (as opposed to a malformed/corrupt frame, which does close
	// the connection — see TestIntegrationMalformedFrameClosesConnection).
	if r := c.send(t, "SET", "k", "v"); r.Type != protocol.ReplySimple || r.Str != "OK" {
		t.Fatalf("SET after an unknown-command error = %+v; want simple OK", r)
	}
}

func TestIntegrationMalformedFrameClosesConnection(t *testing.T) {
	addr := startTestServer(t)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("this is not a valid RESP-lite frame\r\n")); err != nil {
		t.Fatalf("write: %v", err)
	}

	r := bufio.NewReader(conn)
	reply, err := protocol.ReadReply(r)
	if err != nil {
		t.Fatalf("ReadReply: %v", err)
	}
	if reply.Type != protocol.ReplyError {
		t.Fatalf("reply to a malformed frame = %+v; want an error reply", reply)
	}

	// The server closes the connection after a protocol error; confirm a
	// subsequent read observes that (EOF or a read error), not a hang.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatalf("expected the connection to be closed after a protocol error")
	}
}

// TestIntegrationConcurrentClients drives many real client connections
// against one real server concurrently, each doing an independent
// SET-then-GET, and checks every one observed its own write correctly —
// the same property proven at the Engine layer in
// internal/engine/engine_test.go, but here exercised through the actual
// goroutine-per-connection networking code.
func TestIntegrationConcurrentClients(t *testing.T) {
	addr := startTestServer(t)

	const clients = 25
	done := make(chan error, clients)
	for i := 0; i < clients; i++ {
		go func(i int) {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				done <- err
				return
			}
			defer conn.Close()
			r := bufio.NewReader(conn)

			// Each client uses its own key so the assertion is
			// unambiguous regardless of scheduling order.
			key := fmt.Sprintf("client-key-%d", i)
			val := "value"

			if _, err := conn.Write(protocol.Encode(protocol.Command{Op: "SET", Args: []string{key, val}})); err != nil {
				done <- err
				return
			}
			if _, err := protocol.ReadReply(r); err != nil {
				done <- err
				return
			}
			if _, err := conn.Write(protocol.Encode(protocol.Command{Op: "GET", Args: []string{key}})); err != nil {
				done <- err
				return
			}
			reply, err := protocol.ReadReply(r)
			if err != nil {
				done <- err
				return
			}
			if reply.Type != protocol.ReplyBulk || reply.Str != val {
				done <- fmt.Errorf("key %s: want %q, got reply %+v", key, val, reply)
				return
			}
			done <- nil
		}(i)
	}

	for i := 0; i < clients; i++ {
		if err := <-done; err != nil {
			t.Fatalf("concurrent client failed: %v", err)
		}
	}
}
