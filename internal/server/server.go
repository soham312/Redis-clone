// Package server implements the TCP front end: a listener that accepts
// connections and hands each one to its own goroutine.
//
// Goroutine-per-connection, not thread-per-connection or an event loop,
// is the idiomatic Go answer here, and it holds up well under load for
// reasons that don't apply to most other languages:
//   - A goroutine starts with a ~2KB stack that grows and shrinks as
//     needed, versus an OS thread's fixed stack (often 1-8MB, reserved
//     upfront on most platforms). Ten thousand idle connections as
//     goroutines costs tens of MB; as OS threads it can mean tens of GB
//     of stack space reserved before any of them even do anything.
//   - Goroutines are scheduled M:N onto a small number of OS threads by
//     the Go runtime, so blocking on a slow client's socket read parks
//     that goroutine cheaply instead of tying up a kernel thread (or,
//     with an event-loop/async model, forcing every handler to be
//     written in non-blocking, callback/future style just to avoid
//     tying one up). Blocking, synchronous-looking code — one goroutine,
//     one connection, a plain for loop — is both the simplest code to
//     write here AND the high-concurrency-capable code, which isn't true
//     in most languages: normally you pick one or the other.
package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"goredis/internal/engine"
	"goredis/internal/protocol"
)

// Server accepts TCP connections and dispatches decoded commands to an
// Engine, one goroutine per connection.
type Server struct {
	addr   string
	engine *engine.Engine

	listener net.Listener

	mu    sync.Mutex
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup

	quit chan struct{}
}

func New(addr string, eng *engine.Engine) *Server {
	return &Server{
		addr:   addr,
		engine: eng,
		conns:  make(map[net.Conn]struct{}),
		quit:   make(chan struct{}),
	}
}

// Addr returns the address ListenAndServe actually bound to, or nil if it
// hasn't bound yet. Mainly useful for tests that listen on "127.0.0.1:0"
// (an OS-assigned ephemeral port, so parallel test runs never collide on a
// fixed port) and need to find out which port that turned out to be.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// ListenAndServe binds addr and serves connections until Shutdown is
// called (in which case it returns nil) or a genuine accept error occurs.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	// Store the listener under s.mu (the same lock Shutdown uses) rather
	// than as a bare field write, for two reasons: it's the only thing
	// that makes s.listener safe to read from Shutdown, which can run on
	// another goroutine at any time — and checking s.quit in the same
	// critical section closes a real ordering gap, not just a race-
	// detector complaint: without it, a Shutdown() that runs to
	// completion *before* this line (nothing to close yet, nothing in
	// s.conns yet, WaitGroup empty) would have its signal missed entirely,
	// and this server would carry on accepting connections forever.
	s.mu.Lock()
	select {
	case <-s.quit:
		s.mu.Unlock()
		ln.Close()
		return nil
	default:
		s.listener = ln
		s.mu.Unlock()
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.quit:
				// Shutdown closed the listener on purpose; this Accept
				// error is the expected way that unblocks us, not a
				// real failure.
				return nil
			default:
				return err
			}
		}

		s.trackConn(conn, true)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.trackConn(conn, false)
			handleConn(conn, s.engine)
		}()
	}
}

func (s *Server) trackConn(c net.Conn, add bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if add {
		s.conns[c] = struct{}{}
	} else {
		delete(s.conns, c)
	}
}

// Shutdown stops accepting new connections, forces every currently-open
// connection closed (which unblocks each connection goroutine's in-flight
// Read, causing its loop to exit), and waits for all connection goroutines
// to actually finish before returning — so by the time Shutdown returns,
// nothing can still be calling into the Engine, and it's safe for the
// caller to close the Engine (flushing/closing the AOF, stopping the
// store's background expiry goroutine) right after.
//
// close(s.quit) rather than a boolean flag is what lets ListenAndServe's
// blocked Accept() call reliably tell "the listener closed because we're
// shutting down" apart from "the listener closed because something broke"
// — the same channel-as-signal pattern used for the active-expiry
// sweeper's shutdown in internal/store/expiry.go.
func (s *Server) Shutdown() {
	close(s.quit)

	s.mu.Lock()
	if s.listener != nil {
		s.listener.Close()
	}
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()

	s.wg.Wait()
}

// handleConn is the body of one connection's goroutine: decode a command,
// execute it, write the reply, repeat until the client disconnects or the
// stream can no longer be reliably parsed.
func handleConn(conn net.Conn, eng *engine.Engine) {
	defer conn.Close()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	for {
		cmd, err := protocol.Decode(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return // client disconnected cleanly
			}
			// Any other decode error — including a connection Shutdown
			// force-closed out from under us (surfaces as a read error
			// here, not io.EOF) or a malformed/truncated frame — means
			// we've lost sync with the byte stream. There's no reliable
			// way to find the start of the next command from a corrupt
			// position, so report it and close rather than risk silently
			// misinterpreting later bytes as an unrelated command.
			protocol.WriteError(w, fmt.Sprintf("ERR protocol error: %v", err))
			w.Flush()
			return
		}

		result, err := eng.Execute(cmd)
		if err != nil {
			protocol.WriteError(w, err.Error())
		} else {
			writeResult(w, result)
		}

		if err := w.Flush(); err != nil {
			return // client gone
		}
	}
}

// writeResult renders an engine.Result as the matching RESP-lite reply.
func writeResult(w io.Writer, r engine.Result) error {
	switch r.Kind {
	case engine.KindOK:
		return protocol.WriteOK(w)
	case engine.KindNil:
		return protocol.WriteNil(w)
	case engine.KindString:
		return protocol.WriteBulkString(w, r.Str)
	case engine.KindInt:
		return protocol.WriteInt(w, r.Int)
	case engine.KindStrings:
		return protocol.WriteArray(w, r.Strs)
	default:
		return protocol.WriteError(w, "ERR internal error: unknown result kind")
	}
}
