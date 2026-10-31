// lifecycle-demo exercises the v2 transport with two owned loopback processes.
// Its timers and integer chunks are fixtures, not model inference benchmarks.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
)

func main() {
	mode := flag.String("mode", "server", "server or client")
	address := flag.String("address", "127.0.0.1:0", "explicit TCP endpoint")
	flag.Parse()
	var err error
	switch *mode {
	case "server":
		err = server(*address)
	case "client":
		err = client(*address)
	default:
		err = fmt.Errorf("unknown mode %q", *mode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func emit(value any) { _ = json.NewEncoder(os.Stdout).Encode(value) }
func server(address string) error {
	s, err := rpc.NewServer(rpc.ServerConfig{})
	if err != nil {
		return err
	}
	var canceled atomic.Int64
	var drainActive atomic.Bool
	if err = rpc.RegisterUnary(s, "/demo/echo", func(ctx context.Context, n int) (int, error) { return n, ctx.Err() }); err != nil {
		return err
	}
	if err = rpc.RegisterUnary(s, "/demo/canceled", func(context.Context, int) (int64, error) { return canceled.Load(), nil }); err != nil {
		return err
	}
	if err = rpc.RegisterStream(s, "/demo/items", func(ctx context.Context, n int, send func(int) error) error {
		for i := 0; i < n; i++ {
			if e := send(i); e != nil {
				return e
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err = rpc.RegisterStream(s, "/demo/cancel", func(ctx context.Context, _ int, send func(int) error) error {
		if e := send(1); e != nil {
			return e
		}
		<-ctx.Done()
		canceled.Add(1)
		return ctx.Err()
	}); err != nil {
		return err
	}
	if err = rpc.RegisterStream(s, "/demo/drain", func(ctx context.Context, _ int, send func(int) error) error {
		drainActive.Store(true)
		defer drainActive.Store(false)
		if e := send(1); e != nil {
			return e
		}
		timer := time.NewTimer(200 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		return send(2)
	}); err != nil {
		return err
	}
	l, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	defer s.Close()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
	defer signal.Stop(signals)
	emit(map[string]any{"phase": "ready", "address": l.Addr().String(), "evidence": "fixture-no-model"})
	select {
	case <-signals:
	case err := <-done:
		return fmt.Errorf("serve stopped before signal: %v", err)
	}
	emit(map[string]any{"phase": "drain-starting", "stats": s.Stats(), "drain_handler_active": drainActive.Load()})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = s.Drain(ctx)
	serveErr := <-done
	emit(map[string]any{"phase": "drained", "stats": s.Stats(), "canceled_handlers": canceled.Load(), "error": errorText(err)})
	if err != nil {
		return err
	}
	return serveErr
}
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
func client(address string) error {
	c, err := rpc.Dial(context.Background(), address, rpc.ClientConfig{StreamWindow: 2})
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	var out int
	if err = c.Call(ctx, "/demo/echo", 42, &out); err != nil || out != 42 {
		return fmt.Errorf("echo: %d %v", out, err)
	}
	items, err := c.Stream(ctx, "/demo/items", 20)
	if err != nil {
		return err
	}
	for i := 0; i < 20; i++ {
		if err = items.Recv(&out); err != nil || out != i {
			return fmt.Errorf("chunk %d: %d %v", i, out, err)
		}
	}
	if err = items.Recv(nil); err != io.EOF {
		return fmt.Errorf("stream terminal: %v", err)
	}
	stream, err := c.Stream(ctx, "/demo/cancel", 0)
	if err != nil {
		return err
	}
	if err = stream.Recv(&out); err != nil {
		return err
	}
	_ = stream.Close()
	var canceled int64
	for canceled != 1 {
		if err = c.Call(ctx, "/demo/canceled", 0, &canceled); err != nil {
			return err
		}
		if canceled != 1 {
			time.Sleep(time.Millisecond)
		}
	}
	drain, err := c.Stream(ctx, "/demo/drain", 0)
	if err != nil {
		return err
	}
	if err = drain.Recv(&out); err != nil || out != 1 {
		return fmt.Errorf("drain first: %d %v", out, err)
	}
	emit(map[string]any{"phase": "drain-ready"})
	if err = drain.Recv(&out); err != nil || out != 2 {
		return fmt.Errorf("drain last: %d %v", out, err)
	}
	if err = drain.Recv(nil); err != io.EOF {
		return fmt.Errorf("drain EOF: %v", err)
	}
	c.Close()
	emit(map[string]any{"phase": "passed", "chunks": 20, "remote_canceled": canceled, "drain_eof": true, "stats": c.Stats(), "evidence": "fixture-no-model"})
	return nil
}
