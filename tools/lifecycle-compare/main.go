// This validation-only adapter invokes public APIs, never copies an old core.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	next "github.com/KanaDoodle/KanaRPC"
	old "github.com/KanaDoodle/KanaRPC-Go/rpc"
)

type Message struct{ Value int }
type behavior struct {
	started, stopped, release chan struct{}
	once                      sync.Once
}

func newBehavior() *behavior {
	return &behavior{started: make(chan struct{}), stopped: make(chan struct{}), release: make(chan struct{})}
}
func (b *behavior) run(ctx context.Context, n int) (int, error) {
	close(b.started)
	defer close(b.stopped)
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-b.release:
		return n, nil
	}
}
func (b *behavior) unblock() { b.once.Do(func() { close(b.release) }) }

type Legacy struct{ B *behavior }

func (s *Legacy) Wait(in, out *Message) error {
	n, err := s.B.run(context.Background(), in.Value)
	out.Value = n
	return err
}
func (s *Legacy) Echo(in, out *Message) error { *out = *in; return nil }

type Cooperative struct{ B *behavior }

func (s *Cooperative) Wait(ctx context.Context, in, out *Message) error {
	n, err := s.B.run(ctx, in.Value)
	out.Value = n
	return err
}
func (s *Cooperative) Echo(ctx context.Context, in, out *Message) error { *out = *in; return ctx.Err() }

type endpoint struct {
	call  func(context.Context, string, int) (int, error)
	drain func(context.Context) error
	ready func() bool
	close func()
}

func setup(mode string, b *behavior) (endpoint, error) {
	if mode == "new-v2" {
		s, err := next.NewServer(next.ServerConfig{})
		if err != nil {
			return endpoint{}, err
		}
		if err = next.RegisterUnary(s, "/demo/Wait", b.run); err != nil {
			return endpoint{}, err
		}
		if err = next.RegisterUnary(s, "/demo/Echo", func(ctx context.Context, n int) (int, error) { return n, ctx.Err() }); err != nil {
			return endpoint{}, err
		}
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return endpoint{}, err
		}
		done := make(chan error, 1)
		go func() { done <- s.Serve(l) }()
		if err = until(s.Ready); err != nil {
			return endpoint{}, err
		}
		c, err := next.Dial(context.Background(), l.Addr().String(), next.ClientConfig{})
		if err != nil {
			return endpoint{}, err
		}
		return endpoint{call: func(ctx context.Context, m string, n int) (int, error) {
			var out int
			e := c.Call(ctx, "/demo/"+m, n, &out)
			return out, e
		}, drain: s.Drain, ready: s.Ready, close: func() {
			b.unblock()
			c.Close()
			s.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = s.Wait(ctx)
			<-done
		}}, nil
	}
	s, err := old.NewServer(old.ServerConfig{Address: "127.0.0.1:0"})
	if err != nil {
		return endpoint{}, err
	}
	cooperative := mode == "old-cooperative"
	if cooperative {
		s.Register("demo", &Cooperative{b})
	} else {
		s.Register("demo", &Legacy{b})
	}
	done := make(chan error, 1)
	go func() { done <- s.Start() }()
	if err = until(s.Ready); err != nil {
		return endpoint{}, err
	}
	c, err := old.NewDirectClient(s.Addr().String(), old.ClientConfig{CooperativeUnary: cooperative, Timeout: time.Second})
	if err != nil {
		return endpoint{}, err
	}
	return endpoint{call: func(ctx context.Context, m string, n int) (int, error) {
		var out Message
		e := c.Invoke(ctx, "demo", m, &Message{n}, &out)
		return out.Value, e
	}, drain: s.Drain, ready: s.Ready, close: func() { b.unblock(); c.Close(); s.Shutdown(); <-done }}, nil
}
func until(condition func() bool) error {
	limit := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(limit) {
			return errors.New("state transition timeout")
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}
func wait(ch <-chan struct{}) error {
	select {
	case <-ch:
		return nil
	case <-time.After(3 * time.Second):
		return errors.New("handler signal timeout")
	}
}
func run(mode, scenario string) (map[string]any, error) {
	b := newBehavior()
	ep, err := setup(mode, b)
	if err != nil {
		return nil, err
	}
	defer ep.close()
	result := map[string]any{"implementation": mode, "scenario": scenario, "scope": "loopback lifecycle behavior; no throughput or inference claim"}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	type reply struct {
		value int
		err   error
	}
	done := make(chan reply, 1)
	go func() { n, e := ep.call(ctx, "Wait", 42); done <- reply{n, e} }()
	if err = wait(b.started); err != nil {
		return result, err
	}
	if scenario == "cancel" {
		cancel()
		answer := <-done
		result["caller_canceled"] = errors.Is(answer.err, context.Canceled)
		select {
		case <-b.stopped:
			result["remote_stopped_before_release"] = true
		case <-time.After(100 * time.Millisecond):
			result["remote_stopped_before_release"] = false
		}
		b.unblock()
		if err = wait(b.stopped); err != nil {
			return result, err
		}
		if mode != "old-legacy" && (result["caller_canceled"] != true || result["remote_stopped_before_release"] != true) {
			return result, errors.New("cooperative cancellation contract failed")
		}
		return result, nil
	}
	drain := make(chan error, 1)
	go func() {
		deadline, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		drain <- ep.drain(deadline)
	}()
	if err = until(func() bool { return !ep.ready() }); err != nil {
		return result, err
	}
	_, admissionErr := ep.call(context.Background(), "Echo", 7)
	result["new_call_rejected"] = admissionErr != nil
	b.unblock()
	answer := <-done
	result["existing_result_delivered"] = answer.err == nil && answer.value == 42
	result["existing_error"] = errorString(answer.err)
	result["drain_error"] = errorString(<-drain)
	if mode != "old-legacy" && (result["existing_result_delivered"] != true || result["drain_error"] != "" || result["new_call_rejected"] != true) {
		return result, errors.New("cooperative drain contract failed")
	}
	return result, nil
}
func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
func main() {
	mode := flag.String("implementation", "new-v2", "old-legacy, old-cooperative, new-v2")
	scenario := flag.String("scenario", "cancel", "cancel or drain")
	flag.Parse()
	if (*mode != "old-legacy" && *mode != "old-cooperative" && *mode != "new-v2") || (*scenario != "cancel" && *scenario != "drain") {
		fmt.Fprintln(os.Stderr, "invalid experiment")
		os.Exit(2)
	}
	result, err := run(*mode, *scenario)
	if err != nil {
		if result == nil {
			result = map[string]any{}
		}
		result["failure"] = err.Error()
	}
	_ = json.NewEncoder(os.Stdout).Encode(result)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
