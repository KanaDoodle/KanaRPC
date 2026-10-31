package kanarpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func governed(t testing.TB, config GovernanceConfig, register func(*Server)) (*Server, *Client) {
	t.Helper()
	s, address := start(t, ServerConfig{Governance: &config}, register)
	return s, connect(t, address, ClientConfig{Timeout: 3 * time.Second})
}
func TestMethodFairQueueAgainstWorkConservingFIFO(t *testing.T) {
	for _, policy := range []QueuePolicy{FIFO, MethodFair} {
		t.Run(string(policy), func(t *testing.T) {
			started := make(chan string, 8)
			release := make(chan struct{}, 8)
			s, c := governed(t, GovernanceConfig{MaxExecuting: 1, QueueCapacity: 8, QueueTimeout: time.Second, QueuePolicy: policy}, func(s *Server) {
				for _, path := range []string{"/demo/slow", "/demo/fast"} {
					path := path
					require(t, RegisterUnary(s, path, func(ctx context.Context, n int) (int, error) {
						started <- fmt.Sprintf("%s:%d", path, n)
						select {
						case <-release:
							return n, nil
						case <-ctx.Done():
							return 0, ctx.Err()
						}
					}))
				}
			})
			done := make(chan error, 4)
			call := func(path string, n int) {
				go func() {
					var out int
					e := c.Call(context.Background(), path, n, &out)
					if e == nil && out != n {
						e = ErrProtocol
					}
					done <- e
				}()
			}
			call("/demo/slow", 0)
			if got := <-started; got != "/demo/slow:0" {
				t.Fatal(got)
			}
			for i, path := range []string{"/demo/slow", "/demo/slow", "/demo/fast"} {
				call(path, i+1)
				want := i + 1
				eventually(t, func() bool { return s.Governance().Queued == want })
			}
			release <- struct{}{}
			next := <-started
			if policy == FIFO && next != "/demo/slow:1" || policy == MethodFair && next != "/demo/fast:3" {
				t.Fatal("unexpected scheduling order", policy, next)
			}
			for i := 0; i < 3; i++ {
				release <- struct{}{}
			}
			for i := 0; i < 4; i++ {
				require(t, <-done)
			}
			eventually(t, func() bool {
				return s.Stats().InFlight == 0 && s.Stats().Queued == 0 && s.Stats().Executing == 0 && s.MemoryStats().Receive.Used == 0
			})
			t.Log("first after blocked request:", next)
		})
	}
}
func TestQueuedCancelAndExpiryNeverInvokeHandler(t *testing.T) {
	for _, cancelQueued := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelQueued), func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int64
			queueTimeout := 40 * time.Millisecond
			if cancelQueued {
				queueTimeout = time.Second
			}
			s, c := governed(t, GovernanceConfig{MaxExecuting: 1, QueueCapacity: 2, QueueTimeout: queueTimeout}, func(s *Server) {
				require(t, RegisterUnary(s, "/demo/hold", func(ctx context.Context, n int) (int, error) {
					calls.Add(1)
					if n == 0 {
						close(started)
						select {
						case <-release:
						case <-ctx.Done():
							return 0, ctx.Err()
						}
					}
					return n, nil
				}))
			})
			first := make(chan error, 1)
			go func() { var out int; first <- c.Call(context.Background(), "/demo/hold", 0, &out) }()
			signal(t, started)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			second := make(chan error, 1)
			go func() { var out int; second <- c.Call(ctx, "/demo/hold", 1, &out) }()
			eventually(t, func() bool { return s.Stats().Queued == 1 })
			if cancelQueued {
				cancel()
			}
			err := <-second
			if cancelQueued && !errors.Is(err, context.Canceled) || !cancelQueued && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			eventually(t, func() bool { return s.Stats().Queued == 0 })
			if calls.Load() != 1 {
				t.Fatal("queued canceled work executed", calls.Load())
			}
			close(release)
			require(t, <-first)
			eventually(t, func() bool { return s.Stats().InFlight == 0 && s.MemoryStats().Receive.Used == 0 })
		})
	}
}
func TestGovernedDrainRunsAlreadyAcceptedQueue(t *testing.T) {
	started, release := make(chan struct{}, 2), make(chan struct{}, 2)
	s, c := governed(t, GovernanceConfig{MaxExecuting: 1, QueueCapacity: 2, QueueTimeout: time.Second}, func(s *Server) {
		require(t, RegisterUnary(s, "/demo/hold", func(ctx context.Context, n int) (int, error) {
			started <- struct{}{}
			select {
			case <-release:
				return n, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}))
	})
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(n int) { var out int; done <- c.Call(context.Background(), "/demo/hold", n, &out) }(i)
		if i == 0 {
			signal(t, started)
		} else {
			eventually(t, func() bool { return s.Stats().Queued == 1 })
		}
	}
	drain := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		drain <- s.Drain(ctx)
	}()
	eventually(t, func() bool { return !s.Ready() })
	release <- struct{}{}
	signal(t, started)
	release <- struct{}{}
	require(t, <-done)
	require(t, <-done)
	require(t, <-drain)
}
func TestTerminalWaitDoesNotConsumeExecutionSlots(t *testing.T) {
	s, address := start(t, ServerConfig{Governance: &GovernanceConfig{MaxExecuting: 1, QueueCapacity: 2}}, echo)
	raw, err := net.Dial("tcp", address)
	require(t, err)
	defer raw.Close()
	rawOpen(t, raw, 1, "/demo/echo", false, 1)
	_, _ = rawTerminal(t, raw)
	eventually(t, func() bool { return s.Stats().Executing == 0 })
	if s.Stats().InFlight != 1 {
		t.Fatal("receipt waiter was not retained")
	}
	c := connect(t, address, ClientConfig{})
	var out int
	require(t, c.Call(context.Background(), "/demo/echo", 9, &out))
	if out != 9 {
		t.Fatal(out)
	}
}

func TestFullQueueDoesNotRejectMethodWithFreeExecutionCapacity(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	s, c := governed(t, GovernanceConfig{MaxExecuting: 2, QueueCapacity: 1, QueueTimeout: time.Second, Methods: []MethodPolicy{{Method: "/demo/hold", Max: 1, Initial: 1}}}, func(s *Server) {
		echo(s)
		require(t, RegisterUnary(s, "/demo/hold", func(ctx context.Context, n int) (int, error) {
			if n == 0 {
				close(started)
			}
			select {
			case <-release:
				return n, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}))
	})
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(n int) { var out int; done <- c.Call(context.Background(), "/demo/hold", n, &out) }(i)
		if i == 0 {
			signal(t, started)
		} else {
			eventually(t, func() bool { return s.Stats().Queued == 1 })
		}
	}
	var out int
	err := c.Call(context.Background(), "/demo/echo", 9, &out)
	close(release)
	require(t, <-done)
	require(t, <-done)
	if err != nil || out != 9 {
		t.Fatal("available execution slot stranded behind another method's full queue", out, err)
	}
}
func TestExpiredRequestDoesNotInvokeScheduledHandler(t *testing.T) {
	s, err := NewServer(ServerConfig{})
	require(t, err)
	s.stats.InFlight = 1
	s.stats.Executing = 1
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var calls atomic.Int64
	p := &serverConn{server: s, ctx: ctx, requests: make(map[uint64]*execution)}
	r := &execution{peer: p, ctx: ctx, cancel: func(error) {}, stop: func() {}, started: time.Now(), path: "/demo/expired"}
	r.run(method{run: func(context.Context, json.RawMessage, *Sender) error { calls.Add(1); return nil }}, nil)
	if calls.Load() != 0 {
		t.Fatal("canceled request still invoked handler")
	}
}
func TestMethodWeightedRotationUnderBacklog(t *testing.T) {
	started := make(chan string, 32)
	release := make(chan struct{}, 32)
	s, c := governed(t, GovernanceConfig{MaxExecuting: 1, QueueCapacity: 32, QueueTimeout: 2 * time.Second, Methods: []MethodPolicy{{Method: "/demo/a", Weight: 2}, {Method: "/demo/b", Weight: 1}}}, func(s *Server) {
		for _, path := range []string{"/demo/a", "/demo/b", "/demo/hold"} {
			path := path
			require(t, RegisterUnary(s, path, func(ctx context.Context, n int) (int, error) {
				started <- path
				select {
				case <-release:
					return n, nil
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			}))
		}
	})
	done := make(chan error, 25)
	call := func(path string, n int) {
		go func() { var out int; done <- c.Call(context.Background(), path, n, &out) }()
	}
	call("/demo/hold", 0)
	<-started
	for i := 0; i < 12; i++ {
		call("/demo/a", i)
		call("/demo/b", i)
	}
	eventually(t, func() bool { return s.Stats().Queued == 24 })
	counts := map[string]int{}
	for i := 0; i < 12; i++ {
		release <- struct{}{}
		counts[<-started]++
	}
	if counts["/demo/a"] != 8 || counts["/demo/b"] != 4 {
		t.Fatal(counts)
	}
	for i := 0; i < 13; i++ {
		release <- struct{}{}
	}
	for i := 0; i < 25; i++ {
		require(t, <-done)
	}
}
func TestAIMDBoundsAndNeutralCancellationSyntheticSamples(t *testing.T) {
	s, err := NewServer(ServerConfig{Governance: &GovernanceConfig{MaxExecuting: 8, SampleWindow: 4, Adaptive: true, Methods: []MethodPolicy{{Method: "/demo/echo", Min: 2, Initial: 8, Max: 8, Target: 5 * time.Millisecond}}}})
	require(t, err)
	echo(s)
	s.state = serving
	r := &execution{path: "/demo/echo", peer: &serverConn{server: s}}
	sample := func(status Status, duration time.Duration) {
		s.mu.Lock()
		s.stats.Executing = 1
		m := s.governor.methods[r.path]
		m.running = 1
		m.saturated = true
		s.handlerDoneLocked(r, status, duration)
		s.mu.Unlock()
	}
	for i := 0; i < 40; i++ {
		sample(Status{Code: OK}, 10*time.Millisecond)
	}
	state := s.Governance()
	if state.Methods[0].Limit != 2 {
		t.Fatal(state)
	}
	before := state.Methods[0].Samples
	for i := 0; i < 8; i++ {
		sample(Status{Code: Canceled}, time.Second)
	}
	if s.Governance().Methods[0].Samples != before {
		t.Fatal("cancellation sampled as congestion")
	}
	for i := 0; i < 32; i++ {
		sample(Status{Code: OK}, time.Millisecond)
	}
	if s.Governance().Methods[0].Limit != 8 {
		t.Fatal(s.Governance())
	}
	t.Log("synthetic controller input, not measured backend performance")
}
func TestPressureIgnoresIdleConnectionsAndClientGateIsPreSend(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	s, address := start(t, ServerConfig{Governance: &GovernanceConfig{MaxExecuting: 1, QueueCapacity: 8, Feedback: true}}, func(s *Server) {
		require(t, RegisterUnary(s, "/demo/echo", func(ctx context.Context, n int) (int, error) {
			if n == 0 {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			}
			return n, nil
		}))
	})
	c := connect(t, address, ClientConfig{Feedback: &FeedbackConfig{}})
	for i := 0; i < 3; i++ {
		_ = connect(t, address, ClientConfig{})
	}
	var out int
	require(t, c.Call(context.Background(), "/demo/echo", 1, &out))
	c.resources.feedback.mu.Lock()
	pressure := c.resources.feedback.methods["/demo/echo"].pressure
	c.resources.feedback.mu.Unlock()
	if pressure.PeersDemand != 1 {
		t.Fatal("idle peers diluted demand", pressure)
	}
	done := make(chan error, 1)
	go func() { var out int; done <- c.Call(context.Background(), "/demo/echo", 0, &out) }()
	signal(t, started)
	before := s.Stats().Admitted
	var status *Status
	err := c.Call(context.Background(), "/demo/echo", 2, &out)
	if !errors.As(err, &status) || status.Code != ResourceExhausted || !strings.Contains(status.Message, "local pressure") {
		t.Fatal(err)
	}
	if s.Stats().Admitted != before {
		t.Fatal("locally rejected business reached server")
	}
	close(release)
	require(t, <-done)
	if c.FeedbackStats().Limited != 1 {
		t.Fatal(c.FeedbackStats())
	}
}
