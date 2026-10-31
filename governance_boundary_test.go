package kanarpc

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/testcert"
	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

func TestGovernancePolicyMustMatchRegisteredMethodBeforeServe(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "tcp", true: "quic"}[native], func(t *testing.T) {
			s, err := NewServer(ServerConfig{Governance: &GovernanceConfig{Methods: []MethodPolicy{{Method: "/typo/absent"}}}})
			require(t, err)
			defer s.Close()
			echo(s)
			if native {
				credentials, e := testcert.New()
				require(t, e)
				listener, e := ListenQUIC("127.0.0.1:0", TLSOptions{Config: credentials.Server}, QUICConfig{})
				require(t, e)
				defer listener.Close()
				require(t, listener.Close())
				err = s.ServeQUIC(listener)
			} else {
				listener, e := net.Listen("tcp", "127.0.0.1:0")
				require(t, e)
				require(t, listener.Close())
				err = s.Serve(listener)
			}
			if err == nil || !strings.Contains(err.Error(), "/typo/absent") {
				t.Fatal("ignored unregistered policy", err)
			}
			s.mu.Lock()
			state := s.state
			s.mu.Unlock()
			if state != fresh {
				t.Fatal("invalid configuration took ownership", state)
			}
		})
	}
}

func TestRemovedQueueEntriesDoNotRetainRequestGraph(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	s, c := governed(t, GovernanceConfig{MaxExecuting: 1, QueueTimeout: time.Second}, func(s *Server) {
		require(t, RegisterUnary(s, "/demo/hold", func(ctx context.Context, n int) (int, error) {
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
	done := make(chan error, 2)
	go func() { var out int; done <- c.Call(context.Background(), "/demo/hold", 0, &out) }()
	signal(t, started)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { var out int; done <- c.Call(ctx, "/demo/hold", 1, &out) }()
	eventually(t, func() bool { return s.Stats().Queued == 1 })
	s.mu.Lock()
	backing := s.governor.queue[:cap(s.governor.queue)]
	s.mu.Unlock()
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	eventually(t, func() bool { return s.Stats().Queued == 0 })
	s.mu.Lock()
	retained := backing[0] != nil
	s.mu.Unlock()
	close(release)
	require(t, <-done)
	if retained {
		t.Fatal("removed request retained in queue backing array")
	}
}

func TestQUICCloseBarrierUsesSharedReceiveBudget(t *testing.T) {
	credentials, err := testcert.New()
	require(t, err)
	listener, err := ListenQUIC("127.0.0.1:0", TLSOptions{Config: credentials.Server}, QUICConfig{})
	require(t, err)
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		peer, e := listener.listener.Accept(ctx)
		if e != nil {
			done <- e
			return
		}
		defer peer.CloseWithError(nativeShutdown, "fixture done")
		stream, e := peer.AcceptStream(ctx)
		if e != nil {
			done <- e
			return
		}
		_ = stream.SetDeadline(time.Now().Add(time.Second))
		_, e = wire.Read(stream, wire.HardLimit)
		if e == nil {
			e = wire.Write(stream, wire.Frame{Kind: wire.Data, ID: 1, Number: 1, Payload: []byte(strings.Repeat("x", 2048))}, wire.HardLimit)
		}
		done <- e
		<-ctx.Done()
	}()
	c, err := DialQUIC(ctx, listener.Addr().String(), TLSOptions{Config: credentials.Client}, QUICConfig{}, ClientConfig{Memory: MemoryConfig{ReceiveBytes: 1024}})
	require(t, err)
	defer c.Close()
	if c.closeBarrier() {
		t.Fatal("accepted wrong barrier response")
	}
	require(t, <-done)
	if m := c.MemoryStats().Receive; m.Rejected != 1 || m.Used != 0 || m.Peak != 0 {
		t.Fatal("barrier bypassed payload budget", m)
	}
	c.Close()
}

func TestNativeGovernanceQueueCancellationAndDeadlineDrain(t *testing.T) {
	started := make(chan int, 2)
	release := make(chan struct{}, 2)
	var calls atomic.Int64
	s, c, _ := startQUIC(t, ServerConfig{Governance: &GovernanceConfig{MaxExecuting: 1, QueueCapacity: 4, QueueTimeout: time.Second}}, QUICConfig{}, func(s *Server) {
		require(t, RegisterUnary(s, "/demo/hold", func(ctx context.Context, n int) (int, error) {
			calls.Add(1)
			started <- n
			select {
			case <-release:
				return n, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}))
	})
	done := make(chan error, 3)
	call := func(ctx context.Context, n int) {
		go func() { var out int; done <- c.Call(ctx, "/demo/hold", n, &out) }()
	}
	call(context.Background(), 0)
	if <-started != 0 {
		t.Fatal("unexpected first execution")
	}
	ctx, cancel := context.WithCancel(context.Background())
	call(ctx, 1)
	eventually(t, func() bool { return s.Stats().Queued == 1 })
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	eventually(t, func() bool { return s.Stats().Queued == 0 })
	call(context.Background(), 2)
	eventually(t, func() bool { return s.Stats().Queued == 1 })
	drained := make(chan error, 1)
	go func() {
		ctx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		drained <- s.Drain(ctx)
	}()
	eventually(t, func() bool { return !s.Ready() })
	release <- struct{}{}
	if <-started != 2 {
		t.Fatal("canceled queued handler invoked")
	}
	release <- struct{}{}
	require(t, <-done)
	require(t, <-done)
	require(t, <-drained)
	if calls.Load() != 2 || s.Stats().NativeStreams != 0 || s.MemoryStats().Receive.Used != 0 {
		t.Fatal(calls.Load(), s.Stats(), s.MemoryStats())
	}
}

func TestNativePressureAdmissionIsSharedAcrossExchanges(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	s, c, _ := startQUIC(t, ServerConfig{Governance: &GovernanceConfig{Feedback: true, FeedbackTTL: time.Second, Methods: []MethodPolicy{{Method: "/demo/hold", Min: 1, Initial: 1, Max: 1}}}}, QUICConfig{}, func(s *Server) {
		echo(s)
		require(t, RegisterUnary(s, "/demo/hold", func(ctx context.Context, n int) (int, error) {
			if n == 1 {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			}
			return n, nil
		}))
	}, ClientConfig{Feedback: &FeedbackConfig{}, Timeout: 2 * time.Second})
	var out int
	require(t, c.Call(context.Background(), "/demo/hold", 0, &out))
	if c.FeedbackStats().Accepted != 1 {
		t.Fatal(c.FeedbackStats())
	}
	done := make(chan error, 1)
	go func() { var out int; done <- c.Call(context.Background(), "/demo/hold", 1, &out) }()
	signal(t, started)
	err := c.Call(context.Background(), "/demo/hold", 2, &out)
	var status *Status
	if !errors.As(err, &status) || status.Code != ResourceExhausted || status.Message != "local pressure admission" {
		t.Fatal(err)
	}
	require(t, c.Call(context.Background(), "/demo/echo", 7, &out))
	if out != 7 || s.Stats().Admitted != 3 || s.Stats().Connections != 1 {
		t.Fatal(out, s.Stats())
	}
	close(release)
	require(t, <-done)
	for _, method := range c.FeedbackStats().Methods {
		if method.Active != 0 {
			t.Fatal(method)
		}
	}
	eventually(t, func() bool { return c.Stats().Active == 0 && s.Stats().NativeStreams == 0 })
}

func TestEscapedErrorAndPressureFitSmallTerminalFrame(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "tcp", true: "quic"}[native], func(t *testing.T) {
			register := func(s *Server) {
				echo(s)
				require(t, RegisterUnary(s, "/demo/error", func(context.Context, int) (int, error) {
					return 0, &Status{Code: InvalidArgument, Message: strings.Repeat("\x00", 256)}
				}))
			}
			sc := ServerConfig{IO: IOConfig{MaxFrameBytes: 1024}, Governance: &GovernanceConfig{Feedback: true}}
			cc := ClientConfig{IO: IOConfig{MaxFrameBytes: 1024}, Feedback: &FeedbackConfig{}}
			var caller Caller
			if native {
				_, c, _ := startQUIC(t, sc, QUICConfig{}, register, cc)
				caller = c
			} else {
				_, address := start(t, sc, register)
				caller = connect(t, address, cc)
			}
			var out int
			err := caller.Call(context.Background(), "/demo/error", 0, &out)
			var status *Status
			if !errors.As(err, &status) || status.Code != InvalidArgument {
				t.Fatal("oversized END lost structured error", err)
			}
			require(t, caller.Call(context.Background(), "/demo/echo", 7, &out))
			if out != 7 {
				t.Fatal(out)
			}
		})
	}
}
