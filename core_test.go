package kanarpc

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

func require(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func signal(t testing.TB, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for lifecycle signal")
	}
}
func eventually(t testing.TB, condition func() bool) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(until) {
			t.Fatal("resource state failed to converge")
		}
		time.Sleep(time.Millisecond)
	}
}
func start(t testing.TB, cfg ServerConfig, register func(*Server)) (*Server, string) {
	t.Helper()
	s, err := NewServer(cfg)
	require(t, err)
	register(s)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require(t, err)
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	eventually(t, s.Ready)
	t.Cleanup(func() {
		s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := s.Wait(ctx); err != nil {
			t.Error("server retained resources", s.Stats(), err)
		}
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return s, l.Addr().String()
}
func connect(t testing.TB, address string, cfg ClientConfig) *Client {
	t.Helper()
	c, err := Dial(context.Background(), address, cfg)
	require(t, err)
	t.Cleanup(c.Close)
	return c
}
func echo(s *Server) {
	_ = RegisterUnary(s, "/demo/echo", func(ctx context.Context, in int) (int, error) { return in, ctx.Err() })
}

func TestUnaryAndStreamShareMultiplexedConnection(t *testing.T) {
	s, address := start(t, ServerConfig{MaxPerConnection: 128}, func(s *Server) {
		echo(s)
		require(t, RegisterStream(s, "/demo/items", func(ctx context.Context, count int, send func(int) error) error {
			for i := 0; i < count; i++ {
				if err := send(i); err != nil {
					return err
				}
			}
			return nil
		}))
	})
	c := connect(t, address, ClientConfig{MaxInFlight: 128, StreamWindow: 2})
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var result int
			err := c.Call(context.Background(), "/demo/echo", i, &result)
			if err != nil || result != i {
				t.Errorf("call %d: %d %v", i, result, err)
			}
		}(i)
	}
	for k := 0; k < 4; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream, err := c.Stream(context.Background(), "/demo/items", 40)
			if err != nil {
				t.Error(err)
				return
			}
			defer stream.Close()
			for i := 0; i < 40; i++ {
				var out int
				if err := stream.Recv(&out); err != nil || out != i {
					t.Errorf("item %d: %d %v", i, out, err)
					return
				}
			}
			if err := stream.Recv(nil); err != io.EOF {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	eventually(t, func() bool { return s.Stats().InFlight == 0 && c.Stats().Pending == 0 })
	if s.Stats().Delivered != 68 || s.Stats().LostTerminals != 0 {
		t.Fatal(s.Stats())
	}
}

func TestDefaultUnaryCancellationAndDeadlineReleaseRemoteWork(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprint(deadline), func(t *testing.T) {
			started, stopped := make(chan struct{}), make(chan struct{})
			s, address := start(t, ServerConfig{MaxInFlight: 1}, func(s *Server) {
				echo(s)
				require(t, RegisterUnary(s, "/demo/wait", func(ctx context.Context, _ int) (int, error) {
					close(started)
					defer close(stopped)
					<-ctx.Done()
					return 0, ctx.Err()
				}))
			})
			c := connect(t, address, ClientConfig{})
			ctx, cancel := context.WithCancel(context.Background())
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 120*time.Millisecond)
			}
			defer cancel()
			done := make(chan error, 1)
			go func() { var out int; done <- c.Call(ctx, "/demo/wait", 0, &out) }()
			signal(t, started)
			if !deadline {
				cancel()
			}
			err := <-done
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			signal(t, stopped)
			eventually(t, func() bool { return s.Stats().InFlight == 0 && c.Stats().Pending == 0 })
			var out int
			require(t, c.Call(context.Background(), "/demo/echo", 27, &out))
			if out != 27 {
				t.Fatal(out)
			}
		})
	}
}

func TestBackpressureKeepsControlAndOtherRequestsLive(t *testing.T) {
	full, attempt, third, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	s, address := start(t, ServerConfig{}, func(s *Server) {
		echo(s)
		require(t, RegisterStream(s, "/demo/slow", func(ctx context.Context, _ int, send func(int) error) error {
			defer close(stopped)
			if err := send(0); err != nil {
				return err
			}
			if err := send(1); err != nil {
				return err
			}
			close(full)
			close(attempt)
			if err := send(2); err != nil {
				return err
			}
			close(third)
			<-ctx.Done()
			return ctx.Err()
		}))
	})
	c := connect(t, address, ClientConfig{StreamWindow: 2})
	stream, err := c.Stream(context.Background(), "/demo/slow", 0)
	require(t, err)
	defer stream.Close()
	signal(t, full)
	signal(t, attempt)
	select {
	case <-third:
		t.Fatal("sender exceeded receive credits")
	case <-time.After(30 * time.Millisecond):
	}
	eventually(t, func() bool { return c.Stats().InFlightBufferedChunks == 2 })
	var out int
	require(t, c.Call(context.Background(), "/demo/echo", 8, &out))
	if out != 8 {
		t.Fatal(out)
	}
	require(t, stream.Recv(&out))
	if out != 0 {
		t.Fatal(out)
	}
	signal(t, third)
	require(t, stream.Close())
	signal(t, stopped)
	if err := stream.Recv(&out); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	eventually(t, func() bool { return s.Stats().InFlight == 0 && c.Stats().Pending == 0 })
}

func TestAdmissionRejectsWithoutBlockingCancellation(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	s, address := start(t, ServerConfig{MaxInFlight: 1}, func(s *Server) {
		echo(s)
		require(t, RegisterStream(s, "/demo/hold", func(ctx context.Context, _ int, _ func(int) error) error {
			close(started)
			defer close(stopped)
			<-ctx.Done()
			return ctx.Err()
		}))
	})
	c := connect(t, address, ClientConfig{})
	stream, err := c.Stream(context.Background(), "/demo/hold", 0)
	require(t, err)
	signal(t, started)
	for i := 0; i < 30; i++ {
		var out int
		err := c.Call(context.Background(), "/demo/echo", i, &out)
		var status *Status
		if !errors.As(err, &status) || status.Code != ResourceExhausted {
			t.Fatal(err)
		}
	}
	require(t, stream.Close())
	signal(t, stopped)
	eventually(t, func() bool { return s.Stats().InFlight == 0 })
	if s.Stats().Admitted != 1 || s.Stats().Rejected != 30 {
		t.Fatal(s.Stats())
	}
}

func TestDrainSealsAdmissionAndDeliversExistingCall(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	s, address := start(t, ServerConfig{}, func(s *Server) {
		echo(s)
		require(t, RegisterUnary(s, "/demo/hold", func(ctx context.Context, n int) (int, error) {
			close(started)
			select {
			case <-release:
				return n, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}))
	})
	c := connect(t, address, ClientConfig{})
	result := make(chan error, 1)
	go func() {
		var out int
		err := c.Call(context.Background(), "/demo/hold", 42, &out)
		if err == nil && out != 42 {
			err = fmt.Errorf("lost response: %d", out)
		}
		result <- err
	}()
	signal(t, started)
	drain := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		drain <- s.Drain(ctx)
	}()
	eventually(t, func() bool { return !s.Ready() })
	var out int
	err := c.Call(context.Background(), "/demo/echo", 0, &out)
	var status *Status
	if !errors.As(err, &status) || status.Code != Unavailable {
		t.Fatal(err)
	}
	select {
	case err := <-drain:
		t.Fatal("drain returned before execution", err)
	default:
	}
	close(release)
	require(t, <-result)
	require(t, <-drain)
	if stats := s.Stats(); stats.Delivered != 1 || stats.InFlight != 0 || stats.Connections != 0 {
		t.Fatal(stats)
	}
}

func TestDrainDeadlineDoesNotPretendToKillUncooperativeHandler(t *testing.T) {
	started, release, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
	s, address := start(t, ServerConfig{}, func(s *Server) {
		require(t, RegisterUnary(s, "/demo/ignore", func(context.Context, int) (int, error) {
			close(started)
			defer close(stopped)
			<-release
			return 1, nil
		}))
	})
	c := connect(t, address, ClientConfig{})
	call := make(chan error, 1)
	go func() { var out int; call <- c.Call(context.Background(), "/demo/ignore", 0, &out) }()
	signal(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := s.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if s.Stats().Executing != 1 {
		t.Fatal("uncooperative work disappeared from accounting", s.Stats())
	}
	select {
	case <-stopped:
		t.Fatal("handler was claimed terminated")
	default:
	}
	if err := <-call; err == nil {
		t.Fatal("closed call succeeded")
	}
	close(release)
	signal(t, stopped)
	eventually(t, func() bool { return s.Stats().InFlight == 0 })
}

func rawOpen(t testing.TB, conn net.Conn, id uint64, path string, stream bool, window uint32) {
	t.Helper()
	body, err := json.Marshal(openRecord{Method: path, BudgetNanos: int64(time.Second), Window: window, Stream: stream, Request: json.RawMessage("0")})
	require(t, err)
	require(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
	require(t, wire.Write(conn, wire.Frame{Kind: wire.Open, ID: id, Payload: body}, wire.HardLimit))
}
func rawTerminal(t testing.TB, conn net.Conn) (wire.Frame, endRecord) {
	t.Helper()
	for {
		f, err := wire.Read(conn, wire.HardLimit)
		require(t, err)
		if f.Kind == wire.End {
			var end endRecord
			require(t, json.Unmarshal(f.Payload, &end))
			return f, end
		}
	}
}
func TestDrainWaitsForExactReceiptAndMissingReceiptIsVisible(t *testing.T) {
	for _, ack := range []bool{true, false} {
		t.Run(fmt.Sprint(ack), func(t *testing.T) {
			s, address := start(t, ServerConfig{AckTimeout: 100 * time.Millisecond}, echo)
			conn, err := net.Dial("tcp", address)
			require(t, err)
			defer conn.Close()
			rawOpen(t, conn, 1, "/demo/echo", false, 1)
			frame, end := rawTerminal(t, conn)
			if len(end.Receipt) != 32 {
				t.Fatal(end)
			}
			drain := make(chan error, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				drain <- s.Drain(ctx)
			}()
			eventually(t, func() bool { return !s.Ready() })
			select {
			case e := <-drain:
				t.Fatal("drain skipped receipt wait", e)
			default:
			}
			if ack {
				receipt, err := hex.DecodeString(end.Receipt)
				require(t, err)
				require(t, wire.Write(conn, wire.Frame{Kind: wire.Ack, ID: frame.ID, Payload: receipt}, wire.HardLimit))
			}
			err = <-drain
			if ack {
				require(t, err)
			} else if !errors.Is(err, ErrDelivery) {
				t.Fatal("missing ACK was hidden", err)
			}
		})
	}
}

func TestInvalidControlAndReusedIDsCloseConnection(t *testing.T) {
	for _, kind := range []string{"premature-ack", "wrong-receipt", "extra-credit", "reuse-id"} {
		t.Run(kind, func(t *testing.T) {
			started := make(chan struct{})
			s, address := start(t, ServerConfig{}, func(s *Server) {
				echo(s)
				require(t, RegisterStream(s, "/demo/wait", func(ctx context.Context, _ int, _ func(int) error) error {
					close(started)
					<-ctx.Done()
					return ctx.Err()
				}))
			})
			conn, err := net.Dial("tcp", address)
			require(t, err)
			defer conn.Close()
			switch kind {
			case "premature-ack", "extra-credit":
				rawOpen(t, conn, 1, "/demo/wait", true, 1)
				signal(t, started)
				f := wire.Frame{Kind: wire.Ack, ID: 1, Payload: make([]byte, 16)}
				if kind == "extra-credit" {
					f = wire.Frame{Kind: wire.Credit, ID: 1, Number: 1}
				}
				require(t, wire.Write(conn, f, wire.HardLimit))
			case "wrong-receipt":
				rawOpen(t, conn, 1, "/demo/echo", false, 1)
				_, _ = rawTerminal(t, conn)
				require(t, wire.Write(conn, wire.Frame{Kind: wire.Ack, ID: 1, Payload: make([]byte, 16)}, wire.HardLimit))
			case "reuse-id":
				rawOpen(t, conn, 1, "/demo/echo", false, 1)
				f, end := rawTerminal(t, conn)
				receipt, _ := hex.DecodeString(end.Receipt)
				require(t, wire.Write(conn, wire.Frame{Kind: wire.Ack, ID: f.ID, Payload: receipt}, wire.HardLimit))
				eventually(t, func() bool { return s.Stats().InFlight == 0 })
				rawOpen(t, conn, 1, "/demo/echo", false, 1)
			}
			if _, err := wire.Read(conn, wire.HardLimit); err == nil {
				t.Fatal("bad control accepted")
			}
			eventually(t, func() bool { return s.Stats().InFlight == 0 && s.Stats().Connections == 0 })
		})
	}
}

func TestErrorsAreTypedAndUnexpectedErrorsDoNotLeak(t *testing.T) {
	_, address := start(t, ServerConfig{}, func(s *Server) {
		require(t, RegisterUnary(s, "/demo/error", func(context.Context, int) (int, error) { return 0, errors.New("secret-api-key") }))
		require(t, RegisterUnary(s, "/demo/panic", func(context.Context, int) (int, error) { panic("secret-api-key") }))
		require(t, RegisterUnary(s, "/demo/status", func(context.Context, int) (int, error) {
			return 0, &Status{Code: ResourceExhausted, Message: "try later"}
		}))
	})
	c := connect(t, address, ClientConfig{})
	for path, code := range map[string]Code{"/demo/error": Internal, "/demo/panic": Internal, "/demo/status": ResourceExhausted, "/demo/missing": NotFound} {
		var out int
		err := c.Call(context.Background(), path, 0, &out)
		var status *Status
		if !errors.As(err, &status) || status.Code != code || strings.Contains(err.Error(), "secret-api-key") {
			t.Fatalf("%s: %v", path, err)
		}
	}
}

func TestHandlerCancellationStatusDoesNotDropReply(t *testing.T) {
	_, address := start(t, ServerConfig{}, func(s *Server) {
		require(t, RegisterUnary(s, "/demo/canceled", func(context.Context, int) (int, error) { return 0, context.Canceled }))
	})
	c := connect(t, address, ClientConfig{Timeout: 150 * time.Millisecond})
	var out int
	if err := c.Call(context.Background(), "/demo/canceled", 0, &out); !errors.Is(err, context.Canceled) {
		t.Fatal("handler cancellation status was dropped:", err)
	}
}

type hostileError struct{}

func (hostileError) Error() string { panic("error formatting must not leak or crash") }
func (hostileError) Is(error) bool { panic("application error classifier panicked") }
func TestErrorClassificationContainsTypedNilAndPanickingErrors(t *testing.T) {
	for _, err := range []error{(*Status)(nil), hostileError{}} {
		func() {
			defer func() {
				if value := recover(); value != nil {
					t.Errorf("error classification escaped panic: %v", value)
				}
			}()
			if got := statusOf(err); got.Code != Internal {
				t.Error(got)
			}
		}()
	}
}

func TestFinalInFlightBufferedChunksSurviveFastTerminal(t *testing.T) {
	s, address := start(t, ServerConfig{}, func(s *Server) {
		require(t, RegisterStream(s, "/demo/one", func(_ context.Context, n int, send func(int) error) error { return send(n) }))
	})
	c := connect(t, address, ClientConfig{})
	for i := 0; i < 200; i++ {
		stream, err := c.Stream(context.Background(), "/demo/one", i)
		require(t, err)
		if i%2 == 0 {
			eventually(t, func() bool { return c.Stats().Pending == 0 })
		}
		var out int
		require(t, stream.Recv(&out))
		if out != i {
			t.Fatal(i, out)
		}
		if err := stream.Recv(nil); err != io.EOF {
			t.Fatal(i, err)
		}
	}
	eventually(t, func() bool { return s.Stats().InFlight == 0 })
}

func TestEmptyStreamAndChunksBeforeRemoteError(t *testing.T) {
	_, address := start(t, ServerConfig{}, func(s *Server) {
		echo(s)
		require(t, RegisterStream(s, "/demo/empty", func(context.Context, int, func(int) error) error { return nil }))
		require(t, RegisterStream(s, "/demo/fails", func(_ context.Context, _ int, send func(int) error) error {
			if err := send(1); err != nil {
				return err
			}
			if err := send(2); err != nil {
				return err
			}
			return &Status{Code: ResourceExhausted, Message: "backend busy"}
		}))
	})
	c := connect(t, address, ClientConfig{StreamWindow: 2})
	empty, err := c.Stream(context.Background(), "/demo/empty", 0)
	require(t, err)
	if err := empty.Recv(nil); err != io.EOF {
		t.Fatal(err)
	}
	stream, err := c.Stream(context.Background(), "/demo/fails", 0)
	require(t, err)
	eventually(t, func() bool { return c.Stats().Pending == 0 })
	for i := 1; i <= 2; i++ {
		var n int
		require(t, stream.Recv(&n))
		if n != i {
			t.Fatal(n)
		}
	}
	var status *Status
	if err := stream.Recv(nil); !errors.As(err, &status) || status.Code != ResourceExhausted {
		t.Fatal(err)
	}
	var out int
	require(t, c.Call(context.Background(), "/demo/echo", 3, &out))
}

func TestServerExecutionBudgetWorksWithoutClientCancellation(t *testing.T) {
	stopped := make(chan struct{})
	s, address := start(t, ServerConfig{MaxRequestDuration: 25 * time.Millisecond}, func(s *Server) {
		require(t, RegisterUnary(s, "/demo/wait", func(ctx context.Context, _ int) (int, error) { defer close(stopped); <-ctx.Done(); return 0, ctx.Err() }))
	})
	c := connect(t, address, ClientConfig{Timeout: time.Second})
	var out int
	if err := c.Call(context.Background(), "/demo/wait", 0, &out); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	signal(t, stopped)
	eventually(t, func() bool { return s.Stats().InFlight == 0 })
}

func TestClientCloseStopsRemoteAndJoinsOwnedWorkers(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	s, address := start(t, ServerConfig{}, func(s *Server) {
		require(t, RegisterStream(s, "/demo/wait", func(ctx context.Context, _ int, _ func(int) error) error {
			close(started)
			defer close(stopped)
			<-ctx.Done()
			return ctx.Err()
		}))
	})
	c := connect(t, address, ClientConfig{})
	stream, err := c.Stream(context.Background(), "/demo/wait", 0)
	require(t, err)
	signal(t, started)
	c.Close()
	signal(t, stopped)
	if err := stream.Recv(nil); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if c.Stats().Pending != 0 {
		t.Fatal(c.Stats())
	}
	eventually(t, func() bool { return s.Stats().Connections == 0 && s.Stats().InFlight == 0 })
}

func TestRegistrationAndConfigurationValidation(t *testing.T) {
	s, err := NewServer(ServerConfig{})
	require(t, err)
	if err := s.HandleUnary("bad", func(context.Context, json.RawMessage) (json.RawMessage, error) { return nil, nil }); err == nil {
		t.Fatal("invalid path")
	}
	if err := RegisterUnary[int, int](s, "/demo/nil", nil); err == nil {
		t.Fatal("nil handler")
	}
	echo(s)
	if err := s.HandleStream("/demo/echo", func(context.Context, json.RawMessage, *Sender) error { return nil }); err == nil {
		t.Fatal("duplicate")
	}
	s.Close()
	if err := RegisterUnary(s, "/demo/late", func(context.Context, int) (int, error) { return 0, nil }); err == nil {
		t.Fatal("closed registration")
	}
	for _, cfg := range []ServerConfig{{MaxConnections: -1}, {MaxInFlight: -1}, {MaxPerConnection: 5000}, {AckTimeout: -1}, {IO: IOConfig{MaxFrameBytes: 1}}} {
		if _, err := NewServer(cfg); err == nil {
			t.Fatal(cfg)
		}
	}
	for _, cfg := range []ClientConfig{{Timeout: -1}, {MaxInFlight: -1}, {StreamWindow: 33}, {ControlTimeout: -1}, {IO: IOConfig{WriteTimeout: -1}}} {
		if _, err := normalizeClient(cfg); err == nil {
			t.Fatal(cfg)
		}
	}
}

func TestLateResponseDoesNotCompleteAnotherCall(t *testing.T) {
	left, right := net.Pipe()
	c, err := NewClient(left, ClientConfig{})
	require(t, err)
	defer c.Close()
	defer right.Close()
	peerDone := make(chan error, 1)
	go func() {
		f, e := wire.Read(right, wire.HardLimit)
		if e != nil {
			peerDone <- e
			return
		}
		old := f.ID
		f, e = wire.Read(right, wire.HardLimit)
		if e != nil || f.Kind != wire.Cancel {
			peerDone <- fmt.Errorf("cancel: %v %v", f, e)
			return
		}
		f, e = wire.Read(right, wire.HardLimit)
		if e != nil {
			peerDone <- e
			return
		}
		end, _ := json.Marshal(endRecord{Status: Status{Code: OK}})
		for _, out := range []wire.Frame{{Kind: wire.Data, ID: old, Number: 1, Payload: []byte("999")}, {Kind: wire.End, ID: old, Payload: end}, {Kind: wire.Data, ID: f.ID, Number: 1, Payload: []byte("7")}, {Kind: wire.End, ID: f.ID, Payload: end}} {
			if e = wire.Write(right, out, wire.HardLimit); e != nil {
				peerDone <- e
				return
			}
		}
		peerDone <- nil
		for {
			if _, e = wire.Read(right, wire.HardLimit); e != nil {
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var out int
	if err := c.Call(ctx, "/demo/echo", 0, &out); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	eventually(t, func() bool { return c.Stats().Pending == 0 })
	require(t, c.Call(context.Background(), "/demo/echo", 7, &out))
	if out != 7 || c.Stats().LateFrames != 2 {
		t.Fatal(out, c.Stats())
	}
	require(t, <-peerDone)
}
