package kanarpc

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

func TestBlockedOpenAndACKRespectCallerBudget(t *testing.T) {
	for _, ack := range []bool{false, true} {
		t.Run(fmt.Sprint(ack), func(t *testing.T) {
			left, right := net.Pipe()
			defer right.Close()
			c, err := NewClient(left, ClientConfig{ControlTimeout: time.Second})
			require(t, err)
			defer c.Close()
			peerDone := make(chan struct{})
			if ack {
				go func() {
					defer close(peerDone)
					f, e := wire.Read(right, wire.HardLimit)
					if e != nil {
						return
					}
					end, _ := json.Marshal(endRecord{Status: Status{Code: OK}, Receipt: "00112233445566778899aabbccddeeff"})
					_ = wire.Write(right, wire.Frame{Kind: wire.End, ID: f.ID, Payload: end}, wire.HardLimit)
				}()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			begin := time.Now()
			var out int
			err = c.Call(ctx, "/demo/echo", 0, &out)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			if time.Since(begin) > 400*time.Millisecond {
				t.Fatal("caller budget ignored", time.Since(begin))
			}
			if ack {
				signal(t, peerDone)
			}
		})
	}
}

func TestIODeadlineRaceKeepsContextErrorIdentity(t *testing.T) {
	for i := 0; i < 60; i++ {
		left, right := net.Pipe()
		c, err := NewClient(left, ClientConfig{})
		require(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		var out int
		err = c.Call(ctx, "/demo/echo", 0, &out)
		cancel()
		c.Close()
		_ = right.Close()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("round %d lost context deadline identity: %T %v", i, err, err)
		}
	}
}

func TestCanceledWriterWaitDoesNotAllocateOrCloseConnection(t *testing.T) {
	_, address := start(t, ServerConfig{}, echo)
	c := connect(t, address, ClientConfig{})
	require(t, c.link.acquire(context.Background()))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	var out int
	err := c.Call(ctx, "/demo/echo", 1, &out)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if c.Stats().Started != 0 {
		t.Fatal("allocated before write admission", c.Stats())
	}
	c.link.release()
	require(t, c.Call(context.Background(), "/demo/echo", 9, &out))
	if out != 9 {
		t.Fatal(out)
	}
}

func TestCancelBetweenWriterPermitAndOpenDoesNotPoisonConnection(t *testing.T) {
	_, address := start(t, ServerConfig{}, echo)
	c := connect(t, address, ClientConfig{})
	for i := 0; i < 100; i++ {
		c.mu.Lock()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
		done := make(chan error, 1)
		go func() { var out int; done <- c.Call(ctx, "/demo/echo", 0, &out) }()
		// The Open writer holds its permit but cannot register under c.mu.
		until := time.Now().Add(time.Second)
		for len(c.link.permit) != 0 && ctx.Err() == nil && time.Now().Before(until) {
			time.Sleep(10 * time.Microsecond)
		}
		<-ctx.Done()
		c.mu.Unlock()
		if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
			cancel()
			t.Fatal(err)
		}
		cancel()
		eventually(t, func() bool { return c.Stats().Pending == 0 })
		var out int
		if err := c.Call(context.Background(), "/demo/echo", 7, &out); err != nil || out != 7 {
			t.Fatalf("round %d: canceled unsent Open poisoned connection: %d %v", i, out, err)
		}
	}
}

func TestClientCapacityAndPreCanceledRequestDoNotExecute(t *testing.T) {
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
	c := connect(t, address, ClientConfig{MaxInFlight: 1})
	done := make(chan error, 1)
	go func() { var out int; done <- c.Call(context.Background(), "/demo/hold", 1, &out) }()
	signal(t, started)
	var out int
	err := c.Call(context.Background(), "/demo/echo", 1, &out)
	var status *Status
	if !errors.As(err, &status) || status.Code != ResourceExhausted {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Call(ctx, "/demo/echo", 2, &out); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s.Stats().Admitted != 1 || c.Stats().Started != 1 {
		t.Fatal(s.Stats(), c.Stats())
	}
	close(release)
	require(t, <-done)
}

type slowRequestEncoder struct{}

// Models the observable window after Deadline passes but before a parent's
// Done cancellation has propagated to a derived request context.
type deadlineBeforeCancellation struct {
	context.Context
	deadline time.Time
}

func (c deadlineBeforeCancellation) Deadline() (time.Time, bool) {
	return c.deadline, true
}
func TestExpiredDeadlineDoesNotTouchConnectionBeforeCancellationPropagation(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	l := newLink(left, IOConfig{MaxFrameBytes: 1024, WriteTimeout: time.Second})
	ctx := deadlineBeforeCancellation{context.Background(), time.Now().Add(-time.Second)}
	if err := l.acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		if err == nil {
			l.release()
		}
		t.Error("expired deadline acquired writer", err)
	}
	require(t, l.acquire(context.Background()))
	defer l.release()
	touched, err := l.writeLocked(ctx, wire.Frame{Kind: wire.Open, ID: 1, Payload: []byte("{}")})
	if touched || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expired unsignaled deadline touched the connection", touched, err)
	}
}

func (slowRequestEncoder) MarshalJSON() ([]byte, error) {
	time.Sleep(20 * time.Millisecond)
	return []byte("1"), nil
}
func TestConfiguredBudgetIncludesRequestEncoding(t *testing.T) {
	s, address := start(t, ServerConfig{}, echo)
	c := connect(t, address, ClientConfig{Timeout: 5 * time.Millisecond})
	var out int
	if err := c.Call(context.Background(), "/demo/echo", slowRequestEncoder{}, &out); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("encoding time was excluded from request budget:", err)
	}
	if s.Stats().Admitted != 0 {
		t.Fatal("expired request reached handler", s.Stats())
	}
}

func TestMalformedPeerIsBoundedAndCancelsOwnedWork(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	s, address := start(t, ServerConfig{}, func(s *Server) {
		require(t, RegisterStream(s, "/demo/wait", func(ctx context.Context, _ int, _ func(int) error) error {
			close(started)
			defer close(stopped)
			<-ctx.Done()
			return ctx.Err()
		}))
	})
	conn, err := net.Dial("tcp", address)
	require(t, err)
	defer conn.Close()
	rawOpen(t, conn, 1, "/demo/wait", true, 1)
	signal(t, started)
	_, err = conn.Write([]byte("invalid record header!!!"))
	require(t, err)
	signal(t, stopped)
	eventually(t, func() bool { return s.Stats().InFlight == 0 && s.Stats().Connections == 0 })
}

func TestClientRejectsSequenceGapsAndUnknownRequest(t *testing.T) {
	for _, futureID := range []bool{false, true} {
		t.Run(fmt.Sprint(futureID), func(t *testing.T) {
			left, right := net.Pipe()
			defer right.Close()
			c, err := NewClient(left, ClientConfig{})
			require(t, err)
			defer c.Close()
			go func() {
				f, e := wire.Read(right, wire.HardLimit)
				if e != nil {
					return
				}
				id, n := f.ID, uint32(2)
				if futureID {
					id++
					n = 1
				}
				_ = wire.Write(right, wire.Frame{Kind: wire.Data, ID: id, Number: n, Payload: []byte("1")}, wire.HardLimit)
			}()
			var out int
			if err := c.Call(context.Background(), "/demo/echo", 1, &out); !errors.Is(err, ErrProtocol) {
				t.Fatal(err)
			}
		})
	}
}

func TestCloseAndDrainRaceWithRequestStarts(t *testing.T) {
	for round := 0; round < 15; round++ {
		s, address := start(t, ServerConfig{}, echo)
		c := connect(t, address, ClientConfig{})
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(i int) { defer wg.Done(); var out int; _ = c.Call(context.Background(), "/demo/echo", i, &out) }(i)
		}
		wg.Add(2)
		go func() { defer wg.Done(); c.Close() }()
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = s.Drain(ctx)
		}()
		wg.Wait()
		eventually(t, func() bool { return s.Stats().InFlight == 0 && s.Stats().Connections == 0 && c.Stats().Pending == 0 })
	}
}

func TestACKFollowedByImmediateDisconnectStillCountsDelivered(t *testing.T) {
	s, address := start(t, ServerConfig{}, echo)
	for i := 0; i < 100; i++ {
		conn, err := net.Dial("tcp", address)
		require(t, err)
		rawOpen(t, conn, 1, "/demo/echo", false, 1)
		frame, end := rawTerminal(t, conn)
		receipt, err := hex.DecodeString(end.Receipt)
		require(t, err)
		require(t, wire.Write(conn, wire.Frame{Kind: wire.Ack, ID: frame.ID, Payload: receipt}, wire.HardLimit))
		_ = conn.Close()
		eventually(t, func() bool { return s.Stats().InFlight == 0 && s.Stats().Connections == 0 })
	}
	if stats := s.Stats(); stats.Delivered != 100 || stats.LostTerminals != 0 {
		t.Fatal("ACK was received before disconnect but delivery accounting lost the race", stats)
	}
}

type resetGateListener struct {
	net.Listener
	entered, release chan struct{}
}

func (l resetGateListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &resetGateConn{Conn: c, entered: l.entered, release: l.release}, nil
}

type resetGateConn struct {
	net.Conn
	resets           atomic.Int32
	entered, release chan struct{}
	at               int32
}

func (c *resetGateConn) SetWriteDeadline(t time.Time) error {
	at := c.at
	if at == 0 {
		at = 2
	}
	if t.IsZero() && c.resets.Add(1) == at {
		close(c.entered)
		<-c.release
	}
	return c.Conn.SetWriteDeadline(t)
}

func TestProtocolFailureWinsSecondaryOpenWriteError(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	c, err := NewClient(&resetGateConn{Conn: left, entered: entered, release: release, at: 1}, ClientConfig{})
	require(t, err)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }); c.Close() })
	done := make(chan error, 1)
	go func() { var out int; done <- c.Call(context.Background(), "/demo/echo", 1, &out) }()
	f, err := wire.Read(right, wire.HardLimit)
	require(t, err)
	signal(t, entered)
	require(t, wire.Write(right, wire.Frame{Kind: wire.Data, ID: f.ID + 1, Number: 1, Payload: []byte("1")}, wire.HardLimit))
	eventually(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.closed && len(c.pending) == 0 })
	releaseOnce.Do(func() { close(release) })
	if err := <-done; !errors.Is(err, ErrProtocol) {
		t.Fatal("secondary socket error overturned a completed protocol failure", err)
	}
}

func TestRemoteRejectionWinsSecondaryOpenWriteError(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	c, err := NewClient(&resetGateConn{Conn: left, entered: entered, release: release, at: 1}, ClientConfig{})
	require(t, err)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }); c.Close() })
	done := make(chan error, 1)
	go func() { var out int; done <- c.Call(context.Background(), "/demo/echo", 1, &out) }()
	f, err := wire.Read(right, wire.HardLimit)
	require(t, err)
	signal(t, entered)
	end, _ := json.Marshal(endRecord{Status: Status{Code: NotFound}})
	require(t, wire.Write(right, wire.Frame{Kind: wire.End, ID: f.ID, Payload: end}, wire.HardLimit))
	eventually(t, func() bool { return c.Stats().Pending == 0 })
	_ = right.Close()
	releaseOnce.Do(func() { close(release) })
	var status *Status
	if err := <-done; !errors.As(err, &status) || status.Code != NotFound {
		t.Fatal("completed remote rejection was overwritten", err)
	}
}
func TestReceivedACKWinsDisconnectDuringWriterRetirement(t *testing.T) {
	s, err := NewServer(ServerConfig{})
	require(t, err)
	echo(s)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	done := make(chan error, 1)
	go func() { done <- s.Serve(resetGateListener{l, entered, release}) }()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Wait(ctx)
		<-done
	})
	eventually(t, s.Ready)
	conn, err := net.Dial("tcp", l.Addr().String())
	require(t, err)
	defer conn.Close()
	rawOpen(t, conn, 1, "/demo/echo", false, 1)
	frame, end := rawTerminal(t, conn)
	signal(t, entered)
	receipt, err := hex.DecodeString(end.Receipt)
	require(t, err)
	require(t, wire.Write(conn, wire.Frame{Kind: wire.Ack, ID: frame.ID, Payload: receipt}, wire.HardLimit))
	_ = conn.Close()
	eventually(t, func() bool { return s.Stats().Connections == 0 })
	releaseOnce.Do(func() { close(release) })
	eventually(t, func() bool { return s.Stats().InFlight == 0 })
	if stats := s.Stats(); stats.Delivered != 1 || stats.LostTerminals != 0 {
		t.Fatal("a received receipt was overturned by later connection retirement", stats)
	}
}

func BenchmarkUnaryLoopback(b *testing.B) {
	_, address := start(b, ServerConfig{}, echo)
	c := connect(b, address, ClientConfig{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out int
		if err := c.Call(context.Background(), "/demo/echo", i, &out); err != nil || out != i {
			b.Fatal(out, err)
		}
	}
}
func BenchmarkStreamingLoopback(b *testing.B) {
	_, address := start(b, ServerConfig{}, func(s *Server) {
		require(b, RegisterStream(s, "/demo/items", func(_ context.Context, n int, send func(int) error) error {
			for i := 0; i < n; i++ {
				if err := send(i); err != nil {
					return err
				}
			}
			return nil
		}))
	})
	c := connect(b, address, ClientConfig{StreamWindow: 8})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := c.Stream(context.Background(), "/demo/items", 16)
		require(b, err)
		for j := 0; j < 16; j++ {
			var out int
			require(b, stream.Recv(&out))
			if out != j {
				b.Fatal(out)
			}
		}
		if err := stream.Recv(nil); err != io.EOF {
			b.Fatal("invalid terminal", err)
		}
	}
}
