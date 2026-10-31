package kanarpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type waitingEncoder struct {
	entered chan struct{}
	release <-chan struct{}
	count   *atomic.Int64
}

type callbackEncoder func() error

func (f callbackEncoder) MarshalJSON() ([]byte, error) { return nil, f() }
func TestQUICParentCauseDoesNotReplaceWrappedCodecError(t *testing.T) {
	_, c, _ := startQUIC(t, ServerConfig{}, QUICConfig{}, echo)
	encoder := callbackEncoder(func() error {
		require(t, c.conn.CloseWithError(nativeCapacity, "fixture close inside codec"))
		<-c.conn.Context().Done()
		return ErrClosed
	})
	err := c.Call(context.Background(), "/demo/echo", encoder, new(int))
	var codec *json.MarshalerError
	if !errors.As(err, &codec) || !errors.Is(err, ErrClosed) {
		t.Fatal("codec failure replaced by unrelated terminal cause", err)
	}
}

func TestClosedQUICParentPreservesTerminalCauseBeforeEncoding(t *testing.T) {
	for _, capacity := range []bool{true, false} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			_, c, _ := startQUIC(t, ServerConfig{}, QUICConfig{}, echo)
			code, want := nativeCapacity, ResourceExhausted
			if !capacity {
				code, want = nativeShutdown, Unavailable
			}
			require(t, c.conn.CloseWithError(code, "fixture terminal cause"))
			<-c.conn.Context().Done()
			var count atomic.Int64
			var out int
			err := c.Call(context.Background(), "/demo/echo", countedEncoder{&count}, &out)
			var status *Status
			if !errors.As(err, &status) || status.Code != want || count.Load() != 0 {
				t.Fatal("terminal cause lost or encoder invoked", err, count.Load())
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := c.Call(ctx, "/demo/echo", 1, &out); !errors.Is(err, context.Canceled) {
				t.Fatal("user cancellation lost", err)
			}
		})
	}
}

func TestClosedQUICParentDoesNotInvokeEncoder(t *testing.T) {
	_, c, _ := startQUIC(t, ServerConfig{}, QUICConfig{}, echo)
	require(t, c.conn.CloseWithError(nativeShutdown, "fixture close"))
	<-c.conn.Context().Done()
	var count atomic.Int64
	var out int
	if err := c.Call(context.Background(), "/demo/echo", countedEncoder{&count}, &out); err == nil {
		t.Fatal("dead session accepted call")
	}
	if count.Load() != 0 {
		t.Fatal("known dead QUIC session encoded request", count.Load())
	}
}

func TestEncodingCapacityWaitCancellationAndCloseAccounting(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	c, err := NewClient(left, ClientConfig{MaxInFlight: 4, IO: IOConfig{MaxFrameBytes: 1024}, Memory: MemoryConfig{EncodingBytes: 2048}})
	require(t, err)
	defer c.Close()
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var count atomic.Int64
	done := make(chan error, 1)
	go func() {
		var out int
		done <- c.Call(context.Background(), "/demo/echo", waitingEncoder{entered, release, &count}, &out)
	}()
	signal(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	second := make(chan error, 1)
	go func() { var out int; second <- c.Call(ctx, "/demo/echo", countedEncoder{&count}, &out) }()
	eventually(t, func() bool { return c.AdmissionStats().EncodingWaiters == 1 })
	cancel()
	if err = <-second; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if count.Load() != 1 || c.MemoryStats().Encoding.Used != 2048 {
		t.Fatal(count.Load(), c.MemoryStats())
	}
	c.Close()
	if a := c.AdmissionStats(); !a.Closed || a.Active != 1 || a.Encoding != 1 {
		t.Fatal(a)
	}
	wait, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if err = c.WaitIdle(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("blocked user encoder was hidden", err)
	}
	once.Do(func() { close(release) })
	if err = <-done; !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	wait2, stop2 := context.WithTimeout(context.Background(), time.Second)
	defer stop2()
	require(t, c.WaitIdle(wait2))
	if a := c.AdmissionStats(); a.Active != 0 || a.Preparing != 0 || a.Encoding != 0 || a.EncodingWaiters != 0 {
		t.Fatal(a)
	}
	if m := c.MemoryStats().Encoding; m.Used != 0 || m.Peak != 2048 {
		t.Fatal(m)
	}
}

func TestPreparedBytesStayChargedDuringWriterWait(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	c, err := NewClient(left, ClientConfig{})
	require(t, err)
	defer c.Close()
	require(t, c.link.acquire(context.Background()))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var count atomic.Int64
	done := make(chan error, 1)
	go func() { var out int; done <- c.Call(ctx, "/demo/echo", countedEncoder{&count}, &out) }()
	eventually(t, func() bool {
		return count.Load() == 1 && c.MemoryStats().Encoding.Used > 0 && c.AdmissionStats().Encoding == 0
	})
	if c.Stats().Started != 0 || c.MemoryStats().Encoding.Used >= 2<<20 {
		t.Fatal(c.Stats(), c.MemoryStats())
	}
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	c.link.release()
	if c.MemoryStats().Encoding.Used != 0 || c.AdmissionStats().Active != 0 || c.nextID != 0 {
		t.Fatal(c.MemoryStats(), c.AdmissionStats(), c.nextID)
	}
}

func TestCloseWhileWaitingEncodingCapacityReturnsClosed(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	c, err := NewClient(left, ClientConfig{IO: IOConfig{MaxFrameBytes: 1024}, Memory: MemoryConfig{EncodingBytes: 2048}})
	require(t, err)
	defer c.Close()
	reserved, err := c.resources.encoding.lease(2048)
	require(t, err)
	defer reserved.release()
	var count atomic.Int64
	done := make(chan error, 1)
	go func() { var out int; done <- c.Call(context.Background(), "/demo/echo", countedEncoder{&count}, &out) }()
	eventually(t, func() bool { return c.AdmissionStats().EncodingWaiters == 1 })
	c.Close()
	err = <-done
	if !errors.Is(err, ErrClosed) || count.Load() != 0 {
		t.Fatal("owner close lost its identity while preparing", err, count.Load())
	}
}

type panicEncoder struct{}

func (panicEncoder) MarshalJSON() ([]byte, error) { panic("fixture encoder panic") }
func TestEncodingFailureOversizeAndPanicReleaseResources(t *testing.T) {
	s, address := start(t, ServerConfig{}, echo)
	c := connect(t, address, ClientConfig{IO: IOConfig{MaxFrameBytes: 1024}, Memory: MemoryConfig{EncodingBytes: 2048}})
	var out int
	for _, request := range []any{make(chan int), strings.Repeat("x", 8192)} {
		if err := c.Call(context.Background(), "/demo/echo", request, &out); err == nil {
			t.Fatal("invalid encoding accepted")
		}
		if c.AdmissionStats().Active != 0 || c.MemoryStats().Encoding.Used != 0 {
			t.Fatal(c.AdmissionStats(), c.MemoryStats())
		}
	}
	panicked := false
	func() {
		defer func() { panicked = recover() != nil }()
		_ = c.Call(context.Background(), "/demo/echo", panicEncoder{}, &out)
	}()
	if !panicked || c.AdmissionStats().Active != 0 || c.MemoryStats().Encoding.Used != 0 || s.Stats().Admitted != 0 {
		t.Fatal(panicked, c.AdmissionStats(), c.MemoryStats(), s.Stats())
	}
	require(t, c.Call(context.Background(), "/demo/echo", 7, &out))
	if out != 7 {
		t.Fatal(out)
	}
}

func TestQUICPreparationAdmissionPrecedesNativeStream(t *testing.T) {
	_, c, _ := startQUIC(t, ServerConfig{}, QUICConfig{}, echo, ClientConfig{MaxInFlight: 1})
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var count atomic.Int64
	done := make(chan error, 1)
	go func() {
		var out int
		done <- c.Call(context.Background(), "/demo/echo", waitingEncoder{entered, release, &count}, &out)
	}()
	signal(t, entered)
	var out int
	err := c.Call(context.Background(), "/demo/echo", countedEncoder{&count}, &out)
	var status *Status
	if !errors.As(err, &status) || status.Code != ResourceExhausted || count.Load() != 1 || c.Stats().Opened != 0 {
		t.Fatal(err, count.Load(), c.Stats())
	}
	c.Close()
	once.Do(func() { close(release) })
	if err = <-done; !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	require(t, c.WaitIdle(context.Background()))
	if c.AdmissionStats().Active != 0 || c.MemoryStats().Encoding.Used != 0 {
		t.Fatal(c.AdmissionStats(), c.MemoryStats())
	}
}

func FuzzPreparedOpen(f *testing.F) {
	f.Add("text", true)
	f.Add("<>&\x00界", false)
	f.Fuzz(func(t *testing.T, input string, streaming bool) {
		if len(input) > 2048 {
			return
		}
		cfg, err := normalizeClient(ClientConfig{IO: IOConfig{MaxFrameBytes: 1024}, Memory: MemoryConfig{EncodingBytes: 2048}})
		require(t, err)
		res, err := newClientResources(cfg)
		require(t, err)
		p, err := prepareRequest(context.Background(), context.Background(), cfg, res, "/demo/echo", input, streaming)
		if err == nil {
			body, e := p.payload()
			require(t, e)
			var record openRecord
			require(t, json.Unmarshal(body, &record))
			expected, _ := json.Marshal(input)
			if len(body) > 1024 || record.BudgetNanos <= 0 || record.Method != "/demo/echo" || record.Stream != streaming || !bytes.Equal(record.Request, expected) {
				t.Fatal(string(body))
			}
			p.done()
		}
		if res.admission.snapshot().Active != 0 || res.encoding.snapshot().Used != 0 || res.encoding.snapshot().Peak > 2048 {
			t.Fatal(res.admission.snapshot(), res.encoding.snapshot())
		}
	})
}

func (w waitingEncoder) MarshalJSON() ([]byte, error) {
	w.count.Add(1)
	w.entered <- struct{}{}
	<-w.release
	return []byte("1"), nil
}

type countedEncoder struct{ count *atomic.Int64 }

func (w countedEncoder) MarshalJSON() ([]byte, error) { w.count.Add(1); return []byte("1"), nil }

func TestClientAdmissionPrecedesRequestEncoding(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	c, err := NewClient(left, ClientConfig{MaxInFlight: 1, Timeout: 3 * time.Second})
	require(t, err)
	defer c.Close()
	entered, release := make(chan struct{}, 16), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var calls atomic.Int64
	done := make(chan error, 16)
	go func() {
		var out int
		done <- c.Call(context.Background(), "/demo/echo", waitingEncoder{entered, release, &calls}, &out)
	}()
	signal(t, entered)
	for i := 0; i < 15; i++ {
		go func() {
			var out int
			done <- c.Call(context.Background(), "/demo/echo", waitingEncoder{entered, release, &calls}, &out)
		}()
	}
	completed := 0
	deadline := time.NewTimer(200 * time.Millisecond)
	defer deadline.Stop()
	for completed < 15 {
		select {
		case err := <-done:
			var status *Status
			if !errors.As(err, &status) || status.Code != ResourceExhausted {
				t.Error("expected pre-encoding admission refusal", err)
			}
			completed++
		case <-deadline.C:
			goto settled
		}
	}
settled:
	count := calls.Load()
	c.Close()
	once.Do(func() { close(release) })
	for completed < 16 {
		<-done
		completed++
	}
	if count != 1 {
		t.Fatalf("MaxInFlight=1 allowed %d concurrent user encoders before admission", count)
	}
}

func TestClosedClientDoesNotInvokeUserEncoder(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	c, err := NewClient(left, ClientConfig{})
	require(t, err)
	c.Close()
	var calls atomic.Int64
	var out int
	if err = c.Call(context.Background(), "/demo/echo", countedEncoder{&calls}, &out); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("closed client invoked user encoder", calls.Load())
	}
}

func TestFeedbackRefusalPrecedesUserEncoding(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	s, address := start(t, ServerConfig{Governance: &GovernanceConfig{Feedback: true, Methods: []MethodPolicy{{Method: "/demo/echo", Min: 1, Initial: 1, Max: 1}}}}, func(s *Server) {
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
	var out int
	require(t, c.Call(context.Background(), "/demo/echo", 1, &out))
	done := make(chan error, 1)
	go func() { var out int; done <- c.Call(context.Background(), "/demo/echo", 0, &out) }()
	signal(t, started)
	var calls atomic.Int64
	err := c.Call(context.Background(), "/demo/echo", countedEncoder{&calls}, &out)
	close(release)
	require(t, <-done)
	var status *Status
	if !errors.As(err, &status) || status.Code != ResourceExhausted {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("feedback refusal happened after user encoding", calls.Load(), s.Stats())
	}
}
