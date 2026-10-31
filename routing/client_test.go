package routing

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/discovery"
)

type source struct {
	mu       sync.Mutex
	snapshot discovery.Snapshot
}

func (s *source) Snapshot() discovery.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.snapshot
	v.Endpoints = append([]rpc.Endpoint(nil), v.Endpoints...)
	return v
}
func (s *source) set(v discovery.Snapshot) { s.mu.Lock(); s.snapshot = v; s.mu.Unlock() }
func endpoint(id string) rpc.Endpoint {
	return rpc.Endpoint{ID: id, Address: "127.0.0.1:19091", Transport: rpc.TCP}
}
func fresh(nodes ...rpc.Endpoint) discovery.Snapshot {
	return discovery.Snapshot{Ready: true, Revision: 1, ClusterID: 1, Endpoints: nodes}
}

type fakeCaller struct {
	calls, closes atomic.Int64
	closed        atomic.Bool
	call          func(context.Context) error
	stream        func(context.Context) (*rpc.Stream, error)
}

func (f *fakeCaller) Call(ctx context.Context, _ string, _, _ any) error {
	f.calls.Add(1)
	if f.call != nil {
		return f.call(ctx)
	}
	return nil
}
func (f *fakeCaller) Stream(ctx context.Context, _ string, _ any) (*rpc.Stream, error) {
	if f.stream != nil {
		return f.stream(ctx)
	}
	return nil, errors.New("fixture stream unsupported")
}
func (f *fakeCaller) Close() { f.closed.Store(true); f.closes.Add(1) }
func (f *fakeCaller) AdmissionStats() rpc.AdmissionStats {
	return rpc.AdmissionStats{Closed: f.closed.Load()}
}
func check(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func until(t testing.TB, f func() bool) {
	t.Helper()
	end := time.Now().Add(5 * time.Second)
	for !f() {
		if time.Now().After(end) {
			t.Fatal("state did not converge")
		}
		time.Sleep(time.Millisecond)
	}
}
func receive[T any](t testing.TB, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("fixture signal timed out")
		var zero T
		return zero
	}
}
func managed(t testing.TB, s Source, cfg Config, dial dialFunc) *Client {
	t.Helper()
	if cfg.Transport == "" {
		cfg.Transport = rpc.TCP
	}
	c, err := newClient(context.Background(), s, cfg, dial)
	check(t, err)
	t.Cleanup(func() {
		c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		check(t, c.Wait(ctx))
	})
	return c
}
func fakeDial(f *fakeCaller) dialFunc {
	return func(context.Context, rpc.Endpoint, rpc.DialOptions) (rpc.Caller, error) { return f, nil }
}
func invoke(c *Client, ctx context.Context) error { return c.Call(ctx, "/test/echo", 1, new(int)) }

func TestStreamPanicReleasesManagedAdmission(t *testing.T) {
	f := &fakeCaller{stream: func(context.Context) (*rpc.Stream, error) { panic("user encoder") }}
	c, err := newClient(context.Background(), &source{snapshot: fresh(endpoint("a"))}, Config{Transport: rpc.TCP, MaxInFlight: 1}, fakeDial(f))
	check(t, err)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic swallowed")
			}
		}()
		_, _ = c.Stream(context.Background(), "/test/stream", 1)
	}()
	if c.Stats().ActiveCalls != 0 {
		t.Error("panic leaked managed admission", c.Stats())
	}
	c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := c.Wait(ctx); err != nil {
		t.Error("panic leaked endpoint reference", err, c.Stats())
	}
}

func TestCoalescedDialOneWaiterCancellation(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	f := &fakeCaller{}
	var dials atomic.Int64
	c := managed(t, &source{snapshot: fresh(endpoint("a"))}, Config{MaxInFlight: 40}, func(ctx context.Context, _ rpc.Endpoint, _ rpc.DialOptions) (rpc.Caller, error) {
		dials.Add(1)
		close(entered)
		select {
		case <-release:
			return f, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	canceled, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() { first <- invoke(c, canceled) }()
	receive(t, entered)
	results := make(chan error, 31)
	for i := 0; i < 31; i++ {
		go func() { results <- invoke(c, context.Background()) }()
	}
	until(t, func() bool { return c.Stats().Coalesced == 31 })
	cancel()
	if err := receive(t, first); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	for i := 0; i < 31; i++ {
		check(t, receive(t, results))
	}
	if dials.Load() != 1 || f.calls.Load() != 31 || c.Stats().PeakConnections != 1 {
		t.Fatal(dials.Load(), f.calls.Load(), c.Stats())
	}
}

func TestAllWaitersCancelAndLateDialIsDisposed(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	f := &fakeCaller{}
	c := managed(t, &source{snapshot: fresh(endpoint("a"))}, Config{MaxConnections: 1}, func(context.Context, rpc.Endpoint, rpc.DialOptions) (rpc.Caller, error) {
		close(entered)
		<-release
		return f, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- invoke(c, ctx) }()
	receive(t, entered)
	cancel()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if stats := c.Stats(); stats.Connections != 1 || stats.Dialing != 1 || stats.Retiring != 1 {
		t.Fatal("uncooperative dial vanished from bound", stats)
	}
	c.Close()
	short, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if err := c.Wait(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	until(t, func() bool { return c.Stats().Connections == 0 })
	if f.calls.Load() != 0 || f.closes.Load() != 1 {
		t.Fatal(f.calls.Load(), f.closes.Load())
	}
}

func TestSnapshotFencesAndRecovery(t *testing.T) {
	a := endpoint("a")
	s := &source{snapshot: fresh(a)}
	f := &fakeCaller{}
	c := managed(t, s, Config{MaxEndpoints: 2}, fakeDial(f))
	check(t, invoke(c, context.Background()))
	cases := []discovery.Snapshot{
		{Ready: false},
		{Ready: true, Revision: 0, ClusterID: 1, Endpoints: []rpc.Endpoint{a}},
		{Ready: true, Revision: 2, ClusterID: 2, Endpoints: []rpc.Endpoint{a}},
		{Ready: true, Revision: 1, ClusterID: 1, Endpoints: []rpc.Endpoint{endpoint("b")}},
		{Ready: true, Revision: 2, ClusterID: 1, Endpoints: []rpc.Endpoint{a, a}},
		{Ready: true, Revision: 2, ClusterID: 1, Endpoints: []rpc.Endpoint{a, endpoint("b"), endpoint("c")}},
	}
	for i, v := range cases {
		s.set(v)
		before := f.calls.Load()
		err := invoke(c, context.Background())
		var refusal *AdmissionError
		if !errors.As(err, &refusal) || f.calls.Load() != before {
			t.Fatal(i, err, f.calls.Load())
		}
		if c.Stats().Ready {
			t.Fatal(i, c.Stats())
		}
	}
	s.set(fresh(a))
	check(t, invoke(c, context.Background()))
	if c.Stats().Dials != 1 {
		t.Fatal("stale parking unnecessarily lost connection", c.Stats())
	}
	v := fresh(a)
	v.Revision = 3
	s.set(v)
	check(t, invoke(c, context.Background()))
	v.Revision = 2
	s.set(v)
	if err := invoke(c, context.Background()); !errors.Is(err, ErrSnapshot) {
		t.Fatal(err)
	}
}

func TestRetirementProtectsAdmittedCallAndABANeverRevives(t *testing.T) {
	a := endpoint("a")
	s := &source{snapshot: fresh(a)}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	old := &fakeCaller{call: func(context.Context) error { close(entered); <-release; return nil }}
	next := &fakeCaller{}
	var dials atomic.Int64
	c := managed(t, s, Config{MaxConnections: 2}, func(context.Context, rpc.Endpoint, rpc.DialOptions) (rpc.Caller, error) {
		if dials.Add(1) == 1 {
			return old, nil
		}
		return next, nil
	})
	done := make(chan error, 1)
	go func() { done <- invoke(c, context.Background()) }()
	receive(t, entered)
	v := fresh()
	v.Revision = 2
	s.set(v)
	check(t, c.refresh(context.Background()))
	if old.closes.Load() != 0 || c.Stats().Retiring != 1 {
		t.Fatal(c.Stats())
	}
	v = fresh(a)
	v.Revision = 3
	s.set(v)
	check(t, invoke(c, context.Background()))
	if dials.Load() != 2 || old.closes.Load() != 0 {
		t.Fatal(dials.Load(), old.closes.Load())
	}
	once.Do(func() { close(release) })
	check(t, receive(t, done))
	until(t, func() bool { return old.closes.Load() == 1 && c.Stats().Connections == 1 })
}

func TestCapacityWaitIncludesRetiringLeases(t *testing.T) {
	a, b := endpoint("a"), endpoint("b")
	s := &source{snapshot: fresh(a)}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	old := &fakeCaller{call: func(context.Context) error { close(entered); <-release; return nil }}
	c := managed(t, s, Config{MaxConnections: 1, MaxInFlight: 2}, func(_ context.Context, key rpc.Endpoint, _ rpc.DialOptions) (rpc.Caller, error) {
		if key.ID == "a" {
			return old, nil
		}
		return &fakeCaller{}, nil
	})
	done := make(chan error, 1)
	go func() { done <- invoke(c, context.Background()) }()
	receive(t, entered)
	v := fresh(b)
	v.Revision = 2
	s.set(v)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiter := make(chan error, 1)
	go func() { waiter <- invoke(c, ctx) }()
	until(t, func() bool { return c.Stats().ActiveCalls == 2 && c.Stats().Retiring == 1 })
	if err := invoke(c, context.Background()); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	cancel()
	if err := receive(t, waiter); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if c.Stats().PeakConnections != 1 {
		t.Fatal(c.Stats())
	}
	once.Do(func() { close(release) })
	check(t, receive(t, done))
	check(t, invoke(c, context.Background()))
}

func TestBackoffFutureCallsOnlyNoBusinessRetry(t *testing.T) {
	s := &source{snapshot: fresh(endpoint("a"))}
	var dials atomic.Int64
	dialErr := errors.New("fixture dial failure")
	f := &fakeCaller{call: func(context.Context) error {
		return &rpc.Status{Code: rpc.Unavailable, Message: "fixture business failure"}
	}}
	c := managed(t, s, Config{DialBackoff: 20 * time.Millisecond}, func(context.Context, rpc.Endpoint, rpc.DialOptions) (rpc.Caller, error) {
		if dials.Add(1) == 1 {
			return nil, dialErr
		}
		return f, nil
	})
	if err := invoke(c, context.Background()); !errors.Is(err, dialErr) {
		t.Fatal(err)
	}
	if err := invoke(c, context.Background()); !errors.Is(err, ErrBackoff) {
		t.Fatal(err)
	}
	if dials.Load() != 1 {
		t.Fatal(dials.Load())
	}
	time.Sleep(25 * time.Millisecond)
	for i := 0; i < 2; i++ {
		if err := invoke(c, context.Background()); err == nil {
			t.Fatal("business failure swallowed")
		}
	}
	if dials.Load() != 2 || f.calls.Load() != 2 {
		t.Fatal("implicit retry", dials.Load(), f.calls.Load())
	}
	f.closed.Store(true)
	if err := invoke(c, context.Background()); err == nil {
		t.Fatal("fixture failure swallowed")
	}
	if dials.Load() != 3 {
		t.Fatal("known dead connection reused", c.Stats())
	}
}

func TestCloseCancelsCallAndWaitJoins(t *testing.T) {
	entered := make(chan struct{})
	f := &fakeCaller{call: func(ctx context.Context) error { close(entered); <-ctx.Done(); return ctx.Err() }}
	c := managed(t, &source{snapshot: fresh(endpoint("a"))}, Config{}, fakeDial(f))
	done := make(chan error, 1)
	go func() { done <- invoke(c, context.Background()) }()
	receive(t, entered)
	c.Close()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	check(t, c.Wait(ctx))
	if f.closes.Load() != 1 || c.Stats().Connections != 0 || c.Stats().ActiveCalls != 0 {
		t.Fatal(c.Stats())
	}
	if err := invoke(c, context.Background()); !errors.Is(err, rpc.ErrClosed) {
		t.Fatal(err)
	}
}

func TestConfigurationFreezesFeedbackForLaterDials(t *testing.T) {
	feedback := &rpc.FeedbackConfig{MaxMethods: 7}
	s := &source{snapshot: fresh(endpoint("a"))}
	seen := make(chan int, 1)
	c := managed(t, s, Config{Dial: rpc.DialOptions{Client: rpc.ClientConfig{Feedback: feedback}}}, func(_ context.Context, _ rpc.Endpoint, options rpc.DialOptions) (rpc.Caller, error) {
		seen <- options.Client.Feedback.MaxMethods
		return &fakeCaller{}, nil
	})
	feedback.MaxMethods = 999
	check(t, invoke(c, context.Background()))
	if n := receive(t, seen); n != 7 {
		t.Fatal("constructor retained mutable feedback configuration", n)
	}
}

type panicSource struct{}

func (panicSource) Snapshot() discovery.Snapshot { panic("fixture source") }
func TestInvalidSourceAndConstructorBounds(t *testing.T) {
	c := managed(t, panicSource{}, Config{}, fakeDial(&fakeCaller{}))
	if err := invoke(c, context.Background()); !errors.Is(err, ErrSnapshot) || c.Stats().Dials != 0 {
		t.Fatal(err, c.Stats())
	}
	for _, cfg := range []Config{{}, {Transport: rpc.TLS}, {Transport: rpc.QUIC}, {Transport: rpc.TCP, MaxConnections: -1}, {Transport: rpc.TCP, MaxInFlight: 4097}, {Transport: rpc.TCP, MaxEndpoints: 4097}, {Transport: rpc.TCP, DialTimeout: -1}, {Transport: rpc.TCP, DialBackoff: -1}, {Transport: rpc.TCP, PollInterval: time.Nanosecond}, {Transport: rpc.TCP, Dial: rpc.DialOptions{Client: rpc.ClientConfig{Timeout: -1}}}} {
		if c, err := New(context.Background(), &source{snapshot: fresh(endpoint("a"))}, cfg); err == nil {
			c.Close()
			t.Fatal("invalid config accepted", cfg)
		}
	}
}

type blockingSource struct {
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (s *blockingSource) Snapshot() discovery.Snapshot {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return fresh(endpoint("a"))
}
func TestSourceContractViolationRemainsVisibleToWait(t *testing.T) {
	release, entered := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	c := managed(t, &blockingSource{entered: entered, release: release}, Config{}, fakeDial(&fakeCaller{}))
	receive(t, entered)
	c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := c.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("blocked provider hidden", err)
	}
	once.Do(func() { close(release) })
}

type stableSource struct{ v discovery.Snapshot }

func (s stableSource) Snapshot() discovery.Snapshot { return s.v }
func TestOversizedSnapshotRejectsBeforeBuildingEndpointIndexes(t *testing.T) {
	// Source already owns this input. Manager-side allocations must not scale
	// with endpoints after the explicit MaxEndpoints fence has been exceeded.
	allocations := func(n int) float64 {
		nodes := make([]rpc.Endpoint, n)
		for i := range nodes {
			nodes[i] = endpoint(strconv.Itoa(i))
		}
		c := &Client{source: stableSource{fresh(nodes...)}, cfg: Config{Transport: rpc.TCP, MaxEndpoints: 1}, ctx: context.Background(), view: make(chan struct{}, 1), changed: make(chan struct{}), desired: map[rpc.Endpoint]struct{}{}}
		c.view <- struct{}{}
		return testing.AllocsPerRun(5, func() {
			if err := c.refresh(context.Background()); !errors.Is(err, ErrSnapshot) {
				t.Fatal(err)
			}
		})
	}
	small, large := allocations(2), allocations(10000)
	if large > small+8 {
		t.Fatalf("oversized snapshot built indexes after capacity rejection: small=%g large=%g", small, large)
	}
}

type slowCloseCaller struct {
	*fakeCaller
	release <-chan struct{}
}

func (f *slowCloseCaller) Close() { <-f.release; f.fakeCaller.Close() }
func TestClosingConnectionStillConsumesCapacity(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	a, b := endpoint("a"), endpoint("b")
	s := &source{snapshot: fresh(a)}
	old := &slowCloseCaller{&fakeCaller{}, release}
	c := managed(t, s, Config{MaxConnections: 1}, func(_ context.Context, key rpc.Endpoint, _ rpc.DialOptions) (rpc.Caller, error) {
		if key == a {
			return old, nil
		}
		return &fakeCaller{}, nil
	})
	check(t, invoke(c, context.Background()))
	v := fresh(b)
	v.Revision = 2
	s.set(v)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := invoke(c, ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if stats := c.Stats(); stats.Connections != 1 || stats.Closing != 1 || stats.Dials != 1 {
		t.Fatal("closing object escaped capacity accounting", stats)
	}
	once.Do(func() { close(release) })
	check(t, invoke(c, context.Background()))
	if stats := c.Stats(); stats.PeakConnections != 1 || stats.Dials != 2 {
		t.Fatal(stats)
	}
}

func TestFailedDialDoesNotCloseTypedNilResult(t *testing.T) {
	var nilClient *rpc.QUICClient
	dialErr := errors.New("fixture failed typed-nil dial")
	c := managed(t, &source{snapshot: fresh(endpoint("a"))}, Config{}, func(context.Context, rpc.Endpoint, rpc.DialOptions) (rpc.Caller, error) { return nilClient, dialErr })
	if err := invoke(c, context.Background()); !errors.Is(err, dialErr) {
		t.Fatal(err)
	}
	c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	check(t, c.Wait(ctx))
}
