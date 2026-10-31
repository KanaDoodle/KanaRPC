package routing

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/internal/testcert"
)

func TestWeightedRoutingUpdateDoesNotReconnect(t *testing.T) {
	a, b := &fakeCaller{}, &fakeCaller{}
	var dials atomic.Int64
	c := managed(t, &source{snapshot: fresh(endpoint("a"), endpoint("b"))}, Config{Policy: WeightedRoundRobin, Weights: map[string]int{"a": 3, "b": 1}}, func(_ context.Context, e rpc.Endpoint, _ rpc.DialOptions) (rpc.Caller, error) {
		dials.Add(1)
		if e.ID == "a" {
			return a, nil
		}
		return b, nil
	})
	for i := 0; i < 40; i++ {
		check(t, invoke(c, context.Background()))
	}
	if a.calls.Load() != 30 || b.calls.Load() != 10 {
		t.Fatal(a.calls.Load(), b.calls.Load())
	}
	check(t, c.SetWeights(map[string]int{"a": 1, "b": 3}))
	for i := 0; i < 40; i++ {
		check(t, invoke(c, context.Background()))
	}
	if a.calls.Load() != 40 || b.calls.Load() != 40 || dials.Load() != 2 {
		t.Fatal(a.calls.Load(), b.calls.Load(), dials.Load())
	}
}
func TestCircuitRejectsAndAllowsOneRecoveryProbe(t *testing.T) {
	var recovering atomic.Bool
	started, release := make(chan struct{}), make(chan struct{})
	f := &fakeCaller{call: func(context.Context) error {
		if !recovering.Load() {
			return &rpc.Status{Code: rpc.Unavailable}
		}
		close(started)
		<-release
		return nil
	}}
	c := managed(t, &source{snapshot: fresh(endpoint("a"))}, Config{Circuit: &CircuitConfig{Window: 2, MinSamples: 2, FailureRatio: 1, OpenTimeout: 20 * time.Millisecond, CountUnavailable: true}}, fakeDial(f))
	for i := 0; i < 2; i++ {
		if e := invoke(c, context.Background()); e == nil {
			t.Fatal("missing failure")
		}
	}
	if e := invoke(c, context.Background()); !errors.Is(e, ErrCircuitOpen) || f.calls.Load() != 2 {
		t.Fatal(e, f.calls.Load())
	}
	recovering.Store(true)
	c.mu.Lock()
	c.routes[endpoint("a")].openUntil = time.Now().Add(-time.Second)
	c.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- invoke(c, context.Background()) }()
	receive(t, started)
	if e := invoke(c, context.Background()); !errors.Is(e, ErrCircuitOpen) || f.calls.Load() != 3 {
		t.Fatal("duplicate half-open probe", e, f.calls.Load())
	}
	close(release)
	check(t, receive(t, done))
	if c.RouteStates()[0].Open {
		t.Fatal(c.RouteStates())
	}
}
func TestCircuitLateGenerationAndLocalRefusalAreNeutral(t *testing.T) {
	c := managed(t, &source{snapshot: fresh(endpoint("a"))}, Config{Circuit: &CircuitConfig{Window: 1, MinSamples: 1, FailureRatio: 1, CountUnavailable: true}}, fakeDial(&fakeCaller{}))
	check(t, c.refresh(context.Background()))
	e := &entry{key: endpoint("a")}
	late, err := c.startRoute(e)
	check(t, err)
	bad, err := c.startRoute(e)
	check(t, err)
	bad(&rpc.Status{Code: rpc.Unavailable})
	c.mu.Lock()
	c.routes[e.key].openUntil = time.Now().Add(-time.Second)
	c.mu.Unlock()
	probe, err := c.startRoute(e)
	check(t, err)
	probe(nil)
	late(&rpc.Status{Code: rpc.Unavailable})
	if state := c.RouteStates()[0]; state.Open || state.Outstanding != 0 {
		t.Fatal(state)
	}
	neutral, err := c.startRoute(e)
	check(t, err)
	neutral(&rpc.RateLimitError{})
	neutral(context.Canceled)
	if state := c.RouteStates()[0]; state.Open || state.Outstanding != 0 || state.Samples != 0 {
		t.Fatal(state)
	}
}
func TestP2CIgnoresUnreadConnectionLeaseCounts(t *testing.T) {
	c := managed(t, &source{snapshot: fresh(endpoint("a"), endpoint("b"))}, Config{Policy: LeastOutstanding}, fakeDial(&fakeCaller{}))
	check(t, c.refresh(context.Background()))
	c.mu.Lock()
	c.routes[endpoint("b")].outstanding = 10
	for i := 0; i < 100; i++ {
		selected, e := c.pickLocked()
		if e != nil || selected.ID != "a" {
			c.mu.Unlock()
			t.Fatal(selected, e)
		}
	}
	c.routes[endpoint("b")].outstanding = 0
	c.mu.Unlock()
}
func TestRoutingWireCompletionReleasesLoadBeforeUnreadLease(t *testing.T) {
	cert, e := testcert.New()
	check(t, e)
	f := serve(t, rpc.TCP, cert, 0)
	c := managed(t, &source{snapshot: fresh(f.node)}, Config{Policy: LeastOutstanding, Dial: options(cert)}, rpc.DialEndpoint)
	stream, e := c.Stream(context.Background(), "/test/items", 3)
	check(t, e)
	defer stream.Close()
	until(t, func() bool {
		v := c.RouteStates()
		return f.s.Stats().Delivered == 1 && len(v) == 1 && v[0].Outstanding == 0 && c.MemoryStats().Unread.Chunks == 3
	})
	if c.Stats().ActiveCalls != 1 || c.ConnectionStats()[0].Users != 1 {
		t.Fatal("unread lease was incorrectly released", c.Stats(), c.ConnectionStats())
	}
	check(t, stream.Close())
	until(t, func() bool { return c.Stats().ActiveCalls == 0 && c.MemoryStats().Unread.Chunks == 0 })
}

func TestPipelineCloseWaitPreservesUnreadRetirement(t *testing.T) {
	cert, e := testcert.New()
	check(t, e)
	f := serve(t, rpc.TCP, cert, 0)
	pool := managed(t, &source{snapshot: fresh(f.node)}, Config{Policy: LeastOutstanding, Dial: options(cert)}, rpc.DialEndpoint)
	pipeline, e := rpc.NewClientPipeline(pool, rpc.PipelineConfig{})
	check(t, e)
	defer pipeline.Close()
	stream, e := pipeline.OpenStream(context.Background(), "/test/items", 3)
	check(t, e)
	defer stream.Close()
	select {
	case <-stream.(rpc.Completion).Done():
	case <-time.After(time.Second):
		t.Fatal("stream did not finish on wire")
	}
	pipeline.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if e = pipeline.Wait(ctx); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("owned unread lease disappeared from Wait", e)
	}
	check(t, stream.Close())
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	check(t, pipeline.Wait(ctx2))
	if pool.MemoryStats().Total.Used != 0 || pool.MemoryStats().Accounts != 0 {
		t.Fatal(pool.MemoryStats())
	}
}
