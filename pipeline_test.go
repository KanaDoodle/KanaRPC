package kanarpc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

type retryFixture struct {
	calls, streams atomic.Int64
	call           func(context.Context, any) error
}

func (f *retryFixture) Call(ctx context.Context, _ string, _ any, out any) error {
	f.calls.Add(1)
	return f.call(ctx, out)
}
func (f *retryFixture) OpenStream(context.Context, string, any) (Receiver, error) {
	f.streams.Add(1)
	return nil, &Status{Code: Unavailable}
}
func (f *retryFixture) Close() {}
func TestPipelineExplicitRetryIsolationAndStreamNoReplay(t *testing.T) {
	type reply struct{ A, B int }
	f := new(retryFixture)
	f.call = func(ctx context.Context, out any) error {
		v := out.(*reply)
		if f.calls.Load() == 1 {
			v.B = 99
			return &Status{Code: Unavailable}
		}
		v.A = 42
		return nil
	}
	var ended atomic.Int64
	p, e := NewClientPipeline(f, PipelineConfig{Retry: RetryPolicy{MaxAttempts: 3, IdempotentMethods: []string{"/demo/safe"}}, Interceptors: []Interceptor{func(ctx context.Context, _ CallInfo) (context.Context, func(CallResult), error) {
		return ctx, func(r CallResult) {
			ended.Add(1)
			if r.Attempts != 2 && r.Streaming == false {
				t.Error(r.Attempts)
			}
		}, nil
	}}})
	require(t, e)
	defer p.Close()
	var out reply
	require(t, p.Call(context.Background(), "/demo/safe", nil, &out))
	if out.A != 42 || out.B != 0 || f.calls.Load() != 2 || ended.Load() != 1 {
		t.Fatal(out, f.calls.Load(), ended.Load())
	}
	_, e = p.OpenStream(context.Background(), "/demo/safe", nil)
	var status *Status
	if !errors.As(e, &status) || status.Code != Unavailable || f.streams.Load() != 1 {
		t.Fatal(e, f.streams.Load())
	}
}
func TestPipelineRetryDefaultsAndCancellationAreNeutral(t *testing.T) {
	f := &retryFixture{call: func(context.Context, any) error { return &Status{Code: Unavailable} }}
	p, e := NewClientPipeline(f, PipelineConfig{Retry: RetryPolicy{MaxAttempts: 3}})
	require(t, e)
	defer p.Close()
	if e = p.Call(context.Background(), "/demo/unsafe", nil, nil); e == nil || f.calls.Load() != 1 {
		t.Fatal(e, f.calls.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	f2 := &retryFixture{call: func(context.Context, any) error { cancel(); return &Status{Code: Unavailable} }}
	p2, e := NewClientPipeline(f2, PipelineConfig{Retry: RetryPolicy{MaxAttempts: 3, IdempotentMethods: []string{"/demo/safe"}}})
	require(t, e)
	defer p2.Close()
	if e = p2.Call(ctx, "/demo/safe", nil, nil); !errors.Is(e, context.Canceled) || f2.calls.Load() != 1 {
		t.Fatal(e, f2.calls.Load())
	}
	if retryable(&RateLimitError{}) || retryable(&BudgetError{}) {
		t.Fatal("local budget/rate refusal became retryable")
	}
}
func TestPipelineCompletionCanCloseOwner(t *testing.T) {
	f := &retryFixture{call: func(context.Context, any) error { return nil }}
	var p *ClientPipeline
	var e error
	p, e = NewClientPipeline(f, PipelineConfig{Interceptors: []Interceptor{func(ctx context.Context, _ CallInfo) (context.Context, func(CallResult), error) {
		return ctx, func(CallResult) { p.Close() }, nil
	}}})
	require(t, e)
	require(t, p.Call(context.Background(), "/demo/echo", nil, nil))
	require(t, p.Wait(context.Background()))
}

type panicStreamFixture struct{}

func (panicStreamFixture) Call(context.Context, string, any, any) error { return nil }
func (panicStreamFixture) OpenStream(context.Context, string, any) (Receiver, error) {
	panic("encoder fixture")
}
func TestPipelineStreamPanicReleasesAdmission(t *testing.T) {
	p, e := NewClientPipeline(panicStreamFixture{}, PipelineConfig{MaxInFlight: 1})
	require(t, e)
	defer p.Close()
	var err error
	func() {
		defer func() { _ = recover() }()
		_, err = p.OpenStream(context.Background(), "/demo/items", nil)
	}()
	if p.AdmissionStats().Active != 0 {
		t.Fatal("stream panic retained pipeline admission", p.AdmissionStats())
	}
	if StatusCode(err) != Internal {
		t.Fatal("stream panic was not contained", err)
	}
	require(t, p.Wait(context.Background()))
}

func TestPipelineRetriesConsumeRateBudget(t *testing.T) {
	quota, e := NewRateLimiter(map[string]RateSpec{"*": {PerSecond: .001, Burst: 1}})
	require(t, e)
	f := &retryFixture{call: func(context.Context, any) error { return &Status{Code: Unavailable} }}
	p, e := NewClientPipeline(f, PipelineConfig{RateLimiter: quota, Retry: RetryPolicy{MaxAttempts: 2, IdempotentMethods: []string{"/demo/safe"}}})
	require(t, e)
	defer p.Close()
	e = p.Call(context.Background(), "/demo/safe", nil, nil)
	var limited *RateLimitError
	if !errors.As(e, &limited) || f.calls.Load() != 1 {
		t.Fatal("retry bypassed attempt rate quota", e, f.calls.Load())
	}
}
