package kanarpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type CallInfo struct {
	Method, Side string
	Streaming    bool
	Started      time.Time
	QueueWait    time.Duration
}
type CallResult struct {
	CallInfo
	Err      error
	Duration time.Duration
	Attempts int
}

// Interceptor enters once, in registration order. Finish runs once in reverse
// order, with no framework mutex held. Server finishes measure handler lifecycle;
// client stream finishes observe protocol completion, not user consumption.
// Hooks are cooperative user code and must return; panics become Internal.
type Interceptor func(context.Context, CallInfo) (context.Context, func(CallResult), error)

func beginInterceptors(ctx context.Context, info CallInfo, hooks []Interceptor) (context.Context, func(error, int), error) {
	finishes := make([]func(CallResult), 0, len(hooks))
	var once sync.Once
	finish := func(err error, attempts int) {
		once.Do(func() {
			v := CallResult{CallInfo: info, Err: err, Duration: time.Since(info.Started), Attempts: attempts}
			for i := len(finishes) - 1; i >= 0; i-- {
				func() { defer func() { _ = recover() }(); finishes[i](v) }()
			}
		})
	}
	for _, hook := range hooks {
		if hook == nil {
			continue
		}
		var end func(CallResult)
		var err error
		func() {
			defer func() {
				if recover() != nil {
					err = &Status{Code: Internal, Message: "interceptor panicked"}
				}
			}()
			ctx, end, err = hook(ctx, info)
		}()
		if end != nil {
			finishes = append(finishes, end)
		}
		if err != nil {
			finish(err, 0)
			return ctx, finish, err
		}
		if ctx == nil {
			err = errors.New("kanarpc: interceptor returned nil context")
			finish(err, 0)
			return nil, finish, err
		}
	}
	return ctx, finish, nil
}
func LoggingInterceptor(logger *slog.Logger) Interceptor {
	return func(ctx context.Context, info CallInfo) (context.Context, func(CallResult), error) {
		return ctx, func(r CallResult) {
			if logger != nil {
				logger.InfoContext(ctx, "rpc completed", "method", info.Method, "side", info.Side, "stream", info.Streaming, "code", statusOf(r.Err).Code, "duration", r.Duration, "attempts", r.Attempts, "queue_wait", info.QueueWait)
			}
		}, nil
	}
}

type RateSpec struct {
	PerSecond float64
	Burst     int
}
type rateBucket struct {
	spec   RateSpec
	tokens float64
	at     time.Time
}
type RateLimiter struct {
	mu       sync.Mutex
	buckets  map[string]*rateBucket
	rejected uint64
}
type RateStats struct {
	Rejected uint64
	Policies int
}
type RateLimitError struct{ Method string }

func (e *RateLimitError) Error() string { return "kanarpc: request rate exceeded" }
func (e *RateLimitError) Unwrap() error { return &Status{Code: ResourceExhausted, Message: e.Error()} }

// NewRateLimiter compiles a bounded static policy table. "*" is a shared total
// bucket, not a dynamically growing per-method or per-tenant cache.
func NewRateLimiter(policies map[string]RateSpec) (*RateLimiter, error) {
	if len(policies) > 1025 {
		return nil, errors.New("kanarpc: too many rate policies")
	}
	r := &RateLimiter{buckets: make(map[string]*rateBucket, len(policies))}
	now := time.Now()
	for method, s := range policies {
		if method != "*" && !validMethod(method) || s.PerSecond <= 0 || s.PerSecond > 1e9 || s.PerSecond != s.PerSecond || s.Burst < 1 || s.Burst > 1e7 {
			return nil, errors.New("kanarpc: invalid rate policy")
		}
		r.buckets[method] = &rateBucket{spec: s, tokens: float64(s.Burst), at: now}
	}
	return r, nil
}
func (r *RateLimiter) Allow(method string) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	bs := []*rateBucket{r.buckets["*"], r.buckets[method]}
	for _, b := range bs {
		if b == nil {
			continue
		}
		b.tokens += now.Sub(b.at).Seconds() * b.spec.PerSecond
		if b.tokens > float64(b.spec.Burst) {
			b.tokens = float64(b.spec.Burst)
		}
		b.at = now
		if b.tokens < 1 {
			r.rejected++
			return &RateLimitError{Method: method}
		}
	}
	for _, b := range bs {
		if b != nil {
			b.tokens--
		}
	}
	return nil
}
func (r *RateLimiter) Stats() RateStats {
	if r == nil {
		return RateStats{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return RateStats{r.rejected, len(r.buckets)}
}
func authFailure(err error) error {
	var status *Status
	if errors.As(err, &status) && (status.Code == PermissionDenied || status.Code == Unauthenticated) {
		return err
	}
	return &Status{Code: Unauthenticated, Message: "authentication failed"}
}
func safeAuthenticate(fn Authenticator, ctx context.Context, info CallInfo) (identity Identity, err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("authentication callback panicked")
		}
	}()
	return fn(ctx, info)
}

// Credentials supplies outgoing call metadata before serialization. Providers
// must honor ctx, and bearer credentials should use authenticated TLS/QUIC.
func Credentials(provider func(context.Context) (Metadata, error)) Interceptor {
	return func(ctx context.Context, _ CallInfo) (context.Context, func(CallResult), error) {
		if provider == nil {
			return ctx, nil, errors.New("kanarpc: credential provider required")
		}
		md, e := provider(ctx)
		if e != nil {
			return ctx, nil, e
		}
		merged := OutgoingMetadata(ctx)
		if merged == nil {
			merged = Metadata{}
		}
		for k, v := range md {
			merged[k] = v
		}
		if e = validateMetadata(merged); e != nil {
			return ctx, nil, &Status{Code: InvalidArgument, Message: "invalid credentials metadata"}
		}
		return WithOutgoingMetadata(ctx, merged), nil, nil
	}
}
