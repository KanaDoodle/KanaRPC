package kanarpc

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"reflect"
	"sync"
	"time"
)

type RetryPolicy struct {
	MaxAttempts       int
	Backoff           time.Duration
	IdempotentMethods []string
}
type PipelineConfig struct {
	Timeout      time.Duration
	MaxInFlight  int
	RateLimiter  *RateLimiter
	Interceptors []Interceptor
	Retry        RetryPolicy
}

// ClientPipeline is the single production call entry for direct clients and
// routing.Client alike; generated SDKs consume this same ServiceCaller.
type ClientPipeline struct {
	inner  ServiceCaller
	cfg    PipelineConfig
	ctx    context.Context
	cancel context.CancelFunc
	gate   *clientAdmission
	safe   map[string]bool
	once   sync.Once
}

func NewClientPipeline(inner ServiceCaller, cfg PipelineConfig) (*ClientPipeline, error) {
	if inner == nil || reflect.ValueOf(inner).Kind() == reflect.Ptr && reflect.ValueOf(inner).IsNil() {
		return nil, errors.New("kanarpc: caller required")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.MaxInFlight == 0 {
		cfg.MaxInFlight = 128
	}
	if cfg.Timeout <= 0 || cfg.MaxInFlight < 1 || cfg.MaxInFlight > 4096 {
		return nil, errors.New("kanarpc: invalid pipeline bounds")
	}
	if cfg.Retry.MaxAttempts == 0 {
		cfg.Retry.MaxAttempts = 1
	}
	if cfg.Retry.Backoff == 0 {
		cfg.Retry.Backoff = 25 * time.Millisecond
	}
	if cfg.Retry.MaxAttempts < 1 || cfg.Retry.MaxAttempts > 5 || cfg.Retry.Backoff < time.Millisecond || cfg.Retry.Backoff > time.Second || len(cfg.Retry.IdempotentMethods) > 1024 {
		return nil, errors.New("kanarpc: invalid retry policy")
	}
	safe := map[string]bool{}
	for _, m := range cfg.Retry.IdempotentMethods {
		if !validMethod(m) {
			return nil, errors.New("kanarpc: invalid idempotent method")
		}
		safe[m] = true
	}
	cfg.Interceptors = append([]Interceptor(nil), cfg.Interceptors...)
	cfg.Retry.IdempotentMethods = append([]string(nil), cfg.Retry.IdempotentMethods...)
	ctx, cancel := context.WithCancel(context.Background())
	return &ClientPipeline{inner: inner, cfg: cfg, ctx: ctx, cancel: cancel, gate: newAdmission(cfg.MaxInFlight), safe: safe}, nil
}
func (p *ClientPipeline) begin(ctx context.Context, method string, stream bool) (context.Context, func(error, int), error) {
	if !validMethod(method) {
		return nil, nil, &Status{Code: InvalidArgument, Message: "invalid method"}
	}
	if e := deadlineError(ctx); e != nil {
		return nil, nil, e
	}
	lease, e := p.gate.enter()
	if e != nil {
		return nil, nil, e
	}
	lease.prepared()
	ctx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	stop := context.AfterFunc(p.ctx, cancel)
	info := CallInfo{Method: method, Side: "client", Streaming: stream, Started: time.Now()}
	ctx, finish, e := beginInterceptors(ctx, info, p.cfg.Interceptors)
	var once sync.Once
	done := func(err error, attempts int) {
		once.Do(func() { defer lease.release(); defer cancel(); defer stop(); finish(err, attempts) })
	}
	if e != nil {
		done(e, 0)
		return nil, nil, e
	}
	return ctx, done, nil
}
func retryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrDelivery) || errors.Is(err, ErrProtocol) || errors.Is(err, ErrCapabilities) {
		return false
	}
	var b *BudgetError
	var q *RateLimitError
	if errors.As(err, &b) || errors.As(err, &q) {
		return false
	}
	var status *Status
	return errors.As(err, &status) && status.Code == Unavailable
}
func (p *ClientPipeline) Call(ctx context.Context, method string, request, response any) (err error) {
	ctx, done, err := p.begin(ctx, method, false)
	if err != nil {
		return err
	}
	attempts := 0
	defer func() {
		if recover() != nil {
			err = &Status{Code: Internal, Message: "caller panicked"}
		}
		done(err, attempts)
	}()
	max := 1
	if p.safe[method] {
		max = p.cfg.Retry.MaxAttempts
	}
	var target reflect.Value
	if max > 1 && response != nil {
		target = reflect.ValueOf(response)
		if target.Kind() != reflect.Ptr || target.IsNil() {
			return &Status{Code: InvalidArgument, Message: "retry response must be a non-nil pointer"}
		}
	}
	for attempts < max {
		if e := deadlineError(ctx); e != nil {
			return e
		}
		if e := p.cfg.RateLimiter.Allow(method); e != nil {
			return e
		}
		attempts++
		destination := response
		var scratch reflect.Value
		if target.IsValid() {
			scratch = reflect.New(target.Elem().Type())
			destination = scratch.Interface()
		}
		err = p.inner.Call(ctx, method, request, destination)
		if err == nil {
			if target.IsValid() {
				target.Elem().Set(scratch.Elem())
			}
			return nil
		}
		if attempts == max || !retryable(err) {
			return err
		}
		delay := p.cfg.Retry.Backoff * time.Duration(1<<uint(attempts-1))
		if delay > time.Second {
			delay = time.Second
		}
		timer := time.NewTimer(time.Duration(rand.Int64N(int64(delay) + 1)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}
func (p *ClientPipeline) OpenStream(ctx context.Context, method string, request any) (result Receiver, err error) {
	ctx, done, e := p.begin(ctx, method, true)
	if e != nil {
		return nil, e
	}
	watching := false
	defer func() {
		if recover() != nil {
			err = &Status{Code: Internal, Message: "stream caller panicked"}
		}
		if !watching {
			done(err, 1)
		}
	}()
	if e = p.cfg.RateLimiter.Allow(method); e != nil {
		done(e, 0)
		return nil, e
	}
	stream, e := p.inner.OpenStream(ctx, method, request)
	if e != nil {
		done(e, 1)
		return nil, e
	}
	if stream == nil || reflect.ValueOf(stream).Kind() == reflect.Ptr && reflect.ValueOf(stream).IsNil() {
		return nil, &Status{Code: Internal, Message: "nil stream"}
	}
	complete, ok := stream.(Completion)
	if !ok {
		_ = stream.Close()
		e = &Status{Code: FailedPrecondition, Message: "stream caller must expose Completion"}
		done(e, 1)
		return nil, e
	}
	// At most MaxInFlight watchers exist, including cooperative finish callbacks.
	// Completion is independent of consuming/closing retained unread buffers.
	watching = true
	go func() {
		defer func() {
			if recover() != nil {
				done(&Status{Code: Internal, Message: "completion callback panicked"}, 1)
			}
		}()
		select {
		case <-complete.Done():
			done(complete.Outcome(), 1)
		case <-ctx.Done():
			select {
			case <-complete.Done():
				done(complete.Outcome(), 1)
			default:
				done(ctx.Err(), 1)
			}
		}
	}()
	return stream, nil
}
func (p *ClientPipeline) Close() {
	p.once.Do(func() {
		p.gate.close()
		p.cancel()
		if c, ok := p.inner.(interface{ Close() }); ok {
			c.Close()
		}
	})
}

// Wait joins protocol completions/interceptors. After Close it also joins an
// owned routing caller's Wait, including application-held stream leases.
func (p *ClientPipeline) Wait(ctx context.Context) error {
	if e := p.gate.wait(ctx); e != nil {
		return e
	}
	if p.ctx.Err() != nil {
		if owner, ok := p.inner.(interface{ Wait(context.Context) error }); ok {
			return owner.Wait(ctx)
		}
	}
	return nil
}
func (p *ClientPipeline) AdmissionStats() AdmissionStats { return p.gate.snapshot() }

// DrainReceiver consumes a stream to its protocol terminal without decoding.
func DrainReceiver(s Receiver) error {
	defer s.Close()
	for {
		err := s.Recv(nil)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
