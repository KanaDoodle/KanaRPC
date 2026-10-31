package kanarpc

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"
)

// AdmissionStats includes accepted preparation, wire calls and owned cleanup.
// Finished unread stream buffers are accounted separately by MemoryStats.
type AdmissionStats struct {
	Limit, Active, Preparing, Encoding, Peak int
	EncodingWaiters                          int
	Rejected                                 uint64
	Closed                                   bool
}
type clientAdmission struct {
	mu      sync.Mutex
	stats   AdmissionStats
	changed chan struct{}
}
type callLease struct {
	gate                         *clientAdmission
	refs                         int
	preparing, encoding, waiting bool
}

func newAdmission(limit int) *clientAdmission {
	return &clientAdmission{stats: AdmissionStats{Limit: limit}, changed: make(chan struct{})}
}
func (a *clientAdmission) notify() { close(a.changed); a.changed = make(chan struct{}) }
func (a *clientAdmission) enter() (*callLease, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.stats.Closed {
		return nil, ErrClosed
	}
	if a.stats.Active >= a.stats.Limit {
		a.stats.Rejected++
		return nil, &Status{Code: ResourceExhausted, Message: "client admission limit"}
	}
	a.stats.Active++
	a.stats.Preparing++
	if a.stats.Active > a.stats.Peak {
		a.stats.Peak = a.stats.Active
	}
	a.notify()
	return &callLease{gate: a, refs: 1, preparing: true}, nil
}
func (l *callLease) retain() {
	l.gate.mu.Lock()
	defer l.gate.mu.Unlock()
	if l.refs <= 0 {
		panic("kanarpc: retired call lease")
	}
	l.refs++
}
func (l *callLease) release() {
	a := l.gate
	a.mu.Lock()
	defer a.mu.Unlock()
	l.refs--
	if l.refs < 0 {
		panic("kanarpc: duplicate call release")
	}
	if l.refs == 0 {
		a.stats.Active--
	}
	a.notify()
}
func (l *callLease) setEncoding(on bool) {
	a := l.gate
	a.mu.Lock()
	defer a.mu.Unlock()
	if l.encoding == on {
		return
	}
	l.encoding = on
	if on {
		a.stats.Encoding++
	} else {
		a.stats.Encoding--
	}
	a.notify()
}
func (l *callLease) setWaiting(on bool) {
	a := l.gate
	a.mu.Lock()
	defer a.mu.Unlock()
	if l.waiting == on {
		return
	}
	l.waiting = on
	if on {
		a.stats.EncodingWaiters++
	} else {
		a.stats.EncodingWaiters--
	}
	a.notify()
}
func (l *callLease) prepared() {
	a := l.gate
	a.mu.Lock()
	defer a.mu.Unlock()
	if l.preparing {
		l.preparing = false
		a.stats.Preparing--
		a.notify()
	}
}
func (a *clientAdmission) close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stats.Closed = true
	a.notify()
}
func (a *clientAdmission) snapshot() AdmissionStats { a.mu.Lock(); defer a.mu.Unlock(); return a.stats }
func (a *clientAdmission) wait(ctx context.Context) error {
	for {
		a.mu.Lock()
		if a.stats.Active == 0 {
			a.mu.Unlock()
			return nil
		}
		changed := a.changed
		a.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}
func (c *Client) AdmissionStats() AdmissionStats     { return c.resources.admission.snapshot() }
func (c *QUICClient) AdmissionStats() AdmissionStats { return c.resources.admission.snapshot() }

// WaitIdle waits for accepted calls/preparation/cleanup, not the idle reader or
// completed unread buffers. Close first to seal admission. User encoders cannot
// be forcibly interrupted; no detached encoding goroutine is created.
func (c *Client) WaitIdle(ctx context.Context) error     { return c.resources.admission.wait(ctx) }
func (c *QUICClient) WaitIdle(ctx context.Context) error { return c.resources.admission.wait(ctx) }

type preparedCall struct {
	ctx, owner             context.Context
	cancel                 context.CancelFunc
	stopOwner              func() bool
	leaveFeedback          func()
	lease                  *callLease
	bytes                  *byteLease
	body                   []byte
	window                 uint32
	method                 string
	streaming, transferred bool
}

func (p *preparedCall) check() error {
	if p.owner.Err() != nil {
		return ErrClosed
	}
	return deadlineError(p.ctx)
}
func (p *preparedCall) done() {
	p.body = nil
	if p.bytes != nil {
		p.bytes.release()
	}
	p.lease.setEncoding(false)
	p.lease.setWaiting(false)
	p.lease.prepared()
	if !p.transferred {
		p.leaveFeedback()
		p.stopOwner()
		p.cancel()
	}
	p.lease.release()
}
func openPrefix(dst []byte, method string, window uint32, streaming bool, budget int64) []byte {
	dst = append(dst, `{"method":`...)
	dst = strconv.AppendQuote(dst, method)
	dst = append(dst, `,"budget_ns":`...)
	dst = strconv.AppendInt(dst, budget, 10)
	dst = append(dst, `,"window":`...)
	dst = strconv.AppendUint(dst, uint64(window), 10)
	dst = append(dst, `,"stream":`...)
	dst = strconv.AppendBool(dst, streaming)
	return append(dst, `,"request":`...)
}
func prepareRequest(ctx, owner context.Context, cfg ClientConfig, res *resources, method string, request any, streaming bool) (result *preparedCall, err error) {
	if !validMethod(method) {
		return nil, &Status{Code: InvalidArgument, Message: "expected /service/method"}
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if owner.Err() != nil {
		return nil, ErrClosed
	}
	lease, err := res.admission.enter()
	if err != nil {
		return nil, err
	}
	budget, cancel := context.WithTimeout(ctx, cfg.Timeout)
	p := &preparedCall{ctx: budget, owner: owner, cancel: cancel, lease: lease, method: method, streaming: streaming, window: uint32(cfg.StreamWindow), leaveFeedback: func() {}}
	p.stopOwner = context.AfterFunc(owner, cancel)
	if !streaming {
		p.window = 1
	}
	success := false
	defer func() {
		if !success {
			p.done()
		}
	}()
	if err = p.check(); err != nil {
		return nil, err
	}
	p.leaveFeedback, err = res.feedback.enter(method)
	if err != nil {
		p.leaveFeedback = func() {}
		return nil, err
	}
	lease.setWaiting(true)
	p.bytes, err = res.encoding.waitLease(p.ctx, int64(cfg.IO.MaxFrameBytes)*2)
	lease.setWaiting(false)
	if err != nil {
		if cause := p.check(); cause != nil {
			return nil, cause
		}
		return nil, err
	}
	if err = p.check(); err != nil {
		return nil, err
	}
	lease.setEncoding(true)
	p.body, err = json.Marshal(request)
	lease.setEncoding(false)
	if err != nil {
		return nil, err
	}
	if err = p.check(); err != nil {
		return nil, err
	}
	deadline, _ := p.ctx.Deadline()
	var prefix [256]byte
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return nil, context.DeadlineExceeded
	}
	n := len(openPrefix(prefix[:0], method, p.window, streaming, int64(remaining))) + 1
	if uint64(len(p.body))+uint64(n) > uint64(cfg.IO.MaxFrameBytes) {
		return nil, &Status{Code: InvalidArgument, Message: "request exceeds frame limit"}
	}
	if !p.bytes.shrink(int64(len(p.body))*2 + int64(n)) {
		panic("kanarpc: encoding reservation underestimated")
	}
	success = true
	return p, nil
}
func (p *preparedCall) payload() ([]byte, error) {
	if err := p.check(); err != nil {
		return nil, err
	}
	deadline, _ := p.ctx.Deadline()
	var meta [256]byte
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return nil, context.DeadlineExceeded
	}
	prefix := openPrefix(meta[:0], p.method, p.window, p.streaming, int64(remaining))
	data := make([]byte, 0, len(prefix)+len(p.body)+1)
	data = append(data, prefix...)
	data = append(data, p.body...)
	data = append(data, '}')
	return data, nil
}
