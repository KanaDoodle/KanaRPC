package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/internal/testcert"
)

const fastPath = "/showcase/fast"
const slowPath = "/showcase/slow"
const streamPath = "/showcase/stream"
const drainPath = "/showcase/drain"

type loadConfig struct {
	Rate        int `json:"rate"`
	SlowPercent int `json:"slow_percent"`
	SlowMS      int `json:"slow_ms"`
	Capacity    int `json:"capacity"`
}

func (c loadConfig) valid() bool {
	return c.Rate >= 0 && c.Rate <= 200 && c.SlowPercent >= 0 && c.SlowPercent <= 100 && c.SlowMS >= 5 && c.SlowMS <= 300 && c.Capacity >= 1 && c.Capacity <= 16
}

type input struct {
	ID   int    `json:"id"`
	Kind string `json:"kind,omitempty"`
}
type chunk struct {
	Sequence int    `json:"sequence"`
	Text     string `json:"text"`
}
type streamRun struct {
	ID              int          `json:"id"`
	Session         int          `json:"session"`
	State           string       `json:"state"`
	Count           int          `json:"count"`
	Chunks          []chunk      `json:"chunks"`
	Ref             rpc.TraceRef `json:"ref"`
	Error           string       `json:"error,omitempty"`
	CancelRequested bool         `json:"cancel_requested"`
	cancel          context.CancelFunc
}
type metric struct {
	Calls             uint64  `json:"calls"`
	Success           uint64  `json:"success"`
	Canceled          uint64  `json:"canceled"`
	Failed            uint64  `json:"failed"`
	ResourceExhausted uint64  `json:"resource_exhausted"`
	BudgetRefusals    uint64  `json:"budget_refusals"`
	P95MS             float64 `json:"p95_ms"`
	LastError         string  `json:"last_error,omitempty"`
	latencies         []time.Duration
	cursor            int
}

func (m *metric) add(d time.Duration, err error) {
	m.Calls++
	if err == nil {
		m.Success++
	} else if errors.Is(err, context.Canceled) {
		m.Canceled++
	} else {
		m.Failed++
		m.LastError = errorText(err)
		if len(m.LastError) > 180 {
			m.LastError = m.LastError[:180]
		}
		var status *rpc.Status
		if errors.As(err, &status) && status.Code == rpc.ResourceExhausted {
			m.ResourceExhausted++
		}
		var budget *rpc.BudgetError
		if errors.As(err, &budget) {
			m.BudgetRefusals++
		}
	}
	if len(m.latencies) < 256 {
		m.latencies = append(m.latencies, d)
	} else {
		m.latencies[m.cursor] = d
		m.cursor = (m.cursor + 1) % 256
	}
}
func (m *metric) snapshot() metric {
	v := *m
	v.latencies = nil
	if len(m.latencies) > 0 {
		d := append([]time.Duration(nil), m.latencies...)
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
		v.P95MS = float64(d[(len(d)-1)*95/100]) / float64(time.Millisecond)
	}
	return v
}

type backend struct {
	mu            sync.Mutex
	active, limit int
	changed       chan struct{}
}

func (b *backend) set(n int) {
	b.mu.Lock()
	b.limit = n
	close(b.changed)
	b.changed = make(chan struct{})
	b.mu.Unlock()
}
func (b *backend) acquire(ctx context.Context) error {
	for {
		b.mu.Lock()
		if b.active < b.limit {
			b.active++
			b.mu.Unlock()
			return nil
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}
func (b *backend) release() {
	b.mu.Lock()
	b.active--
	close(b.changed)
	b.changed = make(chan struct{})
	b.mu.Unlock()
}

type session struct {
	id                 int
	traceStart         uint64
	mode, state        string // app.mu
	server             *rpc.Server
	client             *rpc.QUICClient
	endpoint           string
	credentials        testcert.Credentials
	ctx                context.Context
	cancel             context.CancelFunc
	backend            *backend
	served             chan error
	calls              sync.WaitGroup // Add under app.mu before setting state non-running
	active             int
	generated, dropped uint64
	metrics            map[string]*metric
	report             *rpc.DrainReport
	drainError         string
	drainEntered       chan struct{}
	drainOnce          sync.Once
	faultRef           rpc.TraceRef
}
type history struct {
	Session    int               `json:"session"`
	Mode       string            `json:"mode"`
	Error      string            `json:"error,omitempty"`
	Stats      rpc.ServerStats   `json:"stats"`
	Report     *rpc.DrainReport  `json:"report,omitempty"`
	Metrics    map[string]metric `json:"metrics"`
	TraceStart uint64            `json:"trace_start"`
}
type app struct {
	mu                       sync.Mutex
	op                       sync.Mutex
	ctx                      context.Context
	cancel                   context.CancelFunc
	loop                     sync.WaitGroup
	trace                    *rpc.TraceRecorder
	current                  *session
	config                   loadConfig
	nextID, nextSession      int
	stream                   *streamRun
	busy                     bool
	message                  string
	history                  []history
	pastLost, pastIncomplete uint64
}

func newApp(parent context.Context) (*app, error) {
	trace, err := rpc.NewTraceRecorder(rpc.TraceConfig{Capacity: 4096, SampleEvery: 1})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	a := &app{ctx: ctx, cancel: cancel, trace: trace, config: loadConfig{Rate: 0, SlowPercent: 80, SlowMS: 40, Capacity: 8}, message: "选择一个场景开始。请求走真实本地 QUIC。"}
	if err = a.replaceSession("adaptive"); err != nil {
		cancel()
		return nil, err
	}
	a.loop.Add(1)
	go a.loadLoop()
	return a, nil
}
func (a *app) newSession(mode string) (*session, error) {
	a.mu.Lock()
	a.nextSession++
	id := a.nextSession
	capacity := a.config.Capacity
	a.mu.Unlock()
	ctx, cancel := context.WithCancel(a.ctx)
	s := &session{traceStart: a.trace.Snapshot(0, 1).Last, id: id, mode: mode, state: "starting", ctx: ctx, cancel: cancel, backend: &backend{limit: capacity, changed: make(chan struct{})}, served: make(chan error, 1), drainEntered: make(chan struct{}), metrics: map[string]*metric{fastPath: {}, slowPath: {}}}
	governance := &rpc.GovernanceConfig{MaxExecuting: 16, QueueCapacity: 32, QueueTimeout: 250 * time.Millisecond, SampleWindow: 8, Adaptive: mode == "adaptive", Feedback: true, QueuePolicy: rpc.MethodFair, Methods: []rpc.MethodPolicy{
		{Method: fastPath, Weight: 3, Min: 1, Initial: 8, Max: 8, Target: 15 * time.Millisecond},
		{Method: slowPath, Weight: 1, Min: 1, Initial: 12, Max: 12, Target: 70 * time.Millisecond},
		{Method: streamPath, Min: 1, Initial: 1, Max: 1, Target: time.Second},
		{Method: drainPath, Min: 1, Initial: 1, Max: 1, Target: time.Second},
	}}
	var err error
	s.server, err = rpc.NewServer(rpc.ServerConfig{Trace: a.trace, Governance: governance, MaxInFlight: 96, MaxPerConnection: 96, AckTimeout: 700 * time.Millisecond, MaxRequestDuration: 15 * time.Second})
	if err != nil {
		cancel()
		return nil, err
	}
	fail := func(err error) (*session, error) { cancel(); s.server.Close(); return nil, err }
	if err = rpc.RegisterUnary(s.server, fastPath, func(ctx context.Context, in input) (input, error) {
		if in.Kind == "withhold_ack" {
			ref, _ := rpc.RequestTrace(ctx)
			a.mu.Lock()
			s.faultRef = ref
			a.mu.Unlock()
		}
		return in, delay(ctx, 2*time.Millisecond)
	}); err != nil {
		return fail(err)
	}
	if err = rpc.RegisterUnary(s.server, slowPath, func(ctx context.Context, in input) (input, error) {
		if err := s.backend.acquire(ctx); err != nil {
			return in, err
		}
		defer s.backend.release()
		a.mu.Lock()
		ms := a.config.SlowMS
		a.mu.Unlock()
		return in, delay(ctx, time.Duration(ms)*time.Millisecond)
	}); err != nil {
		return fail(err)
	}
	if err = rpc.RegisterUnary(s.server, drainPath, func(ctx context.Context, in input) (input, error) {
		s.drainOnce.Do(func() { close(s.drainEntered) })
		return in, delay(ctx, 900*time.Millisecond)
	}); err != nil {
		return fail(err)
	}
	if err = rpc.RegisterStream(s.server, streamPath, func(ctx context.Context, in input, send func(chunk) error) error {
		ref, _ := rpc.RequestTrace(ctx)
		a.mu.Lock()
		if a.stream != nil && a.stream.ID == in.ID {
			a.stream.Ref = ref
		}
		a.mu.Unlock()
		for i := 1; i <= 100; i++ {
			if err := delay(ctx, 120*time.Millisecond); err != nil {
				return err
			}
			if err := send(chunk{Sequence: i, Text: fmt.Sprintf("片段 %02d · 从真实 QUIC 流接收", i)}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fail(err)
	}
	s.credentials, err = testcert.New()
	if err != nil {
		return fail(err)
	}
	listener, err := rpc.ListenQUIC("127.0.0.1:0", rpc.TLSOptions{Config: s.credentials.Server}, rpc.QUICConfig{MaxStreams: 128, IdleTimeout: 30 * time.Minute})
	if err != nil {
		return fail(err)
	}
	s.endpoint = listener.Addr().String()
	go func() { s.served <- s.server.ServeQUIC(listener) }()
	ready := time.Now().Add(time.Second)
	for !s.server.Ready() && time.Now().Before(ready) {
		time.Sleep(time.Millisecond)
	}
	if !s.server.Ready() {
		_ = listener.Close()
		return fail(errors.New("RPC server did not become ready"))
	}
	dial, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	s.client, err = rpc.DialQUIC(dial, s.endpoint, rpc.TLSOptions{Config: s.credentials.Client}, rpc.QUICConfig{IdleTimeout: 30 * time.Minute}, rpc.ClientConfig{MaxInFlight: 64, StreamWindow: 4, Timeout: 15 * time.Second, Feedback: &rpc.FeedbackConfig{ObserveOnly: mode == "fixed"}})
	if err != nil {
		s.server.Close()
		<-s.served
		return fail(err)
	}
	s.state = "running"
	return s, nil
}
func delay(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// replaceSession is serialized by op. Historical faults survive new instances.
func (a *app) replaceSession(mode string) error {
	a.mu.Lock()
	old := a.current
	a.config.Rate = 0
	if old != nil {
		old.state = "closing"
	}
	a.mu.Unlock()
	if old != nil {
		old.cancel()
		old.client.Close()
		old.calls.Wait()
		ctx, stop := context.WithTimeout(context.Background(), time.Second)
		report, err := old.server.DrainWithReport(ctx)
		stop()
		old.server.Close()
		join, done := context.WithTimeout(context.Background(), time.Second)
		_ = old.server.Wait(join)
		done()
		<-old.served
		stats := old.server.Stats()
		if old.report != nil {
			report = *old.report
		}
		if old.drainError != "" {
			err = errors.New(old.drainError)
		}
		a.mu.Lock()
		a.pastLost += stats.LostTerminals
		a.pastIncomplete += stats.IncompleteExchanges
		metrics := map[string]metric{}
		for path, m := range old.metrics {
			metrics[path] = m.snapshot()
		}
		a.history = append(a.history, history{old.id, old.mode, errorText(err), stats, &report, metrics, old.traceStart})
		if len(a.history) > 8 {
			a.history = append([]history(nil), a.history[len(a.history)-8:]...)
		}
		a.current = nil
		a.mu.Unlock()
	}
	next, err := a.newSession(mode)
	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		a.message = "新会话启动失败：" + errorText(err)
		return err
	}
	a.current = next
	a.stream = nil
	a.message = "新会话已就绪；历史交付故障继续保留。"
	return nil
}
func (a *app) loadLoop() {
	defer a.loop.Done()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	last := time.Now()
	credit := 0.0
	for {
		select {
		case <-a.ctx.Done():
			return
		case now := <-ticker.C:
			a.mu.Lock()
			s := a.current
			c := a.config
			if s == nil || s.state != "running" || c.Rate == 0 {
				last = now
				credit = 0
				a.mu.Unlock()
				continue
			}
			elapsed := now.Sub(last).Seconds()
			last = now
			if elapsed > .1 {
				elapsed = .1
			}
			credit += elapsed * float64(c.Rate)
			count := int(credit)
			credit -= float64(count)
			for i := 0; i < count; i++ {
				s.generated++
				if s.active >= 40 {
					s.dropped++
					continue
				}
				a.nextID++
				id := a.nextID
				path := fastPath
				if (id*37)%100 < c.SlowPercent {
					path = slowPath
				}
				s.active++
				s.calls.Add(1)
				go a.call(s, path, id)
			}
			a.mu.Unlock()
		}
	}
}
func (a *app) call(s *session, path string, id int) {
	defer s.calls.Done()
	started := time.Now()
	var out input
	ctx, stop := context.WithTimeout(s.ctx, 1500*time.Millisecond)
	defer stop()
	err := s.client.Call(ctx, path, input{ID: id}, &out)
	if err == nil && out.ID != id {
		err = errors.New("response ID mismatch")
	}
	a.mu.Lock()
	s.active--
	s.metrics[path].add(time.Since(started), err)
	a.mu.Unlock()
}
func (a *app) setLoad(c loadConfig) error {
	if !c.valid() {
		return errors.New("负载范围：0–200 RPS，慢请求 0–100%，延迟 5–300ms，容量 1–16")
	}
	a.mu.Lock()
	s := a.current
	if s == nil || s.state != "running" {
		a.mu.Unlock()
		return errors.New("当前会话已停止接纳，请新建会话")
	}
	a.config = c
	a.mu.Unlock()
	s.backend.set(c.Capacity)
	return nil
}
func (a *app) startStream() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.current
	if s == nil || s.state != "running" {
		return errors.New("请先新建运行中的会话")
	}
	if a.stream != nil && (a.stream.State == "active" || a.stream.State == "cancel_requested") {
		return errors.New("已有流在运行，请先取消")
	}
	a.nextID++
	ctx, cancel := context.WithCancel(s.ctx)
	run := &streamRun{ID: a.nextID, Session: s.id, State: "active", cancel: cancel}
	a.stream = run
	s.calls.Add(1)
	go func() {
		defer s.calls.Done()
		defer cancel()
		stream, err := s.client.Stream(ctx, streamPath, input{ID: run.ID})
		if err == nil {
			defer stream.Close()
			for {
				var value chunk
				err = stream.Recv(&value)
				if err != nil {
					break
				}
				a.mu.Lock()
				run.Count++
				run.Chunks = append(run.Chunks, value)
				if len(run.Chunks) > 32 {
					run.Chunks = append([]chunk(nil), run.Chunks[len(run.Chunks)-32:]...)
				}
				a.mu.Unlock()
			}
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		switch {
		case err == io.EOF:
			run.State = "completed"
		case errors.Is(err, context.Canceled):
			run.State = "canceled"
		default:
			run.State = "failed"
			run.Error = errorText(err)
		}
	}()
	return nil
}
func (a *app) cancelStream() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.stream == nil {
		return errors.New("尚未开始流式请求")
	}
	if a.stream.State == "active" {
		a.stream.CancelRequested = true
		a.stream.State = "cancel_requested"
		a.stream.cancel()
	}
	return nil
}
func (a *app) operation(fn func() error) error {
	if !a.op.TryLock() {
		return errors.New("上一个操作尚未完成")
	}
	a.mu.Lock()
	a.busy = true
	a.mu.Unlock()
	go func() {
		defer a.op.Unlock()
		err := fn()
		a.mu.Lock()
		a.busy = false
		if err != nil {
			a.message = errorText(err)
		}
		a.mu.Unlock()
	}()
	return nil
}
func (a *app) drain(omitACK bool) error {
	a.mu.Lock()
	s := a.current
	if s == nil || s.state != "running" {
		a.mu.Unlock()
		return errors.New("当前会话不能再次 drain，请新建会话")
	}
	a.config.Rate = 0
	s.state = "draining"
	a.message = "停止新负载；等待在途执行和回执，最长 2.5 秒。"
	a.mu.Unlock()
	if omitACK {
		closeFault, err := withholdACK(s)
		if err != nil {
			a.mu.Lock()
			s.state = "running"
			a.message = "未能构造缺失回执：" + errorText(err)
			a.mu.Unlock()
			return err
		}
		defer closeFault()
	}
	// One real in-flight call makes admission sealing and graceful completion visible.
	s.calls.Add(1)
	result := make(chan error, 1)
	go func() {
		defer s.calls.Done()
		var out input
		result <- s.client.Call(s.ctx, drainPath, input{Kind: "drain"}, &out)
	}()
	select {
	case <-s.drainEntered:
	case <-time.After(500 * time.Millisecond):
	case <-s.ctx.Done():
	}
	ctx, stop := context.WithTimeout(a.ctx, 2500*time.Millisecond)
	defer stop()
	report, err := s.server.DrainWithReport(ctx)
	a.mu.Lock()
	s.report = &report
	s.drainError = errorText(err)
	s.state = "drained"
	if err != nil {
		s.state = "drain_failed"
	}
	a.message = "Drain 已结束；资源退出和交付结果分别展示。"
	a.mu.Unlock()
	// The report is the actual return-time snapshot; never rewrite it after Wait.
	s.client.Close()
	select {
	case callErr := <-result:
		if callErr != nil {
			a.mu.Lock()
			a.message += " 在途请求：" + errorText(callErr)
			a.mu.Unlock()
		}
	case <-time.After(time.Second):
		a.mu.Lock()
		a.message += " 在途客户端尚未结束。"
		a.mu.Unlock()
	}
	return nil // domain delivery failure is persisted in report, not hidden as HTTP success
}
func (a *app) close() {
	a.cancel()
	a.loop.Wait()
	a.op.Lock()
	defer a.op.Unlock()
	a.mu.Lock()
	s := a.current
	if s != nil {
		s.state = "closed"
	}
	a.mu.Unlock()
	if s != nil {
		s.cancel()
		s.client.Close()
		s.calls.Wait()
		s.server.Close()
		ctx, stop := context.WithTimeout(context.Background(), 2*time.Second)
		_ = s.server.Wait(ctx)
		stop()
	}
}
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
