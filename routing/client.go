// Package routing owns discovery-driven connection lifetimes. It never retries
// a business call and keeps retired connections alive for leased unread streams.
package routing

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/discovery"
)

var (
	ErrNotReady = errors.New("routing: discovery is not fresh")
	ErrSnapshot = errors.New("routing: invalid or regressed discovery snapshot")
	ErrCapacity = errors.New("routing: managed call capacity exhausted")
	ErrBackoff  = errors.New("routing: eligible endpoints are in dial backoff")
	ErrRetired  = errors.New("routing: selected endpoint retired before dispatch")
)

// AdmissionError is returned only before handing the call to a root Caller.
// Errors after dispatch remain unchanged and do not imply non-execution.
type AdmissionError struct {
	Kind error
	Code rpc.Code
}

func (e *AdmissionError) Error() string        { return e.Kind.Error() }
func (e *AdmissionError) Is(target error) bool { return target == e.Kind }
func (e *AdmissionError) Unwrap() error        { return &rpc.Status{Code: e.Code, Message: e.Kind.Error()} }
func refusal(kind error) error {
	code := rpc.Unavailable
	if kind == ErrCapacity {
		code = rpc.ResourceExhausted
	}
	return &AdmissionError{kind, code}
}

// Source.Snapshot must be fast and nonblocking. discovery.Resolver implements
// this interface. The creator keeps ownership of the resolver.
type Source interface{ Snapshot() discovery.Snapshot }
type Config struct {
	Policy                                    SelectionPolicy
	Weights                                   map[string]int
	Circuit                                   *CircuitConfig
	IdleTimeout                               time.Duration
	Memory                                    rpc.MemoryPoolConfig
	Transport                                 rpc.Transport
	Dial                                      rpc.DialOptions
	MaxConnections, MaxInFlight, MaxEndpoints int
	PollInterval, DialTimeout, DialBackoff    time.Duration
}
type Stats struct {
	CircuitRejected, CircuitOpened, CircuitRecovered                                            uint64
	Ready, Closed                                                                               bool
	Endpoints, Connections, Dialing, Retiring, Closing, ActiveCalls, PeakConnections, PeakCalls int
	Dials, DialFailures, Coalesced, Reused, Retired, Rejected, SnapshotErrors                   uint64
	Revision                                                                                    int64
	ClusterID                                                                                   uint64
}
type entry struct {
	lastUsed                           time.Time
	account                            *rpc.MemoryAccount
	retiredAt                          time.Time
	key                                rpc.Endpoint
	caller                             rpc.Caller
	dialing, retiring, closing, closed bool
	dialCancel                         context.CancelFunc
	err                                error
	users                              int
	stamp                              uint64
}
type dialFunc func(context.Context, rpc.Endpoint, rpc.DialOptions) (rpc.Caller, error)

// Client owns bounded connections and application leases, but not its Source.
type Client struct {
	policyWatchers          int
	routes                  map[rpc.Endpoint]*circuitState
	source                  Source
	cfg                     Config
	dial                    dialFunc
	ctx                     context.Context
	cancel                  context.CancelFunc
	view                    chan struct{}
	mu                      sync.Mutex
	changed                 chan struct{}
	closed, loopDone, ready bool
	revision                int64
	cluster                 uint64
	desired                 map[rpc.Endpoint]struct{}
	order                   []rpc.Endpoint
	active                  map[rpc.Endpoint]*entry
	entries                 map[*entry]struct{}
	backoff                 map[rpc.Endpoint]time.Time
	next, clock             uint64
	calls                   int
	stats                   Stats
	memory                  *rpc.MemoryPool
}

// New starts one snapshot polling loop. It does not block for discovery/dial
// readiness; each Call/Stream enforces freshness before root-client handoff.
func New(ctx context.Context, source Source, cfg Config) (*Client, error) {
	return newClient(ctx, source, cfg, rpc.DialEndpoint)
}
func newClient(ctx context.Context, source Source, cfg Config, dial dialFunc) (*Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source == nil || dial == nil {
		return nil, errors.New("routing: source and dialer required")
	}
	if cfg.Transport != rpc.TCP && cfg.Transport != rpc.TLS && cfg.Transport != rpc.QUIC {
		return nil, errors.New("routing: explicit transport required")
	}
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = 8
	}
	if cfg.MaxInFlight == 0 {
		cfg.MaxInFlight = 128
	}
	if cfg.MaxEndpoints == 0 {
		cfg.MaxEndpoints = 128
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 50 * time.Millisecond
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = time.Second
	}
	if cfg.DialBackoff == 0 {
		cfg.DialBackoff = 100 * time.Millisecond
	}
	if cfg.MaxConnections < 1 || cfg.MaxConnections > 256 || cfg.MaxInFlight < 1 || cfg.MaxInFlight > 4096 || cfg.MaxEndpoints < 1 || cfg.MaxEndpoints > 4096 || cfg.PollInterval < time.Millisecond || cfg.PollInterval > time.Minute || cfg.DialTimeout <= 0 || cfg.DialTimeout > time.Minute || cfg.DialBackoff < time.Millisecond || cfg.DialBackoff > time.Minute {
		return nil, errors.New("routing: invalid resource bounds")
	}
	if e := normalizePolicy(&cfg); e != nil {
		return nil, e
	}
	if cfg.Dial.Client.Timeout == 0 {
		cfg.Dial.Client.Timeout = 5 * time.Second
	}
	if cfg.Dial.Client.Timeout < 0 {
		return nil, errors.New("routing: invalid call timeout")
	}
	if cfg.Dial.Client.Feedback != nil {
		copy := *cfg.Dial.Client.Feedback
		cfg.Dial.Client.Feedback = &copy
	}
	if cfg.Transport != rpc.TCP && cfg.Dial.TLS.Config == nil {
		return nil, errors.New("routing: TLS configuration required")
	}
	if cfg.Dial.TLS.Config != nil {
		cfg.Dial.TLS.Config = cfg.Dial.TLS.Config.Clone()
		if cfg.Dial.TLS.Config.RootCAs != nil {
			cfg.Dial.TLS.Config.RootCAs = cfg.Dial.TLS.Config.RootCAs.Clone()
		}
		if cfg.Dial.TLS.Config.ClientCAs != nil {
			cfg.Dial.TLS.Config.ClientCAs = cfg.Dial.TLS.Config.ClientCAs.Clone()
		}
	}
	if cfg.Dial.Client.MemoryAccount != nil {
		return nil, errors.New("routing: memory accounts are owned by the pool")
	}
	memory, err := rpc.NewMemoryPool(cfg.Memory, cfg.MaxConnections)
	if err != nil {
		return nil, err
	}
	if err = memory.CheckClient(cfg.Dial.Client); err != nil {
		return nil, err
	}
	owner, cancel := context.WithCancel(ctx)
	c := &Client{routes: map[rpc.Endpoint]*circuitState{}, memory: memory, source: source, cfg: cfg, dial: dial, ctx: owner, cancel: cancel, view: make(chan struct{}, 1), changed: make(chan struct{}), desired: make(map[rpc.Endpoint]struct{}), active: make(map[rpc.Endpoint]*entry), entries: make(map[*entry]struct{}), backoff: make(map[rpc.Endpoint]time.Time)}
	c.view <- struct{}{}
	go c.loop()
	return c, nil
}
func (c *Client) notifyLocked() { close(c.changed); c.changed = make(chan struct{}) }
func (c *Client) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stats
	s.Ready = c.ready && !c.closed
	s.Closed = c.closed
	s.Endpoints = len(c.desired)
	s.Connections = len(c.entries)
	s.ActiveCalls = c.calls
	s.Revision = c.revision
	s.ClusterID = c.cluster
	for e := range c.entries {
		if e.dialing {
			s.Dialing++
		}
		if e.retiring {
			s.Retiring++
		}
		if e.closing {
			s.Closing++
		}
	}
	return s
}
func (c *Client) loop() {
	defer func() { c.mu.Lock(); c.loopDone = true; c.notifyLocked(); c.mu.Unlock() }()
	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()
	for {
		_ = c.refresh(c.ctx)
		select {
		case <-c.ctx.Done():
			c.Close()
			return
		case <-ticker.C:
		}
	}
}
func readSnapshot(source Source) (snapshot discovery.Snapshot, err error) {
	defer func() {
		if recover() != nil {
			err = ErrSnapshot
		}
	}()
	return source.Snapshot(), nil
}
func (c *Client) refresh(ctx context.Context) error {
	// Serialize read+apply without holding the connection mutex across Source.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.view:
	}
	defer func() { c.view <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return err
	}
	snapshot, err := readSnapshot(c.source)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.ctx.Err() != nil {
		return rpc.ErrClosed
	}
	if err == nil && !snapshot.Ready {
		err = ErrNotReady
	}
	wanted := make(map[rpc.Endpoint]struct{})
	ids := make(map[string]bool)
	if err == nil {
		if snapshot.Revision < 1 || snapshot.ClusterID == 0 || len(snapshot.Endpoints) > c.cfg.MaxEndpoints || c.cluster != 0 && c.cluster != snapshot.ClusterID || snapshot.Revision < c.revision {
			err = ErrSnapshot
		}
		if err == nil {
			for _, endpoint := range snapshot.Endpoints {
				if endpoint.ID == "" || len(endpoint.ID) > 128 || ids[endpoint.ID] || endpoint.Transport != c.cfg.Transport || endpoint.Validate() != nil {
					err = ErrSnapshot
					break
				}
				ids[endpoint.ID] = true
				wanted[endpoint] = struct{}{}
			}
		}
		if err == nil && snapshot.Revision == c.revision && !same(wanted, c.desired) {
			err = ErrSnapshot
		}
	}
	if err != nil {
		if c.ready {
			c.ready = false
			c.notifyLocked()
		}
		if err == ErrSnapshot {
			c.stats.SnapshotErrors++
		}
		return refusal(err)
	}
	changed := !c.ready || c.revision != snapshot.Revision
	c.ready = true
	c.revision = snapshot.Revision
	c.cluster = snapshot.ClusterID
	c.desired = wanted
	c.syncRoutesLocked()
	c.order = c.order[:0]
	for key := range wanted {
		c.order = append(c.order, key)
	}
	sort.Slice(c.order, func(i, j int) bool { return c.order[i].ID < c.order[j].ID })
	for key := range c.backoff {
		if _, ok := wanted[key]; !ok {
			delete(c.backoff, key)
		}
	}
	for key, e := range c.active {
		_, keep := wanted[key]
		if !keep || dead(e.caller) || c.cfg.IdleTimeout > 0 && e.users == 0 && !e.dialing && !e.lastUsed.IsZero() && time.Since(e.lastUsed) >= c.cfg.IdleTimeout {
			c.retireLocked(e)
		}
	}
	if changed {
		c.notifyLocked()
	}
	return nil
}
func same(a, b map[rpc.Endpoint]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}
func dead(caller rpc.Caller) bool {
	if caller == nil {
		return false
	}
	if c, ok := caller.(interface{ AdmissionStats() rpc.AdmissionStats }); ok {
		return c.AdmissionStats().Closed
	}
	return false
}
func (c *Client) retireLocked(e *entry) {
	if !e.retiring {
		e.retiring = true
		e.retiredAt = time.Now()
		e.account.Retire()
		c.stats.Retired++
		if c.active[e.key] == e {
			delete(c.active, e.key)
		}
	}
	if e.dialing {
		e.dialCancel()
	} else if e.users == 0 {
		c.closeLocked(e)
	}
	c.notifyLocked()
}
func (c *Client) removeLocked(e *entry) {
	if e.closed && !e.dialing && e.users == 0 && e.account.Release() == nil {
		delete(c.entries, e)
	}
	c.notifyLocked()
}
func (c *Client) closeLocked(e *entry) {
	if e.closing || e.closed || e.dialing {
		return
	}
	if e.caller == nil {
		e.closed = true
		c.removeLocked(e)
		return
	}
	e.closing = true
	go func() {
		e.caller.Close()
		if idle, ok := e.caller.(interface{ WaitIdle(context.Context) error }); ok {
			_ = idle.WaitIdle(context.Background())
		}
		c.mu.Lock()
		e.closing = false
		e.closed = true
		c.removeLocked(e)
		c.mu.Unlock()
	}()
}
func (c *Client) dialEntry(e *entry, ctx context.Context) {
	options := c.cfg.Dial
	options.Client.MemoryAccount = e.account
	caller, err := c.dial(ctx, e.key, options)
	e.dialCancel()
	// Failed DialEndpoint can carry a typed nil in the Caller interface. The
	// result on error conveys no ownership and must never be installed/closed.
	if err != nil {
		caller = nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e.dialing = false
	e.err = err
	e.caller = caller
	if err != nil {
		c.stats.DialFailures++
		if !e.retiring && !c.closed {
			c.backoff[e.key] = time.Now().Add(c.cfg.DialBackoff)
		}
		c.retireLocked(e)
		c.closeLocked(e)
	} else if c.closed || e.retiring {
		c.retireLocked(e)
		c.closeLocked(e)
	}
	c.notifyLocked()
}
func (c *Client) begin(ctx context.Context) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	c.mu.Lock()
	if c.closed || c.ctx.Err() != nil {
		c.mu.Unlock()
		return nil, nil, rpc.ErrClosed
	}
	if c.calls >= c.cfg.MaxInFlight {
		c.stats.Rejected++
		c.mu.Unlock()
		return nil, nil, refusal(ErrCapacity)
	}
	c.calls++
	if c.calls > c.stats.PeakCalls {
		c.stats.PeakCalls = c.calls
	}
	c.mu.Unlock()
	budget, cancel := context.WithTimeout(ctx, c.cfg.Dial.Client.Timeout)
	stop := context.AfterFunc(c.ctx, cancel)
	var once sync.Once
	return budget, func() {
		once.Do(func() {
			stop()
			cancel()
			c.mu.Lock()
			c.calls--
			c.notifyLocked()
			c.mu.Unlock()
		})
	}, nil
}
func (c *Client) acquire(ctx context.Context) (*entry, error) {
	if err := c.refresh(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, rpc.ErrClosed
	}
	if !c.ready || len(c.order) == 0 {
		c.mu.Unlock()
		return nil, refusal(ErrNotReady)
	}
	selected, pickErr := c.pickLocked()
	c.mu.Unlock()
	if pickErr != nil {
		return nil, pickErr
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := c.refresh(ctx); err != nil {
			return nil, err
		}
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, rpc.ErrClosed
		}
		if _, ok := c.desired[selected]; !c.ready || !ok {
			c.mu.Unlock()
			return nil, refusal(ErrRetired)
		}
		if time.Now().Before(c.backoff[selected]) {
			c.mu.Unlock()
			return nil, refusal(ErrBackoff)
		}
		if e := c.active[selected]; e != nil {
			e.users++
			c.clock++
			e.stamp = c.clock
			if e.dialing {
				c.stats.Coalesced++
			} else {
				c.stats.Reused++
			}
			c.mu.Unlock()
			return e, nil
		}
		if len(c.entries) < c.cfg.MaxConnections {
			account, err := c.memory.NewAccount()
			if err != nil {
				c.mu.Unlock()
				return nil, err
			}
			dialCtx, cancel := context.WithTimeout(c.ctx, c.cfg.DialTimeout)
			e := &entry{account: account, key: selected, users: 1, dialing: true, lastUsed: time.Now(), dialCancel: cancel}
			c.clock++
			e.stamp = c.clock
			c.entries[e] = struct{}{}
			c.active[selected] = e
			c.stats.Dials++
			if len(c.entries) > c.stats.PeakConnections {
				c.stats.PeakConnections = len(c.entries)
			}
			c.mu.Unlock()
			go c.dialEntry(e, dialCtx)
			return e, nil
		}
		var oldest *entry
		for e := range c.entries {
			if !e.dialing && !e.closing && !e.closed && e.users == 0 && (oldest == nil || e.stamp < oldest.stamp) {
				oldest = e
			}
		}
		if oldest != nil {
			c.retireLocked(oldest)
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}
func (c *Client) release(e *entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e.users--
	e.lastUsed = time.Now()
	if e.users < 0 {
		panic("routing: duplicate endpoint release")
	}
	if e.users == 0 {
		if e.dialing {
			c.retireLocked(e)
		} else if e.retiring || c.closed {
			c.closeLocked(e)
		}
	}
	c.removeLocked(e)
}
func (c *Client) readyEntry(ctx context.Context, e *entry) (rpc.Caller, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := c.refresh(ctx); err != nil {
			return nil, err
		}
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, rpc.ErrClosed
		}
		if e.retiring {
			err := e.err
			c.mu.Unlock()
			if err != nil {
				return nil, err
			}
			return nil, refusal(ErrRetired)
		}
		if !e.dialing {
			caller := e.caller
			c.mu.Unlock()
			return caller, nil
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

// Call delegates at most once, including after transport or business failure.
func (c *Client) Call(ctx context.Context, method string, request, response any) error {
	budget, done, err := c.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	e, err := c.acquire(budget)
	if err != nil {
		return err
	}
	defer c.release(e)
	caller, err := c.readyEntry(budget, e)
	if err != nil {
		return err
	}
	finish, err := c.startRoute(e)
	if err != nil {
		return err
	}
	defer func() { finish(context.Canceled) }()
	err = caller.Call(budget, method, request, response)
	finish(err)
	if errors.Is(err, rpc.ErrClosed) || dead(caller) {
		c.mu.Lock()
		c.retireLocked(e)
		c.mu.Unlock()
	}
	return err
}

// OpenStream exposes the managed stream through the generated SDK interface.
func (c *Client) OpenStream(ctx context.Context, method string, request any) (rpc.Receiver, error) {
	return c.Stream(ctx, method, request)
}

// Stream transfers its managed lease to the returned stream until terminal
// consumption or Close. Keep ctx alive for the full streaming lifetime.
func (c *Client) Stream(ctx context.Context, method string, request any) (*Stream, error) {
	budget, done, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	var e *entry
	transferred := false
	defer func() {
		if !transferred {
			if e != nil {
				c.release(e)
			}
			done()
		}
	}()
	e, err = c.acquire(budget)
	if err != nil {
		return nil, err
	}
	caller, err := c.readyEntry(budget, e)
	if err != nil {
		return nil, err
	}
	finish, err := c.startRoute(e)
	if err != nil {
		return nil, err
	}
	handed := false
	defer func() {
		if !handed {
			finish(context.Canceled)
		}
	}()
	stream, err := caller.Stream(budget, method, request)
	if err != nil {
		finish(err)
		if errors.Is(err, rpc.ErrClosed) || dead(caller) {
			c.mu.Lock()
			c.retireLocked(e)
			c.mu.Unlock()
		}
		return nil, err
	}
	transferred = true
	handed = true
	if c.cfg.Policy != RoundRobin || c.cfg.Circuit != nil {
		c.mu.Lock()
		c.policyWatchers++
		c.mu.Unlock()
		go func() {
			defer func() { c.mu.Lock(); c.policyWatchers--; c.notifyLocked(); c.mu.Unlock() }()
			<-stream.Done()
			finish(stream.Outcome())
		}()
	}
	return &Stream{inner: stream, release: func() { c.release(e); done() }}, nil
}

// Close seals admission, cancels managed waits/calls and closes owned IO.
// Use Wait with a fresh deadline context to join resources and stream leases.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	c.memory.Seal()
	c.ready = false
	c.cancel()
	for e := range c.entries {
		c.retireLocked(e)
		if !e.dialing {
			c.closeLocked(e)
		}
	}
	c.notifyLocked()
}

// Wait joins the owned loop/dials/closers and all application stream/call leases.
// Close first; a caller-held unread stream or blocked user codec remains visible.
func (c *Client) Wait(ctx context.Context) error {
	for {
		c.mu.Lock()
		if c.closed && c.loopDone && len(c.entries) == 0 && c.calls == 0 && c.policyWatchers == 0 {
			c.mu.Unlock()
			return nil
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// Stream protects its connection even after wire completion while unread
// data or an application Recv is still owned. Call Close if stopping early.
type Stream struct {
	inner                      *rpc.Stream
	mu                         sync.Mutex
	readers                    int
	closed, terminal, released bool
	release                    func()
}

func (s *Stream) finishRead(terminal bool) {
	s.mu.Lock()
	s.readers--
	if terminal {
		s.terminal = true
	}
	release := (s.closed || s.terminal) && s.readers == 0 && !s.released
	if release {
		s.released = true
	}
	s.mu.Unlock()
	if release {
		s.release()
	}
}
func (s *Stream) Recv(response any) (err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return rpc.ErrClosed
	}
	s.readers++
	s.mu.Unlock()
	returned := false
	defer func() {
		if !returned {
			_ = s.inner.Close()
		}
		s.finishRead(!returned || err != nil)
	}()
	err = s.inner.Recv(response)
	returned = true
	return err
}
func (s *Stream) Close() error {
	err := s.inner.Close()
	s.mu.Lock()
	s.closed = true
	release := s.readers == 0 && !s.released
	if release {
		s.released = true
	}
	s.mu.Unlock()
	if release {
		s.release()
	}
	return err
}

// MemoryStats is an atomic aggregate snapshot, including retired objects.
func (c *Client) MemoryStats() rpc.MemoryPoolStats { return c.memory.Stats() }

// ConnectionStats returns bounded live ownership details; completed history is
// represented by cumulative counters, not an ever-growing retired object list.
type ConnectionStats struct {
	Endpoint                           rpc.Endpoint
	Dialing, Retiring, Closing, Closed bool
	Users                              int
	RetiredFor                         time.Duration
	Memory                             rpc.MemoryAccountStats
}

func (c *Client) ConnectionStats() []ConnectionStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	accounts := c.memory.Accounts()
	byID := make(map[uint64]rpc.MemoryAccountStats, len(accounts))
	for _, a := range accounts {
		byID[a.ID] = a
	}
	out := make([]ConnectionStats, 0, len(c.entries))
	now := time.Now()
	for e := range c.entries {
		s := ConnectionStats{Endpoint: e.key, Dialing: e.dialing, Retiring: e.retiring, Closing: e.closing, Closed: e.closed, Users: e.users, Memory: byID[e.account.ID()]}
		if e.retiring {
			s.RetiredFor = now.Sub(e.retiredAt)
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Memory.ID < out[j].Memory.ID })
	return out
}

func (s *Stream) Done() <-chan struct{} { return s.inner.Done() }
func (s *Stream) Outcome() error        { return s.inner.Outcome() }
