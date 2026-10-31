// Package discovery owns explicit etcd registrations and fresh resolver
// snapshots. It never dials business endpoints or replays an RPC.
package discovery

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	ErrClosed    = errors.New("discovery: closed")
	ErrNotReady  = errors.New("discovery: snapshot is not fresh")
	ErrOwnership = errors.New("discovery: registration belongs to another lease")
	ErrCapacity  = errors.New("discovery: capacity exceeded")
)

type Config struct {
	Endpoints                                    []string
	Namespace                                    string
	TLS                                          *tls.Config
	Username, Password                           string
	RequestTimeout, FreshnessTTL                 time.Duration
	MaxRegistrations, MaxResolvers, MaxEndpoints int
}
type Registry struct {
	cfg             Config
	client          *clientv3.Client
	ctx             context.Context
	cancel          context.CancelFunc
	mu              sync.Mutex
	closed          bool
	registrations   map[*Registration]struct{}
	resolvers       map[*Resolver]struct{}
	wg              sync.WaitGroup
	once            sync.Once
	done            chan struct{}
	cleanupError    error
	cleanupFailures uint64
	closeError      error
}
type Stats struct {
	Registrations, Resolvers int
	Closed                   bool
	CleanupFailures          uint64
}
type record struct {
	Version  int          `json:"version"`
	Endpoint rpc.Endpoint `json:"endpoint"`
}

func validName(name string) bool {
	if len(name) < 1 || len(name) > 128 || name == "." || name == ".." {
		return false
	}
	for _, c := range name {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
func New(ctx context.Context, c Config) (*Registry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.Namespace = strings.TrimSuffix(c.Namespace, "/")
	if !strings.HasPrefix(c.Namespace, "/") || len(c.Namespace) < 2 || len(c.Namespace) > 256 || len(c.Endpoints) < 1 || len(c.Endpoints) > 8 {
		return nil, errors.New("discovery: explicit bounded namespace and endpoints required")
	}
	for _, part := range strings.Split(c.Namespace[1:], "/") {
		if !validName(part) {
			return nil, errors.New("discovery: invalid namespace")
		}
	}
	for _, endpoint := range c.Endpoints {
		if strings.TrimSpace(endpoint) == "" {
			return nil, errors.New("discovery: empty etcd endpoint")
		}
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = time.Second
	}
	if c.FreshnessTTL == 0 {
		c.FreshnessTTL = 3 * time.Second
	}
	if c.MaxRegistrations == 0 {
		c.MaxRegistrations = 64
	}
	if c.MaxResolvers == 0 {
		c.MaxResolvers = 64
	}
	if c.MaxEndpoints == 0 {
		c.MaxEndpoints = 128
	}
	if c.RequestTimeout <= 0 || c.FreshnessTTL < 60*time.Millisecond || c.MaxRegistrations < 1 || c.MaxRegistrations > 4096 || c.MaxResolvers < 1 || c.MaxResolvers > 4096 || c.MaxEndpoints < 1 || c.MaxEndpoints > 4096 {
		return nil, errors.New("discovery: invalid resource bounds")
	}
	owner, cancel := context.WithCancel(ctx)
	client, err := clientv3.New(clientv3.Config{Endpoints: append([]string(nil), c.Endpoints...), TLS: c.TLS, Username: c.Username, Password: c.Password, DialTimeout: c.RequestTimeout, Context: owner, MaxCallRecvMsgSize: 4 << 20, MaxCallSendMsgSize: 1 << 20})
	if err != nil {
		cancel()
		return nil, err
	}
	return &Registry{cfg: c, client: client, ctx: owner, cancel: cancel, registrations: make(map[*Registration]struct{}), resolvers: make(map[*Resolver]struct{}), done: make(chan struct{})}, nil
}
func (r *Registry) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Stats{Registrations: len(r.registrations), Resolvers: len(r.resolvers), Closed: r.closed || r.ctx.Err() != nil, CleanupFailures: r.cleanupFailures}
}
func (r *Registry) operation(ctx context.Context) (context.Context, func()) {
	budget, cancel := context.WithTimeout(ctx, r.cfg.RequestTimeout)
	stop := context.AfterFunc(r.ctx, cancel)
	return budget, func() { stop(); cancel() }
}

// Close cancels workers, lets each revoke only its own lease, then closes the
// etcd client. The caller can stop waiting; cleanup still has bounded RPCs.
func (r *Registry) Close(ctx context.Context) error {
	r.once.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.cancel()
		r.mu.Unlock()
		go func() {
			r.wg.Wait()
			clientError := r.client.Close()
			r.mu.Lock()
			r.closeError = errors.Join(r.cleanupError, clientError)
			r.mu.Unlock()
			close(r.done)
		}()
	})
	select {
	case <-r.done:
		r.mu.Lock()
		err := r.closeError
		r.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (r *Registry) prefix(service string) string {
	return r.cfg.Namespace + "/services/" + service + "/"
}

type Registration struct {
	registry    *Registry
	key         string
	value       []byte
	ttl         int64
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	mu          sync.Mutex
	lease       clientv3.LeaseID
	expires     time.Time
	ready, lost bool
	recoveries  uint64
	stopError   error
}
type RegistrationState struct {
	Ready, OwnershipLost bool
	LeaseID              int64
	Recoveries           uint64
}

func (r *Registration) State() RegistrationState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return RegistrationState{r.ready && r.ctx.Err() == nil && time.Now().Before(r.expires), r.lost, int64(r.lease), r.recoveries}
}
func (r *Registry) Register(ctx context.Context, service string, endpoint rpc.Endpoint, ttl int64) (*Registration, error) {
	if !validName(service) || !validName(endpoint.ID) || ttl < 2 || ttl > 86400 {
		return nil, errors.New("discovery: invalid service, instance ID or lease TTL")
	}
	if err := endpoint.Validate(); err != nil {
		return nil, err
	}
	value, _ := json.Marshal(record{Version: 2, Endpoint: endpoint})
	owner, cancel := context.WithCancel(r.ctx)
	h := &Registration{registry: r, key: r.prefix(service) + endpoint.ID, value: value, ttl: ttl, ctx: owner, cancel: cancel, done: make(chan struct{})}
	r.mu.Lock()
	if r.closed || r.ctx.Err() != nil {
		r.mu.Unlock()
		cancel()
		return nil, ErrClosed
	}
	if len(r.registrations) >= r.cfg.MaxRegistrations {
		r.mu.Unlock()
		cancel()
		return nil, ErrCapacity
	}
	r.registrations[h] = struct{}{}
	r.wg.Add(1)
	r.mu.Unlock()
	if err := h.acquire(ctx); err != nil {
		cancel()
		h.finish()
		return nil, err
	}
	go h.run()
	return h, nil
}
func (h *Registration) revoke(id clientv3.LeaseID) error {
	if id == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), h.registry.cfg.RequestTimeout)
	defer cancel()
	_, err := h.registry.client.Revoke(ctx, id)
	if errors.Is(err, rpctypes.ErrLeaseNotFound) {
		return nil
	}
	return err
}
func (h *Registration) acquire(ctx context.Context) error {
	r := h.registry
	budget, cancel := r.operation(ctx)
	defer cancel()
	h.mu.Lock()
	old := h.lease
	h.ready = false
	h.mu.Unlock()
	lease, err := r.client.Grant(budget, h.ttl)
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		if !keep {
			h.revoke(lease.ID)
		}
	}()
	conditions := []clientv3.Cmp{clientv3.Compare(clientv3.Version(h.key), "=", 0)}
	if old != 0 {
		current, e := r.client.Get(budget, h.key)
		if e != nil {
			return e
		}
		if len(current.Kvs) != 0 {
			if current.Kvs[0].Lease != int64(old) || string(current.Kvs[0].Value) != string(h.value) {
				return ErrOwnership
			}
			conditions = []clientv3.Cmp{clientv3.Compare(clientv3.LeaseValue(h.key), "=", int64(old)), clientv3.Compare(clientv3.Value(h.key), "=", string(h.value))}
		}
	}
	response, err := r.client.Txn(budget).If(conditions...).Then(clientv3.OpPut(h.key, string(h.value), clientv3.WithLease(lease.ID))).Commit()
	if err != nil {
		return err
	}
	if !response.Succeeded {
		return ErrOwnership
	}
	h.mu.Lock()
	h.lease = lease.ID
	h.expires = time.Now().Add(time.Duration(lease.TTL) * time.Second)
	if old != 0 {
		h.recoveries++
	}
	h.mu.Unlock()
	keep = true
	if old != 0 {
		h.revoke(old)
	}
	return nil
}
func (h *Registration) finish() {
	h.cancel()
	h.mu.Lock()
	lease := h.lease
	h.ready = false
	h.mu.Unlock()
	withdrawalError := h.revoke(lease)
	h.mu.Lock()
	h.stopError = withdrawalError
	h.mu.Unlock()
	r := h.registry
	r.mu.Lock()
	delete(r.registrations, h)
	if withdrawalError != nil {
		r.cleanupFailures++
		if r.cleanupError == nil {
			r.cleanupError = withdrawalError
		}
	}
	r.mu.Unlock()
	r.wg.Done()
	close(h.done)
}
func (h *Registration) run() {
	defer h.finish()
	for {
		if h.ctx.Err() != nil {
			return
		}
		h.mu.Lock()
		lease := h.lease
		h.mu.Unlock()
		keepaliveContext, stopKeepalive := context.WithCancel(h.ctx)
		responses, err := h.registry.client.KeepAlive(keepaliveContext, lease)
		if err == nil {
		loop:
			for {
				select {
				case <-h.ctx.Done():
					stopKeepalive()
					return
				case response, ok := <-responses:
					if !ok || response == nil || response.TTL <= 0 {
						break loop
					}
					observed := time.Now()
					if e := h.verify(lease); e != nil {
						h.mu.Lock()
						h.ready = false
						h.mu.Unlock()
						if errors.Is(e, ErrOwnership) {
							stopKeepalive()
							h.mu.Lock()
							h.lost = true
							h.mu.Unlock()
							return
						}
						if errors.Is(e, ErrNotReady) {
							break loop
						}
						continue
					}
					h.mu.Lock()
					h.ready = true
					h.expires = observed.Add(time.Duration(response.TTL) * time.Second)
					h.mu.Unlock()
				}
			}
		}
		stopKeepalive()
		h.mu.Lock()
		h.ready = false
		h.mu.Unlock()
		for {
			if !pause(h.ctx, 50*time.Millisecond) {
				return
			}
			err = h.acquire(h.ctx)
			if err == nil {
				break
			}
			if errors.Is(err, ErrOwnership) {
				h.mu.Lock()
				h.lost = true
				h.mu.Unlock()
				return
			}
		}
	}
}

func (h *Registration) verify(lease clientv3.LeaseID) error {
	ctx, cancel := h.registry.operation(h.ctx)
	defer cancel()
	response, err := h.registry.client.Get(ctx, h.key)
	if err != nil {
		return err
	}
	if len(response.Kvs) == 0 {
		return ErrNotReady
	}
	if response.Kvs[0].Lease != int64(lease) || string(response.Kvs[0].Value) != string(h.value) {
		return ErrOwnership
	}
	return nil
}
func (h *Registration) Close(ctx context.Context) error {
	h.cancel()
	select {
	case <-h.done:
		h.mu.Lock()
		err := h.stopError
		h.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func pause(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
