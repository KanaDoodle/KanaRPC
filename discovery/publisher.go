package discovery

import (
	"context"
	"errors"
	"sync"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
)

// Publisher links service readiness to one owned registration. It withdraws
// only its lease; existing RPCs remain owned by Server/drain and routing leases.
type Publisher struct {
	registry     *Registry
	server       *rpc.Server
	service      string
	endpoint     rpc.Endpoint
	ttl          int64
	period       time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
	mu           sync.Mutex
	registration *Registration
	err          error
	stop         func() bool
}
type PublishState struct {
	Published bool
	LeaseID   int64
	LastError string
}

func (r *Registry) Publish(ctx context.Context, server *rpc.Server, service string, endpoint rpc.Endpoint, ttl int64, period time.Duration) (*Publisher, error) {
	if server == nil || !validName(service) || !validName(endpoint.ID) || ttl < 2 || ttl > 86400 || endpoint.Validate() != nil {
		return nil, errors.New("discovery: invalid publication")
	}
	if period == 0 {
		period = 50 * time.Millisecond
	}
	if period < 10*time.Millisecond || period > time.Second {
		return nil, errors.New("discovery: invalid publication interval")
	}
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	owner, cancel := context.WithCancel(ctx)
	p := &Publisher{registry: r, server: server, service: service, endpoint: endpoint, ttl: ttl, period: period, ctx: owner, cancel: cancel, done: make(chan struct{})}
	p.stop = context.AfterFunc(r.ctx, cancel)
	r.mu.Lock()
	if r.closed || r.ctx.Err() != nil {
		r.mu.Unlock()
		p.stop()
		cancel()
		return nil, ErrClosed
	}
	if len(r.publishers) >= r.cfg.MaxRegistrations {
		r.mu.Unlock()
		p.stop()
		cancel()
		return nil, ErrCapacity
	}
	r.publishers[p] = struct{}{}
	r.wg.Add(1)
	r.mu.Unlock()
	go p.run()
	return p, nil
}
func (p *Publisher) State() PublishState {
	p.mu.Lock()
	registration, err := p.registration, p.err
	p.mu.Unlock()
	v := PublishState{}
	if registration != nil {
		s := registration.State()
		v.Published = s.Ready
		v.LeaseID = s.LeaseID
	}
	if err != nil {
		v.LastError = "publication_operation_failed"
	}
	return v
}
func (p *Publisher) WaitReady(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if p.State().Published {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.done:
			return ErrClosed
		case <-ticker.C:
		}
	}
}
func (p *Publisher) withdraw() {
	p.mu.Lock()
	registration := p.registration
	p.registration = nil
	p.mu.Unlock()
	if registration != nil {
		ctx, cancel := context.WithTimeout(context.Background(), p.registry.cfg.RequestTimeout*2)
		e := registration.Close(ctx)
		cancel()
		p.mu.Lock()
		if e != nil {
			p.err = e
		}
		p.mu.Unlock()
	}
}
func (p *Publisher) run() {
	defer func() {
		p.withdraw()
		p.stop()
		p.cancel()
		p.registry.mu.Lock()
		delete(p.registry.publishers, p)
		p.registry.mu.Unlock()
		p.registry.wg.Done()
		close(p.done)
	}()
	ticker := time.NewTicker(p.period)
	defer ticker.Stop()
	for {
		if p.ctx.Err() != nil {
			return
		}
		p.mu.Lock()
		registration := p.registration
		p.mu.Unlock()
		if !p.server.Health().Serving {
			p.withdraw()
		} else if registration == nil {
			h, e := p.registry.Register(p.ctx, p.service, p.endpoint, p.ttl)
			p.mu.Lock()
			p.registration = h
			p.err = e
			p.mu.Unlock()
		}
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (p *Publisher) Close(ctx context.Context) error {
	p.cancel()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.err
	}
}
