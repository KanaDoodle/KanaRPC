package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Snapshot struct {
	Ready                   bool
	Revision                int64
	ClusterID               uint64
	Endpoints               []rpc.Endpoint
	Resyncs, InvalidRecords uint64
}
type Resolver struct {
	registry         *Registry
	prefix           string
	transport        rpc.Transport
	ctx              context.Context
	cancel           context.CancelFunc
	stopRegistry     func() bool
	done             chan struct{}
	mu               sync.Mutex
	nodes            map[string]rpc.Endpoint
	revision         int64
	cluster          uint64
	ready            bool
	validUntil       time.Time
	resyncs, invalid uint64
}

func (r *Registry) Watch(ctx context.Context, service string, transport rpc.Transport) (*Resolver, error) {
	if !validName(service) || (transport != rpc.TCP && transport != rpc.TLS && transport != rpc.QUIC) {
		return nil, errors.New("discovery: explicit service and transport required")
	}
	owner, cancel := context.WithCancel(ctx)
	h := &Resolver{registry: r, prefix: r.prefix(service), transport: transport, ctx: owner, cancel: cancel, done: make(chan struct{}), nodes: make(map[string]rpc.Endpoint)}
	h.stopRegistry = context.AfterFunc(r.ctx, cancel)
	r.mu.Lock()
	if r.closed || r.ctx.Err() != nil {
		r.mu.Unlock()
		h.stopRegistry()
		cancel()
		return nil, ErrClosed
	}
	if len(r.resolvers) >= r.cfg.MaxResolvers {
		r.mu.Unlock()
		h.stopRegistry()
		cancel()
		return nil, ErrCapacity
	}
	r.resolvers[h] = struct{}{}
	r.wg.Add(1)
	r.mu.Unlock()
	if err := h.resnapshot(); err != nil {
		h.finish()
		return nil, err
	}
	go h.run()
	return h, nil
}
func (h *Resolver) Snapshot() Snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := Snapshot{Ready: h.ready && h.ctx.Err() == nil && time.Now().Before(h.validUntil), Revision: h.revision, ClusterID: h.cluster, Resyncs: h.resyncs, InvalidRecords: h.invalid}
	if result.Ready {
		for _, endpoint := range h.nodes {
			if endpoint.Transport == h.transport {
				result.Endpoints = append(result.Endpoints, endpoint)
			}
		}
		sort.Slice(result.Endpoints, func(i, j int) bool { return result.Endpoints[i].ID < result.Endpoints[j].ID })
	}
	return result
}
func (h *Resolver) Endpoints() ([]rpc.Endpoint, error) {
	snapshot := h.Snapshot()
	if !snapshot.Ready {
		return nil, ErrNotReady
	}
	return snapshot.Endpoints, nil
}
func (h *Resolver) decode(key string, value []byte) (rpc.Endpoint, bool) {
	var record record
	if len(value) > 4096 || json.Unmarshal(value, &record) != nil || record.Version != 2 || !validName(record.Endpoint.ID) || key != h.prefix+record.Endpoint.ID || record.Endpoint.Validate() != nil {
		return rpc.Endpoint{}, false
	}
	return record.Endpoint, true
}
func (h *Resolver) resnapshot() error {
	ctx, cancel := h.registry.operation(h.ctx)
	defer cancel()
	response, err := h.registry.client.Get(ctx, h.prefix, clientv3.WithPrefix(), clientv3.WithLimit(int64(h.registry.cfg.MaxEndpoints+1)))
	if err != nil {
		return err
	}
	if response.More || len(response.Kvs) > h.registry.cfg.MaxEndpoints {
		return ErrCapacity
	}
	nodes := make(map[string]rpc.Endpoint)
	var invalid uint64
	for _, kv := range response.Kvs {
		if endpoint, ok := h.decode(string(kv.Key), kv.Value); ok {
			nodes[string(kv.Key)] = endpoint
		} else {
			invalid++
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cluster != 0 && h.cluster != response.Header.ClusterId {
		return errors.New("discovery: etcd cluster changed")
	}
	h.nodes = nodes
	h.revision = response.Header.Revision
	h.cluster = response.Header.ClusterId
	h.resyncs++
	h.invalid += invalid
	h.ready = true
	h.validUntil = time.Now().Add(h.registry.cfg.FreshnessTTL)
	return nil
}
func (h *Resolver) apply(response clientv3.WatchResponse) error {
	if err := response.Err(); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if response.Header.ClusterId != 0 && response.Header.ClusterId != h.cluster {
		return errors.New("discovery: watch cluster changed")
	}
	// Created can carry a header newer than a backlog that is still pending.
	// It is neither a resume barrier nor proof of watch freshness.
	if response.Created {
		return nil
	}
	if response.IsProgressNotify() {
		if response.Header.Revision >= h.revision {
			h.revision = response.Header.Revision
			h.ready = true
			h.validUntil = time.Now().Add(h.registry.cfg.FreshnessTTL)
		}
		return nil
	}
	before, latest := h.revision, h.revision
	for _, event := range response.Events {
		if event.Kv == nil || event.Kv.ModRevision <= before {
			continue
		}
		key := string(event.Kv.Key)
		if !strings.HasPrefix(key, h.prefix) {
			return errors.New("discovery: event outside prefix")
		}
		if event.Kv.ModRevision > latest {
			latest = event.Kv.ModRevision
		}
		if event.Type == clientv3.EventTypeDelete {
			delete(h.nodes, key)
			continue
		}
		endpoint, ok := h.decode(key, event.Kv.Value)
		if !ok {
			delete(h.nodes, key)
			h.invalid++
			continue
		}
		if _, exists := h.nodes[key]; !exists && len(h.nodes) >= h.registry.cfg.MaxEndpoints {
			h.ready = false
			return ErrCapacity
		}
		h.nodes[key] = endpoint
	}
	h.revision = latest
	// Historical events alone never renew freshness; a progress barrier does.
	return nil
}
func (h *Resolver) watch() error {
	h.mu.Lock()
	revision := h.revision
	h.mu.Unlock()
	ctx, cancel := context.WithCancel(h.ctx)
	defer cancel()
	responses := h.registry.client.Watch(ctx, h.prefix, clientv3.WithPrefix(), clientv3.WithRev(revision+1), clientv3.WithProgressNotify(), clientv3.WithCreatedNotify())
	ticker := time.NewTicker(h.registry.cfg.FreshnessTTL / 3)
	defer ticker.Stop()
	for {
		select {
		case <-h.ctx.Done():
			return h.ctx.Err()
		case response, ok := <-responses:
			if !ok {
				return errors.New("discovery: watch closed")
			}
			if err := h.apply(response); err != nil {
				return err
			}
		case <-ticker.C:
			budget, stop := h.registry.operation(h.ctx)
			err := h.registry.client.RequestProgress(budget)
			stop()
			if err != nil {
				h.mu.Lock()
				h.ready = false
				h.mu.Unlock()
			}
		}
	}
}
func (h *Resolver) run() {
	defer h.finish()
	for {
		_ = h.watch()
		h.mu.Lock()
		h.ready = false
		h.mu.Unlock()
		for {
			if !pause(h.ctx, 50*time.Millisecond) {
				return
			}
			if err := h.resnapshot(); err == nil {
				break
			}
		}
	}
}
func (h *Resolver) finish() {
	h.stopRegistry()
	h.cancel()
	h.mu.Lock()
	h.ready = false
	h.mu.Unlock()
	r := h.registry
	r.mu.Lock()
	delete(r.resolvers, h)
	r.mu.Unlock()
	r.wg.Done()
	close(h.done)
}
func (h *Resolver) Close(ctx context.Context) error {
	h.cancel()
	select {
	case <-h.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
