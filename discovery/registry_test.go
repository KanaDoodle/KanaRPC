package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func check(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func until(t testing.TB, condition func() bool) {
	t.Helper()
	end := time.Now().Add(12 * time.Second)
	for !condition() {
		if time.Now().After(end) {
			t.Fatal("discovery state did not converge")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
func fixture(t testing.TB, endpoint string) (*Registry, *clientv3.Client) {
	t.Helper()
	if endpoint == "" {
		endpoint = os.Getenv("KANARPC_ETCD_ENDPOINT")
	}
	if endpoint == "" {
		t.Skip("owned etcd required: run scripts/verify-discovery.py")
	}
	namespace := fmt.Sprintf("/kanarpc/r7b/%s-%d-%d", strings.ReplaceAll(t.Name(), "/", "-"), os.Getpid(), time.Now().UnixNano())
	r, err := New(context.Background(), Config{Endpoints: []string{endpoint}, Namespace: namespace, RequestTimeout: 250 * time.Millisecond, FreshnessTTL: 600 * time.Millisecond})
	check(t, err)
	directEndpoint := os.Getenv("KANARPC_ETCD_ENDPOINT")
	if directEndpoint == "" {
		directEndpoint = endpoint
	}
	direct, err := clientv3.New(clientv3.Config{Endpoints: []string{directEndpoint}, DialTimeout: time.Second})
	check(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		check(t, r.Close(ctx))
		if stats := r.Stats(); stats.Registrations != 0 || stats.Resolvers != 0 {
			t.Error(stats)
		}
		_, _ = direct.Delete(ctx, namespace+"/", clientv3.WithPrefix())
		_ = direct.Close()
	})
	return r, direct
}
func node(id string, transport rpc.Transport) rpc.Endpoint {
	return rpc.Endpoint{ID: id, Address: "127.0.0.1:19091", Transport: transport}
}
func value(endpoint rpc.Endpoint) string {
	b, _ := json.Marshal(record{Version: 2, Endpoint: endpoint})
	return string(b)
}

func TestRealEtcdRegistrationWatchTransportAndCloseOwnership(t *testing.T) {
	r, direct := fixture(t, "")
	resolver, err := r.Watch(context.Background(), "service", rpc.QUIC)
	check(t, err)
	a, err := r.Register(context.Background(), "service", node("a", rpc.QUIC), 2)
	check(t, err)
	b, err := r.Register(context.Background(), "service", node("b", rpc.TLS), 2)
	check(t, err)
	until(t, func() bool { return a.State().Ready && b.State().Ready && len(resolver.Snapshot().Endpoints) == 1 })
	if _, err := r.Register(context.Background(), "service", node("a", rpc.QUIC), 2); !errors.Is(err, ErrOwnership) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	check(t, a.Close(ctx))
	until(t, func() bool { return len(resolver.Snapshot().Endpoints) == 0 })
	response, err := direct.Get(ctx, r.prefix("service")+"b")
	check(t, err)
	if len(response.Kvs) != 1 {
		t.Fatal("closing one registration deleted another")
	}
	check(t, b.Close(ctx))
	check(t, resolver.Close(ctx))
}
func TestRealEtcdLeaseLossReacquiresAndNeverOverwritesNewOwner(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprint(replacement), func(t *testing.T) {
			r, direct := fixture(t, "")
			registration, err := r.Register(context.Background(), "service", node("a", rpc.TCP), 2)
			check(t, err)
			until(t, func() bool { return registration.State().Ready })
			old := registration.State().LeaseID
			var other clientv3.LeaseID
			if replacement {
				lease, e := direct.Grant(context.Background(), 30)
				check(t, e)
				other = lease.ID
				_, e = direct.Put(context.Background(), registration.key, value(node("a", rpc.TLS)), clientv3.WithLease(other))
				check(t, e)
				defer func() { _, _ = direct.Revoke(context.Background(), other) }()
			}
			_, err = direct.Revoke(context.Background(), clientv3.LeaseID(old))
			check(t, err)
			if !replacement {
				until(t, func() bool {
					state := registration.State()
					return state.Ready && state.LeaseID != old && state.Recoveries > 0
				})
			} else {
				until(t, func() bool { return registration.State().OwnershipLost })
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				check(t, registration.Close(ctx))
				response, e := direct.Get(ctx, registration.key)
				check(t, e)
				if len(response.Kvs) != 1 || response.Kvs[0].Lease != int64(other) {
					t.Fatal("replacement owner was overwritten or revoked")
				}
			}
		})
	}
}

func TestRealEtcdHealthyLeaseDetectsReplacedRegistration(t *testing.T) {
	r, direct := fixture(t, "")
	h, err := r.Register(context.Background(), "service", node("a", rpc.TCP), 2)
	check(t, err)
	until(t, func() bool { return h.State().Ready })
	lease, err := direct.Grant(context.Background(), 30)
	check(t, err)
	defer func() { _, _ = direct.Revoke(context.Background(), lease.ID) }()
	_, err = direct.Put(context.Background(), h.key, value(node("a", rpc.TLS)), clientv3.WithLease(lease.ID))
	check(t, err)
	end := time.Now().Add(3500 * time.Millisecond)
	for !h.State().OwnershipLost && time.Now().Before(end) {
		time.Sleep(5 * time.Millisecond)
	}
	if state := h.State(); state.Ready || !state.OwnershipLost {
		t.Fatal("healthy old lease hid loss of endpoint ownership", state)
	}
	select {
	case <-h.done:
	case <-time.After(time.Second):
		t.Fatal("ownership worker did not retire")
	}
	select {
	case <-h.ctx.Done():
	default:
		t.Fatal("retired registration retained a live child context")
	}
	response, err := direct.Get(context.Background(), h.key)
	check(t, err)
	if len(response.Kvs) != 1 || response.Kvs[0].Lease != int64(lease.ID) {
		t.Fatal("replacement registration disappeared")
	}
}
func TestRealEtcdRegistryCloseRevokesLiveOwnedLeases(t *testing.T) {
	r, direct := fixture(t, "")
	h, err := r.Register(context.Background(), "service", node("a", rpc.TCP), 5)
	check(t, err)
	_, err = r.Watch(context.Background(), "service", rpc.TCP)
	check(t, err)
	until(t, func() bool { return h.State().Ready })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	check(t, r.Close(ctx))
	response, err := direct.Get(ctx, h.key)
	check(t, err)
	if len(response.Kvs) != 0 {
		t.Fatal("registry close left a live owned registration")
	}
}

func TestRealEtcdCloseReportsUnconfirmedLeaseWithdrawal(t *testing.T) {
	endpoint := os.Getenv("KANARPC_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("owned etcd required")
	}
	gate := proxy(t, endpoint)
	r, err := New(context.Background(), Config{Endpoints: []string{"http://" + gate.listener.Addr().String()}, Namespace: fmt.Sprintf("/kanarpc/withdrawal/%d-%d", os.Getpid(), time.Now().UnixNano()), RequestTimeout: 100 * time.Millisecond})
	check(t, err)
	direct, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	check(t, err)
	defer direct.Close()
	h, err := r.Register(context.Background(), "service", node("a", rpc.TCP), 5)
	check(t, err)
	until(t, func() bool { return h.State().Ready })
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = r.Close(ctx)
		_, _ = direct.Revoke(ctx, clientv3.LeaseID(h.State().LeaseID))
	}()
	gate.block(true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	withdrawErr := h.Close(ctx)
	response, err := direct.Get(ctx, h.key)
	check(t, err)
	if len(response.Kvs) != 1 {
		t.Fatal("did not exercise live lease under unavailable registry")
	}
	if withdrawErr == nil {
		t.Fatal("Close claimed success while owned lease withdrawal was not confirmed")
	}
	gate.block(false)
}

// Bare construction delays the real Get->Watch boundary without adding test
// hooks to production code. It still uses the same worker and real etcd RPCs.
func delayedResolver(t testing.TB, r *Registry) *Resolver {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h := &Resolver{registry: r, prefix: r.prefix("service"), transport: rpc.TCP, ctx: ctx, cancel: cancel, done: make(chan struct{}), nodes: make(map[string]rpc.Endpoint)}
	h.stopRegistry = context.AfterFunc(r.ctx, cancel)
	r.mu.Lock()
	r.resolvers[h] = struct{}{}
	r.wg.Add(1)
	r.mu.Unlock()
	check(t, h.resnapshot())
	return h
}
func TestRealEtcdSnapshotWatchGapAndSameRevisionTransaction(t *testing.T) {
	r, direct := fixture(t, "")
	h := delayedResolver(t, r)
	response, err := direct.Txn(context.Background()).Then(clientv3.OpPut(h.prefix+"a", value(node("a", rpc.TCP))), clientv3.OpPut(h.prefix+"b", value(node("b", rpc.TCP)))).Commit()
	check(t, err)
	go h.run()
	until(t, func() bool {
		snapshot := h.Snapshot()
		return snapshot.Ready && snapshot.Revision >= response.Header.Revision && len(snapshot.Endpoints) == 2
	})
}
func TestRealEtcdCompactionFromLaggedRevisionRebuildsSnapshot(t *testing.T) {
	r, direct := fixture(t, "")
	h := delayedResolver(t, r)
	for i := 0; i < 4; i++ {
		_, err := direct.Put(context.Background(), h.prefix+"a", value(node("a", rpc.TCP)))
		check(t, err)
	}
	response, err := direct.Get(context.Background(), h.prefix, clientv3.WithPrefix())
	check(t, err)
	_, err = direct.Compact(context.Background(), response.Header.Revision)
	check(t, err)
	err = h.watch()
	if !errors.Is(err, rpctypes.ErrCompacted) {
		h.finish()
		t.Fatal("did not exercise actual compacted watch", err)
	}
	go h.run()
	until(t, func() bool {
		snapshot := h.Snapshot()
		return snapshot.Ready && snapshot.Resyncs >= 2 && len(snapshot.Endpoints) == 1 && snapshot.Revision >= response.Header.Revision
	})
}

type gateProxy struct {
	listener    net.Listener
	target      string
	mu          sync.Mutex
	blocked     bool
	connections map[net.Conn]struct{}
	wg          sync.WaitGroup
	done        chan struct{}
}

func proxy(t testing.TB, target string) *gateProxy {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	check(t, err)
	p := &gateProxy{listener: l, target: strings.TrimPrefix(target, "http://"), connections: make(map[net.Conn]struct{}), done: make(chan struct{})}
	go func() {
		defer close(p.done)
		for {
			left, e := l.Accept()
			if e != nil {
				return
			}
			p.mu.Lock()
			if p.blocked {
				p.mu.Unlock()
				_ = left.Close()
				continue
			}
			p.connections[left] = struct{}{}
			p.wg.Add(1)
			p.mu.Unlock()
			go p.bridge(left)
		}
	}()
	t.Cleanup(func() { _ = l.Close(); <-p.done; p.block(true); p.wg.Wait() })
	return p
}
func (p *gateProxy) bridge(left net.Conn) {
	defer p.wg.Done()
	defer func() { _ = left.Close(); p.mu.Lock(); delete(p.connections, left); p.mu.Unlock() }()
	right, err := net.DialTimeout("tcp", p.target, time.Second)
	if err != nil {
		return
	}
	defer right.Close()
	p.mu.Lock()
	if p.blocked {
		p.mu.Unlock()
		return
	}
	p.connections[right] = struct{}{}
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.connections, right); p.mu.Unlock() }()
	finished := make(chan struct{})
	go func() { _, _ = io.Copy(right, left); _ = right.Close(); close(finished) }()
	_, _ = io.Copy(left, right)
	_ = left.Close()
	<-finished
}
func (p *gateProxy) block(blocked bool) {
	p.mu.Lock()
	p.blocked = blocked
	if blocked {
		for connection := range p.connections {
			_ = connection.Close()
		}
	}
	p.mu.Unlock()
}
func TestRealEtcdNetworkGateExpiresReadinessAndRestoresLatestState(t *testing.T) {
	endpoint := os.Getenv("KANARPC_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("owned etcd required")
	}
	gate := proxy(t, endpoint)
	r, direct := fixture(t, "http://"+gate.listener.Addr().String())
	_, err := direct.Put(context.Background(), r.prefix("service")+"a", value(node("a", rpc.TCP)))
	check(t, err)
	h, err := r.Watch(context.Background(), "service", rpc.TCP)
	check(t, err)
	until(t, func() bool { return len(h.Snapshot().Endpoints) == 1 })
	gate.block(true)
	until(t, func() bool { _, err := h.Endpoints(); return errors.Is(err, ErrNotReady) })
	if len(h.Snapshot().Endpoints) != 0 {
		t.Fatal("exposed stale endpoints while partitioned")
	}
	_, err = direct.Txn(context.Background()).Then(clientv3.OpDelete(h.prefix+"a"), clientv3.OpPut(h.prefix+"b", value(node("b", rpc.TCP)))).Commit()
	check(t, err)
	gate.block(false)
	until(t, func() bool {
		snapshot := h.Snapshot()
		return snapshot.Ready && len(snapshot.Endpoints) == 1 && snapshot.Endpoints[0].ID == "b"
	})
}
func TestCreatedAndHistoricalEventsCannotRenewFreshness(t *testing.T) {
	r := &Registry{cfg: Config{FreshnessTTL: time.Second, MaxEndpoints: 4}}
	h := &Resolver{registry: r, prefix: "/test/", transport: rpc.TCP, ctx: context.Background(), nodes: make(map[string]rpc.Endpoint), cluster: 1, revision: 10, ready: true, validUntil: time.Now().Add(-time.Second)}
	check(t, h.apply(clientv3.WatchResponse{Header: etcdserverpb.ResponseHeader{ClusterId: 1, Revision: 100}, Created: true}))
	if snapshot := h.Snapshot(); snapshot.Ready || snapshot.Revision != 10 {
		t.Fatal(snapshot)
	}
	check(t, h.apply(clientv3.WatchResponse{Header: etcdserverpb.ResponseHeader{ClusterId: 1, Revision: 11}, Events: []*clientv3.Event{{Kv: &mvccpb.KeyValue{Key: []byte("/test/a"), Value: []byte(value(node("a", rpc.TCP))), ModRevision: 11}}}}))
	if h.Snapshot().Ready {
		t.Fatal("historical event renewed freshness")
	}
	check(t, h.apply(clientv3.WatchResponse{Header: etcdserverpb.ResponseHeader{ClusterId: 1, Revision: 11}}))
	if snapshot := h.Snapshot(); !snapshot.Ready || len(snapshot.Endpoints) != 1 {
		t.Fatal(snapshot)
	}
}
func TestRealEtcdRejectsMalformedIdentityAndBoundsSnapshots(t *testing.T) {
	r, direct := fixture(t, "")
	r.cfg.MaxEndpoints = 2
	_, err := direct.Put(context.Background(), r.prefix("service")+"a", value(node("wrong-id", rpc.TCP)))
	check(t, err)
	h, err := r.Watch(context.Background(), "service", rpc.TCP)
	check(t, err)
	if snapshot := h.Snapshot(); len(snapshot.Endpoints) != 0 || snapshot.InvalidRecords != 1 {
		t.Fatal(snapshot)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	check(t, h.Close(ctx))
	for _, id := range []string{"a", "b", "c"} {
		_, err = direct.Put(ctx, r.prefix("service")+id, value(node(id, rpc.TCP)))
		check(t, err)
	}
	if _, err = r.Watch(ctx, "service", rpc.TCP); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	for _, service := range []string{"", "../escape", "a/b"} {
		if _, err = r.Register(ctx, service, node("x", rpc.TCP), 2); err == nil {
			t.Fatal("accepted invalid service")
		}
	}
}
func TestRegistryConfigValidation(t *testing.T) {
	for _, config := range []Config{{}, {Endpoints: []string{"x"}, Namespace: "/"}, {Endpoints: []string{"x"}, Namespace: "/a/../b"}, {Endpoints: []string{""}, Namespace: "/test"}, {Endpoints: []string{"x"}, Namespace: "/test", MaxResolvers: -1}} {
		if r, err := New(context.Background(), config); err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = r.Close(ctx)
			cancel()
			t.Fatal(config)
		}
	}
}
func FuzzEndpointRecord(f *testing.F) {
	f.Add([]byte(value(node("a", rpc.QUIC))))
	f.Add([]byte("{}"))
	f.Fuzz(func(t *testing.T, data []byte) {
		h := &Resolver{prefix: "/test/"}
		endpoint, ok := h.decode("/test/a", data)
		if ok && (endpoint.ID != "a" || endpoint.Validate() != nil) {
			t.Fatal("invalid endpoint escaped")
		}
	})
}
