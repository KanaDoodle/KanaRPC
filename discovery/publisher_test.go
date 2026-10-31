package discovery

import (
	"context"
	"net"
	"testing"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
)

func TestRealEtcdPublisherFollowsReadinessAndDrain(t *testing.T) {
	registry, _ := fixture(t, "")
	s, e := rpc.NewServer(rpc.ServerConfig{})
	check(t, e)
	defer s.Close()
	s.SetServing(false)
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	check(t, e)
	done := make(chan error, 1)
	go func() { done <- s.Serve(listener) }()
	until(t, s.Ready)
	endpoint := rpc.Endpoint{ID: "readiness", Address: listener.Addr().String(), Transport: rpc.TCP}
	p, e := registry.Publish(context.Background(), s, "app", endpoint, 3, 10*time.Millisecond)
	check(t, e)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer p.Close(ctx)
	resolver, e := registry.Watch(ctx, "app", rpc.TCP)
	check(t, e)
	defer resolver.Close(ctx)
	if p.State().Published || len(resolver.Snapshot().Endpoints) != 0 {
		t.Fatal("published before readiness", p.State())
	}
	s.SetServing(true)
	check(t, p.WaitReady(ctx))
	until(t, func() bool { return len(resolver.Snapshot().Endpoints) == 1 })
	s.SetServing(false)
	until(t, func() bool { return !p.State().Published && len(resolver.Snapshot().Endpoints) == 0 })
	s.SetServing(true)
	check(t, p.WaitReady(ctx))
	until(t, func() bool { return len(resolver.Snapshot().Endpoints) == 1 })
	_, e = s.DrainWithReport(ctx)
	check(t, e)
	until(t, func() bool { return !p.State().Published && len(resolver.Snapshot().Endpoints) == 0 })
	check(t, p.Close(ctx))
	check(t, <-done)
}
