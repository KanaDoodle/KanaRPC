package discovery

import (
	"context"
	"net"
	"testing"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/internal/testcert"
)

func TestRealEtcdDiscoveredTCPAndTLSAndQUICAreDialable(t *testing.T) {
	r, _ := fixture(t, "")
	credentials, err := testcert.New()
	check(t, err)
	for _, transport := range []rpc.Transport{rpc.TCP, rpc.TLS, rpc.QUIC} {
		t.Run(string(transport), func(t *testing.T) {
			s, err := rpc.NewServer(rpc.ServerConfig{})
			check(t, err)
			check(t, rpc.RegisterUnary(s, "/echo/call", func(ctx context.Context, n int) (int, error) { return n, ctx.Err() }))
			done := make(chan error, 1)
			var address string
			if transport == rpc.QUIC {
				l, e := rpc.ListenQUIC("127.0.0.1:0", rpc.TLSOptions{Config: credentials.Server}, rpc.QUICConfig{})
				check(t, e)
				address = l.Addr().String()
				go func() { done <- s.ServeQUIC(l) }()
			} else {
				l, e := net.Listen("tcp", "127.0.0.1:0")
				check(t, e)
				address = l.Addr().String()
				if transport == rpc.TLS {
					go func() { done <- s.ServeTLS(l, rpc.TLSOptions{Config: credentials.Server}) }()
				} else {
					go func() { done <- s.Serve(l) }()
				}
			}
			t.Cleanup(func() {
				s.Close()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				check(t, s.Wait(ctx))
				check(t, <-done)
			})
			until(t, s.Ready)
			registration, err := r.Register(context.Background(), "dialable", rpc.Endpoint{ID: string(transport), Address: address, Transport: transport}, 2)
			check(t, err)
			resolver, err := r.Watch(context.Background(), "dialable", transport)
			check(t, err)
			until(t, func() bool { return registration.State().Ready && len(resolver.Snapshot().Endpoints) == 1 })
			endpoints, err := resolver.Endpoints()
			check(t, err)
			client, err := rpc.DialEndpoint(context.Background(), endpoints[0], rpc.DialOptions{TLS: rpc.TLSOptions{Config: credentials.Client}})
			check(t, err)
			var result int
			check(t, client.Call(context.Background(), "/echo/call", 42, &result))
			if result != 42 {
				t.Fatal(result)
			}
			client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			check(t, registration.Close(ctx))
			check(t, resolver.Close(ctx))
		})
	}
}
