package routing_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/discovery"
	"github.com/KanaDoodle/KanaRPC/routing"
)

// In applications, use a caller-owned *discovery.Resolver as the Source.
// This public-API example uses a fixed snapshot and a real loopback server.
type fixedSource struct{ node rpc.Endpoint }

func (s fixedSource) Snapshot() discovery.Snapshot {
	return discovery.Snapshot{Ready: true, Revision: 1, ClusterID: 1, Endpoints: []rpc.Endpoint{s.node}}
}

func ExampleClient() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	server, err := rpc.NewServer(rpc.ServerConfig{})
	if err != nil {
		panic(err)
	}
	if err = rpc.RegisterUnary(server, "/example/echo", func(_ context.Context, n int) (int, error) { return n, nil }); err != nil {
		panic(err)
	}
	if err = rpc.RegisterStream(server, "/example/items", func(_ context.Context, n int, send func(int) error) error {
		for i := 0; i < n; i++ {
			if err := send(i); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() {
		server.Close()
		if err := server.Wait(ctx); err != nil {
			panic(err)
		}
		if err := <-done; err != nil {
			panic(err)
		}
	}()
	client, err := routing.New(ctx, fixedSource{rpc.Endpoint{ID: "local", Address: listener.Addr().String(), Transport: rpc.TCP}}, routing.Config{
		Transport: rpc.TCP, MaxConnections: 2,
		Memory: rpc.MemoryPoolConfig{DataBytes: 8 << 20, Memory: rpc.MemoryConfig{EncodingBytes: 4 << 20, ReceiveBytes: 4 << 20, SendBytes: 4 << 20}},
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		client.Close()
		if err := client.Wait(ctx); err != nil {
			panic(err)
		}
	}()
	var value int
	if err = client.Call(ctx, "/example/echo", 42, &value); err != nil {
		panic(err)
	}
	fmt.Println(value)
	stream, err := client.Stream(ctx, "/example/items", 2)
	if err != nil {
		panic(err)
	}
	defer stream.Close()
	for {
		err = stream.Recv(&value)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		fmt.Println(value)
	}
	client.Close()
	if err = client.Wait(ctx); err != nil {
		panic(err)
	}
	memory := client.MemoryStats()
	fmt.Println("pool released:", memory.Accounts == 0 && memory.Total.Used == 0 && memory.Created == memory.Released)
	// Output:
	// 42
	// 0
	// 1
	// pool released: true
}
