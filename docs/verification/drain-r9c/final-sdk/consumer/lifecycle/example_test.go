package kanarpc_test

import (
	"context"
	"fmt"
	"net"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
)

func ExampleServer_DrainWithReport() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := rpc.NewServer(rpc.ServerConfig{})
	if err != nil {
		panic(err)
	}
	if err := rpc.RegisterUnary(s, "/example/echo", func(_ context.Context, n int) (int, error) { return n, nil }); err != nil {
		panic(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	defer s.Close()
	c, err := rpc.Dial(ctx, l.Addr().String(), rpc.ClientConfig{})
	if err != nil {
		panic(err)
	}
	defer c.Close()
	var out int
	if err := c.Call(ctx, "/example/echo", 42, &out); err != nil {
		panic(err)
	}
	// Use a fresh shutdown context, not a canceled request context.
	shutdown, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	report, err := s.DrainWithReport(shutdown)
	if err != nil {
		panic(err)
	} // Inspect errors.Is(err, rpc.ErrDelivery) and ctx errors separately.
	if err := <-done; err != nil {
		panic(err)
	}
	fmt.Println(out, report.Quiescent, report.TotalFaults.LostTerminals)
	// Output: 42 true 0
}
