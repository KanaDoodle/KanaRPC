// protobuf-demo runs a generated SDK against a real loopback TCP server.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/examples/echo"
)

type service struct {
	echo.UnimplementedEchoServiceServer
	canceled chan struct{}
}

func (*service) Echo(_ context.Context, in *echo.EchoRequest) (*echo.EchoResponse, error) {
	return &echo.EchoResponse{Text: in.GetText()}, nil
}

func (s *service) Watch(ctx context.Context, in *echo.EchoRequest, send func(*echo.EchoResponse) error) error {
	if in.GetText() == "cancel" {
		if err := send(&echo.EchoResponse{Text: "ready"}); err != nil {
			return err
		}
		<-ctx.Done()
		close(s.canceled)
		return ctx.Err()
	}
	for n := uint32(0); n < in.GetCount(); n++ {
		if err := send(&echo.EchoResponse{Text: in.GetText(), Sequence: n + 1}); err != nil {
			return err
		}
	}
	return nil
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server, err := rpc.NewServer(rpc.ServerConfig{})
	if err != nil {
		return err
	}
	defer server.Close()
	implementation := &service{canceled: make(chan struct{})}
	if err := echo.RegisterEchoServiceServer(server, implementation); err != nil {
		return err
	}
	if err := rpc.RegisterUnary(server, "/json/echo", func(_ context.Context, in string) (string, error) { return in, nil }); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() { server.Close(); <-done }()
	client, err := rpc.Dial(ctx, listener.Addr().String(), rpc.ClientConfig{RequiredFeatures: rpc.FeatureProtobuf | rpc.FeatureServerStreaming})
	if err != nil {
		return err
	}
	defer client.Close()
	caps := client.Capabilities()
	fmt.Printf("capabilities: protobuf=%t streaming=%t max_frame=%d\n", caps.Features&rpc.FeatureProtobuf != 0, caps.Features&rpc.FeatureServerStreaming != 0, caps.MaxFrameBytes)
	sdk := echo.NewEchoServiceClient(client)
	response, err := sdk.Echo(ctx, &echo.EchoRequest{Text: "hello protobuf"})
	if err != nil {
		return err
	}
	fmt.Println("unary:", response.GetText())
	var legacy string
	if err := client.Call(ctx, "/json/echo", "hello json", &legacy); err != nil {
		return err
	}
	fmt.Println("same connection:", legacy)
	stream, err := sdk.Watch(ctx, &echo.EchoRequest{Text: "chunk", Count: 3})
	if err != nil {
		return err
	}
	defer stream.Close()
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		fmt.Printf("stream: %s %d\n", chunk.GetText(), chunk.GetSequence())
	}
	call, stop := context.WithCancel(ctx)
	defer stop()
	cancelStream, err := sdk.Watch(call, &echo.EchoRequest{Text: "cancel"})
	if err != nil {
		return err
	}
	defer cancelStream.Close()
	if _, err := cancelStream.Recv(); err != nil {
		return err
	}
	stop()
	if _, err := cancelStream.Recv(); !errors.Is(err, context.Canceled) {
		return fmt.Errorf("expected canceled stream, got %v", err)
	}
	select {
	case <-implementation.canceled:
		fmt.Println("cancel: remote handler observed context cancellation")
	case <-ctx.Done():
		return ctx.Err()
	}
	report, err := server.DrainWithReport(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("drain: quiescent=%t lost_terminals=%d incomplete_exchanges=%d\n", report.Quiescent, report.TotalFaults.LostTerminals, report.TotalFaults.IncompleteExchanges)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
