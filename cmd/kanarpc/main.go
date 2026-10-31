// kanarpc is a JSON call/stream/health/method-list CLI for KNR2 endpoints.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"io"
	"os"
	"strings"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	address := flag.String("address", "127.0.0.1:9090", "RPC endpoint")
	transport := flag.String("transport", "tcp", "tcp, tls, quic")
	mode := flag.String("mode", "call", "call, health, methods")
	method := flag.String("method", "", "/service/method")
	request := flag.String("request", "{}", "JSON request; @file reads bounded local input")
	descriptor := flag.String("descriptor", "", "FileDescriptorSet for Protobuf calls")
	stream := flag.Bool("stream", false, "receive server stream as JSON lines")
	gzip := flag.Bool("gzip", false, "negotiate gzip")
	ca := flag.String("ca", "", "trusted PEM CA for TLS/QUIC")
	name := flag.String("server-name", "", "TLS server name")
	cert := flag.String("cert", "", "optional mTLS client certificate")
	key := flag.String("key", "", "optional mTLS client private key")
	tokenEnv := flag.String("token-env", "KANARPC_TOKEN", "environment variable containing bearer token")
	timeout := flag.Duration("timeout", 5*time.Second, "total call budget")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	cfg := rpc.ClientConfig{EnableCallOptions: true}
	if *descriptor != "" {
		if *mode != "call" {
			return errors.New("-descriptor is for call mode")
		}
		cfg.RequiredFeatures = rpc.FeatureProtobuf | rpc.FeatureServerStreaming
	}
	if *gzip {
		cfg.Compression = &rpc.CompressionConfig{}
	}
	opts := rpc.DialOptions{Client: cfg}
	kind := rpc.Transport(*transport)
	if kind != rpc.TCP {
		if *ca == "" {
			return errors.New("-ca required for TLS/QUIC")
		}
		pem, e := os.ReadFile(*ca)
		if e != nil {
			return e
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return errors.New("invalid CA")
		}
		opts.TLS.Config = &tls.Config{RootCAs: roots, ServerName: *name}
		if *cert != "" || *key != "" {
			pair, e := tls.LoadX509KeyPair(*cert, *key)
			if e != nil {
				return e
			}
			opts.TLS.Config.Certificates = []tls.Certificate{pair}
		}
	}
	caller, e := rpc.DialEndpoint(ctx, rpc.Endpoint{ID: "cli", Address: *address, Transport: kind}, opts)
	if e != nil {
		return e
	}
	pipeline, e := rpc.NewClientPipeline(rpc.AsServiceCaller(caller), rpc.PipelineConfig{Timeout: *timeout})
	if e != nil {
		caller.Close()
		return e
	}
	defer pipeline.Close()
	md := rpc.Metadata{"request-id": fmt.Sprint(time.Now().UnixNano())}
	if token := os.Getenv(*tokenEnv); token != "" {
		md["authorization"] = "Bearer " + token
	}
	ctx = rpc.WithOutgoingMetadata(ctx, md)
	switch *mode {
	case "health":
		*method = "/kanarpc.health/Check"
	case "methods":
		*method = "/kanarpc.admin/Methods"
	case "call":
	default:
		return errors.New("unknown mode")
	}
	body := []byte(*request)
	if len(body) > 0 && body[0] == '@' {
		file, e := os.Open(string(body[1:]))
		if e != nil {
			return e
		}
		defer file.Close()
		body, e = io.ReadAll(io.LimitReader(file, 1<<20+1))
		if e != nil {
			return e
		}
	}
	if len(body) > 1<<20 || !json.Valid(body) {
		return errors.New("request must be JSON within 1 MiB")
	}
	input := any(json.RawMessage(body))
	newResponse := func() any { return new(json.RawMessage) }
	encodeResponse := func(value any) ([]byte, error) { return json.Marshal(value) }
	if *descriptor != "" {
		f, e := os.Open(*descriptor)
		if e != nil {
			return e
		}
		defer f.Close()
		data, e := io.ReadAll(io.LimitReader(f, 16<<20+1))
		if e != nil {
			return e
		}
		if len(data) > 16<<20 {
			return errors.New("descriptor exceeds 16 MiB")
		}
		var set descriptorpb.FileDescriptorSet
		if e = (proto.UnmarshalOptions{RecursionLimit: 64}).Unmarshal(data, &set); e != nil {
			return e
		}
		files, e := protodesc.NewFiles(&set)
		if e != nil {
			return e
		}
		parts := strings.Split(strings.TrimPrefix(*method, "/"), "/")
		if len(parts) != 2 {
			return errors.New("expected /service/method")
		}
		d, e := files.FindDescriptorByName(protoreflect.FullName(parts[0] + "." + parts[1]))
		if e != nil {
			return e
		}
		methodDesc, ok := d.(protoreflect.MethodDescriptor)
		if !ok || methodDesc.IsStreamingClient() {
			return errors.New("descriptor must identify unary or server-streaming RPC")
		}
		message := dynamicpb.NewMessage(methodDesc.Input())
		if e = protojson.Unmarshal(body, message); e != nil {
			return e
		}
		input = rpc.Protobuf(message)
		*stream = methodDesc.IsStreamingServer()
		newResponse = func() any { return dynamicpb.NewMessage(methodDesc.Output()) }
		encodeResponse = func(value any) ([]byte, error) { return protojson.Marshal(value.(proto.Message)) }
	}
	if *stream {
		receiver, e := pipeline.OpenStream(ctx, *method, input)
		if e != nil {
			return e
		}
		defer receiver.Close()
		for {
			out := newResponse()
			e = receiver.Recv(out)
			if e == io.EOF {
				return nil
			}
			if e != nil {
				return e
			}
			b, e := encodeResponse(out)
			if e != nil {
				return e
			}
			fmt.Println(string(b))
		}
	}
	out := newResponse()
	if e = pipeline.Call(ctx, *method, input, out); e != nil {
		return e
	}
	b, e := encodeResponse(out)
	if e != nil {
		return e
	}
	fmt.Println(string(b))
	return nil
}
