// production-demo executes the complete local integration chain. Certificates
// are ephemeral lab credentials. No external model or production load is used.
package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/discovery"
	"github.com/KanaDoodle/KanaRPC/examples/echo"
	"github.com/KanaDoodle/KanaRPC/internal/testcert"
	"github.com/KanaDoodle/KanaRPC/observability"
	"github.com/KanaDoodle/KanaRPC/routing"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type exporter struct {
	mu    sync.Mutex
	count int
}

func (e *exporter) ExportSpans(_ context.Context, s []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	e.count += len(s)
	e.mu.Unlock()
	return nil
}
func (e *exporter) Shutdown(context.Context) error { return nil }
func (e *exporter) Count() int                     { e.mu.Lock(); defer e.mu.Unlock(); return e.count }

type app struct {
	echo.UnimplementedEchoServiceServer
}

func (app) Echo(ctx context.Context, in *echo.EchoRequest) (*echo.EchoResponse, error) {
	if _, ok := rpc.AuthenticatedIdentity(ctx); !ok {
		return nil, errors.New("missing verified identity")
	}
	if e := rpc.SetTrailer(ctx, rpc.Metadata{"request-id": rpc.IncomingMetadata(ctx)["request-id"]}); e != nil {
		return nil, e
	}
	return &echo.EchoResponse{Text: in.GetText()}, nil
}
func (app) Watch(ctx context.Context, in *echo.EchoRequest, send func(*echo.EchoResponse) error) error {
	for i := uint32(0); i < in.GetCount(); i++ {
		if e := send(&echo.EchoResponse{Text: in.GetText(), Sequence: i}); e != nil {
			return e
		}
	}
	return ctx.Err()
}

type ready struct {
	Endpoint  rpc.Endpoint
	Admin, CA string
}
type staticSource struct{ node rpc.Endpoint }

func (s staticSource) Snapshot() discovery.Snapshot {
	return discovery.Snapshot{Ready: true, Revision: 1, ClusterID: 1, Endpoints: []rpc.Endpoint{s.node}}
}
func must(e error) {
	if e != nil {
		panic(e)
	}
}
func save(path string, value any) {
	b, e := json.MarshalIndent(value, "", "  ")
	must(e)
	if path != "" {
		must(os.WriteFile(path+".tmp", append(b, '\n'), 0600))
		must(os.Rename(path+".tmp", path))
	} else {
		fmt.Println(string(b))
	}
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	mode := flag.String("mode", "self", "self, server, client")
	kind := flag.String("transport", "tcp", "tcp, tls, quic")
	address := flag.String("listen", "127.0.0.1:0", "loopback RPC listener")
	readyPath := flag.String("ready", "", "ready receipt for two-process mode")
	output := flag.String("out", "", "JSON result path")
	etcd := flag.String("etcd", "", "optional real etcd endpoint")
	namespace := flag.String("namespace", "/kanarpc/production-demo", "isolated discovery namespace")
	flag.Parse()
	token := os.Getenv("KANARPC_DEMO_TOKEN")
	if token == "" {
		if *mode != "self" {
			return errors.New("KANARPC_DEMO_TOKEN is required in server/client mode")
		}
		var b [32]byte
		_, e := rand.Read(b[:])
		if e != nil {
			return e
		}
		token = hex.EncodeToString(b[:])
	}
	if *mode != "self" && *mode != "server" && *mode != "client" {
		return errors.New("invalid mode")
	}
	exp := new(exporter)
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer provider.Shutdown(context.Background())
	hook, e := observability.Interceptor(observability.Config{TracerProvider: provider})
	if e != nil {
		return e
	}
	var reg *discovery.Registry
	if *etcd != "" {
		reg, e = discovery.New(context.Background(), discovery.Config{Endpoints: []string{*etcd}, Namespace: *namespace})
		if e != nil {
			return e
		}
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = reg.Close(ctx)
		}()
	}
	var s *rpc.Server
	var registration *discovery.Publisher
	var info ready
	var serveDone chan error
	var admin *http.Server
	if *mode != "client" {
		rate, e := rpc.NewRateLimiter(map[string]rpc.RateSpec{"*": {PerSecond: 1000, Burst: 128}})
		if e != nil {
			return e
		}
		metrics := rpc.NewMetrics(128)
		s, e = rpc.NewServer(rpc.ServerConfig{Compression: &rpc.CompressionConfig{}, RateLimiter: rate, Interceptors: []rpc.Interceptor{hook, metrics.Interceptor(), observability.LoggingInterceptor(slog.New(slog.NewJSONHandler(os.Stderr, nil)))}, Authenticate: func(ctx context.Context, _ rpc.CallInfo) (rpc.Identity, error) {
			if subtle.ConstantTimeCompare([]byte(rpc.IncomingMetadata(ctx)["authorization"]), []byte("Bearer "+token)) != 1 {
				return rpc.Identity{}, &rpc.Status{Code: rpc.Unauthenticated}
			}
			return rpc.Identity{Subject: "demo-client"}, nil
		}})
		if e != nil {
			return e
		}
		defer s.Close()
		metrics.AttachServer(s)
		must(echo.RegisterEchoServiceServer(s, app{}))
		must(rpc.RegisterHealth(s))
		must(rpc.RegisterIntrospection(s))
		must(rpc.RegisterUnary(s, "/demo/json", func(ctx context.Context, value json.RawMessage) (json.RawMessage, error) { return value, ctx.Err() }))
		cert, e := testcert.New()
		if e != nil {
			return e
		}
		serveDone = make(chan error, 1)
		transport := rpc.Transport(*kind)
		if transport == rpc.QUIC {
			l, e := rpc.ListenQUIC(*address, rpc.TLSOptions{Config: cert.Server}, rpc.QUICConfig{KeepAlivePeriod: 5 * time.Second})
			if e != nil {
				return e
			}
			info.Endpoint = rpc.Endpoint{ID: "production-demo", Address: l.Addr().String(), Transport: transport}
			go func() { serveDone <- s.ServeQUIC(l) }()
		} else if transport == rpc.TCP || transport == rpc.TLS {
			l, e := net.Listen("tcp", *address)
			if e != nil {
				return e
			}
			info.Endpoint = rpc.Endpoint{ID: "production-demo", Address: l.Addr().String(), Transport: transport}
			go func() {
				if transport == rpc.TLS {
					serveDone <- s.ServeTLS(l, rpc.TLSOptions{Config: cert.Server})
				} else {
					serveDone <- s.Serve(l)
				}
			}()
		} else {
			return errors.New("unknown transport")
		}
		until := time.Now().Add(3 * time.Second)
		for !s.Ready() {
			if time.Now().After(until) {
				return errors.New("server did not become ready")
			}
			time.Sleep(time.Millisecond)
		}
		dir, e := os.MkdirTemp("", "kanarpc-demo-ca-")
		if e != nil {
			return e
		}
		defer os.RemoveAll(dir)
		info.CA = filepath.Join(dir, "ca.pem")
		must(os.WriteFile(info.CA, cert.CAPEM, 0600))
		listener, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			return e
		}
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics)
		mux.Handle("/healthz", s.HealthHandler())
		admin = &http.Server{Handler: mux, ReadHeaderTimeout: time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, IdleTimeout: 10 * time.Second, MaxHeaderBytes: 8192}
		go admin.Serve(listener)
		defer admin.Close()
		info.Admin = "http://" + listener.Addr().String()
		if reg != nil {
			registration, e = reg.Publish(context.Background(), s, "production-demo", info.Endpoint, 5, 50*time.Millisecond)
			if e != nil {
				return e
			}
		}
		if registration != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			e = registration.WaitReady(ctx)
			cancel()
			if e != nil {
				return e
			}
		}
		if *readyPath != "" {
			save(*readyPath, info)
		}
	} else {
		if *readyPath == "" {
			return errors.New("-ready required")
		}
		b, e := os.ReadFile(*readyPath)
		if e != nil {
			return e
		}
		if e = json.Unmarshal(b, &info); e != nil {
			return e
		}
	}
	if *mode != "server" {
		result, e := exercise(info, reg, token, hook)
		if e != nil {
			return e
		}
		if s != nil {
			s.SetServing(false)
			if registration != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				e = registration.Close(ctx)
				cancel()
				if e != nil {
					return e
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			report, e := s.DrainWithReport(ctx)
			cancel()
			if e != nil {
				return e
			}
			result["drain"] = report
			result["compression"] = s.CompressionStats()
			must(<-serveDone)
		}
		result["captured_spans"] = exp.Count()
		result["scope"] = "real local RPC and OTel instrumentation; ephemeral certificates; no model inference or WAN claim"
		save(*output, result)
		return nil
	}
	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-signalCtx.Done()
	s.SetServing(false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if registration != nil {
		if e = registration.Close(ctx); e != nil {
			return e
		}
	}
	report, e := s.DrainWithReport(ctx)
	save(*output, map[string]any{"drain": report, "captured_spans": exp.Count()})
	if e != nil {
		return e
	}
	return <-serveDone
}
func exercise(info ready, reg *discovery.Registry, token string, hook rpc.Interceptor) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := rpc.ClientConfig{RequiredFeatures: rpc.FeatureProtobuf | rpc.FeatureServerStreaming, EnableCallOptions: true, Compression: &rpc.CompressionConfig{}}
	options := rpc.DialOptions{Client: cfg, QUIC: rpc.QUICConfig{KeepAlivePeriod: 5 * time.Second}}
	if info.Endpoint.Transport != rpc.TCP {
		pem, e := os.ReadFile(info.CA)
		if e != nil {
			return nil, e
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("invalid CA")
		}
		options.TLS.Config = &tls.Config{RootCAs: roots, ServerName: "localhost"}
	}
	var source routing.Source = staticSource{info.Endpoint}
	var resolver *discovery.Resolver
	var e error
	if reg != nil {
		resolver, e = reg.Watch(ctx, "production-demo", info.Endpoint.Transport)
		if e != nil {
			return nil, e
		}
		defer func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = resolver.Close(stopCtx)
		}()
		source = resolver
	}
	pool, e := routing.New(ctx, source, routing.Config{Transport: info.Endpoint.Transport, Dial: options, Policy: routing.LeastOutstanding, Circuit: &routing.CircuitConfig{CountUnavailable: true}, IdleTimeout: time.Minute})
	if e != nil {
		return nil, e
	}
	defer func() {
		pool.Close()
		wait, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		_ = pool.Wait(wait)
	}()
	pipeline, e := rpc.NewClientPipeline(pool, rpc.PipelineConfig{Interceptors: []rpc.Interceptor{hook, observability.LoggingInterceptor(slog.New(slog.NewJSONHandler(os.Stderr, nil))), rpc.Credentials(func(context.Context) (rpc.Metadata, error) {
		return rpc.Metadata{"authorization": "Bearer " + token}, nil
	})}})
	if e != nil {
		return nil, e
	}
	defer pipeline.Close()
	sdk := echo.NewEchoServiceClient(pipeline)
	capture := new(rpc.MetadataCapture)
	callctx := rpc.WithOutgoingMetadata(ctx, rpc.Metadata{"request-id": "complete-chain"})
	text := strings.Repeat("KanaRPC-", 8192)
	reply, e := sdk.Echo(rpc.CaptureTrailers(callctx, capture), &echo.EchoRequest{Text: text})
	if e != nil || reply.GetText() != text {
		return nil, fmt.Errorf("echo validation: %w", e)
	}
	if capture.Snapshot()["request-id"] != "complete-chain" {
		return nil, errors.New("response metadata missing")
	}
	stream, e := sdk.Watch(callctx, &echo.EchoRequest{Text: text, Count: 20})
	if e != nil {
		return nil, e
	}
	for n := uint32(0); n < 20; n++ {
		r, e := stream.Recv()
		if e != nil || r.GetSequence() != n || r.GetText() != text {
			return nil, fmt.Errorf("stream validation: %w", e)
		}
	}
	if _, e = stream.Recv(); e != io.EOF {
		return nil, fmt.Errorf("stream terminal: %w", e)
	}
	must(stream.Close())
	stream, e = sdk.Watch(callctx, &echo.EchoRequest{Text: text, Count: 100000})
	if e != nil {
		return nil, e
	}
	if _, e = stream.Recv(); e != nil {
		return nil, e
	}
	if e = stream.Close(); e != nil {
		return nil, e
	}
	if e = pipeline.Wait(ctx); e != nil {
		return nil, e
	}
	var health rpc.HealthStatus
	if e = pipeline.Call(callctx, "/kanarpc.health/Check", struct{}{}, &health); e != nil || !health.Serving {
		return nil, fmt.Errorf("health: %w", e)
	}
	var methods []rpc.MethodInfo
	if e = pipeline.Call(callctx, "/kanarpc.admin/Methods", struct{}{}, &methods); e != nil || len(methods) < 4 {
		return nil, fmt.Errorf("methods: %w", e)
	}
	client := &http.Client{Timeout: time.Second}
	response, e := client.Get(info.Admin + "/metrics")
	if e != nil {
		return nil, e
	}
	data, e := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	response.Body.Close()
	if e != nil || !strings.Contains(string(data), "kanarpc_calls_completed_total") {
		return nil, errors.New("metrics missing")
	}
	return map[string]any{"passed": true, "transport": info.Endpoint.Transport, "discovery_etcd": reg != nil, "response_bytes": len(text), "stream_chunks": 20, "cancel": "completed", "health": health, "methods": methods, "routes": pool.RouteStates(), "pool": pool.Stats(), "trailers": capture.Snapshot()}, nil
}
