package kanarpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/testcert"
	"github.com/KanaDoodle/KanaRPC/internal/wire"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func productionFixture(t *testing.T, transport string, register func(*Server)) (*Server, Caller, *Metrics) {
	t.Helper()
	metrics := NewMetrics(64)
	cfg := ServerConfig{IO: IOConfig{MaxFrameBytes: 64 << 10}, Compression: &CompressionConfig{}, Interceptors: []Interceptor{metrics.Interceptor()}, Authenticate: func(ctx context.Context, info CallInfo) (Identity, error) {
		if IncomingMetadata(ctx)["authorization"] != "Bearer fixture" {
			return Identity{}, &Status{Code: Unauthenticated}
		}
		return Identity{Subject: "tester"}, nil
	}}
	client := ClientConfig{RequiredFeatures: FeatureProtobuf | FeatureServerStreaming, EnableCallOptions: true, Compression: &CompressionConfig{}, IO: IOConfig{MaxFrameBytes: 64 << 10}, StreamWindow: 4}
	var s *Server
	var caller Caller
	if transport == "quic" {
		s, caller, _ = startQUIC(t, cfg, QUICConfig{}, register, client)
	} else if transport == "tcp" {
		var address string
		s, address = start(t, cfg, register)
		caller = connect(t, address, client)
	} else {
		cert, e := testcert.New()
		require(t, e)
		s, e = NewServer(cfg)
		require(t, e)
		register(s)
		l, e := net.Listen("tcp", "127.0.0.1:0")
		require(t, e)
		done := make(chan error, 1)
		go func() { done <- s.ServeTLS(l, TLSOptions{Config: cert.Server}) }()
		eventually(t, s.Ready)
		c, e := DialTLS(context.Background(), l.Addr().String(), TLSOptions{Config: cert.Client}, client)
		require(t, e)
		caller = c
		t.Cleanup(func() {
			c.Close()
			s.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			require(t, s.Wait(ctx))
			require(t, <-done)
		})
	}
	metrics.AttachServer(s)
	return s, caller, metrics
}
func productionContext() context.Context {
	return WithOutgoingMetadata(context.Background(), Metadata{"authorization": "Bearer fixture", "request-id": "production-test"})
}
func TestProductionChainAcrossTransports(t *testing.T) {
	for _, transport := range []string{"tcp", "tls", "quic"} {
		t.Run(transport, func(t *testing.T) {
			var handlerCalls atomic.Int64
			s, c, metrics := productionFixture(t, transport, func(s *Server) {
				require(t, RegisterHealth(s))
				require(t, RegisterIntrospection(s))
				require(t, RegisterUnary(s, "/production/echo", func(ctx context.Context, in string) (string, error) {
					handlerCalls.Add(1)
					id, ok := AuthenticatedIdentity(ctx)
					if !ok || id.Subject != "tester" {
						return "", errors.New("missing identity")
					}
					if IncomingMetadata(ctx)["request-id"] != "production-test" {
						return "", errors.New("metadata lost")
					}
					if e := SetTrailer(ctx, Metadata{"request-id": "production-test"}); e != nil {
						return "", e
					}
					return in, nil
				}))
				require(t, RegisterProtobufUnary(s, "/production/proto", func() *wrapperspb.StringValue { return new(wrapperspb.StringValue) }, func(ctx context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) { return in, nil }))
				require(t, RegisterStream(s, "/production/items", func(ctx context.Context, n int, send func(string) error) error {
					for i := 0; i < n; i++ {
						if e := send(strings.Repeat("x", 8192)); e != nil {
							return e
						}
					}
					return nil
				}))
			})
			hooks := NewMetrics(64)
			pipeline, e := NewClientPipeline(AsServiceCaller(c), PipelineConfig{Interceptors: []Interceptor{hooks.Interceptor()}})
			require(t, e)
			defer pipeline.Close()
			ctx := productionContext()
			var capture MetadataCapture
			ctx = CaptureTrailers(ctx, &capture)
			text := strings.Repeat("payload-", 4096)
			var out string
			require(t, pipeline.Call(ctx, "/production/echo", text, &out))
			if out != text || capture.Snapshot()["request-id"] != "production-test" {
				t.Fatal(len(out), capture.Snapshot())
			}
			var protoOut wrapperspb.StringValue
			require(t, pipeline.Call(ctx, "/production/proto", Protobuf(wrapperspb.String(text)), &protoOut))
			if protoOut.Value != text {
				t.Fatal("protobuf mismatch")
			}
			var status *Status
			if e = pipeline.Call(context.Background(), "/production/echo", text, &out); !errors.As(e, &status) || status.Code != Unauthenticated || handlerCalls.Load() != 1 {
				t.Fatal(e, handlerCalls.Load())
			}
			stream, e := pipeline.OpenStream(ctx, "/production/items", 20)
			require(t, e)
			for n := 0; n < 20; n++ {
				var chunk string
				require(t, stream.Recv(&chunk))
				if len(chunk) != 8192 {
					t.Fatal(len(chunk))
				}
			}
			if e = stream.Recv(nil); e != io.EOF {
				t.Fatal(e)
			}
			require(t, stream.Close())
			stream, e = pipeline.OpenStream(ctx, "/production/items", 100000)
			require(t, e)
			require(t, stream.Recv(&out))
			require(t, stream.Close())
			eventually(t, func() bool { return s.Stats().InFlight == 0 })
			s.SetServing(false)
			var health HealthStatus
			require(t, pipeline.Call(ctx, "/kanarpc.health/Check", struct{}{}, &health))
			if health.Serving {
				t.Fatal(health)
			}
			if e = pipeline.Call(ctx, "/production/echo", text, &out); !errors.As(e, &status) || status.Code != Unavailable {
				t.Fatal(e)
			}
			s.SetServing(true)
			require(t, pipeline.Wait(context.Background()))
			http := httptest.NewRecorder()
			metrics.ServeHTTP(http, httptest.NewRequest("GET", "/metrics", nil))
			if !strings.Contains(http.Body.String(), "kanarpc_budget_used_bytes") || strings.Contains(http.Body.String(), "Bearer fixture") {
				t.Fatal(http.Body.String())
			}
			drain, e := s.DrainWithReport(context.Background())
			require(t, e)
			if !drain.Quiescent || drain.TotalFaults != (DeliveryFaults{}) {
				t.Fatal(drain)
			}
			pipeline.Close()
			memory := c.(interface{ MemoryStats() MemoryStats }).MemoryStats()
			if memory.Receive.Used != 0 || memory.Encoding.Used != 0 || memory.Send.Used != 0 {
				t.Fatal(memory)
			}
		})
	}
}

func TestProductionGzipBoundsAndCancellationIsolation(t *testing.T) {
	body := []byte(strings.Repeat("a", 32768))
	compressed, used, e := gzipBody(context.Background(), body, 1)
	require(t, e)
	if !used {
		t.Fatal("fixture did not compress")
	}
	for _, n := range []uint32{0, 1, 32767, 32769} {
		if _, e = inflateBody(context.Background(), compressed, n); e == nil {
			t.Fatal("accepted wrong plain length", n)
		}
	}
	if _, e = inflateBody(context.Background(), append(append([]byte{}, compressed...), 0), 32768); e == nil {
		t.Fatal("accepted trailing bytes")
	}
	if _, e = inflateBody(context.Background(), append(append([]byte{}, compressed...), compressed...), 32768); e == nil {
		t.Fatal("accepted second member")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	memory, _ := memoryConfig(MemoryConfig{})
	r := newResources(memory)
	c := &Client{resources: r, cfg: ClientConfig{Compression: &CompressionConfig{}}, pending: map[uint64]*pending{}, nextID: 1}
	c.link = &link{cfg: IOConfig{MaxFrameBytes: 65536}}
	p := &pending{client: c, ctx: ctx, id: 1, options: true, window: 4}
	c.pending[1] = p
	packet := make([]byte, 5+len(compressed))
	packet[0] = 1
	binary.BigEndian.PutUint32(packet[1:], 32768)
	copy(packet[5:], compressed)
	if !c.handleFrame(wire.Frame{Kind: wire.DataOptions, ID: 1, Number: 1, Payload: packet}) || c.closed || r.receive.snapshot().Used != 0 {
		t.Fatal("canceled decompression poisoned connection")
	}
}
func TestProductionMetadataRejectedBeforeEncoding(t *testing.T) {
	var encoded atomic.Int64
	_, address := start(t, ServerConfig{}, func(s *Server) { echo(s) })
	c := connect(t, address, ClientConfig{EnableCallOptions: true})
	ctx := WithOutgoingMetadata(context.Background(), Metadata{"authorization": strings.Repeat("x", MaxMetadataBytes+1)})
	var out int
	e := c.Call(ctx, "/demo/echo", countMarshal{calls: &encoded}, &out)
	if e == nil || encoded.Load() != 0 || c.Stats().Started != 0 {
		t.Fatal(e, encoded.Load(), c.Stats())
	}
}

// Local user encoder used to prove rejection order without timing assumptions.
type countMarshal struct{ calls *atomic.Int64 }

func (v countMarshal) MarshalJSON() ([]byte, error) { v.calls.Add(1); return []byte("1"), nil }
func TestProductionRateAndInterceptorBoundaries(t *testing.T) {
	limiter, e := NewRateLimiter(map[string]RateSpec{"*": {PerSecond: .001, Burst: 2}, "/demo/echo": {PerSecond: .001, Burst: 1}})
	require(t, e)
	require(t, limiter.Allow("/demo/echo"))
	var rate *RateLimitError
	if e = limiter.Allow("/demo/echo"); !errors.As(e, &rate) {
		t.Fatal(e)
	}
	require(t, limiter.Allow("/demo/other"))
	if limiter.Stats().Rejected != 1 {
		t.Fatal(limiter.Stats())
	}
	var order []int
	var mu sync.Mutex
	hook := func(n int) Interceptor {
		return func(ctx context.Context, _ CallInfo) (context.Context, func(CallResult), error) {
			mu.Lock()
			order = append(order, n)
			mu.Unlock()
			return ctx, func(CallResult) { mu.Lock(); order = append(order, -n); mu.Unlock() }, nil
		}
	}
	ctx, end, e := beginInterceptors(context.Background(), CallInfo{Started: time.Now()}, []Interceptor{hook(1), hook(2)})
	require(t, e)
	if ctx == nil {
		t.Fatal("nil context")
	}
	end(nil, 1)
	end(nil, 1)
	if fmt.Sprint(order) != "[1 2 -2 -1]" {
		t.Fatal(order)
	}
}

func TestMetadataEncodedLimitIncludesSeparators(t *testing.T) {
	metadata := Metadata{"a": strings.Repeat("x", 4082), "b": ""}
	if e := validateMetadata(metadata); e == nil {
		t.Fatal("JSON separators bypassed metadata byte limit")
	}
}

func TestCallOptionsStreamRequiresAdvertisedStreaming(t *testing.T) {
	cfg, e := normalizeClient(ClientConfig{EnableCallOptions: true})
	require(t, e)
	cfg.negotiatedFeatures = FeatureCallOptions
	res, e := newClientResources(cfg)
	require(t, e)
	var encoded atomic.Int64
	_, e = prepareRequest(context.Background(), context.Background(), cfg, res, "/demo/items", countMarshal{calls: &encoded}, true)
	if !errors.Is(e, ErrCapabilities) || encoded.Load() != 0 || res.admission.snapshot().Active != 0 {
		t.Fatal(e, encoded.Load(), res.admission.snapshot())
	}
}

func TestProductionRealRetryAndAttemptQuota(t *testing.T) {
	for _, burst := range []int{1, 2} {
		t.Run(fmt.Sprint(burst), func(t *testing.T) {
			var calls atomic.Int64
			s, address := start(t, ServerConfig{}, func(s *Server) {
				require(t, RegisterUnary(s, "/production/retry", func(ctx context.Context, n int) (int, error) {
					if calls.Add(1) == 1 {
						return 0, &Status{Code: Unavailable}
					}
					return n, nil
				}))
			})
			c := connect(t, address, ClientConfig{})
			quota, e := NewRateLimiter(map[string]RateSpec{"*": {PerSecond: .001, Burst: burst}})
			require(t, e)
			p, e := NewClientPipeline(c, PipelineConfig{RateLimiter: quota, Retry: RetryPolicy{MaxAttempts: 2, IdempotentMethods: []string{"/production/retry"}}})
			require(t, e)
			defer p.Close()
			var out int
			e = p.Call(context.Background(), "/production/retry", 42, &out)
			if burst == 1 {
				var rate *RateLimitError
				if !errors.As(e, &rate) || calls.Load() != 1 {
					t.Fatal(e, calls.Load())
				}
			} else if e != nil || out != 42 || calls.Load() != 2 {
				t.Fatal(e, out, calls.Load())
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			report, e := s.DrainWithReport(ctx)
			require(t, e)
			if !report.Quiescent || report.TotalFaults != (DeliveryFaults{}) || s.Stats().Delivered != uint64(burst) {
				t.Fatal(report, s.Stats())
			}
		})
	}
}
func TestProductionServerRatePreservesControlAndBudgetCause(t *testing.T) {
	quota, e := NewRateLimiter(map[string]RateSpec{"*": {PerSecond: .001, Burst: 1}})
	require(t, e)
	s, address := start(t, ServerConfig{RateLimiter: quota}, echo)
	c := connect(t, address, ClientConfig{})
	var out int
	require(t, c.Call(context.Background(), "/demo/echo", 7, &out))
	if e = c.Call(context.Background(), "/demo/echo", 8, &out); StatusCode(e) != ResourceExhausted {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	report, e := s.DrainWithReport(ctx)
	require(t, e)
	if quota.Stats().Rejected != 1 || s.MemoryStats().Receive.Rejected != 0 || s.MemoryStats().Send.Rejected != 0 || s.Stats().Delivered != 1 || report.TotalFaults != (DeliveryFaults{}) {
		t.Fatal(quota.Stats(), report)
	}
}

func TestServerInterceptorObservesHandlerPanic(t *testing.T) {
	observed := make(chan CallResult, 1)
	hook := func(ctx context.Context, info CallInfo) (context.Context, func(CallResult), error) {
		return ctx, func(result CallResult) { observed <- result }, nil
	}
	_, address := start(t, ServerConfig{Interceptors: []Interceptor{hook}}, func(s *Server) {
		require(t, RegisterUnary(s, "/production/panic", func(context.Context, int) (int, error) {
			panic("handler failure")
		}))
	})
	c := connect(t, address, ClientConfig{EnableCallOptions: true})
	var out int
	if e := c.Call(context.Background(), "/production/panic", 1, &out); StatusCode(e) != Internal {
		t.Fatal("client did not receive Internal", e)
	}
	select {
	case result := <-observed:
		if StatusCode(result.Err) != Internal {
			t.Fatal("server interceptor reported panic as success", result.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("missing completion hook")
	}
}
