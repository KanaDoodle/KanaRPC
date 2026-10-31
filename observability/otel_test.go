package observability_test

import (
	"context"
	"net"
	"testing"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/observability"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestRealClientServerTraceParentAndMetric(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer tp.Shutdown(context.Background())
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())
	hook, e := observability.Interceptor(observability.Config{TracerProvider: tp, MeterProvider: mp})
	if e != nil {
		t.Fatal(e)
	}
	s, e := rpc.NewServer(rpc.ServerConfig{Interceptors: []rpc.Interceptor{hook}})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = rpc.RegisterUnary(s, "/trace/echo", func(ctx context.Context, n int) (int, error) {
		if rpc.IncomingMetadata(ctx)["traceparent"] == "" {
			t.Error("missing propagated trace")
		}
		return n, nil
	}); e != nil {
		t.Fatal(e)
	}
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	go s.Serve(l)
	c, e := rpc.Dial(ctx, l.Addr().String(), rpc.ClientConfig{EnableCallOptions: true})
	if e != nil {
		t.Fatal(e)
	}
	p, e := rpc.NewClientPipeline(c, rpc.PipelineConfig{Interceptors: []rpc.Interceptor{hook}})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	var out int
	if e = p.Call(ctx, "/trace/echo", 42, &out); e != nil || out != 42 {
		t.Fatal(e, out)
	}
	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatal("expected actual server+client spans", len(spans))
	}
	if spans[0].SpanContext.TraceID() != spans[1].SpanContext.TraceID() || spans[0].Parent.SpanID() != spans[1].SpanContext.SpanID() {
		t.Fatal("disconnected traces", spans)
	}
	var data metricdata.ResourceMetrics
	if e = reader.Collect(ctx, &data); e != nil || len(data.ScopeMetrics) == 0 {
		t.Fatal(e, data)
	}
	p.Close()
	if _, e = s.DrainWithReport(ctx); e != nil {
		t.Fatal(e)
	}
}
