// Package observability adapts the common KanaRPC lifecycle hook to OTel. The
// application owns providers, exporters, sampling and provider shutdown.
package observability

import (
	"context"
	"log/slog"
	"sync"

	rpc "github.com/KanaDoodle/KanaRPC"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Config struct {
	MaxMethods     int
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	Propagator     propagation.TextMapPropagator
}

func Interceptor(cfg Config) (rpc.Interceptor, error) {
	tp := cfg.TracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	mp := cfg.MeterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	prop := cfg.Propagator
	if prop == nil {
		prop = propagation.TraceContext{}
	}
	if cfg.MaxMethods < 1 || cfg.MaxMethods > 4096 {
		cfg.MaxMethods = 1024
	}
	var mu sync.Mutex
	methods := map[string]bool{}
	tracer := tp.Tracer("github.com/KanaDoodle/KanaRPC")
	meter := mp.Meter("github.com/KanaDoodle/KanaRPC")
	calls, err := meter.Int64Counter("kanarpc.rpc.completed", metric.WithDescription("Completed client calls or server workers"))
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram("kanarpc.rpc.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	queue, err := meter.Float64Histogram("kanarpc.rpc.queue.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, info rpc.CallInfo) (context.Context, func(rpc.CallResult), error) {
		kind := trace.SpanKindClient
		if info.Side == "server" {
			kind = trace.SpanKindServer
			ctx = prop.Extract(ctx, propagation.MapCarrier(rpc.IncomingMetadata(ctx)))
		}
		method := info.Method
		mu.Lock()
		if !methods[method] {
			if len(methods) >= cfg.MaxMethods {
				method = "_other"
			} else {
				methods[method] = true
			}
		}
		mu.Unlock()
		attrs := []attribute.KeyValue{attribute.String("rpc.system", "kanarpc"), attribute.String("rpc.method", method), attribute.String("rpc.side", info.Side)}
		ctx, span := tracer.Start(ctx, method, trace.WithSpanKind(kind), trace.WithAttributes(attrs...))
		if info.Side == "client" {
			md := rpc.OutgoingMetadata(ctx)
			if md == nil {
				md = rpc.Metadata{}
			}
			prop.Inject(ctx, propagation.MapCarrier(md))
			ctx = rpc.WithOutgoingMetadata(ctx, md)
		}
		return ctx, func(r rpc.CallResult) {
			defer span.End()
			code := "ok"
			if r.Err != nil {
				code = "error"
				span.SetStatus(codes.Error, "rpc failed")
			}
			labels := append(attrs, attribute.String("rpc.outcome", code), attribute.String("kanarpc.status", string(rpc.StatusCode(r.Err))))
			calls.Add(ctx, 1, metric.WithAttributes(labels...))
			duration.Record(ctx, r.Duration.Seconds(), metric.WithAttributes(labels...))
			if info.Side == "server" {
				queue.Record(ctx, info.QueueWait.Seconds(), metric.WithAttributes(labels...))
			}
		}, nil
	}, nil
}

// LoggingInterceptor correlates the common structured log with the active span.
func LoggingInterceptor(logger *slog.Logger) rpc.Interceptor {
	return func(ctx context.Context, info rpc.CallInfo) (context.Context, func(rpc.CallResult), error) {
		callLogger := logger
		if callLogger != nil {
			span := trace.SpanContextFromContext(ctx)
			if span.IsValid() {
				callLogger = callLogger.With("trace_id", span.TraceID().String(), "span_id", span.SpanID().String())
			}
		}
		return rpc.LoggingInterceptor(callLogger)(ctx, info)
	}
}
