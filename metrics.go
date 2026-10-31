package kanarpc

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
)

type metricKey struct {
	Method, Side string
	Code         Code
}
type metricValue struct {
	Count, Attempts uint64
	Sum             float64
	Buckets         [8]uint64
}

var latencyBuckets = [8]float64{.001, .005, .01, .025, .05, .1, .5, 1}

// Metrics exports bounded Prometheus exposition. Auth/body/identity values are
// never labels. Server hooks measure worker time; core admission/fault gauges
// are exported independently so refusals remain visible.
type Metrics struct {
	mu     sync.Mutex
	max    int
	values map[metricKey]metricValue
	server *Server
}

func NewMetrics(maxSeries int) *Metrics {
	if maxSeries < 1 || maxSeries > 4096 {
		maxSeries = 1024
	}
	return &Metrics{max: maxSeries, values: map[metricKey]metricValue{}}
}
func (m *Metrics) AttachServer(s *Server) { m.mu.Lock(); m.server = s; m.mu.Unlock() }
func (m *Metrics) Interceptor() Interceptor {
	return func(ctx context.Context, info CallInfo) (context.Context, func(CallResult), error) {
		return ctx, func(r CallResult) {
			code := statusOf(r.Err).Code
			k := metricKey{info.Method, info.Side, code}
			seconds := r.Duration.Seconds()
			m.mu.Lock()
			defer m.mu.Unlock()
			if _, ok := m.values[k]; !ok && len(m.values) >= m.max {
				k.Method = "_other"
			}
			v := m.values[k]
			v.Count++
			v.Attempts += uint64(r.Attempts)
			v.Sum += seconds
			for i, b := range latencyBuckets {
				if seconds <= b {
					v.Buckets[i]++
				}
			}
			m.values[k] = v
		}, nil
	}
}
func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	values := make(map[metricKey]metricValue, len(m.values))
	for k, v := range m.values {
		values[k] = v
	}
	s := m.server
	m.mu.Unlock()
	keys := make([]metricKey, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j]) })
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintln(w, "# TYPE kanarpc_calls_completed_total counter\n# TYPE kanarpc_call_duration_seconds histogram")
	for _, k := range keys {
		v := values[k]
		labels := fmt.Sprintf("method=%s,side=%s,code=%s", strconv.Quote(k.Method), strconv.Quote(k.Side), strconv.Quote(string(k.Code)))
		fmt.Fprintf(w, "kanarpc_calls_completed_total{%s} %d\nkanarpc_attempts_total{%s} %d\n", labels, v.Count, labels, v.Attempts)
		for i, b := range latencyBuckets {
			fmt.Fprintf(w, "kanarpc_call_duration_seconds_bucket{%s,le=%q} %d\n", labels, strconv.FormatFloat(b, 'g', -1, 64), v.Buckets[i])
		}
		fmt.Fprintf(w, "kanarpc_call_duration_seconds_bucket{%s,le=\"+Inf\"} %d\nkanarpc_call_duration_seconds_sum{%s} %g\nkanarpc_call_duration_seconds_count{%s} %d\n", labels, v.Count, labels, v.Sum, labels, v.Count)
	}
	if s != nil {
		v := s.Stats()
		health := s.Health()
		ready := 0
		if health.Serving {
			ready = 1
		}
		fmt.Fprintf(w, "kanarpc_serving %d\nkanarpc_inflight %d\nkanarpc_queued %d\nkanarpc_executing %d\nkanarpc_rejected_total %d\nkanarpc_lost_terminals_total %d\nkanarpc_incomplete_exchanges_total %d\n", ready, v.InFlight, v.Queued, v.Executing, v.Rejected, v.LostTerminals, v.IncompleteExchanges)
		compression := s.CompressionStats()
		fmt.Fprintf(w, "kanarpc_gzip_prepared_total %d\nkanarpc_body_plain_bytes_total %d\nkanarpc_body_encoded_bytes_total %d\n", compression.GzipPrepared, compression.PlainBytes, compression.EncodedBodyBytes)
		mem := s.MemoryStats()
		for name, b := range map[string]ByteStats{"encoding": mem.Encoding, "receive": mem.Receive, "send": mem.Send, "control_receive": mem.ControlReceive, "control_send": mem.ControlSend} {
			fmt.Fprintf(w, "kanarpc_budget_used_bytes{class=%q} %d\nkanarpc_budget_rejected_total{class=%q} %d\n", name, b.Used, name, b.Rejected)
		}
		fmt.Fprintf(w, "kanarpc_rate_rejected_total %d\n", s.cfg.RateLimiter.Stats().Rejected)
	}
}

// HealthHandler is for an explicitly bound administrative listener.
func (s *Server) HealthHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v := s.Health()
		if !v.Serving {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		fmt.Fprintln(w, v.State)
	})
}
