// governance-lab measures real loopback RPC around a bounded timer/slot
// fixture. It does not load a model or claim inference throughput benefits.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/internal/testcert"
)

type request struct {
	ID      int    `json:"id"`
	Payload string `json:"payload"`
}
type ready struct {
	Endpoint rpc.Endpoint
	Admin    string
	CA       []byte
}
type backend struct {
	mu            sync.Mutex
	active, limit int
	changed       chan struct{}
}

func (b *backend) acquire(ctx context.Context) error {
	for {
		b.mu.Lock()
		if b.active < b.limit {
			b.active++
			b.mu.Unlock()
			return nil
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}
func (b *backend) release() {
	b.mu.Lock()
	b.active--
	close(b.changed)
	b.changed = make(chan struct{})
	b.mu.Unlock()
}
func (b *backend) set(limit int) {
	b.mu.Lock()
	b.limit = limit
	close(b.changed)
	b.changed = make(chan struct{})
	b.mu.Unlock()
}
func write(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(path+".tmp", append(b, '\n'), 0600); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}
func text(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
func config(profile, scene string) (rpc.GovernanceConfig, error) {
	c := rpc.GovernanceConfig{MaxExecuting: 16, QueueCapacity: 64, QueueTimeout: 60 * time.Millisecond, QueuePolicy: rpc.MethodFair, Feedback: true, SampleWindow: 16, Methods: []rpc.MethodPolicy{{Method: "/lab/fast", Target: 4 * time.Millisecond}, {Method: "/lab/slow", Target: 10 * time.Millisecond, Min: 2, Initial: 16, Max: 16}}}
	if scene == "unattainable" {
		c.Methods[1].Target = 5 * time.Millisecond
	}
	switch profile {
	case "fixed-fifo":
		c.QueuePolicy = rpc.FIFO
	case "fixed-fair":
	case "fixed4-fair":
		c.Methods[1].Min = 4
		c.Methods[1].Initial = 4
		c.Methods[1].Max = 4
	case "fixed8-fair":
		c.Methods[1].Min = 8
		c.Methods[1].Initial = 8
		c.Methods[1].Max = 8
	case "adaptive", "feedback":
		c.Adaptive = true
	case "guarded", "guarded-feedback":
		c.Adaptive = true
		c.Methods[1].FallbackLimit = 4
		c.Methods[1].FallbackWindows = 3
	default:
		return c, errors.New("unknown profile")
	}
	return c, nil
}
func main() {
	mode := flag.String("mode", "worker", "worker or client")
	kind := flag.String("transport", "tcp", "explicit transport")
	profile := flag.String("profile", "fixed-fifo", "fixed-fifo, fixed-fair, fixed4-fair, fixed8-fair, adaptive, feedback, guarded, guarded-feedback")
	scene := flag.String("scene", "capacity-shift", "low-load, steady, capacity-shift, unattainable")
	readyPath := flag.String("ready", "", "ready JSON")
	output := flag.String("output", "", "result JSON")
	duration := flag.Duration("duration", 2*time.Second, "offered duration")
	seed := flag.Uint64("seed", 7, "shuffle seed")
	flag.Parse()
	var err error
	if *readyPath == "" || *output == "" || *duration < time.Second {
		err = errors.New("explicit paths and duration>=1s required")
	} else if *mode == "worker" {
		err = worker(*profile, *scene, rpc.Transport(*kind), *readyPath, *output, *duration)
	} else if *mode == "client" {
		err = client(*profile, *scene, *readyPath, *output, *duration, *seed)
	} else {
		err = errors.New("unknown mode")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func worker(profile, scene string, kind rpc.Transport, readyPath, output string, duration time.Duration) error {
	governance, err := config(profile, scene)
	if err != nil {
		return err
	}
	s, err := rpc.NewServer(rpc.ServerConfig{MaxInFlight: 128, MaxPerConnection: 96, Governance: &governance})
	if err != nil {
		return err
	}
	resources := &backend{limit: 8, changed: make(chan struct{})}
	if scene == "unattainable" {
		resources.limit = 4
	}
	var mu sync.Mutex
	seen := map[int]bool{}
	duplicates := 0
	attempted, finished, canceled := map[string]int{}, map[string]int{}, map[string]int{}
	for _, path := range []string{"/lab/fast", "/lab/slow"} {
		path := path
		err = rpc.RegisterUnary(s, path, func(ctx context.Context, in request) (request, error) {
			mu.Lock()
			if seen[in.ID] {
				duplicates++
			}
			seen[in.ID] = true
			attempted[path]++
			mu.Unlock()
			err := func() error {
				if path == "/lab/slow" {
					if e := resources.acquire(ctx); e != nil {
						return e
					}
					defer resources.release()
				}
				delay := time.Millisecond
				if path == "/lab/slow" {
					delay = 4 * time.Millisecond
					if scene == "unattainable" {
						delay = 20 * time.Millisecond
					}
				}
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-timer.C:
					return nil
				}
			}()
			mu.Lock()
			if err != nil {
				canceled[path]++
			} else {
				finished[path]++
			}
			mu.Unlock()
			return in, err
		})
		if err != nil {
			return err
		}
	}
	credentials, err := testcert.New()
	if err != nil {
		return err
	}
	serve := make(chan error, 1)
	meta := ready{}
	if kind == rpc.QUIC {
		l, e := rpc.ListenQUIC("127.0.0.1:0", rpc.TLSOptions{Config: credentials.Server}, rpc.QUICConfig{MaxStreams: 128})
		if e != nil {
			return e
		}
		meta.Endpoint = rpc.Endpoint{Address: l.Addr().String(), Transport: kind}
		go func() { serve <- s.ServeQUIC(l) }()
	} else {
		if kind != rpc.TCP && kind != rpc.TLS {
			return errors.New("unknown transport")
		}
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			return e
		}
		meta.Endpoint = rpc.Endpoint{Address: l.Addr().String(), Transport: kind}
		if kind == rpc.TLS {
			go func() { serve <- s.ServeTLS(l, rpc.TLSOptions{Config: credentials.Server}) }()
		} else {
			go func() { serve <- s.Serve(l) }()
		}
	}
	defer s.Close()
	runContext, stopRun := context.WithCancel(context.Background())
	defer stopRun()
	var begin sync.Once
	var phases sync.WaitGroup
	admin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	snapshot := func() map[string]any {
		return map[string]any{"stats": s.Stats(), "memory": s.MemoryStats(), "governance": s.Governance()}
	}
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(snapshot()) })
	mux.HandleFunc("/begin", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(405)
			return
		}
		begin.Do(func() {
			if scene == "capacity-shift" {
				phases.Add(1)
				go func() {
					defer phases.Done()
					for _, limit := range []int{2, 8} {
						timer := time.NewTimer(duration / 3)
						select {
						case <-runContext.Done():
							timer.Stop()
							return
						case <-timer.C:
							resources.set(limit)
						}
					}
				}()
			}
		})
		w.WriteHeader(204)
	})
	web := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	webDone := make(chan error, 1)
	go func() { webDone <- web.Serve(admin) }()
	defer func() { _ = web.Close(); <-webDone }()
	meta.Admin = "http://" + admin.Addr().String()
	meta.CA = credentials.CAPEM
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
	defer signal.Stop(signals)
	limit := time.Now().Add(time.Second)
	for !s.Ready() && time.Now().Before(limit) {
		time.Sleep(time.Millisecond)
	}
	if !s.Ready() {
		return errors.New("server not ready")
	}
	if err = write(readyPath, meta); err != nil {
		return err
	}
	select {
	case <-signals:
	case e := <-serve:
		return fmt.Errorf("early serve exit: %v", e)
	}
	stopRun()
	phases.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	drainErr := s.Drain(ctx)
	serveErr := <-serve
	mu.Lock()
	result := snapshot()
	result["attempted"] = attempted
	result["finished"] = finished
	result["canceled"] = canceled
	result["duplicate_ids"] = duplicates
	result["unique_executions"] = len(seen)
	result["drain_error"] = text(drainErr)
	mu.Unlock()
	if err = write(output, result); err != nil {
		return err
	}
	if drainErr != nil {
		return drainErr
	}
	return serveErr
}

type offer struct {
	ID                                    int
	Method                                string
	Scheduled, Started, Latency, Lateness int64
	Result                                string
}

func client(profile, scene, readyPath, output string, duration time.Duration, seed uint64) error {
	b, err := os.ReadFile(readyPath)
	if err != nil {
		return err
	}
	var meta ready
	if err = json.Unmarshal(b, &meta); err != nil {
		return err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(meta.CA) {
		return errors.New("bad lab CA")
	}
	clients := make([]rpc.Caller, 4)
	for i := range clients {
		cfg := rpc.ClientConfig{Timeout: 250 * time.Millisecond, MaxInFlight: 128}
		if profile == "feedback" || profile == "guarded-feedback" {
			cfg.Feedback = &rpc.FeedbackConfig{}
		}
		clients[i], err = rpc.DialEndpoint(context.Background(), meta.Endpoint, rpc.DialOptions{Client: cfg, TLS: rpc.TLSOptions{Config: &tls.Config{RootCAs: pool, ServerName: "localhost"}}})
		if err != nil {
			return err
		}
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()
	httpClient := &http.Client{Timeout: time.Second}
	defer httpClient.CloseIdleConnections()
	response, err := httpClient.Post(meta.Admin+"/begin", "application/json", strings.NewReader("{}"))
	if err != nil {
		return err
	}
	_ = response.Body.Close()
	if response.StatusCode != 204 {
		return errors.New("begin failed")
	}
	fastRate, slowRate := 200, 1200
	switch scene {
	case "low-load":
		fastRate, slowRate = 10, 10
	case "unattainable":
		fastRate, slowRate = 20, 200
	case "steady", "capacity-shift":
	default:
		return errors.New("unknown scene")
	}
	fastN, slowN := int(duration.Seconds()*float64(fastRate)), int(duration.Seconds()*float64(slowRate))
	total := fastN + slowN
	methods := make([]string, total)
	for i := range methods {
		methods[i] = "/lab/slow"
		if i < fastN {
			methods[i] = "/lab/fast"
		}
	}
	random := rand.New(rand.NewPCG(seed, seed+1))
	random.Shuffle(total, func(i, j int) { methods[i], methods[j] = methods[j], methods[i] })
	rows := make([]offer, total)
	slots := make(chan struct{}, 128)
	var group sync.WaitGroup
	var samples []json.RawMessage
	pollContext, stopPoll := context.WithCancel(context.Background())
	pollDone := make(chan struct{})
	begin := time.Now()
	go func() {
		defer close(pollDone)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-pollContext.Done():
				return
			case <-ticker.C:
				response, e := httpClient.Get(meta.Admin + "/stats")
				if e == nil {
					data, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
					_ = response.Body.Close()
					if json.Valid(data) {
						sample, _ := json.Marshal(struct {
							ObservedNS int64
							State      json.RawMessage
						}{time.Since(begin).Nanoseconds(), data})
						samples = append(samples, sample)
					}
				}
			}
		}
	}()
	payload := strings.Repeat("x", 128)
	for i, path := range methods {
		scheduled := begin.Add(time.Duration(int64(duration) * int64(i) / int64(total)))
		if wait := time.Until(scheduled); wait > 0 {
			time.Sleep(wait)
		}
		now := time.Now()
		row := offer{ID: i + 1, Method: path, Scheduled: scheduled.Sub(begin).Nanoseconds(), Started: now.Sub(begin).Nanoseconds(), Lateness: now.Sub(scheduled).Nanoseconds()}
		if now.Sub(scheduled) > 50*time.Millisecond {
			row.Result = "generator_late"
			rows[i] = row
			continue
		}
		select {
		case slots <- struct{}{}:
		default:
			row.Result = "generator_limited"
			rows[i] = row
			continue
		}
		group.Add(1)
		go func(index int, row offer) {
			defer group.Done()
			defer func() { <-slots }()
			ctx, cancel := context.WithDeadline(context.Background(), scheduled.Add(250*time.Millisecond))
			defer cancel()
			var out request
			start := time.Now()
			row.Started = start.Sub(begin).Nanoseconds()
			row.Lateness = start.Sub(scheduled).Nanoseconds()
			e := clients[index%len(clients)].Call(ctx, row.Method, request{ID: row.ID, Payload: payload}, &out)
			row.Latency = time.Since(start).Nanoseconds()
			row.Result = "ok"
			if e != nil {
				var status *rpc.Status
				if errors.As(e, &status) {
					row.Result = string(status.Code)
					if strings.Contains(status.Message, "local pressure") {
						row.Result = "local_pressure"
					}
				} else if errors.Is(e, context.DeadlineExceeded) {
					row.Result = "deadline_exceeded"
				} else if errors.Is(e, context.Canceled) {
					row.Result = "canceled"
				} else {
					row.Result = "transport_error:" + e.Error()
				}
			} else if out.ID != row.ID || out.Payload != payload {
				row.Result = "corrupt_response"
			}
			rows[index] = row
		}(i, row)
	}
	group.Wait()
	elapsed := time.Since(begin)
	stopPoll()
	<-pollDone
	clientState := make([]map[string]any, 0, 4)
	for _, client := range clients {
		client.Close()
		state := map[string]any{}
		switch c := client.(type) {
		case *rpc.Client:
			state["rpc"] = c.Stats()
			state["memory"] = c.MemoryStats()
			state["feedback"] = c.FeedbackStats()
		case *rpc.QUICClient:
			state["rpc"] = c.Stats()
			state["memory"] = c.MemoryStats()
			state["feedback"] = c.FeedbackStats()
		}
		clientState = append(clientState, state)
	}
	counts := map[string]map[string]int{}
	percentiles := map[string]map[string]int64{}
	for _, path := range []string{"/lab/fast", "/lab/slow"} {
		counts[path] = map[string]int{}
		var latencies []int64
		for _, row := range rows {
			if row.Method != path {
				continue
			}
			counts[path][row.Result]++
			if row.Result == "ok" {
				latencies = append(latencies, row.Latency)
			}
		}
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		p := func(n int) int64 {
			if len(latencies) == 0 {
				return 0
			}
			return latencies[(len(latencies)-1)*n/100]
		}
		percentiles[path] = map[string]int64{"p50": p(50), "p95": p(95), "p99": p(99)}
	}
	return write(output, map[string]any{"profile": profile, "scene": scene, "offered": total, "elapsed_ns": elapsed.Nanoseconds(), "counts": counts, "latency_success_only": percentiles, "offers": rows, "samples": samples, "clients": clientState, "evidence": "real loopback open-loop RPC around timer/dynamic-slot fixture; no model or production-capacity claim"})
}
