// resource-lab is an owned loopback resource experiment, not model inference.
// It deliberately compiles against both the R8A and R8B root modules.
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
	"net"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/internal/testcert"
)

type message struct {
	ID          int64
	Payload     string
	N, Sequence int
}
type ready struct {
	Endpoint rpc.Endpoint
	Admin    string
	CA       []byte
}
type heap struct {
	Goroutines, FDs                                int
	HeapAlloc, HeapObjects, StackInuse, TotalAlloc uint64
	NumGC                                          uint32
}

func memory() heap {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fds := -1
	for _, path := range []string{"/proc/self/fd", "/dev/fd"} {
		if entries, err := os.ReadDir(path); err == nil {
			fds = len(entries)
			break
		}
	}
	return heap{runtime.NumGoroutine(), fds, m.HeapAlloc, m.HeapObjects, m.StackInuse, m.TotalAlloc, m.NumGC}
}
func save(path string, value any) error {
	b, e := json.MarshalIndent(value, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile(path, append(b, '\n'), 0600)
}
func inspect(c rpc.Caller) map[string]any {
	result := map[string]any{}
	for _, name := range []string{"Stats", "MemoryStats", "AdmissionStats"} {
		m := reflect.ValueOf(c).MethodByName(name)
		if m.IsValid() {
			v := m.Call(nil)
			result[name] = v[0].Interface()
		}
	}
	return result
}
func main() {
	mode := flag.String("mode", "worker", "worker/client")
	kind := flag.String("transport", "tcp", "tcp/tls/quic")
	readyPath := flag.String("ready", "", "ready file")
	out := flag.String("output", "", "result file")
	duration := flag.Duration("duration", 60*time.Second, "total active soak duration")
	cycles := flag.Int("cycles", 6, "load/idle cycles")
	flag.Parse()
	var err error
	if *out == "" || *readyPath == "" {
		err = errors.New("explicit paths required")
	} else if *mode == "worker" {
		err = worker(rpc.Transport(*kind), *readyPath, *out)
	} else if *mode == "client" && *duration >= time.Second && *cycles > 0 && *cycles <= 1024 && *duration/time.Duration(*cycles) >= time.Second {
		err = client(*readyPath, *out, *duration, *cycles)
	} else {
		err = errors.New("invalid arguments")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func worker(kind rpc.Transport, readyPath, output string) error {
	s, err := rpc.NewServer(rpc.ServerConfig{MaxPerConnection: 128})
	if err != nil {
		return err
	}
	defer s.Close()
	var calls atomic.Uint64
	if err = rpc.RegisterUnary(s, "/lab/echo", func(ctx context.Context, in message) (message, error) { calls.Add(1); return in, ctx.Err() }); err != nil {
		return err
	}
	for _, method := range []string{"/lab/items", "/lab/cancel"} {
		method := method
		if err = rpc.RegisterStream(s, method, func(ctx context.Context, in message, send func(message) error) error {
			calls.Add(1)
			if method == "/lab/cancel" {
				in.Sequence = 0
				if e := send(in); e != nil {
					return e
				}
				<-ctx.Done()
				return ctx.Err()
			}
			for n := 0; n < in.N; n++ {
				in.Sequence = n
				if e := send(in); e != nil {
					return e
				}
			}
			return ctx.Err()
		}); err != nil {
			return err
		}
	}
	creds, err := testcert.New()
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	meta := ready{CA: creds.CAPEM}
	if kind == rpc.QUIC {
		l, e := rpc.ListenQUIC("127.0.0.1:0", rpc.TLSOptions{Config: creds.Server}, rpc.QUICConfig{})
		if e != nil {
			return e
		}
		meta.Endpoint = rpc.Endpoint{Transport: kind, Address: l.Addr().String()}
		go func() { done <- s.ServeQUIC(l) }()
	} else {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			return e
		}
		meta.Endpoint = rpc.Endpoint{Transport: kind, Address: l.Addr().String()}
		if kind == rpc.TLS {
			go func() { done <- s.ServeTLS(l, rpc.TLSOptions{Config: creds.Server}) }()
		} else if kind == rpc.TCP {
			go func() { done <- s.Serve(l) }()
		} else {
			l.Close()
			return errors.New("bad transport")
		}
	}
	admin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	meta.Admin = "http://" + admin.Addr().String()
	snapshot := func() map[string]any {
		return map[string]any{"rpc": s.Stats(), "memory": s.MemoryStats(), "heap": memory(), "handler_calls": calls.Load()}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			runtime.GC()
		}
		_ = json.NewEncoder(w).Encode(snapshot())
	})
	web := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	webDone := make(chan error, 1)
	go func() { webDone <- web.Serve(admin) }()
	defer func() { _ = web.Close(); <-webDone }()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
	defer signal.Stop(signals)
	for until := time.Now().Add(time.Second); !s.Ready() && time.Now().Before(until); {
		time.Sleep(time.Millisecond)
	}
	if !s.Ready() {
		return errors.New("not ready")
	}
	if err = save(readyPath, meta); err != nil {
		return err
	}
	select {
	case <-signals:
	case err = <-done:
		return fmt.Errorf("early serve exit: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = s.Drain(ctx)
	serveErr := <-done
	if e := save(output, snapshot()); e != nil {
		return e
	}
	if err != nil {
		return err
	}
	return serveErr
}

type blocker struct {
	entered chan struct{}
	release <-chan struct{}
	count   *atomic.Int64
}

func (b blocker) MarshalJSON() ([]byte, error) {
	data := make([]byte, 64<<10)
	for i := range data {
		data[i] = 'x'
	}
	data[0], data[len(data)-1] = '"', '"'
	b.count.Add(1)
	b.entered <- struct{}{}
	<-b.release
	return data, nil
}
func client(readyPath, output string, duration time.Duration, cycles int) error {
	data, err := os.ReadFile(readyPath)
	if err != nil {
		return err
	}
	var meta ready
	if err = json.Unmarshal(data, &meta); err != nil {
		return err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(meta.CA) {
		return errors.New("bad CA")
	}
	dial := func(limit int) (rpc.Caller, error) {
		return rpc.DialEndpoint(context.Background(), meta.Endpoint, rpc.DialOptions{Client: rpc.ClientConfig{MaxInFlight: limit, Timeout: 3 * time.Second}, TLS: rpc.TLSOptions{Config: &tls.Config{RootCAs: pool, ServerName: "localhost"}}})
	}
	httpClient := &http.Client{Timeout: time.Second}
	defer httpClient.CloseIdleConnections()
	server := func(gc bool) (map[string]any, error) {
		method := http.MethodGet
		if gc {
			method = http.MethodPost
		}
		req, _ := http.NewRequest(method, meta.Admin+"/stats", nil)
		r, e := httpClient.Do(req)
		if e != nil {
			return nil, e
		}
		defer r.Body.Close()
		var result map[string]any
		e = json.NewDecoder(r.Body).Decode(&result)
		return result, e
	}
	idle := func() error {
		until := time.Now().Add(3 * time.Second)
		for time.Now().Before(until) {
			s, e := server(false)
			if e != nil {
				return e
			}
			v := s["rpc"].(map[string]any)
			if v["InFlight"].(float64) == 0 && v["Executing"].(float64) == 0 && v["NativeStreams"].(float64) == 0 {
				return nil
			}
			time.Sleep(time.Millisecond)
		}
		return errors.New("worker retained work")
	}
	c, err := dial(8)
	if err != nil {
		return err
	}
	entered, release := make(chan struct{}, 64), make(chan struct{})
	var encoderCalls atomic.Int64
	var group sync.WaitGroup
	var mu sync.Mutex
	burstCounts := map[string]int{}
	for i := 0; i < 64; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			var out message
			e := c.Call(ctx, "/lab/echo", blocker{entered, release, &encoderCalls}, &out)
			mu.Lock()
			burstCounts[classify(e)]++
			mu.Unlock()
		}()
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		c.Close()
		close(release)
		group.Wait()
		return errors.New("no burst encoder started")
	}
	time.Sleep(120 * time.Millisecond)
	runtime.GC()
	burst := map[string]any{"encoders_entered": encoderCalls.Load(), "held_client": inspect(c), "held_heap": memory()}
	start := time.Now()
	c.Close()
	burst["close_ns"] = time.Since(start).Nanoseconds()
	burst["closed_client"] = inspect(c)
	close(release)
	group.Wait()
	burst["outcomes"] = burstCounts
	if err = idle(); err != nil {
		return err
	}
	runtime.GC()
	burst["released_heap"] = memory()
	burst["released_client"] = inspect(c)
	c, err = dial(64)
	if err != nil {
		return err
	}
	defer c.Close()
	payload := strings.Repeat("x", 16<<10)
	var next atomic.Int64
	call := func(kind int) error {
		in := message{ID: next.Add(1), Payload: payload, N: 4}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if kind < 12 {
			var out message
			e := c.Call(ctx, "/lab/echo", in, &out)
			if e == nil && (out.ID != in.ID || out.Payload != payload) {
				return errors.New("corrupt response")
			}
			return e
		}
		method := "/lab/items"
		if kind == 15 {
			method = "/lab/cancel"
		}
		stream, e := c.Stream(ctx, method, in)
		if e != nil {
			return e
		}
		defer stream.Close()
		count := 4
		if kind == 15 {
			count = 1
		}
		for n := 0; n < count; n++ {
			var out message
			if e = stream.Recv(&out); e != nil {
				return e
			}
			if out.ID != in.ID || out.Sequence != n || out.Payload != payload {
				return errors.New("corrupt response")
			}
		}
		if kind == 15 {
			return stream.Close()
		}
		if e = stream.Recv(nil); e != io.EOF {
			return fmt.Errorf("terminal: %v", e)
		}
		return nil
	}
	for n := 0; n < 128; n++ {
		if err = call(n % 16); err != nil {
			return err
		}
	}
	if err = idle(); err != nil {
		return err
	}
	runtime.GC()
	baselineClient := memory()
	baselineServer, err := server(true)
	if err != nil {
		return err
	}
	// Write observations outside the measured heap. Retaining samples/phases in
	// slices made the observer itself grow with elapsed time in earlier runs.
	phaseFile, err := os.OpenFile(output+".phases.jsonl", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer phaseFile.Close()
	sampleFile, err := os.OpenFile(output+".samples.jsonl", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer sampleFile.Close()
	phaseWriter, sampleWriter := json.NewEncoder(phaseFile), json.NewEncoder(sampleFile)
	allCounts := map[string]int{}
	sampleCount := 0
	for cycle := 0; cycle < cycles; cycle++ {
		begin := time.Now()
		until := begin.Add(duration / time.Duration(cycles))
		counts := map[string]int{}
		var workers sync.WaitGroup
		for w := 0; w < 8; w++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for time.Now().Before(until) {
					e := call(int(next.Load() % 16))
					mu.Lock()
					counts[classify(e)]++
					mu.Unlock()
				}
			}()
		}
		workersDone := make(chan struct{})
		go func() { workers.Wait(); close(workersDone) }()
		ticker := time.NewTicker(time.Second)
	load:
		for {
			select {
			case <-workersDone:
				break load
			case <-ticker.C:
				if e := sampleWriter.Encode(map[string]any{"cycle": cycle, "client_heap": memory(), "client": inspect(c)}); e != nil {
					ticker.Stop()
					workers.Wait()
					return e
				}
				sampleCount++
			}
		}
		ticker.Stop()
		if err = idle(); err != nil {
			return err
		}
		// Finished unread streams are intentionally retained, then all closed.
		streams := make([]*rpc.Stream, 0, 8)
		for i := 0; i < 8; i++ {
			stream, e := c.Stream(context.Background(), "/lab/items", message{ID: next.Add(1), Payload: payload, N: 4})
			if e != nil {
				return e
			}
			streams = append(streams, stream)
		}
		if err = idle(); err != nil {
			return err
		}
		unread := inspect(c)
		for _, stream := range streams {
			if e := stream.Close(); e != nil {
				return e
			}
		}
		runtime.GC()
		serverState, e := server(true)
		if e != nil {
			return e
		}
		state := map[string]any{"cycle": cycle, "active_elapsed_ns": time.Since(begin).Nanoseconds(), "counts": counts, "completed_unread": unread, "client": inspect(c), "client_heap": memory(), "server": serverState}
		if e := phaseWriter.Encode(state); e != nil {
			return e
		}
		for k, v := range counts {
			allCounts[k] += v
		}
	}
	c.Close()
	if err = idle(); err != nil {
		return err
	}
	runtime.GC()
	finalServer, err := server(true)
	if err != nil {
		return err
	}
	result := map[string]any{"transport": meta.Endpoint.Transport, "active_duration_ns": duration.Nanoseconds(), "cycles": cycles, "burst": burst, "baseline_client_heap": baselineClient, "baseline_server": baselineServer, "phase_file": phaseFile.Name(), "sample_file": sampleFile.Name(), "sample_count": sampleCount, "all_counts": allCounts, "final_client": inspect(c), "final_client_heap": memory(), "final_server": finalServer, "scope": "owned real loopback; bounded 8-worker mixed unary/stream/cancel, 16KiB payload; observations streamed to files; timer/custom encoder fixtures; forced-GC idle samples; no model/RSS/infinite-leak claim"}
	if err = save(output, result); err != nil {
		return err
	}
	if len(allCounts) != 1 || allCounts["ok"] == 0 {
		return fmt.Errorf("soak outcomes: %v", allCounts)
	}
	return nil
}
func classify(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, rpc.ErrClosed) {
		return "closed"
	}
	var s *rpc.Status
	if errors.As(err, &s) {
		return string(s.Code)
	}
	return "other"
}
