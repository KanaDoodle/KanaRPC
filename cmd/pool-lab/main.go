// pool-lab uses resource-lab's real workers and a local snapshot fixture. It
// compiles unchanged on R8D; absent pool statistics are explicitly unsupported.
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
	"net/http"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/discovery"
	"github.com/KanaDoodle/KanaRPC/routing"
)

type ready struct {
	Endpoint rpc.Endpoint
	Admin    string
	CA       []byte
}
type message struct {
	ID          int64
	Payload     string
	N, Sequence int
}
type source struct {
	mu       sync.Mutex
	nodes    []rpc.Endpoint
	revision int64
}

func (s *source) Snapshot() discovery.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return discovery.Snapshot{Ready: true, Revision: s.revision, ClusterID: 1, Endpoints: append([]rpc.Endpoint(nil), s.nodes...)}
}
func (s *source) set(nodes ...rpc.Endpoint) {
	s.mu.Lock()
	s.nodes = nodes
	s.revision++
	s.mu.Unlock()
}
func inspect(c *routing.Client) any {
	method := reflect.ValueOf(c).MethodByName("MemoryStats")
	if !method.IsValid() {
		return map[string]any{"supported": false}
	}
	return method.Call(nil)[0].Interface()
}
func configure(cfg *routing.Config, tight bool) bool {
	f := reflect.ValueOf(cfg).Elem().FieldByName("Memory")
	if !f.IsValid() {
		return false
	}
	m := f.FieldByName("Memory")
	data, enc, recv, send := int64(4<<20), int64(2<<20), int64(2<<20), int64(2<<20)
	if tight {
		data, enc, recv, send = 131072, 65536, 24576, 65536
	}
	f.FieldByName("DataBytes").SetInt(data)
	m.FieldByName("EncodingBytes").SetInt(enc)
	m.FieldByName("ReceiveBytes").SetInt(recv)
	m.FieldByName("SendBytes").SetInt(send)
	return true
}
func heap() any {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fds := -1
	for _, p := range []string{"/proc/self/fd", "/dev/fd"} {
		if files, e := os.ReadDir(p); e == nil {
			fds = len(files)
			break
		}
	}
	return map[string]any{"goroutines": runtime.NumGoroutine(), "fds": fds, "heap_alloc": m.HeapAlloc, "heap_objects": m.HeapObjects, "total_alloc": m.TotalAlloc, "gc": m.NumGC}
}
func main() {
	paths := flag.String("ready", "", "two comma-separated ready files")
	output := flag.String("output", "", "result path")
	duration := flag.Duration("duration", 180*time.Second, "active mixed workload duration")
	cycles := flag.Int("cycles", 12, "retirement cycles")
	tight := flag.Bool("tight", false, "deliberately saturating pool budget")
	flag.Parse()
	if e := run(*paths, *output, *duration, *cycles, *tight); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run(paths, output string, duration time.Duration, cycles int, tight bool) error {
	if duration < time.Second || cycles < 1 || duration/time.Duration(cycles) < time.Second || output == "" {
		return errors.New("invalid explicit experiment parameters")
	}
	var metas []ready
	var nodes []rpc.Endpoint
	roots := x509.NewCertPool()
	for i, path := range strings.Split(paths, ",") {
		b, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		var v ready
		if e = json.Unmarshal(b, &v); e != nil {
			return e
		}
		v.Endpoint.ID = fmt.Sprint(i)
		if !roots.AppendCertsFromPEM(v.CA) {
			return errors.New("invalid CA")
		}
		metas = append(metas, v)
		nodes = append(nodes, v.Endpoint)
	}
	if len(nodes) != 2 || nodes[0].Transport != nodes[1].Transport {
		return errors.New("two identical transports required")
	}
	opts := rpc.DialOptions{TLS: rpc.TLSOptions{Config: &tls.Config{RootCAs: roots, ServerName: "localhost"}}, Client: rpc.ClientConfig{Timeout: 3 * time.Second, MaxInFlight: 16, StreamWindow: 8, IO: rpc.IOConfig{MaxFrameBytes: 32768}, Memory: rpc.MemoryConfig{EncodingBytes: 65536, ReceiveBytes: 2 << 20}}}
	cfg := routing.Config{Transport: nodes[0].Transport, Dial: opts, MaxConnections: 2, MaxInFlight: 8, PollInterval: time.Millisecond, DialBackoff: time.Millisecond}
	supported := configure(&cfg, tight)
	view := &source{nodes: nodes, revision: 1}
	c, e := routing.New(context.Background(), view, cfg)
	if e != nil {
		return e
	}
	defer c.Close()
	phaseFile, e := os.OpenFile(output+".phases.jsonl", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer phaseFile.Close()
	writer := json.NewEncoder(phaseFile)
	defer func() {
		c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := c.Wait(ctx)
		_ = writer.Encode(map[string]any{"checkpoint": "exit_cleanup", "memory": inspect(c), "routing": c.Stats(), "wait_error": fmt.Sprint(err)})
	}()
	httpClient := &http.Client{Timeout: time.Second}
	defer httpClient.CloseIdleConnections()
	server := func(meta ready) (map[string]any, error) {
		req, _ := http.NewRequest(http.MethodPost, meta.Admin+"/stats", nil)
		r, e := httpClient.Do(req)
		if e != nil {
			return nil, e
		}
		defer r.Body.Close()
		var v map[string]any
		e = json.NewDecoder(r.Body).Decode(&v)
		return v, e
	}
	idle := func() error {
		until := time.Now().Add(4 * time.Second)
		for time.Now().Before(until) {
			all := true
			for _, m := range metas {
				v, e := server(m)
				if e != nil {
					return e
				}
				s := v["rpc"].(map[string]any)
				if s["InFlight"].(float64) != 0 || s["Executing"].(float64) != 0 || s["NativeStreams"].(float64) != 0 {
					all = false
				}
			}
			if all {
				return nil
			}
			time.Sleep(time.Millisecond)
		}
		return errors.New("workers not idle")
	}
	class := func(err error) string {
		if err == nil {
			return "success"
		}
		if errors.Is(err, context.Canceled) {
			return "canceled"
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return "deadline"
		}
		if errors.Is(err, rpc.ErrClosed) {
			return "closed"
		}
		var status *rpc.Status
		if errors.As(err, &status) {
			return fmt.Sprint("status_", status.Code)
		}
		return "transport_or_admission"
	}
	var ids atomic.Int64
	totals := map[string]int64{}
	var totalsMu sync.Mutex
	record := func(err error) { totalsMu.Lock(); totals[class(err)]++; totalsMu.Unlock() }
	baselineHeap := heap()
	begin := time.Now()
	for cycle := 0; cycle < cycles; cycle++ {
		view.set(nodes...)
		stop := time.Now().Add(duration / time.Duration(cycles))
		var wg sync.WaitGroup
		for worker := 0; worker < 8; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(stop) {
					id := ids.Add(1)
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					in := message{ID: id, Payload: strings.Repeat("x", 16384), N: 4}
					var err error
					if id%4 == 0 {
						var out message
						err = c.Call(ctx, "/lab/echo", in, &out)
						if err == nil && (out.ID != id || out.Payload != in.Payload) {
							err = errors.New("integrity failure")
							panic(err)
						}
					} else {
						method := "/lab/items"
						if id%4 == 1 {
							method = "/lab/cancel"
						}
						stream, e := c.Stream(ctx, method, in)
						err = e
						if e == nil {
							for n := 0; ; n++ {
								var out message
								e = stream.Recv(&out)
								if e == io.EOF {
									break
								}
								if e != nil {
									err = e
									break
								}
								if out.ID != id || out.Payload != in.Payload || out.Sequence != n {
									panic("integrity failure")
								}
								if method == "/lab/cancel" {
									cancel()
								}
							}
							_ = stream.Close()
						}
					}
					cancel()
					record(err)
				}
			}()
		}
		wg.Wait()
		if e = idle(); e != nil {
			return e
		}
		if e = writer.Encode(map[string]any{"checkpoint": "before_retire", "cycle": cycle, "memory": inspect(c), "routing": c.Stats()}); e != nil {
			return e
		}
		// The first completed stream remains unread during a discovery retirement.
		view.set(nodes[0])
		one, e := c.Stream(context.Background(), "/lab/items", message{ID: ids.Add(1), Payload: strings.Repeat("u", 16384), N: 1})
		if e != nil {
			return fmt.Errorf("first unread open: %w; memory=%+v; routing=%+v", e, inspect(c), c.Stats())
		}
		if e = idle(); e != nil {
			one.Close()
			return e
		}
		view.set(nodes[1])
		two, e := c.Stream(context.Background(), "/lab/items", message{ID: ids.Add(1), Payload: strings.Repeat("v", 16384), N: 1})
		secondOpen := class(e)
		if e = idle(); e != nil {
			one.Close()
			if two != nil {
				two.Close()
			}
			return e
		}
		held := map[string]any{"memory": inspect(c), "routing": c.Stats(), "heap": heap(), "second_open": secondOpen}
		var out message
		if e = one.Recv(&out); e != nil {
			one.Close()
			if two != nil {
				two.Close()
			}
			return fmt.Errorf("first unread lost: %w", e)
		}
		if len(out.Payload) != 16384 {
			return errors.New("unread corruption")
		}
		if e = one.Recv(nil); e != io.EOF {
			return fmt.Errorf("first unread terminal: %v", e)
		}
		one.Close()
		secondTerminal := "not_open"
		if two != nil {
			for {
				e = two.Recv(nil)
				if e != nil {
					secondTerminal = class(e)
					if e == io.EOF {
						secondTerminal = "eof"
					}
					break
				}
			}
			two.Close()
		}
		if e = idle(); e != nil {
			return e
		}
		until := time.Now().Add(3 * time.Second)
		for c.Stats().Retiring != 0 && time.Now().Before(until) {
			time.Sleep(time.Millisecond)
		}
		if c.Stats().Retiring != 0 {
			return errors.New("retired entry not released")
		}
		runtime.GC()
		servers := []map[string]any{}
		for _, m := range metas {
			v, e := server(m)
			if e != nil {
				return e
			}
			servers = append(servers, v)
		}
		if e = writer.Encode(map[string]any{"cycle": cycle, "elapsed_ns": time.Since(begin).Nanoseconds(), "held": held, "second_terminal": secondTerminal, "memory": inspect(c), "routing": c.Stats(), "heap": heap(), "servers": servers}); e != nil {
			return e
		}
	}
	c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wait := c.Wait(ctx)
	runtime.GC()
	result := map[string]any{"supported": supported, "tight": tight, "transport": nodes[0].Transport, "duration_ns": duration.Nanoseconds(), "elapsed_ns": time.Since(begin).Nanoseconds(), "cycles": cycles, "offered": ids.Load() - int64(cycles*2), "outcomes": totals, "initial_heap": baselineHeap, "final_heap": heap(), "final_memory": inspect(c), "final_routing": c.Stats(), "wait_error": fmt.Sprint(wait), "scope": "local loopback, real sockets, two processes and snapshot fixture; fixed 16KiB payload; retirement/GC observations; mixed load intentionally includes cancellation and tight-budget rejection; no throughput/RSS/dual-host/inference claim"}
	f, e := os.Create(output)
	if e != nil {
		return e
	}
	e = json.NewEncoder(f).Encode(result)
	f.Close()
	if e != nil {
		return e
	}
	return wait
}
