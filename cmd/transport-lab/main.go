// transport-lab uses integer IDs and fixed JSON chunks, never a model backend.
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
	"sync"
	"syscall"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/internal/testcert"
)

type message struct {
	ID       int    `json:"id"`
	Payload  string `json:"payload"`
	Count    int    `json:"count"`
	Sequence int    `json:"sequence"`
}
type ready struct {
	Endpoint rpc.Endpoint `json:"endpoint"`
	Admin    string       `json:"admin"`
	CA       []byte       `json:"ca_pem"`
}
type offer struct {
	ID    int    `json:"id"`
	Nanos int64  `json:"nanos"`
	Error string `json:"error,omitempty"`
}
type outcome struct {
	Transport     rpc.Transport `json:"transport"`
	Workload      string        `json:"workload"`
	Offers        []offer       `json:"offers"`
	Completed     int           `json:"completed"`
	Elapsed       time.Duration `json:"elapsed_ns"`
	P50, P95, P99 int64
	Stats         any    `json:"stats"`
	Evidence      string `json:"evidence"`
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
func main() {
	mode := flag.String("mode", "worker", "worker or client")
	kind := flag.String("transport", "tcp", "tcp, tls, quic")
	readyPath := flag.String("ready", "", "owned ready JSON path")
	output := flag.String("output", "", "result JSON path")
	workload := flag.String("workload", "unary", "unary or stream")
	count := flag.Int("count", 3000, "offered requests")
	workers := flag.Int("workers", 8, "bounded closed-loop concurrency")
	seed := flag.Uint64("seed", 7, "offered ID shuffle seed")
	flag.Parse()
	var err error
	if *readyPath == "" || *output == "" {
		err = errors.New("explicit ready/output paths required")
	} else if *mode == "worker" {
		err = worker(rpc.Transport(*kind), *readyPath, *output)
	} else if *mode == "client" {
		err = client(*readyPath, *output, *workload, *count, *workers, *seed)
	} else {
		err = errors.New("unknown mode")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func worker(kind rpc.Transport, readyPath, output string) error {
	credentials, err := testcert.New()
	if err != nil {
		return err
	}
	s, err := rpc.NewServer(rpc.ServerConfig{MaxPerConnection: 64})
	if err != nil {
		return err
	}
	var mu sync.Mutex
	seen := make(map[int]bool)
	duplicates := 0
	mark := func(id int) {
		mu.Lock()
		defer mu.Unlock()
		if seen[id] {
			duplicates++
		}
		seen[id] = true
	}
	if err = rpc.RegisterUnary(s, "/lab/echo", func(ctx context.Context, in message) (message, error) { mark(in.ID); return in, ctx.Err() }); err != nil {
		return err
	}
	if err = rpc.RegisterStream(s, "/lab/items", func(ctx context.Context, in message, send func(message) error) error {
		mark(in.ID)
		for i := 0; i < in.Count; i++ {
			out := in
			out.Sequence = i
			if err := send(out); err != nil {
				return err
			}
		}
		return ctx.Err()
	}); err != nil {
		return err
	}
	done := make(chan error, 1)
	config := ready{}
	if kind == rpc.QUIC {
		listener, e := rpc.ListenQUIC("127.0.0.1:0", rpc.TLSOptions{Config: credentials.Server}, rpc.QUICConfig{})
		if e != nil {
			return e
		}
		config.Endpoint = rpc.Endpoint{Address: listener.Addr().String(), Transport: kind}
		go func() { done <- s.ServeQUIC(listener) }()
	} else {
		if kind != rpc.TCP && kind != rpc.TLS {
			return errors.New("invalid worker transport")
		}
		listener, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			return e
		}
		config.Endpoint = rpc.Endpoint{Address: listener.Addr().String(), Transport: kind}
		if kind == rpc.TLS {
			go func() { done <- s.ServeTLS(listener, rpc.TLSOptions{Config: credentials.Server}) }()
		} else {
			go func() { done <- s.Serve(listener) }()
		}
	}
	defer s.Close()
	admin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(s.Stats()) })
	web := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	webDone := make(chan error, 1)
	go func() { webDone <- web.Serve(admin) }()
	defer func() { _ = web.Close(); <-webDone }()
	config.Admin = "http://" + admin.Addr().String()
	config.CA = credentials.CAPEM
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
	defer signal.Stop(signals)
	until := time.Now().Add(time.Second)
	for !s.Ready() && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if !s.Ready() {
		return errors.New("server did not become ready")
	}
	if err = write(readyPath, config); err != nil {
		return err
	}
	select {
	case <-signals:
	case err = <-done:
		return fmt.Errorf("serve ended: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = s.Drain(ctx)
	serveErr := <-done
	mu.Lock()
	executions, dupes := len(seen), duplicates
	mu.Unlock()
	_ = write(output, map[string]any{"stats": s.Stats(), "executions": executions, "duplicates": dupes, "drain_error": textError(err), "evidence": "fixture-no-model"})
	if err != nil {
		return err
	}
	return serveErr
}
func textError(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
func client(readyPath, output, workload string, count, workers int, seed uint64) error {
	if count < 1 || count > 100000 || workers < 1 || workers > 32 || (workload != "unary" && workload != "stream") {
		return errors.New("invalid load bounds")
	}
	b, err := os.ReadFile(readyPath)
	if err != nil {
		return err
	}
	var config ready
	if err = json.Unmarshal(b, &config); err != nil {
		return err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(config.CA) {
		return errors.New("invalid lab public CA")
	}
	c, err := rpc.DialEndpoint(context.Background(), config.Endpoint, rpc.DialOptions{TLS: rpc.TLSOptions{Config: &tls.Config{RootCAs: pool, ServerName: "localhost"}}, Client: rpc.ClientConfig{MaxInFlight: 64, StreamWindow: 8}})
	if err != nil {
		return err
	}
	defer c.Close()
	payload := ""
	for len(payload) < 128 {
		payload += "x"
	}
	call := func(id int) error {
		in := message{ID: id, Payload: payload, Count: 16}
		if workload == "unary" {
			var out message
			e := c.Call(context.Background(), "/lab/echo", in, &out)
			if e == nil && (out.ID != id || out.Payload != payload) {
				return errors.New("corrupt unary response")
			}
			return e
		}
		stream, e := c.Stream(context.Background(), "/lab/items", in)
		if e != nil {
			return e
		}
		defer stream.Close()
		for i := 0; i < 16; i++ {
			var out message
			if e = stream.Recv(&out); e != nil {
				return e
			}
			if out.ID != id || out.Sequence != i || out.Payload != payload {
				return errors.New("corrupt stream response")
			}
		}
		if e = stream.Recv(nil); e != io.EOF {
			return fmt.Errorf("terminal: %v", e)
		}
		return nil
	}
	for i := 0; i < 64; i++ {
		if err = call(-i - 1); err != nil {
			return err
		}
	}
	ids := make([]int, count)
	for i := range ids {
		ids[i] = i + 1
	}
	random := rand.New(rand.NewPCG(seed, seed+1))
	random.Shuffle(count, func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	result := outcome{Transport: config.Endpoint.Transport, Workload: workload, Evidence: "closed-loop loopback fixture-no-model; warm connection; no inference claim", Offers: make([]offer, count)}
	jobs := make(chan int)
	var group sync.WaitGroup
	begin := time.Now()
	for i := 0; i < workers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range jobs {
				start := time.Now()
				e := call(ids[index])
				result.Offers[index] = offer{ID: ids[index], Nanos: time.Since(start).Nanoseconds(), Error: textError(e)}
			}
		}()
	}
	for index := range ids {
		jobs <- index
	}
	close(jobs)
	group.Wait()
	result.Elapsed = time.Since(begin)
	latencies := make([]int64, 0, count)
	for _, row := range result.Offers {
		if row.Error == "" {
			result.Completed++
			latencies = append(latencies, row.Nanos)
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(percent int) int64 {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[(len(latencies)-1)*percent/100]
	}
	result.P50 = percentile(50)
	result.P95 = percentile(95)
	result.P99 = percentile(99)
	c.Close()
	switch client := c.(type) {
	case *rpc.Client:
		result.Stats = client.Stats()
	case *rpc.QUICClient:
		result.Stats = client.Stats()
	}
	if err = write(output, result); err != nil {
		return err
	}
	if result.Completed != count {
		return errors.New("offered requests failed; partial results retained")
	}
	return nil
}
