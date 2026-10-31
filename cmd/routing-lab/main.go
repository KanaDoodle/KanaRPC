// routing-lab is an independent client for transport-lab workers. Snapshot
// input is a fixed local fixture; real etcd correctness is tested separately.
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
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/discovery"
	"github.com/KanaDoodle/KanaRPC/routing"
)

type ready struct {
	Endpoint rpc.Endpoint `json:"endpoint"`
	CA       []byte       `json:"ca_pem"`
}
type message struct {
	ID       int    `json:"id"`
	Payload  string `json:"payload"`
	Count    int    `json:"count"`
	Sequence int    `json:"sequence"`
}
type offer struct {
	ID    int    `json:"id"`
	Nanos int64  `json:"nanos"`
	Error string `json:"error,omitempty"`
}
type source struct{ nodes []rpc.Endpoint }

func (s source) Snapshot() discovery.Snapshot {
	return discovery.Snapshot{Ready: true, Revision: 1, ClusterID: 1, Endpoints: append([]rpc.Endpoint(nil), s.nodes...)}
}

type stream interface {
	Recv(any) error
	Close() error
}

func text(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
func main() {
	paths := flag.String("ready", "", "comma-separated two worker ready files")
	output := flag.String("output", "", "output JSON")
	mode := flag.String("strategy", "managed", "direct, dial, managed, churn")
	workload := flag.String("workload", "unary", "unary or stream")
	count := flag.Int("count", 1000, "offered calls")
	workers := flag.Int("workers", 8, "closed-loop callers")
	seed := flag.Uint64("seed", 7, "shuffle seed")
	payloadBytes := flag.Int("payload-bytes", 128, "fixed request/response payload bytes, 0..65536")
	flag.Parse()
	if err := run(*paths, *output, *mode, *workload, *count, *workers, *seed, *payloadBytes); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(paths, output, mode, workload string, count, workers int, seed uint64, payloadBytes int) error {
	if payloadBytes < 0 || payloadBytes > 65536 || output == "" || count < 1 || count > 100000 || workers < 1 || workers > 32 || (workload != "unary" && workload != "stream") || (mode != "direct" && mode != "dial" && mode != "managed" && mode != "churn") {
		return errors.New("invalid explicit experiment parameters")
	}
	nodes := []rpc.Endpoint{}
	roots := x509.NewCertPool()
	for i, path := range strings.Split(paths, ",") {
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var v ready
		if err = json.Unmarshal(b, &v); err != nil {
			return err
		}
		v.Endpoint.ID = fmt.Sprint(i)
		if !roots.AppendCertsFromPEM(v.CA) {
			return errors.New("invalid public lab CA")
		}
		nodes = append(nodes, v.Endpoint)
	}
	if len(nodes) != 2 || nodes[0].Transport != nodes[1].Transport {
		return errors.New("two workers with identical transport required")
	}
	options := rpc.DialOptions{Client: rpc.ClientConfig{MaxInFlight: 64, StreamWindow: 8}, TLS: rpc.TLSOptions{Config: &tls.Config{RootCAs: roots, ServerName: "localhost"}}}
	dial := func(ctx context.Context, node rpc.Endpoint) (rpc.Caller, error) {
		budget, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		return rpc.DialEndpoint(budget, node, options)
	}
	var pool *routing.Client
	var direct []rpc.Caller
	var dials, next atomic.Uint64
	if mode == "managed" || mode == "churn" {
		limit := 2
		if mode == "churn" {
			limit = 1
		}
		var err error
		pool, err = routing.New(context.Background(), source{nodes}, routing.Config{Transport: nodes[0].Transport, Dial: options, MaxConnections: limit, MaxInFlight: 64, DialTimeout: 3 * time.Second})
		if err != nil {
			return err
		}
		defer pool.Close()
	} else if mode == "direct" {
		for _, node := range nodes {
			c, err := dial(context.Background(), node)
			if err != nil {
				for _, c := range direct {
					c.Close()
				}
				return err
			}
			direct = append(direct, c)
			dials.Add(1)
		}
		defer func() {
			for _, c := range direct {
				c.Close()
			}
		}()
	}
	call := func(id int) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var caller rpc.Caller
		if pool == nil {
			index := int((next.Add(1) - 1) % uint64(len(nodes)))
			if mode == "dial" {
				var err error
				dials.Add(1)
				caller, err = dial(ctx, nodes[index])
				if err != nil {
					return err
				}
				defer caller.Close()
			} else {
				caller = direct[index]
			}
		}
		in := message{ID: id, Payload: strings.Repeat("x", payloadBytes), Count: 16}
		if workload == "unary" {
			var out message
			var err error
			if pool != nil {
				err = pool.Call(ctx, "/lab/echo", in, &out)
			} else {
				err = caller.Call(ctx, "/lab/echo", in, &out)
			}
			if err == nil && (out.ID != id || out.Payload != in.Payload) {
				return errors.New("corrupt unary response")
			}
			return err
		}
		var s stream
		var err error
		if pool != nil {
			s, err = pool.Stream(ctx, "/lab/items", in)
		} else {
			s, err = caller.Stream(ctx, "/lab/items", in)
		}
		if err != nil {
			return err
		}
		defer s.Close()
		for i := 0; i < 16; i++ {
			var out message
			if err = s.Recv(&out); err != nil {
				return err
			}
			if out.ID != id || out.Sequence != i || out.Payload != in.Payload {
				return errors.New("corrupt stream response")
			}
		}
		if err = s.Recv(nil); err != io.EOF {
			return fmt.Errorf("terminal: %v", err)
		}
		return nil
	}
	for i := 0; i < 64; i++ {
		if err := call(-i - 1); err != nil {
			return fmt.Errorf("warmup: %w", err)
		}
	}
	ids := make([]int, count)
	for i := range ids {
		ids[i] = i + 1
	}
	random := rand.New(rand.NewPCG(seed, seed+1))
	random.Shuffle(count, func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	offers := make([]offer, count)
	jobs := make(chan int)
	var group sync.WaitGroup
	start := time.Now()
	for i := 0; i < workers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range jobs {
				begin := time.Now()
				err := call(ids[index])
				offers[index] = offer{ids[index], time.Since(begin).Nanoseconds(), text(err)}
			}
		}()
	}
	for i := range ids {
		jobs <- i
	}
	close(jobs)
	group.Wait()
	elapsed := time.Since(start)
	latencies := []int64{}
	for _, row := range offers {
		if row.Error == "" {
			latencies = append(latencies, row.Nanos)
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(n int) int64 {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[(len(latencies)-1)*n/100]
	}
	var stats any
	var memory any
	waitErr := error(nil)
	if pool != nil {
		pool.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		waitErr = pool.Wait(ctx)
		cancel()
		s := pool.Stats()
		dials.Store(s.Dials)
		stats = s
		method := reflect.ValueOf(pool).MethodByName("MemoryStats")
		if method.IsValid() {
			memory = method.Call(nil)[0].Interface()
		}
	} else {
		for _, c := range direct {
			c.Close()
		}
	}
	result := map[string]any{"transport": nodes[0].Transport, "payload_bytes": payloadBytes, "memory": memory, "strategy": mode, "workload": workload, "offers": offers, "completed": len(latencies), "offered": count, "elapsed_ns": elapsed.Nanoseconds(), "P50": percentile(50), "P95": percentile(95), "P99": percentile(99), "dials_including_64_warmups": dials.Load(), "stats": stats, "wait_error": text(waitErr), "evidence": "loopback integer/JSON fixture; two fixed local snapshot endpoints; no real discovery latency or model performance"}
	b, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(output, append(b, '\n'), 0600); err != nil {
		return err
	}
	if waitErr != nil {
		return waitErr
	}
	if len(latencies) != count {
		return errors.New("some offered calls failed; all outcomes retained")
	}
	return nil
}
