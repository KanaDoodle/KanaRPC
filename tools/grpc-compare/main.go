// grpc-compare uses one persistent connection and identical protobuf payloads.
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
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/internal/testcert"
	"github.com/KanaDoodle/KanaRPC/tools/grpc-compare/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type sample struct {
	ID, Nanos int64
	Error     string `json:",omitempty"`
}
type snapshot struct {
	CPU            int64
	Alloc, Mallocs uint64
	Goroutines     int
	Calls, Active  int64
	Connections    int64
}

func profiles(prefix string) (func(), error) {
	if prefix == "" {
		return func() {}, nil
	}
	f, err := os.Create(prefix + ".cpu.pprof")
	if err != nil {
		return nil, err
	}
	if err = pprof.StartCPUProfile(f); err != nil {
		f.Close()
		return nil, err
	}
	runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(10000)
	var once sync.Once
	return func() {
		once.Do(func() {
			pprof.StopCPUProfile()
			f.Close()
			runtime.SetBlockProfileRate(0)
			runtime.SetMutexProfileFraction(0)
			for _, name := range []string{"allocs", "mutex", "block"} {
				out, e := os.Create(prefix + "." + name + ".pprof")
				if e == nil {
					_ = pprof.Lookup(name).WriteTo(out, 0)
					out.Close()
				}
			}
		})
	}, nil
}

func snap(s *service, l *listener) snapshot {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		panic(err)
	}
	v := snapshot{CPU: r.Utime.Sec*1e9 + int64(r.Utime.Usec)*1e3 + r.Stime.Sec*1e9 + int64(r.Stime.Usec)*1e3, Alloc: m.TotalAlloc, Mallocs: m.Mallocs, Goroutines: runtime.NumGoroutine()}
	if s != nil {
		v.Calls = s.calls.Load()
		v.Active = s.active.Load()
		v.Connections = l.accepted.Load()
	}
	return v
}

type service struct {
	pb.UnimplementedBenchServer
	calls, active atomic.Int64
}

func (s *service) Echo(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	s.calls.Add(1)
	s.active.Add(1)
	defer s.active.Add(-1)
	return &pb.Reply{Payload: in.GetPayload()}, ctx.Err()
}
func (s *service) stream(ctx context.Context, in *pb.Request, send func(*pb.Reply) error) error {
	s.calls.Add(1)
	s.active.Add(1)
	defer s.active.Add(-1)
	for n := uint32(0); n < in.GetChunks(); n++ {
		if err := send(&pb.Reply{Payload: in.GetPayload(), Sequence: n}); err != nil {
			return err
		}
	}
	return ctx.Err()
}
func (s *service) Watch(in *pb.Request, stream grpc.ServerStreamingServer[pb.Reply]) error {
	return s.stream(stream.Context(), in, stream.Send)
}

type listener struct {
	net.Listener
	accepted atomic.Int64
}

func (l *listener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e == nil {
		l.accepted.Add(1)
	}
	return c, e
}

type ready struct {
	Address, Admin string
	CA             []byte
}

func write(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile(path, append(b, '\n'), 0600)
}
func main() {
	mode := flag.String("mode", "server", "")
	engine := flag.String("engine", "kana", "")
	transport := flag.String("transport", "tcp", "")
	readyFile := flag.String("ready", "", "")
	output := flag.String("out", "", "")
	size := flag.Int("size", 64, "")
	workers := flag.Int("workers", 1, "")
	chunks := flag.Int("chunks", 1, "")
	seconds := flag.Duration("duration", 2*time.Second, "")
	flag.Parse()
	var err error
	if *readyFile == "" || *output == "" || (*engine != "kana" && *engine != "grpc") || (*transport != "tcp" && *transport != "tls") || *workers < 1 || *workers > 64 || *chunks < 1 || *size < 0 || *size > 65536 || *seconds <= 0 {
		err = errors.New("invalid comparison arguments")
	} else if *mode == "server" {
		err = serve(*engine, *transport, *readyFile, *output)
	} else if *mode == "client" {
		err = load(*engine, *transport, *readyFile, *output, *size, *workers, *chunks, *seconds)
	} else {
		err = errors.New("invalid mode")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func serve(engine, transport, readyFile, output string) error {
	cert, err := testcert.New()
	if err != nil {
		return err
	}
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	l := &listener{Listener: raw}
	defer l.Close()
	s := new(service)
	var stop func() error
	done := make(chan error, 1)
	if engine == "kana" {
		server, e := rpc.NewServer(rpc.ServerConfig{MaxInFlight: 128, MaxPerConnection: 128})
		if e != nil {
			return e
		}
		if e = rpc.RegisterProtobufUnary(server, "/comparison.v1.Bench/Echo", func() *pb.Request { return new(pb.Request) }, s.Echo); e != nil {
			return e
		}
		if e = rpc.RegisterProtobufStream(server, "/comparison.v1.Bench/Watch", func() *pb.Request { return new(pb.Request) }, s.stream); e != nil {
			return e
		}
		go func() {
			if transport == "tls" {
				done <- server.ServeTLS(l, rpc.TLSOptions{Config: cert.Server})
			} else {
				done <- server.Serve(l)
			}
		}()
		stop = func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			r, e := server.DrainWithReport(ctx)
			_ = write(output+".drain.json", r)
			if e != nil {
				server.Close()
			}
			return e
		}
	} else {
		opts := []grpc.ServerOption{grpc.MaxConcurrentStreams(128), grpc.MaxRecvMsgSize(1 << 20), grpc.MaxSendMsgSize(1 << 20)}
		if transport == "tls" {
			opts = append(opts, grpc.Creds(credentials.NewTLS(cert.Server)))
		}
		server := grpc.NewServer(opts...)
		pb.RegisterBenchServer(server, s)
		go func() { done <- server.Serve(l) }()
		stop = func() error {
			finished := make(chan struct{})
			go func() { server.GracefulStop(); close(finished) }()
			select {
			case <-finished:
				return nil
			case <-time.After(3 * time.Second):
				server.Stop()
				return errors.New("grpc graceful stop deadline")
			}
		}
	}
	admin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer admin.Close()
	quit := make(chan struct{})
	var once sync.Once
	mux := http.NewServeMux()
	var profileMu sync.Mutex
	stopProfile := func() {}
	defer func() { profileMu.Lock(); defer profileMu.Unlock(); stopProfile() }()
	mux.HandleFunc("/profile/start", func(w http.ResponseWriter, r *http.Request) {
		profileMu.Lock()
		defer profileMu.Unlock()
		var e error
		stopProfile, e = profiles(os.Getenv("KANARPC_COMPARE_PROFILE"))
		if e != nil {
			http.Error(w, e.Error(), 500)
		}
	})
	mux.HandleFunc("/profile/stop", func(w http.ResponseWriter, r *http.Request) {
		profileMu.Lock()
		defer profileMu.Unlock()
		stopProfile()
	})
	mux.HandleFunc("/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("gc") == "1" {
			runtime.GC()
		}
		_ = json.NewEncoder(w).Encode(snap(s, l))
	})
	mux.HandleFunc("/stop", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200); once.Do(func() { close(quit) }) })
	h := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	go h.Serve(admin)
	defer h.Close()
	if err = write(readyFile, ready{l.Addr().String(), "http://" + admin.Addr().String(), cert.CAPEM}); err != nil {
		return err
	}
	<-quit
	err = stop()
	serveErr := <-done
	if serveErr != nil && !errors.Is(serveErr, net.ErrClosed) && !errors.Is(serveErr, grpc.ErrServerStopped) {
		err = errors.Join(err, serveErr)
	}
	_ = write(output, snap(s, l))
	return err
}

type receiver interface {
	Recv() (*pb.Reply, error)
	Close() error
}
type kanaStream struct{ s *rpc.Stream }

func (k kanaStream) Recv() (*pb.Reply, error) {
	out := new(pb.Reply)
	e := k.s.Recv(out)
	return out, e
}
func (k kanaStream) Close() error { return k.s.Close() }

type grpcStream struct {
	grpc.ServerStreamingClient[pb.Reply]
	cancel context.CancelFunc
}

func (g grpcStream) Close() error { g.cancel(); return nil }
func load(engine, transport, readyFile, output string, size, workers, chunks int, duration time.Duration) error {
	var info ready
	b, e := os.ReadFile(readyFile)
	if e != nil {
		return e
	}
	if e = json.Unmarshal(b, &info); e != nil {
		return e
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(info.CA) {
		return errors.New("invalid CA")
	}
	tlsConfig := &tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS13}
	var call func(context.Context, *pb.Request) (*pb.Reply, error)
	var stream func(context.Context, *pb.Request) (receiver, error)
	if engine == "kana" {
		cfg := rpc.ClientConfig{RequiredFeatures: rpc.FeatureProtobuf | rpc.FeatureServerStreaming, MaxInFlight: 128, StreamWindow: 32, Timeout: 2 * time.Second}
		var c *rpc.Client
		if transport == "tls" {
			c, e = rpc.DialTLS(context.Background(), info.Address, rpc.TLSOptions{Config: tlsConfig}, cfg)
		} else {
			c, e = rpc.Dial(context.Background(), info.Address, cfg)
		}
		if e != nil {
			return e
		}
		defer c.Close()
		call = func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			out := new(pb.Reply)
			e := c.Call(ctx, "/comparison.v1.Bench/Echo", rpc.Protobuf(in), out)
			return out, e
		}
		stream = func(ctx context.Context, in *pb.Request) (receiver, error) {
			s, e := c.Stream(ctx, "/comparison.v1.Bench/Watch", rpc.Protobuf(in))
			return kanaStream{s}, e
		}
	} else {
		var cred credentials.TransportCredentials = insecure.NewCredentials()
		if transport == "tls" {
			cred = credentials.NewTLS(tlsConfig)
		}
		c, e := grpc.NewClient("passthrough:///"+info.Address, grpc.WithTransportCredentials(cred), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1<<20), grpc.MaxCallSendMsgSize(1<<20)))
		if e != nil {
			return e
		}
		defer c.Close()
		sdk := pb.NewBenchClient(c)
		call = func(ctx context.Context, in *pb.Request) (*pb.Reply, error) { return sdk.Echo(ctx, in) }
		stream = func(ctx context.Context, in *pb.Request) (receiver, error) {
			ctx, cancel := context.WithCancel(ctx)
			s, e := sdk.Watch(ctx, in)
			if e != nil {
				cancel()
				return nil, e
			}
			return grpcStream{s, cancel}, nil
		}
	}
	in := &pb.Request{Payload: strings.Repeat("x", size), Chunks: uint32(chunks)}
	execute := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if chunks == 1 {
			out, e := call(ctx, in)
			if e != nil {
				return e
			}
			if out.GetPayload() != in.Payload {
				return errors.New("unary mismatch")
			}
			return nil
		}
		s, e := stream(ctx, in)
		if e != nil {
			return e
		}
		defer s.Close()
		for n := 0; n < chunks; n++ {
			out, e := s.Recv()
			if e != nil {
				return e
			}
			if out.GetPayload() != in.Payload || out.GetSequence() != uint32(n) {
				return errors.New("stream mismatch")
			}
		}
		_, e = s.Recv()
		if e != io.EOF {
			return fmt.Errorf("expected stream EOF: %v", e)
		}
		return nil
	}
	for n := 0; n < 64; n++ {
		if e = execute(); e != nil {
			return fmt.Errorf("warmup: %w", e)
		}
	}
	httpClient := &http.Client{Timeout: 4 * time.Second}
	defer httpClient.CloseIdleConnections()
	serverSnap := func(gc bool) (snapshot, error) {
		url := info.Admin + "/snapshot"
		if gc {
			url += "?gc=1"
		}
		r, e := httpClient.Get(url)
		if e != nil {
			return snapshot{}, e
		}
		defer r.Body.Close()
		var s snapshot
		e = json.NewDecoder(r.Body).Decode(&s)
		return s, e
	}
	serverBefore, e := serverSnap(true)
	if e != nil {
		return e
	}
	runtime.GC()
	if os.Getenv("KANARPC_COMPARE_PROFILE") != "" {
		r, e := httpClient.Get(info.Admin + "/profile/start")
		if e != nil {
			return e
		}
		r.Body.Close()
		if r.StatusCode != 200 {
			return errors.New("server profile failed")
		}
	}
	stopProfile, e := profiles(os.Getenv("KANARPC_COMPARE_PROFILE"))
	if e != nil {
		return e
	}
	defer stopProfile()
	before := snap(nil, nil)
	start := time.Now()
	end := start.Add(duration)
	rows := make([][]sample, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			id := int64(w)
			for time.Now().Before(end) {
				t := time.Now()
				err := execute()
				v := sample{ID: id, Nanos: time.Since(t).Nanoseconds()}
				if err != nil {
					v.Error = err.Error()
				}
				rows[w] = append(rows[w], v)
				id += int64(workers)
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	after := snap(nil, nil)
	stopProfile()
	if os.Getenv("KANARPC_COMPARE_PROFILE") != "" {
		r, e := httpClient.Get(info.Admin + "/profile/stop")
		if e != nil {
			return e
		}
		r.Body.Close()
	}
	serverAfter, e := serverSnap(false)
	if e != nil {
		return e
	}
	all := []sample{}
	latencies := []int64{}
	success := 0
	for _, part := range rows {
		for _, v := range part {
			all = append(all, v)
			latencies = append(latencies, v.Nanos)
			if v.Error == "" {
				success++
			}
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pct := func(q float64) int64 {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[int(float64(len(latencies)-1)*q)]
	}
	result := map[string]any{"engine": engine, "transport": transport, "size": size, "workers": workers, "chunks": chunks, "offered": len(all), "succeeded": success, "failed": len(all) - success, "elapsed_ns": elapsed.Nanoseconds(), "p50_ns": pct(.5), "p95_ns": pct(.95), "p99_ns": pct(.99), "client_before": before, "client_after": after, "server_before": serverBefore, "server_after": serverAfter, "offers": all}
	if e = write(output, result); e != nil {
		return e
	}
	if serverAfter.Connections != 1 {
		return fmt.Errorf("expected one physical connection, got %d", serverAfter.Connections)
	}
	if serverAfter.Calls-serverBefore.Calls != int64(len(all)) {
		return errors.New("offered/admitted count mismatch")
	}
	return nil
}
