package kanarpc

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/testcert"
	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

type diagnosticWriter struct {
	*bufio.Writer
	file *os.File
}

func (w diagnosticWriter) Close() error {
	if err := w.Flush(); err != nil {
		_ = w.file.Close()
		return err
	}
	return w.file.Close()
}
func diagnosticTracer(t testing.TB, dir, prefix string) func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace {
	if dir == "" {
		return nil
	}
	return func(_ context.Context, client bool, id quic.ConnectionID) qlogwriter.Trace {
		role := "server"
		if client {
			role = "client"
		}
		path := filepath.Join(dir, fmt.Sprintf("%s-%s-%s.sqlog", prefix, id, role))
		f, err := os.Create(path)
		if err != nil {
			t.Error(err)
			return nil
		}
		trace := qlogwriter.NewConnectionFileSeq(diagnosticWriter{bufio.NewWriter(f), f}, client, id, []string{qlog.EventSchema})
		go trace.Run()
		return trace
	}
}

// Diagnostic repetitions are opt-in via env; the normal test does 2 RPC dials.
// Both modes use the same real RPC listener/TLS/options. "native" calls only
// quic-go DialAddr/Close, so any pre-accept failure there precedes RPC code.
func TestQUICHandshakeChurnDiagnostic(t *testing.T) {
	count := 2
	if s := os.Getenv("KANARPC_CHURN_ATTEMPTS"); s != "" {
		n, err := strconv.Atoi(s)
		require(t, err)
		if n < 1 || n > 100000 {
			t.Fatal("invalid attempts")
		}
		count = n
	}
	dir := os.Getenv("KANARPC_QUIC_DIAG")
	var output *os.File
	if dir != "" {
		require(t, os.MkdirAll(dir, 0700))
		var err error
		output, err = os.OpenFile(filepath.Join(dir, "attempts.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		require(t, err)
		defer output.Close()
	}
	credentials, err := testcert.New()
	require(t, err)
	if value := os.Getenv("KANARPC_IPV4_RESERVATIONS"); value != "" {
		n, err := strconv.Atoi(value)
		require(t, err)
		if n < 0 || n > 256 {
			t.Fatal("invalid IPv4 reservation bound")
		}
		for i := 0; i < n; i++ {
			packet, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			require(t, err)
			defer packet.Close()
		}
		t.Logf("owned IPv4 background port reservations=%d", n)
	}
	s, err := NewServer(ServerConfig{MaxConnections: 128})
	require(t, err)
	echo(s)
	security, handshake, err := prepareTLS(TLSOptions{Config: credentials.Server}, true, quicALPN)
	require(t, err)
	options, config, err := quicConfig(QUICConfig{}, handshake)
	require(t, err)
	config.Tracer = diagnosticTracer(t, dir, "accept")
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	require(t, err)
	transport := &quic.Transport{Conn: packet}
	native, err := transport.Listen(security, config)
	require(t, err)
	listener := &QUICListener{listener: native, transport: transport, packet: packet, cfg: options}
	done := make(chan error, 1)
	go func() { done <- s.ServeQUIC(listener) }()
	eventually(t, s.Ready)
	defer func() {
		s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		require(t, s.Wait(ctx))
		require(t, <-done)
	}()
	var failures atomic.Int64
	modes := []string{"rpc"}
	if os.Getenv("KANARPC_CHURN_ATTEMPTS") != "" {
		modes = []string{"native", "rpc"}
	}
	if mode := os.Getenv("KANARPC_CHURN_MODE"); mode != "" {
		if mode != "native" && mode != "rpc" {
			t.Fatal("invalid mode")
		}
		modes = []string{mode}
	}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			failed := 0
			for i := 0; i < count; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				start := time.Now()
				stage := "dial"
				local := ""
				var failure error
				if mode == "native" {
					clientTLS, timeout, e := prepareTLS(TLSOptions{Config: credentials.Client}, false, quicALPN)
					require(t, e)
					_, cfg, e := quicConfig(QUICConfig{}, timeout)
					require(t, e)
					cfg.MaxIncomingStreams = -1
					cfg.Tracer = diagnosticTracer(t, dir, fmt.Sprintf("native-%06d", i))
					conn, e := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, cfg)
					failure = e
					if e == nil {
						local = conn.LocalAddr().String()
						stage = "close"
						failure = conn.CloseWithError(nativeShutdown, "diagnostic close")
					}
				} else {
					client, e := DialQUIC(ctx, listener.Addr().String(), TLSOptions{Config: credentials.Client}, QUICConfig{}, ClientConfig{})
					failure = e
					if e == nil {
						local = client.conn.LocalAddr().String()
						stage = "call"
						var out int
						failure = client.Call(ctx, "/demo/echo", i, &out)
						if failure == nil && out != i {
							failure = ErrProtocol
						}
						client.Close()
					}
				}
				elapsed := time.Since(start)
				ctxErr := ctx.Err()
				cancel()
				row := map[string]any{"mode": mode, "attempt": i, "stage": stage, "elapsed_ns": elapsed.Nanoseconds(), "local": local, "server": listener.Addr().String(), "error": "", "context_error": "", "server_stats": s.Stats()}
				if failure != nil {
					row["error"] = failure.Error()
					row["error_type"] = fmt.Sprintf("%T", failure)
					failed++
					failures.Add(1)
					t.Logf("FAIL mode=%s attempt=%d stage=%s elapsed=%s error=%v server=%+v", mode, i, stage, elapsed, failure, s.Stats())
				}
				if ctxErr != nil {
					row["context_error"] = ctxErr.Error()
				}
				if output != nil {
					require(t, json.NewEncoder(output).Encode(row))
				}
				if failed >= 3 {
					break
				}
			}
			t.Logf("mode=%s failures=%d limit=%d", mode, failed, count)
		})
	}
	if failures.Load() != 0 {
		t.Fatalf("observed %d connection/exchange failures; diagnostics preserved", failures.Load())
	}
}
