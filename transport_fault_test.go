package kanarpc

import (
	"context"
	"encoding/json"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/testcert"
)

func TestTransportFaultBlastRadiusControls(t *testing.T) {
	for _, kind := range []Transport{TCP, TLS, QUIC} {
		t.Run(string(kind), func(t *testing.T) {
			credentials, err := testcert.New()
			require(t, err)
			s, err := NewServer(ServerConfig{})
			require(t, err)
			started, release := make(chan struct{}), make(chan struct{})
			var executions atomic.Int64
			require(t, RegisterUnary(s, "/lab/hold", func(ctx context.Context, n int) (int, error) {
				executions.Add(1)
				close(started)
				select {
				case <-ctx.Done():
					return 0, ctx.Err()
				case <-release:
					return n, nil
				}
			}))
			echo(s)
			done := make(chan error, 1)
			var endpoint Endpoint
			if kind == QUIC {
				l, e := ListenQUIC("127.0.0.1:0", TLSOptions{Config: credentials.Server}, QUICConfig{})
				require(t, e)
				endpoint = Endpoint{Address: l.Addr().String(), Transport: kind}
				go func() { done <- s.ServeQUIC(l) }()
			} else {
				l, e := net.Listen("tcp", "127.0.0.1:0")
				require(t, e)
				endpoint = Endpoint{Address: l.Addr().String(), Transport: kind}
				if kind == TLS {
					go func() { done <- s.ServeTLS(l, TLSOptions{Config: credentials.Server}) }()
				} else {
					go func() { done <- s.Serve(l) }()
				}
			}
			eventually(t, s.Ready)
			c, err := DialEndpoint(context.Background(), endpoint, DialOptions{TLS: TLSOptions{Config: credentials.Client}})
			require(t, err)
			t.Cleanup(func() {
				c.Close()
				s.Close()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				require(t, s.Wait(ctx))
				require(t, <-done)
			})
			neighbor := make(chan error, 1)
			go func() { var out int; neighbor <- c.Call(context.Background(), "/lab/hold", 42, &out) }()
			signal(t, started)
			if kind == QUIC {
				q := c.(*QUICClient)
				stream, e := q.conn.OpenStreamSync(context.Background())
				require(t, e)
				_ = stream.SetDeadline(time.Now().Add(time.Second))
				_, e = stream.Write([]byte("invalid record header!!!"))
				require(t, e)
				var b [1]byte
				if _, e = stream.Read(b[:]); e == nil {
					t.Fatal("malformed stream accepted")
				}
				stream.CancelRead(nativeCancel)
				stream.CancelWrite(nativeCancel)
			} else {
				byteClient := c.(*Client)
				require(t, byteClient.link.acquire(context.Background()))
				_, e := byteClient.link.conn.Write([]byte("invalid record header!!!"))
				byteClient.link.release()
				require(t, e)
			}
			var out int
			nextErr := c.Call(context.Background(), "/demo/echo", 7, &out)
			close(release)
			neighborErr := <-neighbor
			if kind == QUIC {
				require(t, nextErr)
				require(t, neighborErr)
			} else {
				if nextErr == nil || neighborErr == nil {
					t.Fatal("byte-stream corruption did not invalidate shared transport")
				}
				reconnected, e := DialEndpoint(context.Background(), endpoint, DialOptions{TLS: TLSOptions{Config: credentials.Client}})
				require(t, e)
				require(t, reconnected.Call(context.Background(), "/demo/echo", 7, &out))
				reconnected.Close()
			}
			if executions.Load() != 1 {
				t.Fatal("business execution replayed", executions.Load())
			}
			b, _ := json.Marshal(map[string]any{"transport": kind, "neighbor_completed": neighborErr == nil, "next_on_same_transport_completed": nextErr == nil, "hold_executions": executions.Load(), "scope": "application-frame corruption, not packet loss"})
			t.Log(string(b))
		})
	}
}
