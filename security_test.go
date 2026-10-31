package kanarpc

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/testcert"
)

func startTLS(t testing.TB, options TLSOptions, cfg ServerConfig) (*Server, string) {
	t.Helper()
	s, err := NewServer(cfg)
	require(t, err)
	echo(s)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require(t, err)
	done := make(chan error, 1)
	go func() { done <- s.ServeTLS(l, options) }()
	eventually(t, s.Ready)
	t.Cleanup(func() {
		s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require(t, s.Wait(ctx))
		require(t, <-done)
	})
	return s, l.Addr().String()
}
func TestTLSVerifiedAndMutualAuthentication(t *testing.T) {
	for _, mutual := range []bool{false, true} {
		t.Run(map[bool]string{false: "server-auth", true: "mutual"}[mutual], func(t *testing.T) {
			creds, err := testcert.New()
			require(t, err)
			if mutual {
				creds.Server.ClientAuth = tls.RequireAndVerifyClientCert
			}
			_, address := startTLS(t, TLSOptions{Config: creds.Server}, ServerConfig{})
			c, err := DialTLS(context.Background(), address, TLSOptions{Config: creds.Client}, ClientConfig{})
			require(t, err)
			defer c.Close()
			var out int
			require(t, c.Call(context.Background(), "/demo/echo", 42, &out))
			if out != 42 {
				t.Fatal(out)
			}
			for _, kind := range []string{"hostname", "ca", "alpn", "missing-client-cert"} {
				if kind == "missing-client-cert" && !mutual {
					continue
				}
				bad := creds.Client.Clone()
				switch kind {
				case "hostname":
					bad.ServerName = "not-localhost"
				case "ca":
					other, e := testcert.New()
					require(t, e)
					bad.RootCAs = other.Client.RootCAs
				case "alpn":
					bad.NextProtos = []string{"wrong-v2"}
				case "missing-client-cert":
					bad.Certificates = nil
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				var conn net.Conn
				if kind == "alpn" {
					conn, err = (&tls.Dialer{Config: bad}).DialContext(ctx, "tcp", address)
				} else {
					var client *Client
					client, err = DialTLS(ctx, address, TLSOptions{Config: bad}, ClientConfig{})
					if err == nil {
						err = client.Call(ctx, "/demo/echo", 1, &out)
						client.Close()
					}
				}
				cancel()
				if conn != nil {
					closeSocket(conn)
				}
				if err == nil {
					t.Fatal("accepted invalid trust configuration", kind)
				}
			}
		})
	}
}
func TestTLSStalledHandshakeIsBoundedAndDoesNotBlockAccept(t *testing.T) {
	creds, err := testcert.New()
	require(t, err)
	s, address := startTLS(t, TLSOptions{Config: creds.Server, HandshakeTimeout: 40 * time.Millisecond}, ServerConfig{MaxConnections: 2})
	stalled, err := net.Dial("tcp", address)
	require(t, err)
	defer stalled.Close()
	eventually(t, func() bool { return s.Stats().Connections == 1 })
	c, err := DialTLS(context.Background(), address, TLSOptions{Config: creds.Client}, ClientConfig{})
	require(t, err)
	defer c.Close()
	var out int
	require(t, c.Call(context.Background(), "/demo/echo", 7, &out))
	eventually(t, func() bool { return s.Stats().Connections == 1 })
}
func TestRejectUnsafeTLSConfig(t *testing.T) {
	for _, options := range []TLSOptions{{}, {Config: &tls.Config{InsecureSkipVerify: true}}, {Config: &tls.Config{MaxVersion: tls.VersionTLS12}}, {Config: &tls.Config{GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return nil, nil }}}, {Config: &tls.Config{}, HandshakeTimeout: -1}} {
		if _, _, err := prepareTLS(options, false, tlsALPN); err == nil {
			t.Fatal("unsafe config accepted")
		}
	}
}
