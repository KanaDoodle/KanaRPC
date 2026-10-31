package kanarpc

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/testcert"
	quic "github.com/quic-go/quic-go"
)

func testFDCount() int {
	for _, path := range []string{"/proc/self/fd", "/dev/fd"} {
		entries, err := os.ReadDir(path)
		if err == nil {
			return len(entries)
		}
	}
	return -1
}

func TestQUICAddressResolutionBudgetAndFamily(t *testing.T) {
	for _, tc := range []struct{ address, ip, host string }{{"127.0.0.1:443", "127.0.0.1", "127.0.0.1"}, {"[::1]:443", "::1", "::1"}, {"[::ffff:127.0.0.1]:443", "127.0.0.1", "::ffff:127.0.0.1"}, {"[fe80::1%lo0]:443", "fe80::1", "fe80::1%lo0"}} {
		address, host, err := resolveQUICAddress(context.Background(), tc.address, net.DefaultResolver)
		require(t, err)
		if address.IP.String() != tc.ip || host != tc.host || address.Port != 443 {
			t.Fatal(address, host)
		}
	}
	entered := make(chan struct{}, 4)
	resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, _, err := resolveQUICAddress(ctx, "budget.invalid.:443", resolver); done <- err }()
	signal(t, entered)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("resolution escaped caller budget", err)
	}
	for _, bad := range []string{"missing-port", ":443", "localhost:-1", "localhost:65536"} {
		if _, _, err := resolveQUICAddress(context.Background(), bad, net.DefaultResolver); err == nil {
			t.Fatal("accepted invalid address", bad)
		}
	}
}

func TestQUICOwnedPacketCloseFailureAndPeerShutdown(t *testing.T) {
	s, c, _ := startQUIC(t, ServerConfig{}, QUICConfig{}, echo)
	if c.packet == nil {
		t.Fatal("packet ownership absent")
	}
	s.Close()
	<-c.conn.Context().Done()
	eventually(t, func() bool { return errors.Is(c.packet.SetReadDeadline(time.Now()), net.ErrClosed) })
	c.Close()
	if !errors.Is(c.packet.SetWriteDeadline(time.Now()), net.ErrClosed) {
		t.Fatal("Close retained packet")
	}
	blackhole, err := net.ListenPacket("udp4", "127.0.0.1:0")
	require(t, err)
	defer blackhole.Close()
	credentials, err := testcert.New()
	require(t, err)
	before := testFDCount()
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		_, err = DialQUIC(ctx, blackhole.LocalAddr().String(), TLSOptions{Config: credentials.Client}, QUICConfig{}, ClientConfig{})
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	}
	eventually(t, func() bool { return testFDCount() <= before })
	if after := testFDCount(); before >= 0 && after > before {
		t.Fatal("failed handshakes retained file descriptors", before, after)
	}
}

func TestQUICHostnameInferenceAndIPv6AfterFamilySelection(t *testing.T) {
	for _, host := range []string{"localhost", "::1"} {
		t.Run(host, func(t *testing.T) {
			credentials, err := testcert.New()
			require(t, err)
			credentials.Client.ServerName = ""
			s, err := NewServer(ServerConfig{})
			require(t, err)
			echo(s)
			address := "127.0.0.1:0"
			if host == "::1" {
				address = "[::1]:0"
			}
			l, err := ListenQUIC(address, TLSOptions{Config: credentials.Server}, QUICConfig{})
			require(t, err)
			done := make(chan error, 1)
			go func() { done <- s.ServeQUIC(l) }()
			eventually(t, s.Ready)
			defer func() {
				s.Close()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				require(t, s.Wait(ctx))
				require(t, <-done)
			}()
			_, port, err := net.SplitHostPort(l.Addr().String())
			require(t, err)
			c, err := DialQUIC(context.Background(), net.JoinHostPort(host, port), TLSOptions{Config: credentials.Client}, QUICConfig{}, ClientConfig{})
			require(t, err)
			defer c.Close()
			var out int
			require(t, c.Call(context.Background(), "/demo/echo", 1, &out))
			if out != 1 {
				t.Fatal(out)
			}
			is4 := c.conn.LocalAddr().(*net.UDPAddr).IP.To4() != nil
			if is4 != (host != "::1") {
				t.Fatal("wrong address family", c.conn.LocalAddr())
			}
		})
	}
}

func FuzzQUICLiteralAddress(f *testing.F) {
	f.Add("127.0.0.1:443")
	f.Add("[::1]:443")
	f.Add("[::ffff:127.0.0.1]:0")
	f.Add("[fe80::1%lo0]:65535")
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 1024 {
			return
		}
		host, _, err := net.SplitHostPort(input)
		if err != nil {
			return
		}
		literal, err := netip.ParseAddr(host)
		if err != nil {
			return
		}
		// Literal-only inputs never issue DNS traffic.
		address, name, err := resolveQUICAddress(context.Background(), input, net.DefaultResolver)
		if err == nil && (name != host || address.Port < 0 || address.Port > 65535 || address.IP.String() != literal.Unmap().WithZone("").String() || address.Zone != literal.Unmap().Zone()) {
			t.Fatal(input, address, name)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, _, err := resolveQUICAddress(ctx, input, net.DefaultResolver); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}

func collidingUDPPair(t testing.TB) (*net.UDPConn, *net.UDPConn) {
	t.Helper()
	owned := make(map[int]*net.UDPConn)
	for i := 0; i < 64; i++ {
		socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		require(t, err)
		owned[socket.LocalAddr().(*net.UDPAddr).Port] = socket
		t.Cleanup(func() { _ = socket.Close() })
	}
	// Fixed-port binds correctly reject the collision. The defect is in the
	// kernel's port=0 ephemeral allocation for this dual-stack wildcard socket.
	for i := 0; i < 8192; i++ {
		socket, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero})
		require(t, err)
		port := socket.LocalAddr().(*net.UDPAddr).Port
		if sink := owned[port]; sink != nil {
			t.Logf("acquired owned cross-family collision port=%d after %d allocations", port, i+1)
			t.Cleanup(func() { _ = socket.Close() })
			return socket, sink
		}
		_ = socket.Close()
	}
	t.Fatal("could not acquire affected kernel ephemeral allocation in bounded search")
	return nil, nil
}

// Darwin permits a dual-stack IPv6 wildcard bind to reuse an IPv4 port.
// Replies to the dual-stack socket's IPv4 traffic then reach the IPv4 owner.
// This test supplies both sockets itself; it never reads another app's traffic.
func TestQUICDarwinCrossFamilyPortCollision(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin-specific cross-family UDP demultiplexing; Linux has a separate control")
	}
	old, sink := collidingUDPPair(t)
	port := sink.LocalAddr().(*net.UDPAddr).Port
	if old.LocalAddr().(*net.UDPAddr).IP.To4() != nil {
		t.Fatal("fixture did not create the affected dual-stack socket", old.LocalAddr())
	}
	echo, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require(t, err)
	defer echo.Close()
	require(t, old.SetDeadline(time.Now().Add(time.Second)))
	require(t, echo.SetDeadline(time.Now().Add(time.Second)))
	require(t, sink.SetDeadline(time.Now().Add(time.Second)))
	_, err = old.WriteToUDP([]byte("request"), echo.LocalAddr().(*net.UDPAddr))
	require(t, err)
	var body [32]byte
	_, peer, err := echo.ReadFromUDP(body[:])
	require(t, err)
	_, err = echo.WriteToUDP([]byte("reply"), peer)
	require(t, err)
	n, _, err := sink.ReadFromUDP(body[:])
	require(t, err)
	if string(body[:n]) != "reply" {
		t.Fatal("wrong sink data")
	}
	require(t, old.SetReadDeadline(time.Now().Add(10*time.Millisecond)))
	if _, _, err = old.ReadFromUDP(body[:]); err == nil {
		t.Fatal("expected reply was not diverted to IPv4 owner")
	}
	_, err = net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: port})
	if err == nil {
		t.Fatal("same-family bind did not reject occupied port")
	}
}

// Acquire a collision with a bounded search, then deterministically demonstrate
// diverted handshake replies using only owned sockets. Global OS port ranges
// and unrelated listeners remain untouched.
func TestQUICCollisionBlocksHandshakeBeforeRPC(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin-specific collision reproduction")
	}
	credentials, err := testcert.New()
	require(t, err)
	s, err := NewServer(ServerConfig{})
	require(t, err)
	echo(s)
	listener, err := ListenQUIC("127.0.0.1:0", TLSOptions{Config: credentials.Server}, QUICConfig{})
	require(t, err)
	done := make(chan error, 1)
	go func() { done <- s.ServeQUIC(listener) }()
	eventually(t, s.Ready)
	defer func() {
		s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require(t, s.Wait(ctx))
		require(t, <-done)
	}()
	packet, sink := collidingUDPPair(t)
	tlsCfg, timeout, err := prepareTLS(TLSOptions{Config: credentials.Client}, false, quicALPN)
	require(t, err)
	_, cfg, err := quicConfig(QUICConfig{}, timeout)
	require(t, err)
	remote, err := net.ResolveUDPAddr("udp", listener.Addr().String())
	require(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err = quic.Dial(ctx, packet, remote, tlsCfg, cfg)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("collision did not reproduce lost handshake", err)
	}
	require(t, sink.SetReadDeadline(time.Now().Add(time.Second)))
	var data [2048]byte
	n, _, err := sink.ReadFromUDP(data[:])
	require(t, err)
	if n < 1200 {
		t.Fatal("IPv4 sink did not receive the QUIC server's Initial", n)
	}
	if s.Stats().Admitted != 0 || s.Stats().Connections != 0 {
		t.Fatal("collision crossed into RPC admission", s.Stats())
	}
	// The managed socket allocator must reserve an address in the same family
	// as its IPv4 destination; the old default DialAddr policy fails this fence.
	client, err := DialQUIC(context.Background(), listener.Addr().String(), TLSOptions{Config: credentials.Client}, QUICConfig{}, ClientConfig{})
	require(t, err)
	defer client.Close()
	if client.conn.LocalAddr().(*net.UDPAddr).IP.To4() == nil {
		t.Fatal("RPC IPv4 destination still uses cross-family wildcard socket", client.conn.LocalAddr())
	}
	var out int
	require(t, client.Call(context.Background(), "/demo/echo", 42, &out))
	if out != 42 {
		t.Fatal(out)
	}
}
