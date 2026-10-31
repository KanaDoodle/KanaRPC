package netfault

import (
	"errors"
	"net"
	"testing"
	"time"
)

func TestDropRecoveryDelayAndBoundedClose(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	r, err := NewUDP(server.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	address, err := net.ResolveUDPAddr("udp4", r.Address())
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.DialUDP("udp4", nil, address)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = r.Set(ToServer, Plan{DropNext: 1}); err != nil {
		t.Fatal(err)
	}
	_, err = c.Write([]byte("dropped"))
	if err != nil {
		t.Fatal(err)
	}
	_ = server.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	var b [16]byte
	if _, _, err = server.ReadFromUDP(b[:]); err == nil {
		t.Fatal("drop plan did not drop")
	}
	if err = r.Set(ToServer, Plan{Delay: 10 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = c.Write([]byte("forwarded"))
	if err != nil {
		t.Fatal(err)
	}
	_ = server.SetReadDeadline(time.Now().Add(time.Second))
	n, peer, err := server.ReadFromUDP(b[:])
	if err != nil || string(b[:n]) != "forwarded" || time.Since(start) < 10*time.Millisecond {
		t.Fatal(n, err, time.Since(start))
	}
	_, err = server.WriteToUDP([]byte("reply"), peer)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	n, err = c.Read(b[:])
	if err != nil || string(b[:n]) != "reply" {
		t.Fatal(n, err)
	}
	if err = r.Set(ToServer, Plan{Delay: time.Second}); err != nil {
		t.Fatal(err)
	}
	_, err = c.Write([]byte("held"))
	if err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(time.Second)
	for r.Stats().ToServer.PendingBytes == 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	start = time.Now()
	r.Close()
	s := r.Stats()
	if time.Since(start) > 500*time.Millisecond || !s.Closed || s.ToServer.PendingBytes != 0 || s.ToClient.PendingBytes != 0 || s.ToServer.Dropped != 1 || s.ToServer.PeakPendingBytes > 65536 {
		t.Fatal(s, time.Since(start))
	}
	if err = r.Set(ToClient, Plan{}); !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
}
