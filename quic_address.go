package kanarpc

import (
	"context"
	"net"
	"net/netip"
)

// Keep name resolution inside the handshake budget. Preserve the original
// hostname for TLS even though quic.Dial receives a resolved UDP address.
func resolveQUICAddress(ctx context.Context, address string, resolver *net.Resolver) (*net.UDPAddr, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	host, service, err := net.SplitHostPort(address)
	if err != nil {
		return nil, "", err
	}
	if host == "" {
		return nil, "", &net.AddrError{Err: "explicit QUIC host required", Addr: address}
	}
	port, err := resolver.LookupPort(ctx, "udp", service)
	if err != nil {
		return nil, "", err
	}
	if literal, err := netip.ParseAddr(host); err == nil {
		return net.UDPAddrFromAddrPort(netip.AddrPortFrom(literal.Unmap(), uint16(port))), host, nil
	}
	addresses, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, "", err
	}
	if len(addresses) == 0 {
		return nil, "", &net.DNSError{Err: "no addresses", Name: host}
	}
	chosen := addresses[0]
	// Match ResolveUDPAddr's IPv4 preference; no implicit connection retry.
	for _, address := range addresses {
		if address.IP.To4() != nil {
			chosen = address
			break
		}
	}
	return &net.UDPAddr{IP: chosen.IP, Port: port, Zone: chosen.Zone}, host, nil
}

func listenQUICPacket(ctx context.Context, remote *net.UDPAddr) (net.PacketConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	network, local := "udp6", "[::]:0"
	if remote.IP.To4() != nil {
		network, local = "udp4", "0.0.0.0:0"
	}
	// An explicit family is essential: Darwin's ephemeral dual-stack wildcard
	// allocation can collide with an existing IPv4 port and divert replies.
	return (&net.ListenConfig{}).ListenPacket(ctx, network, local)
}
