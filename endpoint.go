package kanarpc

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"strings"
)

type Transport string

const (
	TCP  Transport = "tcp"
	TLS  Transport = "tls"
	QUIC Transport = "quic"
)

type Endpoint struct {
	ID        string    `json:"id"`
	Address   string    `json:"address"`
	Transport Transport `json:"transport"`
}

func (e Endpoint) Validate() error {
	if e.Transport != TCP && e.Transport != TLS && e.Transport != QUIC {
		return errors.New("kanarpc: explicit transport required")
	}
	host, port, err := net.SplitHostPort(e.Address)
	if err != nil || host == "" || len(e.Address) > 512 {
		return errors.New("kanarpc: invalid endpoint address")
	}
	if address, err := netip.ParseAddr(host); err == nil {
		if address.IsUnspecified() {
			return errors.New("kanarpc: unspecified address is not an advertised endpoint")
		}
	} else {
		for _, label := range strings.Split(strings.TrimSuffix(host, "."), ".") {
			if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
				return errors.New("kanarpc: invalid endpoint hostname")
			}
			for _, c := range label {
				if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
					return errors.New("kanarpc: invalid endpoint hostname")
				}
			}
		}
	}
	for _, c := range port {
		if c < '0' || c > '9' {
			return errors.New("kanarpc: numeric endpoint port required")
		}
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return errors.New("kanarpc: invalid endpoint port")
	}
	return nil
}

type Caller interface {
	Call(context.Context, string, any, any) error
	Stream(context.Context, string, any) (*Stream, error)
	Close()
}
type DialOptions struct {
	Client ClientConfig
	TLS    TLSOptions
	QUIC   QUICConfig
}

func DialEndpoint(ctx context.Context, endpoint Endpoint, options DialOptions) (Caller, error) {
	if err := endpoint.Validate(); err != nil {
		return nil, err
	}
	switch endpoint.Transport {
	case TCP:
		return Dial(ctx, endpoint.Address, options.Client)
	case TLS:
		return DialTLS(ctx, endpoint.Address, options.TLS, options.Client)
	case QUIC:
		return DialQUIC(ctx, endpoint.Address, options.TLS, options.QUIC, options.Client)
	}
	return nil, ErrProtocol
}
