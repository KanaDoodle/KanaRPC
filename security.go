package kanarpc

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"time"
)

const tlsALPN = "kanarpc-v2-tls"
const quicALPN = "kanarpc-v2-quic"

type TLSOptions struct {
	Config           *tls.Config
	HandshakeTimeout time.Duration
}

func prepareTLS(options TLSOptions, server bool, alpn string) (*tls.Config, time.Duration, error) {
	if options.Config == nil {
		return nil, 0, errors.New("kanarpc: TLS config required")
	}
	c := options.Config.Clone()
	if c.InsecureSkipVerify || c.GetConfigForClient != nil || c.MaxVersion != 0 && c.MaxVersion < tls.VersionTLS13 {
		return nil, 0, errors.New("kanarpc: insecure or dynamically replaceable TLS config")
	}
	if server && len(c.Certificates) == 0 && c.GetCertificate == nil {
		return nil, 0, errors.New("kanarpc: server certificate required")
	}
	if server && c.ClientAuth != tls.NoClientCert && c.ClientAuth != tls.RequireAndVerifyClientCert {
		return nil, 0, errors.New("kanarpc: client certificates must be required and verified")
	}
	if server && c.ClientAuth == tls.RequireAndVerifyClientCert && c.ClientCAs == nil {
		return nil, 0, errors.New("kanarpc: client CA required for mTLS")
	}
	c.MinVersion = tls.VersionTLS13
	c.NextProtos = []string{alpn}
	timeout := options.HandshakeTimeout
	if timeout == 0 {
		timeout = 3 * time.Second
	}
	if timeout <= 0 {
		return nil, 0, errors.New("kanarpc: invalid handshake timeout")
	}
	return c, timeout, nil
}

// ServeTLS counts accepted sockets, including pending bounded handshakes,
// against MaxConnections. A stalled handshake does not block other accepts.
func (s *Server) ServeTLS(listener net.Listener, options TLSOptions) error {
	c, timeout, err := prepareTLS(options, true, tlsALPN)
	if err != nil {
		return err
	}
	return s.serveRecords(listener, c, timeout)
}

func DialTLS(ctx context.Context, address string, options TLSOptions, cfg ClientConfig) (*Client, error) {
	var err error
	if cfg, err = normalizeClient(cfg); err != nil {
		return nil, err
	}
	c, timeout, err := prepareTLS(options, false, tlsALPN)
	if err != nil {
		return nil, err
	}
	if cfg.Timeout < timeout {
		timeout = cfg.Timeout
	}
	budget, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := (&tls.Dialer{Config: c}).DialContext(budget, "tcp", address)
	if err != nil {
		return nil, err
	}
	if conn.(*tls.Conn).ConnectionState().NegotiatedProtocol != tlsALPN {
		closeSocket(conn)
		return nil, ErrProtocol
	}
	client, err := NewClient(conn, cfg)
	if err != nil {
		closeSocket(conn)
	}
	return client, err
}
