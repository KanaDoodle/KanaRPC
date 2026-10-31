package kanarpc

import (
	"context"
	"errors"
	"net"
	"sort"
	"time"
)

type TCPKeepAliveConfig struct {
	Idle, Interval time.Duration
	Count          int
}

func normalizeKeepAlive(c *TCPKeepAliveConfig) (*TCPKeepAliveConfig, error) {
	if c == nil {
		return nil, nil
	}
	v := *c
	if v.Idle == 0 {
		v.Idle = 30 * time.Second
	}
	if v.Interval == 0 {
		v.Interval = 10 * time.Second
	}
	if v.Count == 0 {
		v.Count = 3
	}
	if v.Idle < time.Second || v.Interval < time.Second || v.Count < 1 || v.Count > 100 {
		return nil, errors.New("kanarpc: invalid keepalive policy")
	}
	return &v, nil
}
func keepAliveConfig(c *TCPKeepAliveConfig) net.KeepAliveConfig {
	if c == nil {
		return net.KeepAliveConfig{}
	}
	return net.KeepAliveConfig{Enable: true, Idle: c.Idle, Interval: c.Interval, Count: c.Count}
}
func applyKeepAlive(conn recordChannel, c *TCPKeepAliveConfig) error {
	if c == nil {
		return nil
	}
	for {
		if inner, ok := conn.(interface{ NetConn() net.Conn }); ok {
			conn = inner.NetConn()
			continue
		}
		break
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		return tcp.SetKeepAliveConfig(keepAliveConfig(c))
	}
	return nil
}

type HealthStatus struct {
	Serving bool   `json:"serving"`
	State   string `json:"state"`
}

func (s *Server) SetServing(serving bool) {
	s.mu.Lock()
	s.notServing = !serving
	s.notifyLocked()
	s.mu.Unlock()
}
func (s *Server) Health() HealthStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := HealthStatus{Serving: s.state == serving && !s.notServing}
	switch s.state {
	case fresh:
		v.State = "starting"
	case serving:
		v.State = "serving"
		if s.notServing {
			v.State = "not_serving"
		}
	case draining:
		v.State = "draining"
	default:
		v.State = "closed"
	}
	return v
}

type MethodInfo struct {
	Path      string `json:"path"`
	Streaming bool   `json:"streaming"`
	Codec     string `json:"codec"`
}

func (s *Server) Methods() []MethodInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]MethodInfo, 0, len(s.methods))
	for path, m := range s.methods {
		codec := "json"
		if m.encoding == encodingProtobuf {
			codec = "protobuf"
		}
		out = append(out, MethodInfo{path, m.streaming, codec})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}
func healthMethod(method string) bool {
	return method == "/kanarpc.health/Check" || method == "/kanarpc.health/Watch"
}

// RegisterHealth exposes JSON health Check/Watch over the same authenticated
// transport and optional authentication policy. State is application-controlled.
func RegisterHealth(s *Server) error {
	if err := RegisterUnary(s, "/kanarpc.health/Check", func(context.Context, struct{}) (HealthStatus, error) { return s.Health(), nil }); err != nil {
		return err
	}
	return RegisterStream(s, "/kanarpc.health/Watch", func(ctx context.Context, _ struct{}, send func(HealthStatus) error) error {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		var previous HealthStatus
		first := true
		for {
			v := s.Health()
			if first || v != previous {
				if e := send(v); e != nil {
					return e
				}
				first = false
				previous = v
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		}
	})
}

// RegisterIntrospection is opt-in and uses the same authentication middleware.
// It publishes method names/shapes, not a gRPC reflection service.
func RegisterIntrospection(s *Server) error {
	return RegisterUnary(s, "/kanarpc.admin/Methods", func(context.Context, struct{}) ([]MethodInfo, error) { return s.Methods(), nil })
}
