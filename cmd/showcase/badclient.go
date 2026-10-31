package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
	quic "github.com/quic-go/quic-go"
)

// This deliberately noncompliant lab peer is separate from the SDK. It reads
// a real END and withholds its ACK. No production cancellation/receipt switch
// is added to the client. The connection stays alive through server drain.
func withholdACK(s *session) (func(), error) {
	ctx, stop := context.WithTimeout(s.ctx, 2*time.Second)
	defer stop()
	security := s.credentials.Client.Clone()
	security.NextProtos = []string{"kanarpc-v2-quic"}
	remote, err := net.ResolveUDPAddr("udp4", s.endpoint)
	if err != nil {
		return nil, err
	}
	packet, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	transport := &quic.Transport{Conn: packet}
	conn, err := transport.Dial(ctx, remote, security, &quic.Config{HandshakeIdleTimeout: time.Second, MaxIdleTimeout: 5 * time.Second})
	if err != nil {
		_ = transport.Close()
		_ = packet.Close()
		return nil, err
	}
	var once sync.Once
	closePeer := func() {
		once.Do(func() {
			_ = conn.CloseWithError(0, "showcase lab peer finished")
			_ = transport.Close()
			_ = packet.Close()
		})
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		closePeer()
		return nil, err
	}
	_ = stream.SetDeadline(time.Now().Add(2 * time.Second))
	body, err := json.Marshal(struct {
		Method  string `json:"method"`
		Budget  int64  `json:"budget_ns"`
		Window  int    `json:"window"`
		Request input  `json:"request"`
	}{fastPath, int64(5 * time.Second), 1, input{Kind: "withhold_ack"}})
	if err == nil {
		err = wire.Write(stream, wire.Frame{Kind: wire.Open, ID: 1, Payload: body}, wire.HardLimit)
	}
	if err != nil {
		closePeer()
		return nil, err
	}
	for {
		f, readErr := wire.Read(stream, wire.HardLimit)
		if readErr != nil {
			closePeer()
			return nil, readErr
		}
		if f.Kind != wire.End {
			continue
		}
		var end struct {
			Code    string `json:"code"`
			Receipt string `json:"receipt"`
		}
		if err = json.Unmarshal(f.Payload, &end); err != nil {
			closePeer()
			return nil, err
		}
		if len(end.Receipt) != 32 {
			closePeer()
			return nil, fmt.Errorf("实验请求未取得回执义务（%s），没有伪造未回执", end.Code)
		}
		if end.Code != "ok" {
			closePeer()
			return nil, errors.New("实验请求未成功执行")
		}
		return closePeer, nil
	}
}
