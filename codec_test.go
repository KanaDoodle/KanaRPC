package kanarpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/testcert"
	"github.com/KanaDoodle/KanaRPC/internal/wire"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func protoFixture(t *testing.T, transport string, register func(*Server)) (*Server, Caller) {
	t.Helper()
	if transport == "quic" {
		s, c, _ := startQUIC(t, ServerConfig{}, QUICConfig{}, register, ClientConfig{RequiredFeatures: FeatureProtobuf | FeatureServerStreaming, StreamWindow: 2})
		return s, c
	}
	if transport == "tcp" {
		s, address := start(t, ServerConfig{}, register)
		return s, connect(t, address, ClientConfig{RequiredFeatures: FeatureProtobuf | FeatureServerStreaming, StreamWindow: 2})
	}
	credentials, err := testcert.New()
	require(t, err)
	s, err := NewServer(ServerConfig{})
	require(t, err)
	register(s)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require(t, err)
	done := make(chan error, 1)
	go func() { done <- s.ServeTLS(l, TLSOptions{Config: credentials.Server}) }()
	eventually(t, s.Ready)
	c, err := DialTLS(context.Background(), l.Addr().String(), TLSOptions{Config: credentials.Client}, ClientConfig{RequiredFeatures: FeatureProtobuf | FeatureServerStreaming, StreamWindow: 2})
	require(t, err)
	t.Cleanup(func() {
		c.Close()
		s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		require(t, s.Wait(ctx))
		require(t, <-done)
	})
	return s, c
}

func registerProtoEcho(t testing.TB, s *Server) {
	t.Helper()
	require(t, RegisterProtobufUnary(s, "/proto/echo", func() *wrapperspb.StringValue { return new(wrapperspb.StringValue) },
		func(_ context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) { return in, nil }))
	require(t, RegisterProtobufUnary(s, "/proto/empty", func() *emptypb.Empty { return new(emptypb.Empty) },
		func(_ context.Context, in *emptypb.Empty) (*emptypb.Empty, error) { return in, nil }))
}

func TestProtobufMixedCallsAndEmptyMessages(t *testing.T) {
	for _, transport := range []string{"tcp", "tls", "quic"} {
		t.Run(transport, func(t *testing.T) {
			s, caller := protoFixture(t, transport, func(s *Server) { echo(s); registerProtoEcho(t, s) })
			var wg sync.WaitGroup
			for n := 0; n < 16; n++ {
				wg.Add(1)
				go func(n int) {
					defer wg.Done()
					var out int
					if err := caller.Call(context.Background(), "/demo/echo", n, &out); err != nil || out != n {
						t.Errorf("json %d: %d %v", n, out, err)
					}
					message := wrapperspb.String(strings.Repeat("界", n))
					// Binary bytes that are invalid JSON travel without base64 wrapping.
					message.ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 99, protowire.BytesType), []byte{0, 255, 10}))
					var response wrapperspb.StringValue
					if err := caller.Call(context.Background(), "/proto/echo", Protobuf(message), &response); err != nil || !proto.Equal(message, &response) {
						t.Errorf("protobuf %d: %v %v", n, &response, err)
					}
				}(n)
			}
			wg.Wait()
			require(t, caller.Call(context.Background(), "/proto/empty", Protobuf(new(emptypb.Empty)), new(emptypb.Empty)))
			// An encoding mismatch is rejected before either business handler runs.
			before := s.Stats().Admitted
			if err := caller.Call(context.Background(), "/demo/echo", Protobuf(wrapperspb.String("wrong")), new(wrapperspb.StringValue)); statusOf(err).Code != InvalidArgument {
				t.Fatal("protobuf sent to JSON method", err)
			}
			if err := caller.Call(context.Background(), "/proto/echo", "wrong", new(wrapperspb.StringValue)); statusOf(err).Code != InvalidArgument {
				t.Fatal("JSON sent to protobuf method", err)
			}
			if s.Stats().Admitted != before {
				t.Fatal("codec mismatch entered business execution")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			report, err := s.DrainWithReport(ctx)
			require(t, err)
			if !report.Quiescent || report.TotalFaults.LostTerminals != 0 {
				t.Fatal(report)
			}
		})
	}
}

func TestProtobufStreamCancellationAndUnreadMemory(t *testing.T) {
	for _, transport := range []string{"tcp", "tls", "quic"} {
		t.Run(transport, func(t *testing.T) {
			exited := make(chan struct{})
			s, caller := protoFixture(t, transport, func(s *Server) {
				registerProtoEcho(t, s)
				require(t, RegisterProtobufStream(s, "/proto/items", func() *wrapperspb.Int32Value { return new(wrapperspb.Int32Value) },
					func(ctx context.Context, in *wrapperspb.Int32Value, send func(*wrapperspb.StringValue) error) error {
						if in.Value < 0 {
							defer close(exited)
							if err := send(wrapperspb.String("first")); err != nil {
								return err
							}
							<-ctx.Done()
							return ctx.Err()
						}
						for n := int32(0); n < in.Value; n++ {
							if err := send(wrapperspb.String(strings.Repeat("x", int(n)))); err != nil {
								return err
							}
						}
						return nil
					}))
			})
			stream, err := caller.Stream(context.Background(), "/proto/items", Protobuf(wrapperspb.Int32(2)))
			require(t, err)
			// The first response is a valid zero-byte protobuf DATA record.
			for _, expected := range []string{"", "x"} {
				var out wrapperspb.StringValue
				require(t, stream.Recv(&out))
				if out.Value != expected {
					t.Fatal(out.Value)
				}
			}
			if err := stream.Recv(nil); err != io.EOF {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			stream, err = caller.Stream(ctx, "/proto/items", Protobuf(wrapperspb.Int32(-1)))
			require(t, err)
			require(t, stream.Recv(new(wrapperspb.StringValue)))
			cancel()
			if err := stream.Recv(nil); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			signal(t, exited)
			eventually(t, func() bool { return s.Stats().InFlight == 0 })
			// Remote completion does not dispose of an unread response buffer.
			stream, err = caller.Stream(context.Background(), "/proto/items", Protobuf(wrapperspb.Int32(2)))
			require(t, err)
			eventually(t, func() bool {
				return s.Stats().InFlight == 0 && caller.(interface{ MemoryStats() MemoryStats }).MemoryStats().Receive.Used > 0
			})
			require(t, stream.Close())
			eventually(t, func() bool { return caller.(interface{ MemoryStats() MemoryStats }).MemoryStats().Receive.Used == 0 })
		})
	}
}

type countedProto struct {
	*wrapperspb.StringValue
	count *atomic.Int64
}

func (m *countedProto) ProtoReflect() protoreflect.Message {
	m.count.Add(1)
	return m.StringValue.ProtoReflect()
}

func TestProtobufAdmissionLimitsAndMalformedMessages(t *testing.T) {
	s, address := start(t, ServerConfig{}, func(s *Server) { registerProtoEcho(t, s) })
	c := connect(t, address, ClientConfig{RequiredFeatures: FeatureProtobuf, IO: IOConfig{MaxFrameBytes: 1024}, MaxInFlight: 1, Feedback: &FeedbackConfig{}})
	var calls atomic.Int64
	message := &countedProto{wrapperspb.String("value"), &calls}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Call(ctx, "/proto/echo", Protobuf(message), new(wrapperspb.StringValue)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	lease, err := c.resources.admission.enter()
	require(t, err)
	if err := c.Call(context.Background(), "/proto/echo", Protobuf(message), new(wrapperspb.StringValue)); statusOf(err).Code != ResourceExhausted {
		t.Fatal(err)
	}
	lease.release()
	pressure := validPressure("/proto/echo", 1)
	pressure.Recommended = 1
	c.resources.feedback.observe(pressure.Method, &pressure)
	leave, err := c.resources.feedback.enter(pressure.Method)
	require(t, err)
	feedbackErr := c.Call(context.Background(), "/proto/echo", Protobuf(message), new(wrapperspb.StringValue))
	leave()
	if statusOf(feedbackErr).Code != ResourceExhausted {
		t.Fatal("feedback did not reject before protobuf encoding", feedbackErr)
	}
	reserved, err := c.resources.encoding.lease(c.MemoryStats().Encoding.Limit)
	require(t, err)
	wait, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	waitErr := c.Call(wait, "/proto/echo", Protobuf(message), new(wrapperspb.StringValue))
	stop()
	reserved.release()
	if !errors.Is(waitErr, context.DeadlineExceeded) {
		t.Fatal("encoding reservation ignored deadline", waitErr)
	}
	if calls.Load() != 0 {
		t.Fatal("protobuf code ran before admission", calls.Load())
	}
	for _, value := range []any{Protobuf(nil), Protobuf((*wrapperspb.StringValue)(nil)), Protobuf(wrapperspb.String(strings.Repeat("x", 2048)))} {
		if err := c.Call(context.Background(), "/proto/echo", value, new(wrapperspb.StringValue)); statusOf(err).Code != InvalidArgument {
			t.Fatal(err)
		}
	}
	if c.AdmissionStats().Active != 0 || c.MemoryStats().Encoding.Used != 0 || s.Stats().Admitted != 0 {
		t.Fatal(c.AdmissionStats(), c.MemoryStats(), s.Stats())
	}
	c.Close()
	if err := c.Call(context.Background(), "/proto/echo", Protobuf(message), new(wrapperspb.StringValue)); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("closed client encoded protobuf")
	}
	// A malformed binary payload reaches the protobuf decoder, not the handler.
	conn, err := net.Dial("tcp", address)
	require(t, err)
	defer conn.Close()
	metadata := protobufOpenMetadata(nil, "/proto/echo", 1, false, int64(time.Second))
	data := binary.BigEndian.AppendUint32(nil, uint32(len(metadata)))
	data = append(data, metadata...)
	data = append(data, 255)
	require(t, wire.Write(conn, wire.Frame{Kind: wire.OpenEncoded, ID: 1, Payload: data}, 1024))
	f, err := wire.Read(conn, 1024)
	require(t, err)
	defer f.Release()
	if f.Kind != wire.End || !bytes.Contains(f.Payload, []byte(`"invalid_argument"`)) {
		t.Fatal(string(f.Payload))
	}
}

func TestProtobufRegistrationAtomicAndEnvelopeValidation(t *testing.T) {
	s, err := NewServer(ServerConfig{})
	require(t, err)
	valid := ProtobufMethod{Path: "/proto/one", NewRequest: func() proto.Message { return new(emptypb.Empty) }, Unary: func(_ context.Context, in proto.Message) (proto.Message, error) { return in, nil }}
	bad := valid
	bad.Path = "invalid"
	if RegisterProtobufMethods(s, valid, bad) == nil || len(s.methods) != 0 {
		t.Fatal("partial registration", s.methods)
	}
	if RegisterProtobufMethods(s, valid, valid) == nil || len(s.methods) != 0 {
		t.Fatal("duplicate registration")
	}
	require(t, RegisterProtobufMethods(s, valid))
	for _, payload := range [][]byte{nil, {0, 0, 0, 255}, {0, 0, 0, 0}} {
		if _, _, err := decodeOpen(wire.Frame{Kind: wire.OpenEncoded, Payload: payload}); err == nil {
			t.Fatal(payload)
		}
	}
	metadata := []byte(`{"method":"/proto/one","codec":"other"}`)
	data := binary.BigEndian.AppendUint32(nil, uint32(len(metadata)))
	data = append(data, metadata...)
	if _, _, err := decodeOpen(wire.Frame{Kind: wire.OpenEncoded, Payload: data}); err == nil {
		t.Fatal("unknown codec accepted")
	}
	cfg, err := normalizeClient(ClientConfig{})
	require(t, err)
	r, err := newClientResources(cfg)
	require(t, err)
	release, err := r.reserveRead(wire.OpenEncoded, 1024)
	require(t, err)
	if r.snapshot().Receive.Used != 2048 || r.snapshot().ControlReceive.Used != 0 {
		t.Fatal(r.snapshot())
	}
	release()
	if r.snapshot().Receive.Used != 0 {
		t.Fatal(r.snapshot())
	}
}

func FuzzEncodedOpen(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0})
	meta := protobufOpenMetadata(nil, "/proto/echo", 1, false, int64(time.Second))
	seed := binary.BigEndian.AppendUint32(nil, uint32(len(meta)))
	seed = append(seed, meta...)
	f.Add(seed)
	f.Fuzz(func(t *testing.T, b []byte) {
		request, encoding, err := decodeOpen(wire.Frame{Kind: wire.OpenEncoded, Payload: b})
		if err == nil && (encoding != encodingProtobuf || request.Codec != "protobuf" || len(b) < 4 || len(request.Request) > len(b)-4) {
			t.Fatal("invalid envelope accepted")
		}
	})
}
