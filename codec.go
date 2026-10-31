package kanarpc

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
	"google.golang.org/protobuf/proto"
)

type payloadEncoding uint8

const (
	encodingJSON payloadEncoding = iota
	encodingProtobuf
	maxOpenMetadata = 1024
)

// ProtobufRequest selects binary Protobuf for one call, including its responses.
// Construct it with Protobuf. JSON remains the default for unwrapped requests.
type ProtobufRequest struct{ message proto.Message }

// Protobuf wraps a message without encoding it. Admission and byte reservation
// still happen before user message code runs. Do not mutate a message during Call
// or Stream creation; the Protobuf runtime does not support concurrent mutation.
func Protobuf(message proto.Message) ProtobufRequest { return ProtobufRequest{message} }

func requestEncoding(request any) (payloadEncoding, any) {
	switch v := request.(type) {
	case ProtobufRequest:
		return encodingProtobuf, v.message
	case *ProtobufRequest:
		if v == nil {
			return encodingProtobuf, nil
		}
		return encodingProtobuf, v.message
	default:
		return encodingJSON, request
	}
}

func nilMessage(m proto.Message) bool { return m == nil || !m.ProtoReflect().IsValid() }

func marshalProtobuf(value any, limit uint32) ([]byte, error) {
	m, ok := value.(proto.Message)
	if !ok || nilMessage(m) {
		return nil, errors.New("kanarpc: expected a non-nil protobuf message")
	}
	if uint64(proto.Size(m)) > uint64(limit) {
		return nil, errors.New("kanarpc: protobuf message exceeds frame limit")
	}
	b, err := proto.Marshal(m)
	if err == nil && uint64(len(b)) > uint64(limit) {
		return nil, errors.New("kanarpc: protobuf message exceeds frame limit")
	}
	return b, err
}

func unmarshalProtobuf(body []byte, value any) error {
	m, ok := value.(proto.Message)
	if !ok || nilMessage(m) {
		return errors.New("kanarpc: expected a non-nil protobuf response")
	}
	return (proto.UnmarshalOptions{RecursionLimit: 64}).Unmarshal(body, m)
}

// decodeOpen keeps JSON OPEN byte-compatible and gives binary calls a distinct
// record kind. Legacy peers reject that kind rather than executing it as JSON.
func decodeOpen(f wire.Frame) (openRecord, payloadEncoding, error) {
	var request openRecord
	if f.Kind == wire.OpenOptions {
		return decodeOptionsOpen(f)
	}
	if f.Kind == wire.Open {
		err := json.Unmarshal(f.Payload, &request)
		if err != nil || request.Codec != "" || request.Compression != "" || request.AcceptGzip || request.PlainBytes != 0 || len(request.Metadata) > 0 || !json.Valid(request.Request) {
			return request, encodingJSON, ErrProtocol
		}
		return request, encodingJSON, nil
	}
	if len(f.Payload) < 4 {
		return request, encodingProtobuf, ErrProtocol
	}
	n := binary.BigEndian.Uint32(f.Payload[:4])
	if n == 0 || n > maxOpenMetadata || uint64(n)+4 > uint64(len(f.Payload)) {
		return request, encodingProtobuf, ErrProtocol
	}
	if err := json.Unmarshal(f.Payload[4:4+n], &request); err != nil || request.Codec != "protobuf" || request.Request != nil || request.Compression != "" || request.AcceptGzip || request.PlainBytes != 0 || len(request.Metadata) > 0 {
		return request, encodingProtobuf, ErrProtocol
	}
	request.Request = f.Payload[4+n:]
	return request, encodingProtobuf, nil
}

func protobufOpenMetadata(dst []byte, method string, window uint32, streaming bool, budget int64) []byte {
	dst = openFields(dst, method, window, streaming, budget)
	return append(dst, `,"codec":"protobuf"}`...)
}

// Receiver is shared by direct and routing-managed streams. Close releases a
// stream early; EOF and errors also let a routing stream release its references.
type Receiver interface {
	Recv(any) error
	Close() error
}

// ServiceCaller is the transport-independent interface used by generated SDKs.
// Implementations honor Protobuf(request) and preserve stream ownership.
type ServiceCaller interface {
	Call(context.Context, string, any, any) error
	OpenStream(context.Context, string, any) (Receiver, error)
}

func (c *Client) OpenStream(ctx context.Context, method string, request any) (Receiver, error) {
	return c.Stream(ctx, method, request)
}

func (c *QUICClient) OpenStream(ctx context.Context, method string, request any) (Receiver, error) {
	return c.Stream(ctx, method, request)
}

// ProtobufMethod binds one method to fresh request messages. Exactly one of
// Unary or Stream must be set. Generated registration supplies typed adapters.
type ProtobufMethod struct {
	Path       string
	NewRequest func() proto.Message
	Unary      func(context.Context, proto.Message) (proto.Message, error)
	Stream     func(context.Context, proto.Message, func(proto.Message) error) error
}

// RegisterProtobufMethods installs a service atomically before Serve. Invalid or
// duplicate bindings leave the server unchanged.
func RegisterProtobufMethods(s *Server, bindings ...ProtobufMethod) error {
	if s == nil {
		return errors.New("kanarpc: nil server")
	}
	methods := make(map[string]method, len(bindings))
	for _, binding := range bindings {
		if binding.NewRequest == nil || (binding.Unary == nil) == (binding.Stream == nil) {
			return errors.New("kanarpc: invalid protobuf method binding")
		}
		if _, exists := methods[binding.Path]; exists {
			return errors.New("kanarpc: duplicate method")
		}
		b := binding
		methods[b.Path] = method{encoding: encodingProtobuf, streaming: b.Stream != nil, run: func(ctx context.Context, body json.RawMessage, out *Sender) error {
			request := b.NewRequest()
			if nilMessage(request) {
				return &Status{Code: Internal, Message: "protobuf request factory returned nil"}
			}
			if err := unmarshalProtobuf(body, request); err != nil {
				return &Status{Code: InvalidArgument, Message: "protobuf request decode failed"}
			}
			send := func(response proto.Message) error {
				data, err := marshalProtobuf(response, out.request.peer.link.cfg.MaxFrameBytes)
				if err != nil {
					return err
				}
				return out.Send(data)
			}
			if b.Stream != nil {
				return b.Stream(ctx, request, send)
			}
			response, err := b.Unary(ctx, request)
			if err != nil {
				return err
			}
			return send(response)
		}}
	}
	return s.registerMethods(methods)
}

func RegisterProtobufUnary[Request, Response proto.Message](s *Server, path string, newRequest func() Request, handler func(context.Context, Request) (Response, error)) error {
	if newRequest == nil || handler == nil {
		return errors.New("kanarpc: nil protobuf handler or request factory")
	}
	return RegisterProtobufMethods(s, ProtobufMethod{Path: path,
		NewRequest: func() proto.Message { return newRequest() },
		Unary: func(ctx context.Context, request proto.Message) (proto.Message, error) {
			return handler(ctx, request.(Request))
		},
	})
}

func RegisterProtobufStream[Request, Response proto.Message](s *Server, path string, newRequest func() Request, handler func(context.Context, Request, func(Response) error) error) error {
	if newRequest == nil || handler == nil {
		return errors.New("kanarpc: nil protobuf handler or request factory")
	}
	return RegisterProtobufMethods(s, ProtobufMethod{Path: path,
		NewRequest: func() proto.Message { return newRequest() },
		Stream: func(ctx context.Context, request proto.Message, send func(proto.Message) error) error {
			return handler(ctx, request.(Request), func(response Response) error { return send(response) })
		},
	})
}

type serviceCallerAdapter struct{ Caller }

func (c serviceCallerAdapter) OpenStream(ctx context.Context, method string, request any) (Receiver, error) {
	return c.Stream(ctx, method, request)
}

// AsServiceCaller bridges the older concrete-Stream Caller contract without
// changing implementations or introducing a second call lifecycle.
func AsServiceCaller(c Caller) ServiceCaller {
	if c == nil {
		return nil
	}
	if s, ok := c.(ServiceCaller); ok {
		return s
	}
	return serviceCallerAdapter{c}
}
