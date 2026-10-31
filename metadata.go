package kanarpc

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"unicode/utf8"
)

// Metadata is UTF-8 call metadata. Keys are lowercase ASCII; credentials are
// untrusted until Authenticate validates them. Values are never logged by core.
type Metadata map[string]string

const MaxMetadataBytes = 4096

func cloneMetadata(m Metadata) Metadata {
	if m == nil {
		return nil
	}
	out := make(Metadata, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
func validateMetadata(m Metadata) error {
	n := 0
	encoded := 2
	entries := 0
	if len(m) > 64 {
		return errors.New("kanarpc: too many metadata keys")
	}
	for k, v := range m {
		if len(k) == 0 || len(k) > 64 || !utf8.ValidString(v) || strings.ContainsAny(v, "\r\n\x00") {
			return errors.New("kanarpc: invalid metadata")
		}
		for _, c := range k {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
				return errors.New("kanarpc: invalid metadata key")
			}
		}
		if entries > 0 {
			encoded++
		}
		entries++
		encoded += len(k) + 5
		for _, c := range v {
			switch {
			case c < 32 || c == '<' || c == '>' || c == '&' || c == 0x2028 || c == 0x2029:
				encoded += 6
			case c == '"' || c == '\\':
				encoded += 2
			default:
				encoded += utf8.RuneLen(c)
			}
		}
		if encoded > MaxMetadataBytes {
			return errors.New("kanarpc: encoded metadata exceeds limit")
		}
		n += len(k) + len(v)
		if n > MaxMetadataBytes {
			return errors.New("kanarpc: metadata exceeds limit")
		}
	}
	return nil
}

type outgoingMetadataKey struct{}
type incomingMetadataKey struct{}

// WithOutgoingMetadata copies m; validation happens before request encoding.
func WithOutgoingMetadata(ctx context.Context, m Metadata) context.Context {
	return context.WithValue(ctx, outgoingMetadataKey{}, cloneMetadata(m))
}
func OutgoingMetadata(ctx context.Context) Metadata {
	m, _ := ctx.Value(outgoingMetadataKey{}).(Metadata)
	return cloneMetadata(m)
}
func IncomingMetadata(ctx context.Context) Metadata {
	m, _ := ctx.Value(incomingMetadataKey{}).(Metadata)
	return cloneMetadata(m)
}

type Identity struct {
	Subject string
	Claims  map[string]string
}
type identityKey struct{}

func AuthenticatedIdentity(ctx context.Context) (Identity, bool) {
	v, ok := ctx.Value(identityKey{}).(Identity)
	if ok {
		v.Claims = cloneMetadata(v.Claims)
	}
	return v, ok
}

// Authenticator validates credentials from IncomingMetadata. It executes in the
// bounded request worker, before decompression and application decoding.
type Authenticator func(context.Context, CallInfo) (Identity, error)

// MetadataCapture reads response trailers safely, including while a call runs.
// Trailers become available with END; they are not initial response headers.
type MetadataCapture struct {
	mu    sync.Mutex
	value Metadata
}

func (c *MetadataCapture) Snapshot() Metadata {
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneMetadata(c.value)
}

type captureKey struct{}

func CaptureTrailers(ctx context.Context, capture *MetadataCapture) context.Context {
	return context.WithValue(ctx, captureKey{}, capture)
}

type trailerWriterKey struct{}

func SetTrailer(ctx context.Context, metadata Metadata) error {
	r, ok := ctx.Value(trailerWriterKey{}).(*execution)
	if !ok || !r.options {
		return &Status{Code: FailedPrecondition, Message: "response metadata requires call options"}
	}
	if e := validateMetadata(metadata); e != nil {
		return &Status{Code: InvalidArgument, Message: e.Error()}
	}
	encoded, e := json.Marshal(metadata)
	limit := r.peer.link.cfg.MaxFrameBytes
	if limit > 4096 {
		limit = 4096
	}
	if e != nil || len(encoded) > int(limit)-512 {
		return &Status{Code: InvalidArgument, Message: "trailer exceeds control limit"}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminal {
		return ErrClosed
	}
	r.trailers = cloneMetadata(metadata)
	return nil
}
