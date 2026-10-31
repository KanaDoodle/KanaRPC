// Package kanarpc implements context-first unary and server-streaming RPC.
// The v2 experimental protocol is intentionally incompatible with KanaRPC-Go.
package kanarpc

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

type Code string

const (
	OK                 Code = "ok"
	Canceled           Code = "canceled"
	DeadlineExceeded   Code = "deadline_exceeded"
	ResourceExhausted  Code = "resource_exhausted"
	Unavailable        Code = "unavailable"
	InvalidArgument    Code = "invalid_argument"
	Internal           Code = "internal"
	NotFound           Code = "not_found"
	Unauthenticated    Code = "unauthenticated"
	PermissionDenied   Code = "permission_denied"
	FailedPrecondition Code = "failed_precondition"
)

type Status struct {
	Code    Code   `json:"code"`
	Message string `json:"message,omitempty"`
}

func (s *Status) Error() string {
	if s == nil {
		return "kanarpc: nil status"
	}
	return string(s.Code) + ": " + s.Message
}
func (s *Status) Is(target error) bool {
	if s == nil {
		return false
	}
	return s.Code == Canceled && target == context.Canceled || s.Code == DeadlineExceeded && target == context.DeadlineExceeded
}

var (
	ErrClosed   = errors.New("kanarpc: closed")
	ErrDraining = errors.New("kanarpc: draining")
	ErrProtocol = errors.New("kanarpc: peer violated protocol")
	ErrDelivery = errors.New("kanarpc: terminal receipt not acknowledged")
)

func knownCode(c Code) bool {
	switch c {
	case OK, Canceled, DeadlineExceeded, ResourceExhausted, Unavailable, InvalidArgument, Internal, NotFound, Unauthenticated, PermissionDenied, FailedPrecondition:
		return true
	}
	return false
}

// Only explicit Status messages are public. Unexpected handler errors and
// panic values stay out of the response, avoiding accidental credential leaks.
func statusOf(err error) (result Status) {
	// Application-defined Is/As/Unwrap methods can panic too. Keep error
	// conversion inside the same trust boundary as the handler invocation.
	defer func() {
		if recover() != nil {
			result = Status{Code: Internal, Message: "handler error classification failed"}
		}
	}()
	if err == nil {
		return Status{Code: OK}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return Status{Code: DeadlineExceeded}
	}
	if errors.Is(err, context.Canceled) {
		return Status{Code: Canceled}
	}
	var s *Status
	if errors.As(err, &s) && s != nil && knownCode(s.Code) && s.Code != OK && len(s.Message) <= 256 && utf8.ValidString(s.Message) {
		return *s
	}
	return Status{Code: Internal, Message: "handler failed"}
}

func validMethod(method string) bool {
	if len(method) < 3 || len(method) > 128 || method[0] != '/' || strings.Count(method, "/") != 2 {
		return false
	}
	parts := strings.Split(method[1:], "/")
	for _, p := range parts {
		if p == "" {
			return false
		}
		for _, c := range p {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
				return false
			}
		}
	}
	return true
}

type openRecord struct {
	Metadata    Metadata        `json:"metadata,omitempty"`
	PlainBytes  uint32          `json:"plain_bytes,omitempty"`
	Compression string          `json:"compression,omitempty"`
	AcceptGzip  bool            `json:"accept_gzip,omitempty"`
	Method      string          `json:"method"`
	BudgetNanos int64           `json:"budget_ns"`
	Window      uint32          `json:"window"`
	Stream      bool            `json:"stream"`
	Codec       string          `json:"codec,omitempty"`
	Request     json.RawMessage `json:"request,omitempty"`
}
type endRecord struct {
	Metadata Metadata `json:"metadata,omitempty"`
	Status
	Receipt  string    `json:"receipt,omitempty"`
	Pressure *Pressure `json:"pressure,omitempty"`
}

// Limit is normalized to at least 1 KiB. Preserve code and receipt; optional
// diagnostics may need shortening after JSON escaping and pressure metadata.
func marshalEnd(record endRecord, limit uint32) []byte {
	if limit > 4096 {
		limit = 4096 // independent control-payload cap
	}
	for {
		body, _ := json.Marshal(record)
		if len(body) <= int(limit) {
			return body
		}
		if record.Message != "" {
			cut := len(record.Message) / 2
			for cut > 0 && !utf8.RuneStart(record.Message[cut]) {
				cut--
			}
			record.Message = record.Message[:cut]
		} else {
			// A valid code/receipt without optional pressure always fits the
			// minimum frame. Never lose a terminal because of advisory data.
			record.Pressure = nil
		}
	}
}

// StatusCode returns the sanitized framework classification for instrumentation.
func StatusCode(err error) Code { return statusOf(err).Code }
