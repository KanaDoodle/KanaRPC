package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
)

type state struct {
	TraceStart       uint64                 `json:"trace_start"`
	Scope            string                 `json:"scope"`
	Now              time.Time              `json:"now"`
	Session          int                    `json:"session"`
	Mode             string                 `json:"mode"`
	State            string                 `json:"state"`
	Endpoint         string                 `json:"endpoint"`
	Busy             bool                   `json:"busy"`
	Message          string                 `json:"message"`
	Config           loadConfig             `json:"config"`
	Active           int                    `json:"active"`
	Generated        uint64                 `json:"generated"`
	GeneratorDropped uint64                 `json:"generator_dropped"`
	Metrics          map[string]metric      `json:"metrics"`
	Stats            rpc.ServerStats        `json:"stats"`
	Memory           rpc.MemoryStats        `json:"memory"`
	ClientMemory     rpc.MemoryStats        `json:"client_memory"`
	Governance       rpc.GovernanceSnapshot `json:"governance"`
	Feedback         rpc.FeedbackStats      `json:"feedback"`
	Stream           *streamRun             `json:"stream,omitempty"`
	FaultRef         rpc.TraceRef           `json:"fault_ref"`
	Report           *rpc.DrainReport       `json:"report,omitempty"`
	DrainError       string                 `json:"drain_error,omitempty"`
	TotalLost        uint64                 `json:"total_lost"`
	TotalIncomplete  uint64                 `json:"total_incomplete"`
	History          []history              `json:"history"`
	Trace            rpc.TraceSnapshot      `json:"trace"`
}

func (a *app) snapshot(after uint64) state {
	a.mu.Lock()
	s := a.current
	v := state{Scope: "本机同进程 QUIC；定时器与槽位负载；验证范围见运行说明", Now: time.Now(), Config: a.config, Busy: a.busy, Message: a.message, History: append([]history(nil), a.history...), TotalLost: a.pastLost, TotalIncomplete: a.pastIncomplete, Metrics: map[string]metric{}}
	if s != nil {
		v.Session = s.id
		v.TraceStart = s.traceStart
		v.Mode = s.mode
		v.State = s.state
		v.Endpoint = s.endpoint
		v.Active = s.active
		v.Generated = s.generated
		v.GeneratorDropped = s.dropped
		v.Report = s.report
		v.DrainError = s.drainError
		v.FaultRef = s.faultRef
		for path, m := range s.metrics {
			v.Metrics[path] = m.snapshot()
		}
	}
	if a.stream != nil {
		copy := *a.stream
		copy.Chunks = append([]chunk(nil), copy.Chunks...)
		v.Stream = &copy
	}
	a.mu.Unlock()
	if s != nil {
		v.Stats = s.server.Stats()
		v.Memory = s.server.MemoryStats()
		v.ClientMemory = s.client.MemoryStats()
		v.Governance = s.server.Governance()
		v.Feedback = s.client.FeedbackStats()
		v.TotalLost += v.Stats.LostTerminals
		v.TotalIncomplete += v.Stats.IncompleteExchanges
	}
	v.Trace = a.trace.Snapshot(after, 512)
	return v
}
func jsonReply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func decode(w http.ResponseWriter, r *http.Request, v any) error {
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		return errors.New("请发送 application/json")
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(v); err != nil {
		return fmt.Errorf("无效请求：%w", err)
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("只接受单个 JSON 对象")
	}
	return nil
}
func (a *app) routes(files http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		after, _ := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
		jsonReply(w, 200, a.snapshot(after))
	})
	action := func(path string, fn func(http.ResponseWriter, *http.Request) error) {
		mux.HandleFunc("POST "+path, func(w http.ResponseWriter, r *http.Request) {
			if err := fn(w, r); err != nil {
				jsonReply(w, 409, map[string]string{"error": err.Error()})
				return
			}
			jsonReply(w, 202, map[string]bool{"accepted": true})
		})
	}
	action("/api/load", func(w http.ResponseWriter, r *http.Request) error {
		var c loadConfig
		if err := decode(w, r, &c); err != nil {
			return err
		}
		return a.setLoad(c)
	})
	action("/api/preset", func(w http.ResponseWriter, r *http.Request) error {
		var in struct {
			Name string `json:"name"`
		}
		if err := decode(w, r, &in); err != nil {
			return err
		}
		a.mu.Lock()
		c := a.config
		a.mu.Unlock()
		switch in.Name {
		case "overload":
			c = loadConfig{Rate: 120, SlowPercent: 85, SlowMS: 80, Capacity: 2}
		case "recover":
			c = loadConfig{Rate: 25, SlowPercent: 70, SlowMS: 40, Capacity: 8}
		case "stop":
			c.Rate = 0
		default:
			return errors.New("未知场景")
		}
		return a.setLoad(c)
	})
	action("/api/session", func(w http.ResponseWriter, r *http.Request) error {
		var in struct {
			Mode string `json:"mode"`
		}
		if err := decode(w, r, &in); err != nil {
			return err
		}
		if in.Mode != "fixed" && in.Mode != "adaptive" {
			return errors.New("模式必须为 fixed 或 adaptive")
		}
		return a.operation(func() error { return a.replaceSession(in.Mode) })
	})
	action("/api/stream", func(w http.ResponseWriter, r *http.Request) error {
		var in struct {
			Action string `json:"action"`
		}
		if err := decode(w, r, &in); err != nil {
			return err
		}
		switch in.Action {
		case "start":
			return a.startStream()
		case "cancel":
			return a.cancelStream()
		default:
			return errors.New("流操作必须为 start 或 cancel")
		}
	})
	action("/api/drain", func(w http.ResponseWriter, r *http.Request) error {
		var in struct {
			OmitACK bool `json:"omit_ack"`
		}
		if err := decode(w, r, &in); err != nil {
			return err
		}
		return a.operation(func() error { return a.drain(in.OmitACK) })
	})
	mux.Handle("GET /", files)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.Host)
		ip := net.ParseIP(host)
		if err != nil || !(host == "localhost" || ip != nil && ip.IsLoopback()) {
			http.Error(w, "请通过本机地址访问控制台，并保持控制请求同源", 403)
			return
		}
		if r.Method != "GET" && r.Method != "HEAD" {
			if origin := r.Header.Get("Origin"); origin != "" {
				u, e := url.Parse(origin)
				if e != nil || u.Host != r.Host || u.Scheme != "http" {
					http.Error(w, "拒绝跨站操作", 403)
					return
				}
			}
		}
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; object-src 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		mux.ServeHTTP(w, r)
	})
}
