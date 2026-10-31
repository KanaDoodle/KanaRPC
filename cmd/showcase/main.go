// showcase runs a local RPC governance console with embedded web assets.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

//go:embed web/*
var assets embed.FS

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	address := flag.String("listen", "127.0.0.1:8080", "local page address")
	ready := flag.String("ready", "", "optional owned ready JSON path")
	flag.Parse()
	host, _, err := net.SplitHostPort(*address)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if !(host == "localhost" || ip != nil && ip.IsLoopback()) {
		return fmt.Errorf("控制台监听地址应为 loopback 地址，如 127.0.0.1:8080")
	}
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		return fmt.Errorf("页面端口不可用，可用 -listen 127.0.0.1:8090：%w", err)
	}
	defer listener.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	app, err := newApp(ctx)
	if err != nil {
		return err
	}
	defer app.close()
	sub, err := fs.Sub(assets, "web")
	if err != nil {
		return err
	}
	web := &http.Server{Handler: app.routes(http.FileServer(http.FS(sub))), ReadHeaderTimeout: 3 * time.Second, IdleTimeout: 15 * time.Second, WriteTimeout: 5 * time.Second, MaxHeaderBytes: 8192}
	done := make(chan error, 1)
	go func() { done <- web.Serve(listener) }()
	url := "http://" + listener.Addr().String()
	fmt.Printf("KanaRPC 治理控制台\n  %s\n本机 QUIC · 可调负载 · 请求时间线。运行范围见页面说明。Ctrl+C 退出。\n", url)
	if *ready != "" {
		body, _ := json.Marshal(map[string]any{"url": url, "pid": os.Getpid(), "scope": "local single-process QUIC with timer/slot workload"})
		if err = os.WriteFile(*ready+".tmp", body, 0600); err == nil {
			err = os.Rename(*ready+".tmp", *ready)
		}
		if err != nil {
			_ = web.Close()
			return err
		}
	}
	select {
	case <-ctx.Done():
	case err = <-done:
		if err != http.ErrServerClosed {
			return err
		}
		return nil
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = web.Shutdown(shutdown); err != nil {
		_ = web.Close()
	}
	<-done
	fmt.Println("已停止 HTTP 接纳，正在回收本次会话的 RPC 与负载。")
	return nil
}
