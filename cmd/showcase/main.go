// showcase is an offline local demonstration, not a production admin service.
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
		return fmt.Errorf("showcase only binds loopback")
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
	fmt.Printf("KanaRPC 决策演示台\n  %s\n本机真实 QUIC；timer/slot fixture；真实双机与推理未验证。Ctrl+C 退出。\n", url)
	if *ready != "" {
		body, _ := json.Marshal(map[string]any{"url": url, "pid": os.Getpid(), "scope": "local real QUIC, no dual-host or model validation"})
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
	fmt.Println("已停止 HTTP 接纳，正在回收演示拥有的 RPC 与负载。")
	return nil
}
