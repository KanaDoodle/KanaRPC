#!/usr/bin/env python3
"""Generate and run an independent typed SDK; retain negative compiler results."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = '''edition = "2024";
package acceptance;
option go_package = "example.com/protobuf-consumer/pb";
import "google/protobuf/wrappers.proto";
message Request { string text = 1; uint32 count = 2; reserved 3; }
service Echo {
  rpc Call(Request) returns (google.protobuf.StringValue);
  rpc Watch(Request) returns (stream google.protobuf.StringValue);
}
'''
SMOKE = r'''package consumer_test

import (
 "context"
 "errors"
 "fmt"
 "io"
 "net"
 "testing"
 "time"
 rpc "github.com/KanaDoodle/KanaRPC"
 "example.com/protobuf-consumer/pb"
 "google.golang.org/protobuf/proto"
 "google.golang.org/protobuf/types/known/wrapperspb"
)

type implementation struct { pb.UnimplementedEchoServer; canceled chan struct{} }
func (*implementation) Call(ctx context.Context, in *pb.Request) (*wrapperspb.StringValue,error) {
 if !in.HasText() { return nil, fmt.Errorf("missing explicit text presence") }
 return wrapperspb.String(in.GetText()), ctx.Err()
}
func (s *implementation) Watch(ctx context.Context, in *pb.Request, send func(*wrapperspb.StringValue) error) error {
 if in.GetCount()==0 {
  if err:=send(wrapperspb.String("ready")); err!=nil {return err}
  <-ctx.Done(); close(s.canceled); return ctx.Err()
 }
 for n:=uint32(0);n<in.GetCount();n++ {
  if err:=send(wrapperspb.String(fmt.Sprintf("%s:%d",in.GetText(),n)));err!=nil{return err}
 }
 return nil
}
func TestOpaqueGeneratedSDK(t *testing.T) {
 ctx,stop:=context.WithTimeout(context.Background(),5*time.Second);defer stop()
 s,err:=rpc.NewServer(rpc.ServerConfig{});if err!=nil{t.Fatal(err)}
 defer s.Close()
 impl:=&implementation{canceled:make(chan struct{})}
 if err:=pb.RegisterEchoServer(s,impl);err!=nil{t.Fatal(err)}
 l,err:=net.Listen("tcp","127.0.0.1:0");if err!=nil{t.Fatal(err)}
 done:=make(chan error,1);go func(){done<-s.Serve(l)}()
 defer func(){s.Close();if err:=<-done;err!=nil{t.Error(err)}}()
 c,err:=rpc.Dial(ctx,l.Addr().String(),rpc.ClientConfig{RequiredFeatures:rpc.FeatureProtobuf|rpc.FeatureServerStreaming});if err!=nil{t.Fatal(err)}
 defer c.Close()
 sdk:=pb.NewEchoClient(c)
 for _,text:=range []string{"hello opaque",""} {
  in:=pb.Request_builder{Text:proto.String(text)}.Build()
  out,err:=sdk.Call(ctx,in);if err!=nil||out.GetValue()!=text{t.Fatalf("call: %v %v",out,err)}
 }
 if _,err:=sdk.Call(ctx,nil);err==nil{t.Fatal("nil request accepted")}
 stream,err:=sdk.Watch(ctx,pb.Request_builder{Text:proto.String("chunk"),Count:proto.Uint32(3)}.Build())
 if err!=nil{t.Fatal(err)};defer stream.Close()
 for n:=0;n<3;n++ {v,e:=stream.Recv();if e!=nil||v.GetValue()!=fmt.Sprintf("chunk:%d",n){t.Fatalf("stream: %v %v",v,e)}}
 if _,err:=stream.Recv();err!=io.EOF{t.Fatal(err)}
 call,cancel:=context.WithCancel(ctx);defer cancel()
 canceled,err:=sdk.Watch(call,pb.Request_builder{}.Build());if err!=nil{t.Fatal(err)}
 defer canceled.Close()
 if _,err:=canceled.Recv();err!=nil{t.Fatal(err)}
 cancel()
 if _,err:=canceled.Recv();!errors.Is(err,context.Canceled){t.Fatalf("cancel: %v",err)}
 select{case <-impl.canceled:case <-ctx.Done():t.Fatal("remote cancel not observed")}
 report,err:=s.DrainWithReport(ctx)
 if err!=nil||!report.Quiescent||report.TotalFaults.LostTerminals!=0{t.Fatalf("drain: %+v %v",report,err)}
 if c.MemoryStats().Receive.Used!=0 {t.Fatal(c.MemoryStats())}
}
'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--protoc", default="protoc")
    parser.add_argument("--include", type=Path, help="protoc include directory")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    compiler = shutil.which(args.protoc)
    if not compiler:
        raise RuntimeError("protoc required; see docs/protobuf.md")
    compiler = str(Path(compiler).resolve())
    include = args.include or Path(compiler).parent.parent / "include"
    tools = output / "tools"
    tools.mkdir()
    consumer = output / "consumer"
    consumer.mkdir()
    env = dict(os.environ, GOWORK="off")
    result = dict(scope="local independent module; real TCP, Editions 2024 Opaque API, imported message types, unary/stream/cancel/drain; compiler negatives retained", commands=[])

    def run(name, command, cwd=ROOT, expected=None, extra_env=None):
        with (output / (name + ".txt")).open("wb") as log:
            p = subprocess.run(command, cwd=cwd, env=extra_env or env, stdout=log, stderr=log, timeout=180)
        result["commands"].append(dict(name=name, command=command, exit=p.returncode))
        body = (output / (name + ".txt")).read_text()
        if expected is None:
            if p.returncode:
                raise RuntimeError(f"{name} failed; see {name}.txt")
        elif p.returncode == 0 or expected not in body:
            raise RuntimeError(f"{name}: expected compiler rejection containing {expected!r}")
        return body

    try:
        version = subprocess.check_output(["go", "list", "-m", "-f", "{{.Version}}", "google.golang.org/protobuf"], cwd=ROOT, env=env, text=True).strip()
        result["protoc"] = run("protoc-version", [compiler, "--version"]).strip()
        result["protobuf_go"] = version
        run("install-message-plugin", ["go", "install", "google.golang.org/protobuf/cmd/protoc-gen-go@" + version], extra_env=dict(env, GOBIN=str(tools)))
        run("build-rpc-plugin", ["go", "build", "-o", str(tools / "protoc-gen-kanarpc"), "./cmd/protoc-gen-kanarpc"])
        plugins = ["--plugin=protoc-gen-go=" + str(tools / "protoc-gen-go"), "--plugin=protoc-gen-kanarpc=" + str(tools / "protoc-gen-kanarpc")]
        generated = output / "regenerated"
        generated.mkdir()
        run("regenerate-example", [compiler, *plugins, "--go_out=" + str(generated), "--go_opt=paths=source_relative", "--kanarpc_out=" + str(generated), "--kanarpc_opt=paths=source_relative", "examples/echo/echo.proto"])
        result["reproducible"] = {}
        for name in ["echo.pb.go", "echo_kanarpc.pb.go"]:
            relative = Path("examples/echo") / name
            expected, actual = (ROOT / relative).read_bytes(), (generated / relative).read_bytes()
            if expected != actual:
                raise RuntimeError(f"generated file differs: {relative}")
            result["reproducible"][str(relative)] = hashlib.sha256(actual).hexdigest()
        (consumer / "go.mod").write_text("module example.com/protobuf-consumer\n\ngo 1.26.10\n\nrequire github.com/KanaDoodle/KanaRPC v0.0.0\nreplace github.com/KanaDoodle/KanaRPC => " + json.dumps(str(ROOT)) + "\n")
        (consumer / "service.proto").write_text(SCHEMA)
        run("generate-opaque-sdk", [compiler, *plugins, "-I.", "-I" + str(include), "--go_out=.", "--go_opt=module=example.com/protobuf-consumer", "--kanarpc_out=.", "--kanarpc_opt=module=example.com/protobuf-consumer", "service.proto"], consumer)
        if "opaque" not in (consumer / "pb/service.pb.go").read_text():
            raise RuntimeError("message generator did not select the Opaque API")
        (consumer / "smoke_test.go").write_text(SMOKE)
        run("consumer-tidy", ["go", "mod", "tidy"], consumer)
        run("consumer-test", ["go", "test", "-race", "-count=1", "-v", "-timeout=30s", "./..."], consumer)
        # A service implementation written before a new RPC still compiles by
        # embedding the generated default implementation; the new method fails
        # explicitly until implemented.
        (consumer / "service.proto").write_text(SCHEMA.replace("service Echo {", "service Echo { rpc Later(Request) returns (google.protobuf.StringValue);"))
        run("generate-extended-sdk", [compiler, *plugins, "-I.", "-I" + str(include), "--go_out=.", "--go_opt=module=example.com/protobuf-consumer", "--kanarpc_out=.", "--kanarpc_opt=module=example.com/protobuf-consumer", "service.proto"], consumer)
        extension = '''
 _,laterErr:=sdk.Later(ctx,pb.Request_builder{}.Build())
 var status *rpc.Status
 if !errors.As(laterErr,&status)||status.Code!=rpc.NotFound{t.Fatalf("default method: %v",laterErr)}
'''
        (consumer / "smoke_test.go").write_text(SMOKE.replace(" report,err:=s.DrainWithReport(ctx)", extension + " report,err:=s.DrainWithReport(ctx)"))
        run("consumer-extension-test", ["go", "test", "-race", "-count=1", "-v", "-timeout=30s", "./..."], consumer)
        (consumer / "bidi.proto").write_text('syntax="proto3";package negative;option go_package="example.com/protobuf-consumer/negative";message Item{} service Unsupported{rpc Bidi(stream Item)returns(stream Item);}')
        run("reject-bidi", [compiler, *plugins, "-I.", "--kanarpc_out=.", "bidi.proto"], consumer, "client-streaming and bidirectional")
        (consumer / "reserved.proto").write_text('syntax="proto3";message Invalid{reserved 1;string reused=1;}')
        run("reject-reserved-field", [compiler, "-I.", "--descriptor_set_out=reserved.bin", "reserved.proto"], consumer, "reserved number 1")
        result["status"] = "PASS"
    except BaseException as error:
        result.update(status="FAIL", failure=repr(error))
        raise
    finally:
        shutil.rmtree(tools)
        (output / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
