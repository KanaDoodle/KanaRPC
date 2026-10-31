package protogen

import (
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

func descriptor(clientStream bool) *descriptorpb.FileDescriptorProto {
	return &descriptorpb.FileDescriptorProto{Name: proto.String("service.proto"), Package: proto.String("example"), Syntax: proto.String("proto3"),
		Options:     &descriptorpb.FileOptions{GoPackage: proto.String("example.com/sdk;sample")},
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Request")}, {Name: proto.String("Response")}},
		Service: []*descriptorpb.ServiceDescriptorProto{{Name: proto.String("Test"), Method: []*descriptorpb.MethodDescriptorProto{
			{Name: proto.String("Echo"), InputType: proto.String(".example.Request"), OutputType: proto.String(".example.Response")},
			{Name: proto.String("Watch"), InputType: proto.String(".example.Request"), OutputType: proto.String(".example.Response"), ServerStreaming: proto.Bool(true), ClientStreaming: proto.Bool(clientStream)},
		}}},
	}
}

func TestTypedBindingsAndUnsupportedStreaming(t *testing.T) {
	for _, clientStream := range []bool{false, true} {
		file := descriptor(clientStream)
		plugin, err := (protogen.Options{}).New(&pluginpb.CodeGeneratorRequest{FileToGenerate: []string{"service.proto"}, ProtoFile: []*descriptorpb.FileDescriptorProto{file}})
		if err != nil {
			t.Fatal(err)
		}
		err = Generate(plugin)
		if clientStream {
			if err == nil || !strings.Contains(err.Error(), "client-streaming") || len(plugin.Response().File) != 0 {
				t.Fatal(err, plugin.Response())
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		response := plugin.Response()
		if response.GetError() != "" || len(response.File) != 1 {
			t.Fatal(response)
		}
		body := response.File[0].GetContent()
		if _, err := parser.ParseFile(token.NewFileSet(), "sdk.go", body, parser.AllErrors); err != nil {
			t.Fatal(err)
		}
		for _, text := range []string{"/example.Test/Echo", "/example.Test/Watch", "RegisterProtobufMethods", "Protobuf(request)", "OpenStream", "Recv() (*Response, error)"} {
			if !strings.Contains(body, text) {
				t.Fatal("missing generated binding", text)
			}
		}
	}
}
