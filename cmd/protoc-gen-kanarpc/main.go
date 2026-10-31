// protoc-gen-kanarpc generates typed KanaRPC clients and service registration.
package main

import (
	"fmt"
	"os"
	"runtime/debug"
	bindings "github.com/KanaDoodle/KanaRPC/internal/protogen"
	"google.golang.org/protobuf/compiler/protogen"
)

var buildVersion = "(devel)"

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--version" {
		version := buildVersion
		if version == "(devel)" {
			if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" { version = info.Main.Version }
		}
		fmt.Printf("protoc-gen-kanarpc %s sdk-api=1\n", version)
		return
	}
	protogen.Options{}.Run(bindings.Generate)
}
