package kanarpc

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

func FuzzTerminalEncoding(f *testing.F) {
	f.Add(strings.Repeat("\x00", 256))
	f.Add(strings.Repeat("界", 80))
	f.Add("ordinary error")
	f.Fuzz(func(t *testing.T, message string) {
		if len(message) > 256 || !utf8.ValidString(message) {
			return
		}
		p := validPressure("/demo/error", 1)
		record := endRecord{Status: Status{Code: InvalidArgument, Message: message}, Receipt: "00112233445566778899aabbccddeeff", Pressure: &p}
		body := marshalEnd(record, 1024)
		var decoded endRecord
		if len(body) > 1024 || json.Unmarshal(body, &decoded) != nil || decoded.Code != record.Code || decoded.Receipt != record.Receipt || !utf8.ValidString(decoded.Message) || !strings.HasPrefix(message, decoded.Message) {
			t.Fatal("terminal lost its contract", string(body))
		}
	})
}
