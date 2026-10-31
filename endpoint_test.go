package kanarpc

import "testing"

func TestEndpointRejectsUnusableAddresses(t *testing.T) {
	for _, address := range []string{"bad host:123", "foo/bar:123", "host:+123", "0.0.0.0:123", "[::]:123", "host..example:123", "host:0", "host:65536", "tcp://localhost:123"} {
		if err := (Endpoint{Address: address, Transport: TCP}).Validate(); err == nil {
			t.Error("accepted invalid endpoint", address)
		}
	}
	for _, address := range []string{"localhost:123", "127.0.0.1:123", "[::1]:123", "example.com.:443"} {
		if err := (Endpoint{Address: address, Transport: QUIC}).Validate(); err != nil {
			t.Error(address, err)
		}
	}
}
func FuzzEndpoint(f *testing.F) {
	f.Add("localhost:123", "tcp")
	f.Add("[::1]:443", "quic")
	f.Fuzz(func(t *testing.T, address, transport string) {
		_ = (Endpoint{Address: address, Transport: Transport(transport)}).Validate()
	})
}
