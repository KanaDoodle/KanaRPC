// Package testcert creates ephemeral localhost credentials for tests and labs.
// Production constructors never create or persist credentials automatically.
package testcert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"time"
)

type Credentials struct {
	Server, Client *tls.Config
	CAPEM          []byte
}

func New() (Credentials, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return Credentials{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return Credentials{}, err
	}
	ca := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "KanaRPC ephemeral lab CA"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		return Credentials{}, err
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		return Credentials{}, err
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return Credentials{}, err
	}
	serial, err = rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return Credentials{}, err
	}
	leaf := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, parsed, &leafKey.PublicKey, key)
	if err != nil {
		return Credentials{}, err
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	cert := tls.Certificate{Certificate: [][]byte{leafDER, der}, PrivateKey: leafKey}
	return Credentials{Server: &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: pool}, Client: &tls.Config{RootCAs: pool, ServerName: "localhost", Certificates: []tls.Certificate{cert}}, CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, nil
}
