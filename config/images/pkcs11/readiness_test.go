//go:build ignore

// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCertificateBinding(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	other, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	certificate := func(start, end time.Time) []byte {
		t.Helper()
		template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: start, NotAfter: end}
		der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	}
	valid := certificate(now.Add(-time.Hour), now.Add(time.Hour))
	encoded, err := key.PublicKey.ECDH()
	if err != nil {
		t.Fatal(err)
	}
	wantSKI := sha256.Sum256(encoded.Bytes())
	var previous []byte
	for i := 0; i < 2; i++ {
		err := verifyCertificateBinding(valid, func(ski, digest []byte) ([]byte, error) {
			if !bytes.Equal(ski, wantSKI[:]) {
				t.Fatal("wrong Fabric SKI")
			}
			if bytes.Equal(previous, digest) {
				t.Fatal("reused readiness challenge")
			}
			previous = append([]byte(nil), digest...)
			return ecdsa.SignASN1(rand.Reader, key, digest)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for name, input := range map[string][]byte{
		"malformed": []byte("not a certificate"),
		"trailing":  append(append([]byte(nil), valid...), []byte("extra")...),
		"duplicate": append(append([]byte(nil), valid...), valid...),
		"expired":   certificate(now.Add(-2*time.Hour), now.Add(-time.Hour)),
		"future":    certificate(now.Add(time.Hour), now.Add(2*time.Hour)),
	} {
		t.Run(name, func(t *testing.T) {
			if err := verifyCertificateBinding(input, func(_, _ []byte) ([]byte, error) {
				t.Fatal("invalid certificate reached provider")
				return nil, nil
			}); err == nil {
				t.Fatal("accepted invalid certificate")
			}
		})
	}
	for name, sign := range map[string]func([]byte, []byte) ([]byte, error){
		"wrong key":         func(_, digest []byte) ([]byte, error) { return ecdsa.SignASN1(rand.Reader, other, digest) },
		"unavailable":       func(_, _ []byte) ([]byte, error) { return nil, errors.New("secret provider detail") },
		"invalid signature": func(_, _ []byte) ([]byte, error) { return []byte("invalid"), nil },
	} {
		t.Run(name, func(t *testing.T) {
			if err := verifyCertificateBinding(valid, sign); err == nil || bytes.Contains([]byte(err.Error()), []byte("secret")) {
				t.Fatal("accepted failure or leaked provider error")
			}
		})
	}
	key, err = ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyCertificateBinding(certificate(now.Add(-time.Hour), now.Add(time.Hour)), func(_, _ []byte) ([]byte, error) {
		t.Fatal("unsupported curve reached provider")
		return nil, nil
	}); err == nil {
		t.Fatal("accepted unsupported certificate curve")
	}
}

func TestReadinessRefusesSoftwareKeystore(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:7051")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	msp := t.TempDir()
	t.Setenv("CORE_PEER_MSPCONFIGPATH", msp)
	t.Setenv("FABRICOPS_PKCS11_LIBRARY", "/nonexistent/provider.so")
	t.Setenv("FABRICOPS_PKCS11_TOKEN_LABEL", "test-token")
	t.Setenv("FABRICOPS_PKCS11_PIN", "test-secret-pin")
	keystore := filepath.Join(msp, "keystore")
	if err := os.Mkdir(keystore, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(keystore, "key.pem"), []byte("not a usable key"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := checkReadiness(); err == nil || err.Error() != "software MSP keystore is not empty" {
		t.Fatal("software keystore did not cause refusal before provider access")
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := checkReadiness(); err == nil || err.Error() != "peer listener is unavailable" {
		t.Fatal("readiness accepted an unavailable peer listener")
	}
}
