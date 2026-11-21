//go:build ignore

// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp/pkcs11"
	"github.com/hyperledger/fabric-lib-go/bccsp/sw"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
)

func main() {
	// Provider debug logging includes the PIN. Never forward SDK diagnostics,
	// including panics, into kubelet probe events.
	defer func() {
		if recover() != nil {
			notReady()
		}
	}()
	flogging.Init(flogging.Config{Writer: io.Discard, LogSpec: "error"})
	if err := checkReadiness(); err != nil {
		notReady()
	}
}

func notReady() {
	fmt.Fprintln(os.Stderr, "PKCS11 peer is not ready")
	os.Exit(1)
}

func checkReadiness() error {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:7051", time.Second)
	if err != nil {
		return errors.New("peer listener is unavailable")
	}
	_ = conn.Close()
	msp := os.Getenv("CORE_PEER_MSPCONFIGPATH")
	library := os.Getenv("FABRICOPS_PKCS11_LIBRARY")
	label, pin := os.Getenv("FABRICOPS_PKCS11_TOKEN_LABEL"), os.Getenv("FABRICOPS_PKCS11_PIN")
	if !filepath.IsAbs(msp) || !filepath.IsAbs(library) || label == "" || pin == "" {
		return errors.New("provider inputs are incomplete")
	}
	entries, err := os.ReadDir(filepath.Join(msp, "keystore"))
	if (err != nil && !errors.Is(err, os.ErrNotExist)) || len(entries) != 0 {
		return errors.New("software MSP keystore is not empty")
	}
	cert, err := os.ReadFile(filepath.Join(msp, "signcerts", "cert.pem"))
	if err != nil {
		return errors.New("MSP certificate is unavailable")
	}
	return verifyCertificateBinding(cert, func(ski, digest []byte) ([]byte, error) {
		// A fresh process/session and dummy keystore prohibit a software-key
		// fallback. No imports, key generation or persistent success cache.
		provider, err := pkcs11.New(pkcs11.PKCS11Opts{
			Security: 256, Hash: "SHA2", Library: library, Label: label, Pin: pin,
			SoftwareVerify: true,
		}, sw.NewDummyKeyStore())
		if err != nil {
			return nil, errors.New("provider is unavailable")
		}
		key, err := provider.GetKey(ski)
		if err != nil || key == nil || !key.Private() || key.Symmetric() {
			return nil, errors.New("provider signing key is unavailable")
		}
		return provider.Sign(key, digest, crypto.SHA256)
	})
}

func verifyCertificateBinding(certPEM []byte, sign func(ski, digest []byte) ([]byte, error)) error {
	block, rest := pem.Decode(certPEM)
	if block == nil || block.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 || !bytes.HasPrefix(bytes.TrimSpace(certPEM), []byte("-----BEGIN CERTIFICATE-----")) {
		return errors.New("invalid MSP certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return errors.New("invalid MSP certificate")
	}
	public, ok := cert.PublicKey.(*ecdsa.PublicKey)
	now := time.Now()
	if !ok || public.Curve != elliptic.P256() || now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		return errors.New("unsupported or invalid MSP certificate")
	}
	encoded, err := public.ECDH()
	if err != nil {
		return errors.New("invalid MSP public key")
	}
	ski := sha256.Sum256(encoded.Bytes())
	challenge := make([]byte, 32)
	if _, err := rand.Read(challenge); err != nil {
		return errors.New("challenge generation failed")
	}
	digest := sha256.Sum256(challenge)
	signature, err := sign(ski[:], digest[:])
	if err != nil || !ecdsa.VerifyASN1(public, digest[:], signature) {
		return errors.New("provider signature does not match MSP certificate")
	}
	return nil
}
