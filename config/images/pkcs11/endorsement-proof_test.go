//go:build ignore

// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	mb "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	pb "github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric/protoutil"
	"google.golang.org/protobuf/proto"
)

func TestVerifyChannelEndorsement(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	blockHash := []byte("expected genesis block hash")
	proposal := func() *pb.Proposal {
		t.Helper()
		p, _, err := protoutil.CreateProposalFromCIS(cb.HeaderType_ENDORSER_TRANSACTION, "custody-proof", &pb.ChaincodeInvocationSpec{ChaincodeSpec: &pb.ChaincodeSpec{ChaincodeId: &pb.ChaincodeID{Name: "qscc"}, Input: &pb.ChaincodeInput{Args: [][]byte{[]byte("GetChainInfo"), []byte("custody-proof")}}}}, []byte("client"))
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	p := proposal()
	marshal := func(message proto.Message) []byte {
		t.Helper()
		value, err := proto.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	header := &cb.Header{}
	if err := proto.Unmarshal(p.Header, header); err != nil {
		t.Fatal(err)
	}
	hash, err := protoutil.GetProposalHash1(header, p.Payload)
	if err != nil {
		t.Fatal(err)
	}
	action := &pb.ChaincodeAction{ChaincodeId: &pb.ChaincodeID{Name: "qscc"}, Response: &pb.Response{Status: 200, Payload: marshal(&cb.BlockchainInfo{Height: 1, CurrentBlockHash: blockHash})}}
	response := &pb.ProposalResponse{Response: proto.Clone(action.Response).(*pb.Response), Payload: marshal(&pb.ProposalResponsePayload{ProposalHash: hash, Extension: marshal(action)}), Endorsement: &pb.Endorsement{Endorser: marshal(&mb.SerializedIdentity{Mspid: "BankAMSP", IdBytes: certificate})}}
	sign := func(r *pb.ProposalResponse) {
		t.Helper()
		digest := sha256.Sum256(append(append([]byte(nil), r.Payload...), r.Endorsement.Endorser...))
		r.Endorsement.Signature, err = ecdsa.SignASN1(rand.Reader, key, digest[:])
		if err != nil {
			t.Fatal(err)
		}
	}
	sign(response)
	if err := verifyChannelEndorsement(p, response, certificate, "BankAMSP", blockHash); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*pb.ProposalResponse){
		"no endorsement":                 func(r *pb.ProposalResponse) { r.Endorsement = nil },
		"invalid signature":              func(r *pb.ProposalResponse) { r.Endorsement.Signature[0] ^= 1 },
		"tampered payload":               func(r *pb.ProposalResponse) { r.Payload[0] ^= 1 },
		"failed response":                func(r *pb.ProposalResponse) { r.Response.Status = 500 },
		"unsigned response substitution": func(r *pb.ProposalResponse) { r.Response.Payload = []byte("substituted") },
		"other MSP": func(r *pb.ProposalResponse) {
			r.Endorsement.Endorser = marshal(&mb.SerializedIdentity{Mspid: "OtherMSP", IdBytes: certificate})
			sign(r)
		},
		"wrong chaincode": func(r *pb.ProposalResponse) {
			changed := proto.Clone(action).(*pb.ChaincodeAction)
			changed.ChaincodeId.Name = "other"
			r.Payload = marshal(&pb.ProposalResponsePayload{ProposalHash: hash, Extension: marshal(changed)})
			sign(r)
		},
		"wrong ledger": func(r *pb.ProposalResponse) {
			changed := proto.Clone(action).(*pb.ChaincodeAction)
			changed.Response.Payload = marshal(&cb.BlockchainInfo{Height: 2, CurrentBlockHash: blockHash})
			r.Response = proto.Clone(changed.Response).(*pb.Response)
			r.Payload = marshal(&pb.ProposalResponsePayload{ProposalHash: hash, Extension: marshal(changed)})
			sign(r)
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(response).(*pb.ProposalResponse)
			mutate(changed)
			if err := verifyChannelEndorsement(p, changed, certificate, "BankAMSP", blockHash); err == nil {
				t.Fatal("accepted invalid endorsement evidence")
			}
		})
	}
	if err := verifyChannelEndorsement(proposal(), response, certificate, "BankAMSP", blockHash); err == nil {
		t.Fatal("accepted a replay for a different proposal")
	}
	if err := verifyChannelEndorsement(p, response, certificate, "BankAMSP", []byte("other block")); err == nil {
		t.Fatal("accepted the wrong genesis")
	}
	if err := verifyChannelEndorsement(p, response, []byte("other certificate"), "BankAMSP", blockHash); err == nil {
		t.Fatal("accepted a different peer identity")
	}
}
