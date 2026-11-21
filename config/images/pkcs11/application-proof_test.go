//go:build ignore

// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	pb "github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"google.golang.org/protobuf/proto"
)

func TestCommittedTransactionEvidence(t *testing.T) {
	marshal := func(message proto.Message) []byte {
		t.Helper()
		b, err := proto.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	header := &cb.ChannelHeader{Type: int32(cb.HeaderType_ENDORSER_TRANSACTION), ChannelId: "custody-app", TxId: "fresh-transaction"}
	envelope := &cb.Envelope{Payload: marshal(&cb.Payload{Header: &cb.Header{ChannelHeader: marshal(header)}, Data: []byte("submitted transaction")}), Signature: []byte("signed envelope")}
	hash := sha256.Sum256(marshal(envelope))
	expected := hex.EncodeToString(hash[:])
	processed := &pb.ProcessedTransaction{TransactionEnvelope: envelope, ValidationCode: int32(pb.TxValidationCode_VALID)}
	if err := verifyCommittedTransaction(marshal(processed), header.TxId, header.ChannelId, expected); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*pb.ProcessedTransaction){
		"invalid commit":     func(tx *pb.ProcessedTransaction) { tx.ValidationCode = int32(pb.TxValidationCode_MVCC_READ_CONFLICT) },
		"missing envelope":   func(tx *pb.ProcessedTransaction) { tx.TransactionEnvelope = nil },
		"different envelope": func(tx *pb.ProcessedTransaction) { tx.TransactionEnvelope.Signature = []byte("other") },
	} {
		t.Run(name, func(t *testing.T) {
			tx := proto.Clone(processed).(*pb.ProcessedTransaction)
			mutate(tx)
			if err := verifyCommittedTransaction(marshal(tx), header.TxId, header.ChannelId, expected); err == nil {
				t.Fatal("accepted invalid commit evidence")
			}
		})
	}
	if err := verifyCommittedTransaction(marshal(processed), "other-tx", header.ChannelId, expected); err == nil {
		t.Fatal("accepted another transaction")
	}
	if err := verifyCommittedTransaction(marshal(processed), header.TxId, "other-channel", expected); err == nil {
		t.Fatal("accepted another channel")
	}
	if err := verifyCommittedTransaction([]byte("malformed"), header.TxId, header.ChannelId, expected); err == nil {
		t.Fatal("accepted malformed transaction")
	}
}

func TestSettlementEvidence(t *testing.T) {
	valid := []byte(`{"docType":"settlement","id":"proof-id","owner":"BankA","debtor":"BankA","creditor":"BankB","amount":"100","currency":"USD","status":"PENDING"}`)
	if err := verifySettlement(valid, "proof-id"); err != nil {
		t.Fatal(err)
	}
	if err := verifySettlement(valid, "other-id"); err == nil {
		t.Fatal("accepted another settlement")
	}
	if err := verifySettlement([]byte(`{"id":"proof-id","status":"SETTLED"}`), "proof-id"); err == nil {
		t.Fatal("accepted incorrect state")
	}
}
