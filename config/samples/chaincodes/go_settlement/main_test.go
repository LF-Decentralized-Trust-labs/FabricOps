package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/hyperledger/fabric-chaincode-go/shim"
	"github.com/hyperledger/fabric-chaincode-go/shimtest" //nolint:staticcheck // Required MockStub.
	"github.com/hyperledger/fabric-contract-api-go/contractapi"
	peer "github.com/hyperledger/fabric-protos-go/peer"
)

func TestCreateAndReadSettlement(t *testing.T) {
	stub := newSettlementStub(t)

	created := requireSuccessfulSettlement(t, invoke(stub, "tx-create",
		"CreateSettlement", "settlement-100", "BankA", "BankB", "125000", "USD"))
	want := Settlement{
		DocType:  "settlement",
		ID:       "settlement-100",
		Owner:    "BankA",
		Debtor:   "BankA",
		Creditor: "BankB",
		Amount:   "125000",
		Currency: "USD",
		Status:   "PENDING",
	}
	requireSettlement(t, created, want)

	persisted := decodeSettlement(t, stub.State[want.ID])
	requireSettlement(t, persisted, want)

	read := requireSuccessfulSettlement(t, invoke(stub, "tx-read", "ReadSettlement", want.ID))
	requireSettlement(t, read, want)
}

func TestCreateSettlementRejectsDuplicateWithoutOverwrite(t *testing.T) {
	stub := newSettlementStub(t)
	id := "settlement-duplicate"
	requireSuccessfulSettlement(t, invoke(stub, "tx-create",
		"CreateSettlement", id, "BankA", "BankB", "125000", "USD"))
	before := append([]byte(nil), stub.State[id]...)

	response := invoke(stub, "tx-duplicate",
		"CreateSettlement", id, "BankC", "BankD", "999", "EUR")
	if response.Status == shim.OK {
		t.Fatalf("duplicate CreateSettlement succeeded: %s", response.Payload)
	}
	if !strings.Contains(response.Message, "already exists") {
		t.Fatalf("duplicate CreateSettlement error = %q, want an already-exists error", response.Message)
	}
	if !bytes.Equal(stub.State[id], before) {
		t.Fatalf("duplicate CreateSettlement overwrote state: got %s, want %s", stub.State[id], before)
	}
}

func TestReadSettlementRejectsMissingSettlement(t *testing.T) {
	stub := newSettlementStub(t)

	response := invoke(stub, "tx-read-missing", "ReadSettlement", "settlement-missing")
	if response.Status == shim.OK {
		t.Fatalf("ReadSettlement succeeded for a missing settlement: %s", response.Payload)
	}
	if !strings.Contains(response.Message, "does not exist") {
		t.Fatalf("ReadSettlement error = %q, want a does-not-exist error", response.Message)
	}
}

func TestMarkSettledPersistsTransition(t *testing.T) {
	stub := newSettlementStub(t)
	id := "settlement-to-settle"
	requireSuccessfulSettlement(t, invoke(stub, "tx-create",
		"CreateSettlement", id, "BankA", "BankB", "125000", "USD"))

	settled := requireSuccessfulSettlement(t, invoke(stub, "tx-settle", "MarkSettled", id))
	if settled.Status != "SETTLED" {
		t.Fatalf("MarkSettled status = %q, want SETTLED", settled.Status)
	}

	read := requireSuccessfulSettlement(t, invoke(stub, "tx-read", "ReadSettlement", id))
	if read.Status != "SETTLED" {
		t.Fatalf("persisted status = %q, want SETTLED", read.Status)
	}
}

func newSettlementStub(t *testing.T) *shimtest.MockStub {
	t.Helper()

	chaincode, err := contractapi.NewChaincode(new(SettlementContract))
	if err != nil {
		t.Fatalf("create settlement chaincode: %v", err)
	}
	return shimtest.NewMockStub("settlement", chaincode)
}

func invoke(stub *shimtest.MockStub, txID string, args ...string) peer.Response {
	byteArgs := make([][]byte, len(args))
	for index, arg := range args {
		byteArgs[index] = []byte(arg)
	}
	return stub.MockInvoke(txID, byteArgs)
}

func requireSuccessfulSettlement(t *testing.T, response peer.Response) Settlement {
	t.Helper()

	if response.Status != shim.OK {
		t.Fatalf("chaincode response status = %d, message = %q", response.Status, response.Message)
	}
	return decodeSettlement(t, response.Payload)
}

func decodeSettlement(t *testing.T, payload []byte) Settlement {
	t.Helper()

	var settlement Settlement
	if err := json.Unmarshal(payload, &settlement); err != nil {
		t.Fatalf("decode settlement %q: %v", payload, err)
	}
	return settlement
}

func requireSettlement(t *testing.T, got, want Settlement) {
	t.Helper()

	if got != want {
		t.Fatalf("settlement = %+v, want %+v", got, want)
	}
}
