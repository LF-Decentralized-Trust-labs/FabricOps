//go:build ignore

// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	pb "github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric/protoutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

type applicationReceipt struct {
	ID           string `json:"id"`
	TxID         string `json:"txID"`
	EnvelopeHash string `json:"envelopeHash"`
}

func proofProposal(ctx context.Context, client pb.EndorserClient, signer adminSigner, cert []byte, mspID, channel, chaincode string, args ...string) (*pb.Proposal, *pb.ProposalResponse, string, error) {
	var input [][]byte
	for _, arg := range args {
		input = append(input, []byte(arg))
	}
	proposal, txID, err := protoutil.CreateProposalFromCIS(cb.HeaderType_ENDORSER_TRANSACTION, channel, &pb.ChaincodeInvocationSpec{ChaincodeSpec: &pb.ChaincodeSpec{Type: pb.ChaincodeSpec_GOLANG, ChaincodeId: &pb.ChaincodeID{Name: chaincode}, Input: &pb.ChaincodeInput{Args: input}}}, signer.creator)
	if err != nil {
		return nil, nil, "", errors.New("could not create proof proposal")
	}
	signed, err := protoutil.GetSignedProposal(proposal, signer)
	if err != nil {
		return nil, nil, "", errors.New("could not sign proof proposal")
	}
	response, err := client.ProcessProposal(ctx, signed)
	if err != nil {
		return nil, nil, "", errors.New("proof proposal RPC failed")
	}
	if _, err := verifyPeerEndorsement(proposal, response, cert, mspID, chaincode); err != nil {
		return nil, nil, "", err
	}
	return proposal, response, txID, nil
}

func proveApplication(ctx context.Context, client pb.EndorserClient, signer adminSigner, cert []byte, mspID, channel string, readOnly bool) error {
	receiptFile := os.Getenv("FABRICOPS_PROOF_RECEIPT")
	var receipt applicationReceipt
	if readOnly {
		data, err := os.ReadFile(receiptFile)
		if err != nil || json.Unmarshal(data, &receipt) != nil || receipt.ID == "" || receipt.TxID == "" || len(receipt.EnvelopeHash) != 64 {
			return errors.New("invalid application receipt")
		}
	} else {
		nonce := make([]byte, 16)
		if _, err := rand.Read(nonce); err != nil {
			return errors.New("could not generate settlement ID")
		}
		receipt.ID = "proof-" + hex.EncodeToString(nonce)
		proposal, response, txID, err := proofProposal(ctx, client, signer, cert, mspID, channel, "settlement", "CreateSettlement", receipt.ID, "BankA", "BankB", "100", "USD")
		if err != nil {
			return err
		}
		if err := verifySettlement(response.Response.Payload, receipt.ID); err != nil {
			return err
		}
		envelope, err := protoutil.CreateSignedTx(proposal, signer, response)
		if err != nil {
			return errors.New("could not assemble endorsed transaction")
		}
		encoded, err := proto.Marshal(envelope)
		if err != nil {
			return errors.New("could not encode endorsed transaction")
		}
		hash := sha256.Sum256(encoded)
		receipt.TxID, receipt.EnvelopeHash = txID, hex.EncodeToString(hash[:])
		tls, err := credentials.NewClientTLSFromFile(os.Getenv("FABRICOPS_PROOF_ORDERER_CA"), "")
		if err != nil {
			return errors.New("invalid orderer TLS trust")
		}
		connection, err := grpc.NewClient(os.Getenv("FABRICOPS_PROOF_ORDERER"), grpc.WithTransportCredentials(tls))
		if err != nil {
			return errors.New("could not create orderer client")
		}
		defer connection.Close()
		stream, err := ab.NewAtomicBroadcastClient(connection).Broadcast(ctx)
		if err != nil {
			return errors.New("could not open orderer broadcast")
		}
		defer stream.CloseSend()
		if err := stream.Send(envelope); err != nil {
			return errors.New("could not broadcast transaction")
		}
		ack, err := stream.Recv()
		if err != nil || ack.Status != cb.Status_SUCCESS {
			return errors.New("orderer rejected transaction")
		}
	}
	// Orderer acceptance is not commit evidence. Require a signed ledger query
	// reporting VALID for the exact submitted envelope, then a signed state read.
	for {
		_, response, _, err := proofProposal(ctx, client, signer, cert, mspID, channel, "qscc", "GetTransactionByID", channel, receipt.TxID)
		if err == nil {
			if err := verifyCommittedTransaction(response.Response.Payload, receipt.TxID, channel, receipt.EnvelopeHash); err != nil {
				return err
			}
			break
		}
		select {
		case <-ctx.Done():
			return errors.New("transaction did not reach a verifiable commit")
		case <-time.After(time.Second):
		}
	}
	_, response, _, err := proofProposal(ctx, client, signer, cert, mspID, channel, "settlement", "ReadSettlement", receipt.ID)
	if err != nil {
		return err
	}
	if err := verifySettlement(response.Response.Payload, receipt.ID); err != nil {
		return err
	}
	if !readOnly {
		data, err := json.Marshal(receipt)
		if err != nil {
			return errors.New("could not encode application receipt")
		}
		if err := os.WriteFile(receiptFile, data, 0600); err != nil {
			return errors.New("could not persist public application receipt")
		}
	}
	return nil
}

func verifyCommittedTransaction(data []byte, txID, channel, envelopeHash string) error {
	tx := &pb.ProcessedTransaction{}
	if err := proto.Unmarshal(data, tx); err != nil || tx.TransactionEnvelope == nil || tx.ValidationCode != int32(pb.TxValidationCode_VALID) {
		return errors.New("ledger transaction is not VALID")
	}
	encoded, err := proto.Marshal(tx.TransactionEnvelope)
	if err != nil {
		return errors.New("invalid committed envelope")
	}
	hash := sha256.Sum256(encoded)
	if hex.EncodeToString(hash[:]) != envelopeHash {
		return errors.New("committed envelope differs from submitted transaction")
	}
	payload := &cb.Payload{}
	if err := proto.Unmarshal(tx.TransactionEnvelope.Payload, payload); err != nil || payload.Header == nil {
		return errors.New("invalid committed payload")
	}
	header := &cb.ChannelHeader{}
	if err := proto.Unmarshal(payload.Header.ChannelHeader, header); err != nil || header.Type != int32(cb.HeaderType_ENDORSER_TRANSACTION) || header.TxId != txID || header.ChannelId != channel {
		return errors.New("committed transaction or channel does not match")
	}
	return nil
}

func verifySettlement(data []byte, id string) error {
	var value struct{ DocType, ID, Owner, Debtor, Creditor, Amount, Currency, Status string }
	if json.Unmarshal(data, &value) != nil || value.DocType != "settlement" || value.ID != id || value.Owner != "BankA" || value.Debtor != "BankA" || value.Creditor != "BankB" || value.Amount != "100" || value.Currency != "USD" || value.Status != "PENDING" {
		return fmt.Errorf("endorsed settlement state does not match")
	}
	return nil
}
