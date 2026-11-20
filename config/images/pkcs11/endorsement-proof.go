//go:build ignore

// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp/utils"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	mb "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	pb "github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric/protoutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

// This test client signs requests with a software admin identity. It never has
// the peer's signing key, provider configuration, PIN, token state or API token.
func main() {
	if err := proveChannelEndorsement(); err != nil {
		fmt.Fprintln(os.Stderr, "Channel endorsement proof failed")
		os.Exit(1)
	}
	if os.Getenv("FABRICOPS_PROOF_MODE") == "application" || os.Getenv("FABRICOPS_PROOF_MODE") == "application-read" {
		fmt.Println("PASS: application endorsement, VALID commit and settlement state verified")
	} else {
		fmt.Println("PASS: two fresh channel queries have certificate-bound peer endorsements")
	}
}

type adminSigner struct {
	key     *ecdsa.PrivateKey
	creator []byte
}

func (s adminSigner) Serialize() ([]byte, error) { return s.creator, nil }

func (s adminSigner) Sign(message []byte) ([]byte, error) {
	digest := sha256.Sum256(message)
	signature, err := ecdsa.SignASN1(rand.Reader, s.key, digest[:])
	if err != nil {
		return nil, err
	}
	return utils.SignatureToLowS(&s.key.PublicKey, signature)
}

func proveChannelEndorsement() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	mspPath, mspID := os.Getenv("CORE_PEER_MSPCONFIGPATH"), os.Getenv("CORE_PEER_LOCALMSPID")
	certificate, err := os.ReadFile(filepath.Join(mspPath, "signcerts/cert.pem"))
	if err != nil {
		return err
	}
	keyPEM, err := os.ReadFile(filepath.Join(mspPath, "keystore/key.pem"))
	if err != nil {
		return err
	}
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return errors.New("invalid admin key")
	}
	private, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return errors.New("invalid admin key")
	}
	key, ok := private.(*ecdsa.PrivateKey)
	if !ok {
		return errors.New("unsupported admin key")
	}
	creator, err := proto.Marshal(&mb.SerializedIdentity{Mspid: mspID, IdBytes: certificate})
	if err != nil {
		return err
	}
	peerCertificate, err := os.ReadFile(os.Getenv("FABRICOPS_PROOF_PEER_CERT"))
	if err != nil {
		return err
	}
	tls, err := credentials.NewClientTLSFromFile(os.Getenv("CORE_PEER_TLS_ROOTCERT_FILE"), os.Getenv("CORE_PEER_TLS_SERVERHOSTOVERRIDE"))
	if err != nil {
		return err
	}
	connection, err := grpc.NewClient(os.Getenv("CORE_PEER_ADDRESS"), grpc.WithTransportCredentials(tls))
	if err != nil {
		return err
	}
	defer connection.Close()
	client := pb.NewEndorserClient(connection)
	channel := os.Getenv("FABRICOPS_PROOF_CHANNEL")
	if channel == "" {
		return errors.New("proof channel is required")
	}
	mode := os.Getenv("FABRICOPS_PROOF_MODE")
	if mode == "application" || mode == "application-read" {
		return proveApplication(ctx, client, adminSigner{key: key, creator: creator}, peerCertificate, mspID, channel, mode == "application-read")
	}
	if mode != "" {
		return errors.New("unsupported proof mode")
	}
	genesisBytes, err := os.ReadFile(os.Getenv("FABRICOPS_PROOF_BLOCK"))
	if err != nil {
		return err
	}
	genesis := &cb.Block{}
	if err := proto.Unmarshal(genesisBytes, genesis); err != nil || genesis.Header == nil || genesis.Header.Number != 0 {
		return errors.New("invalid genesis block")
	}
	blockHash := protoutil.BlockHeaderHash(genesis.Header)
	for i := 0; i < 2; i++ {
		proposal, _, err := protoutil.CreateProposalFromCIS(cb.HeaderType_ENDORSER_TRANSACTION, channel,
			&pb.ChaincodeInvocationSpec{ChaincodeSpec: &pb.ChaincodeSpec{Type: pb.ChaincodeSpec_GOLANG,
				ChaincodeId: &pb.ChaincodeID{Name: "qscc"}, Input: &pb.ChaincodeInput{Args: [][]byte{[]byte("GetChainInfo"), []byte(channel)}}}}, creator)
		if err != nil {
			return err
		}
		signed, err := protoutil.GetSignedProposal(proposal, adminSigner{key: key, creator: creator})
		if err != nil {
			return err
		}
		response, err := client.ProcessProposal(ctx, signed)
		if err != nil {
			return err
		}
		if err := verifyChannelEndorsement(proposal, response, peerCertificate, mspID, blockHash); err != nil {
			return err
		}
	}
	return nil
}

func verifyChannelEndorsement(proposal *pb.Proposal, response *pb.ProposalResponse, expectedCert []byte, mspID string, blockHash []byte) error {
	action, err := verifyPeerEndorsement(proposal, response, expectedCert, mspID, "qscc")
	if err != nil {
		return err
	}
	info := &cb.BlockchainInfo{}
	if err := proto.Unmarshal(action.Response.Payload, info); err != nil || info.Height != 1 || !bytes.Equal(info.CurrentBlockHash, blockHash) {
		return errors.New("endorsed ledger state does not match the joined genesis")
	}
	return nil
}

func verifyPeerEndorsement(proposal *pb.Proposal, response *pb.ProposalResponse, expectedCert []byte, mspID, chaincode string) (*pb.ChaincodeAction, error) {
	if proposal == nil || response == nil || response.Response == nil || response.Response.Status != 200 || response.Endorsement == nil {
		return nil, errors.New("successful endorsed response is required")
	}
	identity := &mb.SerializedIdentity{}
	if err := proto.Unmarshal(response.Endorsement.Endorser, identity); err != nil || identity.Mspid != mspID {
		return nil, errors.New("endorser MSP does not match")
	}
	expected, _ := pem.Decode(expectedCert)
	actual, _ := pem.Decode(identity.IdBytes)
	if expected == nil || actual == nil || !bytes.Equal(expected.Bytes, actual.Bytes) {
		return nil, errors.New("endorser certificate does not match the published peer")
	}
	certificate, err := x509.ParseCertificate(expected.Bytes)
	if err != nil {
		return nil, errors.New("invalid peer certificate")
	}
	public, ok := certificate.PublicKey.(*ecdsa.PublicKey)
	now := time.Now()
	if !ok || now.Before(certificate.NotBefore) || now.After(certificate.NotAfter) {
		return nil, errors.New("unsupported or time-invalid peer certificate")
	}
	digest := sha256.Sum256(append(append([]byte(nil), response.Payload...), response.Endorsement.Endorser...))
	if !ecdsa.VerifyASN1(public, digest[:], response.Endorsement.Signature) {
		return nil, errors.New("invalid peer endorsement signature")
	}
	header := &cb.Header{}
	if err := proto.Unmarshal(proposal.Header, header); err != nil {
		return nil, errors.New("invalid proposal header")
	}
	wantHash, err := protoutil.GetProposalHash1(header, proposal.Payload)
	if err != nil {
		return nil, errors.New("invalid proposal payload")
	}
	payload := &pb.ProposalResponsePayload{}
	if err := proto.Unmarshal(response.Payload, payload); err != nil || !bytes.Equal(payload.ProposalHash, wantHash) {
		return nil, errors.New("endorsement does not bind the fresh proposal")
	}
	action := &pb.ChaincodeAction{}
	if err := proto.Unmarshal(payload.Extension, action); err != nil || action.ChaincodeId == nil || action.ChaincodeId.Name != chaincode || action.Response == nil || !proto.Equal(action.Response, response.Response) {
		return nil, errors.New("endorsed chaincode result does not match")
	}
	return action, nil
}
