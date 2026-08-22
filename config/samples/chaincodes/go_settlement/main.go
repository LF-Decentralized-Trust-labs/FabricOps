package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

type SettlementContract struct {
	contractapi.Contract
}

type Settlement struct {
	DocType  string `json:"docType"`
	ID       string `json:"id"`
	Owner    string `json:"owner"`
	Debtor   string `json:"debtor"`
	Creditor string `json:"creditor"`
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
	Status   string `json:"status"`
}

func (c *SettlementContract) InitLedger(ctx contractapi.TransactionContextInterface) ([]*Settlement, error) {
	settlements := []*Settlement{
		{
			DocType:  "settlement",
			ID:       "settlement-001",
			Owner:    "BankA",
			Debtor:   "BankA",
			Creditor: "BankB",
			Amount:   "125000",
			Currency: "USD",
			Status:   "PENDING",
		},
		{
			DocType:  "settlement",
			ID:       "settlement-002",
			Owner:    "BankC",
			Debtor:   "BankC",
			Creditor: "BankA",
			Amount:   "73000",
			Currency: "EUR",
			Status:   "PENDING",
		},
	}

	for _, settlement := range settlements {
		exists, err := c.SettlementExists(ctx, settlement.ID)
		if err != nil {
			return nil, err
		}
		if exists {
			continue
		}
		if err := putSettlement(ctx, settlement); err != nil {
			return nil, err
		}
	}

	return settlements, nil
}

func (c *SettlementContract) SettlementExists(ctx contractapi.TransactionContextInterface, id string) (bool, error) {
	bytes, err := ctx.GetStub().GetState(id)
	if err != nil {
		return false, err
	}

	return len(bytes) > 0, nil
}

func (c *SettlementContract) CreateSettlement(
	ctx contractapi.TransactionContextInterface,
	id string,
	debtor string,
	creditor string,
	amount string,
	currency string,
) (*Settlement, error) {
	if err := requireText("id", id); err != nil {
		return nil, err
	}
	if err := requireText("debtor", debtor); err != nil {
		return nil, err
	}
	if err := requireText("creditor", creditor); err != nil {
		return nil, err
	}
	if err := requireText("amount", amount); err != nil {
		return nil, err
	}
	if err := requireText("currency", currency); err != nil {
		return nil, err
	}

	exists, err := c.SettlementExists(ctx, id)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, fmt.Errorf("settlement %s already exists", id)
	}

	settlement := &Settlement{
		DocType:  "settlement",
		ID:       id,
		Owner:    debtor,
		Debtor:   debtor,
		Creditor: creditor,
		Amount:   amount,
		Currency: currency,
		Status:   "PENDING",
	}

	return settlement, putSettlement(ctx, settlement)
}

func (c *SettlementContract) ReadSettlement(ctx contractapi.TransactionContextInterface, id string) (*Settlement, error) {
	bytes, err := ctx.GetStub().GetState(id)
	if err != nil {
		return nil, err
	}
	if len(bytes) == 0 {
		return nil, fmt.Errorf("settlement %s does not exist", id)
	}

	settlement := &Settlement{}
	if err := json.Unmarshal(bytes, settlement); err != nil {
		return nil, err
	}

	return settlement, nil
}

func (c *SettlementContract) MarkSettled(ctx contractapi.TransactionContextInterface, id string) (*Settlement, error) {
	settlement, err := c.ReadSettlement(ctx, id)
	if err != nil {
		return nil, err
	}

	settlement.Status = "SETTLED"
	return settlement, putSettlement(ctx, settlement)
}

func (c *SettlementContract) GetAllSettlements(ctx contractapi.TransactionContextInterface) ([]*Settlement, error) {
	iterator, err := ctx.GetStub().GetStateByRange("", "")
	if err != nil {
		return nil, err
	}
	defer iterator.Close()

	settlements := []*Settlement{}
	for iterator.HasNext() {
		result, err := iterator.Next()
		if err != nil {
			return nil, err
		}

		settlement := &Settlement{}
		if err := json.Unmarshal(result.Value, settlement); err != nil {
			return nil, err
		}
		settlements = append(settlements, settlement)
	}

	return settlements, nil
}

func (c *SettlementContract) QuerySettlementsByOwner(ctx contractapi.TransactionContextInterface, owner string) ([]*Settlement, error) {
	if err := requireText("owner", owner); err != nil {
		return nil, err
	}

	queryBytes, err := json.Marshal(map[string]any{
		"selector": map[string]string{
			"docType": "settlement",
			"owner":   owner,
		},
	})
	if err != nil {
		return nil, err
	}

	iterator, err := ctx.GetStub().GetQueryResult(string(queryBytes))
	if err != nil {
		return nil, err
	}
	defer iterator.Close()

	settlements := []*Settlement{}
	for iterator.HasNext() {
		result, err := iterator.Next()
		if err != nil {
			return nil, err
		}

		settlement := &Settlement{}
		if err := json.Unmarshal(result.Value, settlement); err != nil {
			return nil, err
		}
		settlements = append(settlements, settlement)
	}

	return settlements, nil
}

func putSettlement(ctx contractapi.TransactionContextInterface, settlement *Settlement) error {
	bytes, err := json.Marshal(settlement)
	if err != nil {
		return err
	}

	return ctx.GetStub().PutState(settlement.ID, bytes)
}

func requireText(name string, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	return nil
}

func main() {
	chaincode, err := contractapi.NewChaincode(new(SettlementContract))
	if err != nil {
		fmt.Printf("Error creating settlement chaincode: %s\n", err)
		os.Exit(1)
	}

	if err := chaincode.Start(); err != nil {
		fmt.Printf("Error starting settlement chaincode: %s\n", err)
		os.Exit(1)
	}
}
