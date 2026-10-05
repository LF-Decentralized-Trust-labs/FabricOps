"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { contracts } = require("../index.js");

const [SettlementContract] = contracts;

// Minimal in-memory stand-in for the Fabric stub, so the contract runs without
// Kubernetes or a Fabric network.
function newContext() {
  const state = new Map();
  const puts = [];

  const stub = {
    async getState(key) {
      return state.has(key) ? state.get(key) : Buffer.alloc(0);
    },
    async putState(key, value) {
      puts.push(key);
      state.set(key, value);
    },
  };

  return { ctx: { stub }, state, puts };
}

const create = (contract, ctx, id = "s-1") =>
  contract.createSettlement(ctx, id, "BankA", "BankB", "100", "USD");

test("createSettlement stores a PENDING settlement that readSettlement returns", async () => {
  const contract = new SettlementContract();
  const { ctx } = newContext();

  const created = await create(contract, ctx);
  const read = await contract.readSettlement(ctx, "s-1");

  const expected = {
    docType: "settlement",
    id: "s-1",
    owner: "BankA",
    debtor: "BankA",
    creditor: "BankB",
    amount: "100",
    currency: "USD",
    status: "PENDING",
  };
  assert.deepEqual(created, expected);
  assert.deepEqual(read, expected);
  assert.equal(await contract.settlementExists(ctx, "s-1"), true);
});

test("createSettlement rejects a duplicate id and keeps the existing value", async () => {
  const contract = new SettlementContract();
  const { ctx, state, puts } = newContext();

  await create(contract, ctx);
  const before = state.get("s-1").toString();

  await assert.rejects(
    contract.createSettlement(ctx, "s-1", "BankC", "BankD", "999", "EUR"),
    /Settlement s-1 already exists/,
  );

  assert.equal(state.get("s-1").toString(), before);
  assert.equal(puts.length, 1);
  assert.equal((await contract.readSettlement(ctx, "s-1")).debtor, "BankA");
});

test("readSettlement rejects a missing settlement", async () => {
  const contract = new SettlementContract();
  const { ctx } = newContext();

  assert.equal(await contract.settlementExists(ctx, "missing"), false);
  await assert.rejects(contract.readSettlement(ctx, "missing"), /Settlement missing does not exist/);
});

test("markSettled moves the settlement to SETTLED and persists it", async () => {
  const contract = new SettlementContract();
  const { ctx, state } = newContext();

  await create(contract, ctx);
  const settled = await contract.markSettled(ctx, "s-1");

  assert.equal(settled.status, "SETTLED");
  assert.equal(JSON.parse(state.get("s-1").toString()).status, "SETTLED");
  assert.equal((await contract.readSettlement(ctx, "s-1")).status, "SETTLED");
});

test("markSettled rejects a missing settlement without writing", async () => {
  const contract = new SettlementContract();
  const { ctx, puts } = newContext();

  await assert.rejects(contract.markSettled(ctx, "missing"), /Settlement missing does not exist/);
  assert.equal(puts.length, 0);
});

test("createSettlement requires every field", async () => {
  const contract = new SettlementContract();
  const { ctx, puts } = newContext();

  await assert.rejects(contract.createSettlement(ctx, "", "BankA", "BankB", "100", "USD"), /id is required/);
  await assert.rejects(contract.createSettlement(ctx, "s-1", " ", "BankB", "100", "USD"), /debtor is required/);
  await assert.rejects(contract.createSettlement(ctx, "s-1", "BankA", "", "100", "USD"), /creditor is required/);
  await assert.rejects(contract.createSettlement(ctx, "s-1", "BankA", "BankB", "", "USD"), /amount is required/);
  await assert.rejects(contract.createSettlement(ctx, "s-1", "BankA", "BankB", "100", ""), /currency is required/);
  assert.equal(puts.length, 0);
});
