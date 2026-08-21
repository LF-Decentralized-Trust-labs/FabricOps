"use strict";

const { Contract } = require("fabric-contract-api");

const DEFAULT_PRIVATE_COLLECTION = "bank-a-private-settlements";
const PRIVATE_TRANSIENT_KEY = "settlement";

class SettlementContract extends Contract {
  constructor() {
    super("SettlementContract");
  }

  async initLedger(ctx) {
    const settlements = [
      {
        docType: "settlement",
        id: "settlement-001",
        owner: "BankA",
        debtor: "BankA",
        creditor: "BankB",
        amount: "125000",
        currency: "USD",
        status: "PENDING",
      },
      {
        docType: "settlement",
        id: "settlement-002",
        owner: "BankC",
        debtor: "BankC",
        creditor: "BankA",
        amount: "73000",
        currency: "EUR",
        status: "PENDING",
      },
    ];

    for (const settlement of settlements) {
      const exists = await this.settlementExists(ctx, settlement.id);
      if (!exists) {
        await ctx.stub.putState(settlement.id, Buffer.from(JSON.stringify(settlement)));
      }
    }

    return settlements;
  }

  async settlementExists(ctx, id) {
    const bytes = await ctx.stub.getState(id);
    return Boolean(bytes && bytes.length > 0);
  }

  async createSettlement(ctx, id, debtor, creditor, amount, currency) {
    this.requireText(id, "id");
    this.requireText(debtor, "debtor");
    this.requireText(creditor, "creditor");
    this.requireText(amount, "amount");
    this.requireText(currency, "currency");

    if (await this.settlementExists(ctx, id)) {
      throw new Error(`Settlement ${id} already exists`);
    }

    const settlement = {
      docType: "settlement",
      id,
      owner: debtor,
      debtor,
      creditor,
      amount,
      currency,
      status: "PENDING",
    };

    await ctx.stub.putState(id, Buffer.from(JSON.stringify(settlement)));
    return settlement;
  }

  async readSettlement(ctx, id) {
    const bytes = await ctx.stub.getState(id);
    if (!bytes || bytes.length === 0) {
      throw new Error(`Settlement ${id} does not exist`);
    }

    return JSON.parse(bytes.toString());
  }

  async markSettled(ctx, id) {
    const settlement = await this.readSettlement(ctx, id);
    settlement.status = "SETTLED";

    await ctx.stub.putState(id, Buffer.from(JSON.stringify(settlement)));
    return settlement;
  }

  async createPrivateSettlement(ctx, collection, id) {
    collection = this.collectionOrDefault(collection);
    this.requireText(id, "id");

    const transientMap = ctx.stub.getTransient();
    const settlementBytes = transientMap.get(PRIVATE_TRANSIENT_KEY);
    if (!settlementBytes || settlementBytes.length === 0) {
      throw new Error(`Transient field ${PRIVATE_TRANSIENT_KEY} is required`);
    }

    const settlement = JSON.parse(settlementBytes.toString("utf8"));
    settlement.id = id;
    settlement.owner = settlement.owner || settlement.debtor;
    this.requireText(settlement.debtor, "debtor");
    this.requireText(settlement.creditor, "creditor");
    this.requireText(settlement.amount, "amount");
    this.requireText(settlement.currency, "currency");
    settlement.status = settlement.status || "PENDING";

    const existing = await ctx.stub.getPrivateData(collection, id);
    if (existing && existing.length > 0) {
      throw new Error(`Private settlement ${id} already exists in ${collection}`);
    }

    await ctx.stub.putPrivateData(collection, id, Buffer.from(JSON.stringify(settlement)));
    return {
      id,
      collection,
      status: settlement.status,
    };
  }

  async readPrivateSettlement(ctx, collection, id) {
    collection = this.collectionOrDefault(collection);
    this.requireText(id, "id");

    const bytes = await ctx.stub.getPrivateData(collection, id);
    if (!bytes || bytes.length === 0) {
      throw new Error(`Private settlement ${id} does not exist in ${collection}`);
    }

    return JSON.parse(bytes.toString("utf8"));
  }

  async readPrivateSettlementHash(ctx, collection, id) {
    collection = this.collectionOrDefault(collection);
    this.requireText(id, "id");

    const hash = await ctx.stub.getPrivateDataHash(collection, id);
    if (!hash || hash.length === 0) {
      throw new Error(`Private settlement hash ${id} does not exist in ${collection}`);
    }

    return {
      id,
      collection,
      hash: hash.toString("hex"),
    };
  }

  async getAllSettlements(ctx) {
    const iterator = await ctx.stub.getStateByRange("", "");
    return this.collectQueryResults(iterator);
  }

  async querySettlementsByOwner(ctx, owner) {
    this.requireText(owner, "owner");

    const query = {
      selector: {
        docType: "settlement",
        owner,
      },
    };
    const iterator = await ctx.stub.getQueryResult(JSON.stringify(query));
    return this.collectQueryResults(iterator);
  }

  async collectQueryResults(iterator) {
    const settlements = [];

    try {
      for (;;) {
        const result = await iterator.next();
        if (result.value && result.value.value) {
          settlements.push(JSON.parse(result.value.value.toString("utf8")));
        }
        if (result.done) {
          return settlements;
        }
      }
    } finally {
      await iterator.close();
    }
  }

  requireText(value, name) {
    if (!value || `${value}`.trim() === "") {
      throw new Error(`${name} is required`);
    }
  }

  collectionOrDefault(collection) {
    return collection && `${collection}`.trim() !== "" ? collection : DEFAULT_PRIVATE_COLLECTION;
  }
}

module.exports.contracts = [SettlementContract];
