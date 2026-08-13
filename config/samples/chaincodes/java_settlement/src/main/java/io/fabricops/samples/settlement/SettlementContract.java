package io.fabricops.samples.settlement;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.hyperledger.fabric.contract.Context;
import org.hyperledger.fabric.contract.ContractInterface;
import org.hyperledger.fabric.contract.annotation.Contact;
import org.hyperledger.fabric.contract.annotation.Contract;
import org.hyperledger.fabric.contract.annotation.Default;
import org.hyperledger.fabric.contract.annotation.Info;
import org.hyperledger.fabric.contract.annotation.License;
import org.hyperledger.fabric.contract.annotation.Transaction;
import org.hyperledger.fabric.shim.ledger.KeyValue;
import org.hyperledger.fabric.shim.ledger.QueryResultsIterator;
import org.json.JSONArray;
import org.json.JSONObject;

@Contract(
    name = "SettlementContract",
    info = @Info(
        title = "FabricOps settlement sample",
        description = "Small settlement contract for CCaaS lifecycle tests",
        version = "0.2.0",
        license = @License(name = "Apache-2.0", url = "https://www.apache.org/licenses/LICENSE-2.0"),
        contact = @Contact(name = "FabricOps", email = "fabricops@example.com")
    )
)
@Default
public final class SettlementContract implements ContractInterface {
    @Transaction
    public String initLedger(final Context ctx) {
        Settlement first = newSettlement("settlement-001", "BankA", "BankB", "125000", "USD");
        Settlement second = newSettlement("settlement-002", "BankC", "BankA", "73000", "EUR");

        if (!settlementExists(ctx, first.getId())) {
            putSettlement(ctx, first);
        }
        if (!settlementExists(ctx, second.getId())) {
            putSettlement(ctx, second);
        }

        return getAllSettlements(ctx);
    }

    @Transaction
    public boolean settlementExists(final Context ctx, final String id) {
        byte[] bytes = ctx.getStub().getState(id);
        return bytes != null && bytes.length > 0;
    }

    @Transaction
    public Settlement createSettlement(
        final Context ctx,
        final String id,
        final String debtor,
        final String creditor,
        final String amount,
        final String currency
    ) {
        requireText("id", id);
        requireText("debtor", debtor);
        requireText("creditor", creditor);
        requireText("amount", amount);
        requireText("currency", currency);

        if (settlementExists(ctx, id)) {
            throw new RuntimeException("Settlement " + id + " already exists");
        }

        Settlement settlement = newSettlement(id, debtor, creditor, amount, currency);
        putSettlement(ctx, settlement);
        return settlement;
    }

    @Transaction
    public Settlement readSettlement(final Context ctx, final String id) {
        byte[] bytes = ctx.getStub().getState(id);
        if (bytes == null || bytes.length == 0) {
            throw new RuntimeException("Settlement " + id + " does not exist");
        }

        return Settlement.fromJSONString(new String(bytes, UTF_8));
    }

    @Transaction
    public Settlement markSettled(final Context ctx, final String id) {
        Settlement settlement = readSettlement(ctx, id);
        settlement.setStatus("SETTLED");
        putSettlement(ctx, settlement);
        return settlement;
    }

    @Transaction
    public String getAllSettlements(final Context ctx) {
        JSONArray settlements = new JSONArray();

        try (QueryResultsIterator<KeyValue> results = ctx.getStub().getStateByRange("", "")) {
            for (KeyValue result : results) {
                settlements.put(new JSONObject(result.getStringValue()));
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not read settlements", e);
        }

        return settlements.toString();
    }

    private Settlement newSettlement(
        final String id,
        final String debtor,
        final String creditor,
        final String amount,
        final String currency
    ) {
        Settlement settlement = new Settlement();
        settlement.setId(id);
        settlement.setDebtor(debtor);
        settlement.setCreditor(creditor);
        settlement.setAmount(amount);
        settlement.setCurrency(currency);
        settlement.setStatus("PENDING");
        return settlement;
    }

    private void putSettlement(final Context ctx, final Settlement settlement) {
        ctx.getStub().putState(settlement.getId(), settlement.toJSONString().getBytes(UTF_8));
    }

    private void requireText(final String name, final String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new RuntimeException(name + " is required");
        }
    }
}
