package io.fabricops.samples.settlement;

import org.hyperledger.fabric.contract.annotation.DataType;
import org.hyperledger.fabric.contract.annotation.Property;
import org.json.JSONObject;

@DataType
public final class Settlement {
    @Property
    private String docType;

    @Property
    private String id;

    @Property
    private String owner;

    @Property
    private String debtor;

    @Property
    private String creditor;

    @Property
    private String amount;

    @Property
    private String currency;

    @Property
    private String status;

    public String getDocType() {
        return docType;
    }

    public void setDocType(final String docType) {
        this.docType = docType;
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(final String owner) {
        this.owner = owner;
    }

    public String getDebtor() {
        return debtor;
    }

    public void setDebtor(final String debtor) {
        this.debtor = debtor;
    }

    public String getCreditor() {
        return creditor;
    }

    public void setCreditor(final String creditor) {
        this.creditor = creditor;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(final String amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(final String currency) {
        this.currency = currency;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(final String status) {
        this.status = status;
    }

    public String toJSONString() {
        return new JSONObject(this).toString();
    }

    public static Settlement fromJSONString(final String json) {
        JSONObject object = new JSONObject(json);
        Settlement settlement = new Settlement();
        settlement.setDocType(object.optString("docType", "settlement"));
        settlement.setId(object.getString("id"));
        settlement.setOwner(object.optString("owner", object.getString("debtor")));
        settlement.setDebtor(object.getString("debtor"));
        settlement.setCreditor(object.getString("creditor"));
        settlement.setAmount(object.getString("amount"));
        settlement.setCurrency(object.getString("currency"));
        settlement.setStatus(object.getString("status"));
        return settlement;
    }
}
