# Channel Config Operations

FabricOps models day-two application organization membership as desired channel
state. The first supported operations add or remove an external peer
organization under the channel `Application` group. FabricOps fetches the live
channel config, computes the smallest protobuf update, and keeps evidence after
the helper Job is cleaned up.

## Add An External Application Org

Declare the rendered Application org JSON and leave `state` omitted, or set it
to `Present` explicitly:

```yaml
channels:
  - name: settlement
    orgs:
      - name: BankA
        peers: ["peer0"]
    externalOrgs:
      - name: BankB
        mspID: BankBMSP
        state: Present
        applicationOrgRef:
          configMapKeyRef:
            name: bankb-application-org
            key: org.json
        anchorPeers:
          - host: peer0.bankb.fabricops.io
            port: 7051
```

`Present` is the default so existing federated-join manifests remain valid.
When the MSP already exists in the live channel config, the operation completes
as `AlreadyApplied` without submitting another update.

## Remove An External Application Org

Keep the org entry as an explicit removal request and set `state: Absent`:

```yaml
channels:
  - name: settlement
    orgs:
      - name: BankA
        peers: ["peer0"]
    externalOrgs:
      - name: BankB
        mspID: BankBMSP
        state: Absent
        operationID: membership-002
        adminOrg: BankA
```

`applicationOrgRef` and `anchorPeers` are not used for removal. FabricOps
rejects removal entries that still declare anchor peers so stale endpoint data
does not look active. Remove the MSP from chaincode endorsement and collection
policies before requesting channel removal.

Removing an org from channel configuration does not delete infrastructure in
the participant's cluster, revoke certificates, or remove historical ledger
data. Those are separate operator and governance responsibilities.

`operationID` gives a transition its own immutable Job and evidence names. It
is optional for the first add or removal. Set a new ID when intentionally
repeating an operation with otherwise identical inputs, for example when an org
is removed and later re-admitted with the same MSP definition. Do not change an
ID merely to retry a failed Job; delete that failed Job after correcting its
cause so FabricOps can recreate the same operation.

## Signature Boundaries

With no `requiredSignerMSPIDs`, or with only the selected `adminOrg` MSP,
FabricOps computes and submits the update using that local channel admin.

When multiple MSPs are listed, FabricOps computes the update but does not submit
it. Status moves through `Staged` to `PendingSignatures`, and the unsigned
protobuf envelope is retained for the administrators named by channel policy.
FabricOps never fabricates or bypasses those signatures.

The staged envelope can be retrieved from the status-reported ConfigMap:

```bash
ARTIFACT=$(kubectl get fabricnetwork fabricnetwork-sample -n default \
  -o jsonpath='{.status.channelStatus[?(@.name=="settlement")].externalOrgs[?(@.mspID=="BankBMSP")].artifactConfigMapName}')

kubectl get configmap "$ARTIFACT" -n <founder-org-namespace> \
  -o jsonpath='{.binaryData.config-update\.pb}' | base64 --decode > config-update.pb
```

Automated import and submission of a fully signed handoff is not implemented in
this slice. Until it is, the required admins sign and submit the staged envelope
with the Fabric peer CLI, and the FabricOps status remains
`PendingSignatures`. This is deliberately visible rather than reported as an
automated success.

## Status And Evidence

Each `.status.channelStatus[*].externalOrgs[*]` entry reports:

- `desiredState`: `Present` or `Absent`
- `operationID`: the explicit identity of a repeated membership transition
- `phase`: `Pending`, `Staged`, `PendingSignatures`, `Submitted`,
  `AlreadyApplied`, or `Failed`
- `updateJobName`: the helper Job for this desired-state transition
- `artifactConfigMapName`: the durable update evidence ConfigMap
- `requiredSignerMSPIDs`: the declared signature boundary

The artifact ConfigMap stores:

- `external-org.json`: channel, MSP ID, desired state, outcome, and anchor-peer
  evidence
- `config-update.pb`: the generated update envelope when a channel mutation was
  needed

An `AlreadyApplied` receipt may not contain `config-update.pb` because no new
update was generated.

## Failure Recovery

Failed Jobs are retained for logs and diagnosis. Correct missing artifacts,
identity material, orderer reachability, or channel-policy problems, then delete
the failed update Job so FabricOps can recreate it:

```bash
kubectl logs -n <founder-org-namespace> job/<update-job-name> --all-containers
kubectl delete job -n <founder-org-namespace> <update-job-name>
```

Do not edit an evidence ConfigMap to claim an outcome. If an artifact ConfigMap
does not match the current channel, MSP ID, or desired state, FabricOps leaves
the operation pending and reports the mismatch.

## Current Scope

This slice covers external application org addition/removal and durable update
evidence. Orderer consenter changes and signed-envelope ingestion remain the
next channel-operation slices. Both will use the same explicit status and
artifact model.
