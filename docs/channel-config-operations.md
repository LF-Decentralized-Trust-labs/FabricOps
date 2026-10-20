# Channel Config Operations

FabricOps models day-two application organization membership as desired channel
state. The first supported operations add or remove an external peer
organization under the channel `Application` group. FabricOps fetches the live
channel config, computes the smallest protobuf update, and keeps evidence after
the helper Job is cleaned up.

Locally managed organizations declared under `spec.orgs` can also be added to
an existing channel. This differs from a federated handoff because FabricOps
already owns the joining org's CA, admin identity, and peer infrastructure.

## Add A Local Org To An Existing Channel

After a channel is ready, add the local org and its peer to the existing
channel declaration:

```yaml
orgs:
  - organization:
      name: BankC
      domain: bankc.example.com
      mspName: BankCMSP
    ca:
      db: postgres
    peer:
      instances: 1
      db: CouchDB
      prefix: peer

channels:
  - name: audit
    orgs:
      - name: BankA
        peers: ["peer0", "peer1"]
      - name: BankB
        peers: ["peer0"]
      - name: BankC
        peers: ["peer0"]
```

FabricOps uses successful anchor-update receipts to identify existing member
admins. A peer-join receipt alone is insufficient because a peer can join using
a block that does not yet contain its organization's MSP. An existing member
admin fetches the live configuration and renders BankC's Application org
definition from operator-managed MSP material. If BankC is missing, the Job
stages an unsigned envelope in `<channel>-<org>-local-org-add-result` in that
admin's namespace and channel readiness waits for signatures.

This flow is idempotent. If the MSP is already in the live channel configuration,
FabricOps records the operation as already applied and still uses the freshly
fetched block for the peer join. Adding another peer to an organization already
on the channel remains normal peer scale-up; it does not create another
membership update.

Use the signed handoff below and set `signedUpdateRef` on BankC's channel entry.
After submission, FabricOps waits for a fetched config block containing BankC
before allowing its peer join and anchor update. The fetched config block is
retained as operation evidence. `peer channel join` still receives the original
block 0, which remains unchanged in the channel block ConfigMap; the peer then
catches up through the committed membership update. Local additions always
stage a proposal when membership must change; Fabric policy determines which
admins must sign.

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

Each required administrator signs the same envelope using their own Fabric CLI
admin environment. Pass the resulting file between administrators without
recomputing its config update:

```bash
peer channel signconfigtx -f config-update.pb
```

Store the signed envelope in a Secret in the `FabricNetwork` namespace:

```bash
kubectl create secret generic signed-bankc-admission -n default \
  --from-file=envelope.pb=config-update.pb --dry-run=client -o yaml | kubectl apply -f -
```

Reference it on the joining local org:

```yaml
channels:
  - name: audit
    orgs:
      - name: BankA
        peers: ["peer0", "peer1"]
      - name: BankB
        peers: ["peer0"]
      - name: BankC
        peers: ["peer0"]
        signedUpdateRef:
          secretKeyRef:
            name: signed-bankc-admission
            key: envelope.pb
```

For external additions or removals, put the same `signedUpdateRef` field on the
relevant `externalOrgs` entry. A `configMapKeyRef` is also supported. To retrieve
a local proposal, read `binaryData.config-update.pb` from the ConfigMap reported
in the channel's waiting message, using the same base64 decoding shown above.

The submission Job rejects a different channel, a missing signature list, or
any alteration to the staged config update before contacting Fabric. The
orderer validates signature authenticity and policy satisfaction; listing
`requiredSignerMSPIDs` does not replace Fabric's policy. The submitter's admin
signature is also added by `peer channel update`.

FabricOps waits until the requested membership is visible in a fetched config
block before reporting success. Corrected signed bytes create a new submission
Job, while completed submissions retain durable receipts after Job cleanup.
If another config change makes the staged read versions stale, the orderer can
reject it. Regenerate and sign a fresh proposal rather than editing its delta
under existing signatures.

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

This slice covers local-org addition to an existing channel, external
application org addition/removal, signed-envelope submission, and durable update
evidence. The Node kind e2e flow exercises two-admin admission to two existing
channels, genesis preservation, and a query of pre-admission ledger data through
the new org. Orderer consenter changes remain outstanding.
