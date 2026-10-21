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
FabricOps records the operation as already applied and keeps using the original
genesis block for the peer join. Adding another peer to an organization already
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

If a peer already joined the original genesis block before its MSP was admitted,
keep its ledger and declare the same organization on the channel. FabricOps
still stages membership admission: a successful peer join does not authorize
the organization's admin to fetch or update channel configuration. After the
signed admission commits, the existing peer can catch up and the operator can
complete its anchor update. Do not delete the peer's PVC or replace the channel
genesis block to recover from this situation.

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

## Plan A Raft Consenter Change

`fabricopsctl channel plan-raft-update` plans one consenter addition or removal
offline. It does not change the `FabricNetwork`, reconcile orderer membership,
sign or submit an update, or remove any workloads. BFT reconfiguration,
consensus migration, endpoint moves, and certificate replacement are not
supported by this command.

Fetch a current config block using an existing channel member's Fabric admin
environment, then decode it with Fabric's `configtxlator`:

```bash
peer channel fetch config config-block.pb -c settlement \
  -o <existing-orderer-host>:7050 --tls --cafile <orderer-ca.crt>
configtxlator proto_decode --input config-block.pb --type common.Block \
  --output config-block.json

jq '.data.data[0].payload.data.config.channel_group.groups.Orderer.values.ConsensusType.value.metadata.consenters' \
  config-block.json > consenters.json
```

Create `desired-consenters.json` from that array, retaining existing entries
unchanged and adding or removing exactly one entry. Each entry uses Fabric's
decoded protobuf field names: `host`, `port`, `client_tls_cert`, and
`server_tls_cert`. Certificate fields contain base64-encoded public PEM leaf
certificates, not file paths, root certificates, or private keys. For an orderer
using the same certificate for both TLS roles, an addition can be prepared with:

```bash
jq --arg host orderer2.fo-sample-orderer.svc.cluster.local \
  --rawfile cert orderer2-tls.crt \
  '. + [{host: $host, port: 7050, client_tls_cert: ($cert | @base64), server_tls_cert: ($cert | @base64)}]' \
  consenters.json > desired-consenters.json

fabricopsctl channel plan-raft-update --channel settlement \
  --block config-block.json --consenters desired-consenters.json --out raft-plan.json
```

The command needs neither Kubernetes access nor private identity material. It
checks the block's channel ID and CONFIG transaction type, requires Raft in
normal state, validates certificate encoding and endpoints, rejects duplicate
endpoints or shared certificates across nodes, and rejects changing more than
one consenter. Reordering an otherwise identical list produces `AlreadyApplied`
against the supplied snapshot; it does not contact the live network.

The JSON plan includes the source config sequence, action, target, quorum
sizes, warnings, and `originalConfig`/`updatedConfig`. Only the consenter list
changes; existing list order, policy versions, MSPs, endpoint advertisements,
and Raft tuning are preserved. The config pair can be passed to Fabric tooling:

```bash
jq '.originalConfig' raft-plan.json > original-config.json
jq '.updatedConfig' raft-plan.json > updated-config.json
configtxlator proto_encode --input original-config.json --type common.Config --output original-config.pb
configtxlator proto_encode --input updated-config.json --type common.Config --output updated-config.pb
configtxlator compute_update --channel_id settlement \
  --original original-config.pb --updated updated-config.pb --output raft-update.pb
```

`raft-update.pb` is a `common.ConfigUpdate`, not a signed `common.Envelope`, and
cannot be passed directly to `peer channel update` or an org admission's
`signedUpdateRef`. Use the staging handoff below to obtain an unsigned envelope,
or follow Fabric's standard config-update process. The signed handoff described
below uses `raftUpdate.signedUpdateRef`, not an application org's reference.

Before executing a plan, verify all existing consenters are healthy. For an
addition, prepare the node as a follower and verify it has caught up before
promoting it into the consenter set. For removal, keep the node running until
its removal has committed. The planner does not verify health, certificate
trust/validity, signatures, or ledger catch-up, and quorum counts alone do not
establish safety. It also does not add orderer organizations or update their
advertised endpoints. Fetch and re-plan after another config change rather
than editing a delta that administrators have already signed.

See Fabric's [Raft reconfiguration guidance](https://hyperledger-fabric.readthedocs.io/en/latest/raft_configuration.html)
and [channel participation tutorial](https://hyperledger-fabric.readthedocs.io/en/latest/create_channel/create_channel_participation.html).

## Stage A Reviewed Raft Plan

Store the CLI plan as public configuration in the `FabricNetwork` namespace:

```bash
kubectl create configmap reviewed-raft-plan -n default \
  --from-file=plan.json=raft-plan.json --dry-run=client -o yaml | kubectl apply -f -
```

Add a single `raftUpdate` request to the existing channel:

```yaml
channels:
  - name: settlement
    orgs:
      - name: BankA
        peers: [peer0]
      - name: BankB
        peers: [peer0]
    raftUpdate:
      operationID: add-orderer2-001
      adminOrg: Orderer
      orderer:
        org: Orderer
        name: orderer0
      planRef:
        configMapKeyRef:
          name: reviewed-raft-plan
          key: plan.json
```

`adminOrg` must be a locally managed ordering organization, not an application
organization. Both fields in `orderer` are required, and the source instance
must belong to `adminOrg`. A Secret key reference is also supported, but plans
must contain public configuration only: FabricOps copies the normalized plan
into an immutable ConfigMap in the ordering org's namespace.

The controller independently recomputes the single-consenter delta and rejects
changes to unrelated policies, versions, MSPs, or Raft settings. The staging
Job mounts that ordering org's own admin MSP/TLS material and only the source
orderer's public TLS CA. It fetches a live config block, checks that the selected
ordering MSP is on the channel, and compares the full live config with the
reviewed source snapshot before computing the unsigned update envelope.
Staging neither signs nor submits the update and does not create or delete
orderer workloads. It does not replace the genesis block.

Inspect `.status.channelStatus[*].raftUpdate` for the operation ID, phase,
namespace, Job name, input digest, source config sequence, and artifact ConfigMap
name. A staged change reports `PendingSignatures`, retains `config-update.pb`,
`channel-block.pb`, and `raft-update.json` after Job cleanup, and keeps channel
readiness false while the requested operation is incomplete. This does not
stop the existing network from serving transactions. A no-op plan whose full
source snapshot still matches live config reports `AlreadyApplied` and does
not produce an update envelope.

Retrieve the unsigned envelope using the reported namespace and ConfigMap:

```bash
kubectl get configmap <artifact-configmap> -n <reported-namespace> \
  -o jsonpath='{.binaryData.config-update\.pb}' | base64 --decode > raft-envelope.pb
```

Administrators must collect the signatures required by the channel's live
Orderer policies. Do not reference this envelope from an application org's
`signedUpdateRef`: that path verifies Application membership, not Raft
membership. Staging alone does not verify health, follower catch-up, live quorum,
or signature policy satisfaction.

A stale snapshot causes the staging Job to fail before publishing an envelope.
Fetch current config, generate and review a new plan, and use a new `operationID`.
Content-addressed input snapshots and Job names prevent the new request from
silently reusing an old proposal. Failed Jobs are retained for diagnostics;
successful staging evidence survives Job cleanup. Removing `raftUpdate` withdraws
the staging request; it does not execute or undo a membership change. When
finishing through a manual Fabric update, verify that update independently
before withdrawing the request.

## Submit A Signed Raft Update

Collect the signatures required by the channel's live Orderer policies using
`peer channel signconfigtx -f raft-envelope.pb` in each administrator's own
environment. Never copy another organization's signing keys into the operator's
namespace. Store the resulting envelope in the `FabricNetwork` namespace:

```bash
kubectl create secret generic signed-raft-update -n default \
  --from-file=envelope.pb=raft-envelope.pb --dry-run=client -o yaml | kubectl apply -f -
```

Add this field to the existing channel's `raftUpdate` request:

```yaml
signedUpdateRef:
  secretKeyRef:
    name: signed-raft-update
    key: envelope.pb
```

A ConfigMap key reference is also supported. The submission Job snapshots the
signed bytes with the reviewed plan and unsigned proposal. It verifies the
channel ID, requires signatures, and compares the signed delta with both the
staged proposal and a freshly computed delta from the reviewed config pair.
Fabric's orderer verifies signature authenticity and policy satisfaction; the
operator does not weaken those policies or manufacture other admins' signatures.

Automatic submission currently requires every current and desired consenter to
be declared under `adminOrg`, with matching managed TLS certificates that are
within their validity period. Other-org and unmanaged endpoints are blocked
explicitly, although their proposals can still be staged for manual handling.
Select a source consenter that remains in the desired set.

Before submission, every current consenter must pass its health endpoint and
report an active consenter role through the participation API. An added node
must already be an active follower, caught up to the observed source ledger
height. Keep the node being removed declared and running through commitment.
These are point-in-time checks, not a guarantee against subsequent failures or
a proof of network-wide availability. The Job rechecks the full source config
immediately before submitting the update.

Acceptance by `peer channel update` is not completion. FabricOps fetches a newer
committed config with the exact desired consenter set, then verifies all desired
nodes are active consenters caught up through that config block. Only then does
it publish durable evidence and report `Submitted`. A retry that observes the
committed desired set verifies those conditions without submitting again and
reports `AlreadyApplied`.

The result ConfigMap retains the signed `config-update.pb`, committed
`channel-block.pb`, and `raft-update.json` receipt after Job cleanup. The original
genesis artifact is unchanged. Corrected signed bytes create a different Job;
stale plans require a newly reviewed plan and operation ID. FabricOps does not
delete removed orderer workloads or remove them from other channels.

Focused tests run real Fabric protobuf tooling with controlled peer,
participation-API, and health transports. They cover addition, removal, rejected
preflight checks, stale or altered envelopes, accepted-but-uncommitted updates,
and replay recovery. The Node Kind e2e additionally provisions a third orderer
as a follower, stages and signs its promotion using the ordering admin's own
identity, and commits its subsequent removal. It verifies unchanged genesis
and successful invoke/query traffic after each update while retaining the
removed orderer's workload.

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
