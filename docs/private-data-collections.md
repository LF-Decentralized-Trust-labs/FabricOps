# Private Data Collections

This document captures the first FabricOps private data collections implementation. The goal is compatibility with normal Fabric lifecycle behavior and a smooth mapping from Fablo configs, while keeping the Kubernetes-facing API small enough to validate.

## References

- Hyperledger Fabric private data architecture: https://hyperledger-fabric.readthedocs.io/en/latest/private-data-arch.html
- Fablo sample: `hyperledger-labs/fablo/samples/fablo-config-hlf2-2orgs-2chaincodes-private-data.yaml`
- Fablo config expansion: `hyperledger-labs/fablo/src/extend-config/extendChaincodesConfig.ts`
- Fablo lifecycle scripts: `hyperledger-labs/fablo/src/setup-docker/templates/fabric-docker/scripts/chaincode-functions-v2.sh`

## Implementation Status

FabricOps can install CCaaS packages, approve chaincode definitions, commit them, deploy chaincode workloads, and prove multi-org endorsement. Private data collection declarations are now modeled in the `FabricNetwork` API, rendered as Fabric collection config JSON, stored in per-org ConfigMaps, and mounted into approve and commit lifecycle jobs.

The kind e2e sample now writes private data with transient input, reads it from an authorized BankA peer, verifies a non-member BankB peer cannot return the private payload, and verifies that BankB can still query the private data hash.

In Fabric lifecycle, collection definitions are not part of the chaincode install package. The same collection config file must be supplied during lifecycle approval and commit with `--collections-config`. When collections change later, the full collection definition must be included again with an updated chaincode definition sequence.

## API

Add `privateData` directly to each `chaincodes[]` entry, matching Fablo's user-facing shape while allowing full Fabric collection controls:

```yaml
chaincodes:
  - name: settlement
    version: "0.0.1"
    channel: settlement
    image: ghcr.io/lf-decentralized-trust-labs/fabricops-node-settlement:0.2.0
    sequence: 1
    privateData:
      - name: bank-a-collection
        orgNames: ["BankA"]
      - name: bank-shared-collection
        orgNames: ["BankA", "BankB"]
        requiredPeerCount: 1
        maxPeerCount: 2
        blockToLive: 0
        memberOnlyRead: true
        memberOnlyWrite: true
        endorsementPolicy:
          signaturePolicy: "AND('BankAMSP.member','BankBMSP.member')"
```

Go shape:

```go
type Chaincode struct {
    // existing fields...
    PrivateData []PrivateDataCollection `json:"privateData,omitempty"`
}

type PrivateDataCollection struct {
    Name string `json:"name"`
    OrgNames []string `json:"orgNames"`
    Policy string `json:"policy,omitempty"`
    RequiredPeerCount *int32 `json:"requiredPeerCount,omitempty"`
    MaxPeerCount *int32 `json:"maxPeerCount,omitempty"`
    BlockToLive *int64 `json:"blockToLive,omitempty"`
    MemberOnlyRead *bool `json:"memberOnlyRead,omitempty"`
    MemberOnlyWrite *bool `json:"memberOnlyWrite,omitempty"`
    EndorsementPolicy *PrivateDataEndorsementPolicy `json:"endorsementPolicy,omitempty"`
}

type PrivateDataEndorsementPolicy struct {
    SignaturePolicy string `json:"signaturePolicy,omitempty"`
    ChannelConfigPolicy string `json:"channelConfigPolicy,omitempty"`
}
```

## Defaulting

The current implementation requires `orgNames` for every collection. This keeps the API easy to validate against the declared channel orgs and keeps Fablo config mapping direct.

Defaults:

- `policy`: derive `OR('<MSP>.member',...)` from `orgNames` when omitted.
- `requiredPeerCount`: default to `0`. This keeps single-peer member orgs usable. Production examples should document explicit nonzero values when there are enough authorized peers.
- `maxPeerCount`: default to the number of authorized channel peers minus one, with a minimum of `0`.
- `blockToLive`: default to `0`, meaning never purge by block age.
- `memberOnlyRead`: default to `true`.
- `memberOnlyWrite`: default to `true`.
- `endorsementPolicy`: omit unless explicitly set.

Fablo currently derives `policy` from `orgNames` and writes Fabric collection JSON. FabricOps supports the same shorthand, but does not copy Fablo's anchor-peer-count heuristic blindly because FabricOps channel org peer lists and Kubernetes peer topology are explicit enough to reason from directly.

## Rendered Collection Config

The operator renders `privateData` into the JSON array accepted by Fabric:

```json
[
  {
    "name": "bank-a-collection",
    "policy": "OR('BankAMSP.member')",
    "requiredPeerCount": 0,
    "maxPeerCount": 1,
    "blockToLive": 0,
    "memberOnlyRead": true,
    "memberOnlyWrite": true
  }
]
```

If a collection-level endorsement policy is set, render it using Fabric's nested form:

```json
{
  "endorsementPolicy": {
    "signaturePolicy": "AND('BankAMSP.member','BankBMSP.member')"
  }
}
```

## Reconciliation

FabricOps creates a deterministic collection config ConfigMap for each chaincode and peer org namespace, for example:

- `settlement-settlement-collections`
- key: `collections.json`
- labels: Fabric network, channel, chaincode, component `chaincode`
- annotation: content hash of the rendered JSON

Lifecycle behavior:

- Install jobs do not need the collection config.
- Approve jobs mount the org-local collection ConfigMap and pass `--collections-config /fabricops/chaincode/collections/collections.json`.
- The commit job mounts the same rendered config in the submitter namespace and passes the same flag.
- Chaincode status includes the collection ConfigMap name and rendered JSON hash so users can see which definition was reconciled.

To avoid stale lifecycle jobs, approve and commit job names include a chaincode definition hash, not only the package ID hash. The current hash covers:

- sequence
- endorsement policy
- init-required flag
- raw private data declaration

That makes collection changes visible to reconciliation and avoids accidentally treating an old completed Job as proof for a new chaincode definition.

## Validation

Topology validation fails before runtime Jobs are created when:

- collection names are empty, duplicated within a chaincode, or start with `_`
- `orgNames` is empty
- an `orgNames` entry does not exist in `spec.orgs`
- an `orgNames` entry is not a member of the chaincode channel
- both `endorsementPolicy.signaturePolicy` and `endorsementPolicy.channelConfigPolicy` are set
- `requiredPeerCount` is greater than `maxPeerCount`
- `maxPeerCount` is greater than the number of other authorized peers available for dissemination

The first implementation keeps richer policy syntax validation out of scope and relies on Fabric CLI errors for malformed policy strings, as long as the CR status surfaces the failed Job clearly.

The CRD schema also rejects collection names that do not start with an alphanumeric character, so some name failures are stopped at admission before reconciliation.

## Fablo Mapping

Fablo:

```yaml
privateData:
  - name: org1-collection
    orgNames:
      - Org1
```

FabricOps:

```yaml
privateData:
  - name: org1-collection
    orgNames: ["Org1"]
```

This direct mapping keeps a future Fablo Kubernetes engine simple. More advanced FabricOps-only fields can remain optional and should not be required for Fablo parity.

## Validation

Covered by envtest:

- renders collection JSON from `orgNames`
- rejects unknown or non-channel orgs
- mounts the collection ConfigMap into approve and commit Jobs
- adds `--collections-config` only when collections are declared
- changes approve/commit job names when private data is declared

Covered by kind e2e:

- declares a BankA-only collection on the sample settlement chaincode
- invokes a private write with `--transient`
- queries private data from an authorized BankA peer
- verifies a non-member BankB peer cannot read the private payload
- verifies BankB can query the public private-data hash
