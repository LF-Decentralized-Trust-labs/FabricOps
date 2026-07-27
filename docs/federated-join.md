# Federated Join Operations

FabricOps treats a federated join as a handoff between two independently
operated Kubernetes environments:

- the founder or coordinator cluster owns the existing `FabricNetwork`
- the participant cluster owns its local org infrastructure through
  `FabricParticipant`
- join bundles and imported artifacts are explicit handoff material, not hidden
  cross-cluster state

This keeps the trust boundary visible. A bank, regulator, or partner can run
its own Kubernetes cluster and Fabric CA while still joining a channel that was
started elsewhere.

## Current Automation

On the founder side, `FabricNetwork.spec.channels[].externalOrgs` can reference
rendered Application org JSON from a ConfigMap or Secret. The controller runs a
founder-admin channel config update Job, handles already-admitted MSPs
idempotently, stores durable result ConfigMaps, and reports admission status in
`.status.channelStatus[*].externalOrgs`.

On the participant side, `FabricParticipant` reconciles the local participant
CA, admin identity, peers, imported channel blocks, peer join Jobs, CCaaS
package install, participant-local chaincode workloads, and chaincode approval
through imported orderers.

`fabricopsctl join-bundle` exports membership material from either a
`FabricNetwork`-managed org or a participant-owned `FabricParticipant`. The
same JSON contract is used by the offline validation, planning, Application org
rendering, and unsigned channel-update script commands.

`fabricopsctl invoke` and `fabricopsctl query` can target a normal
`FabricNetwork`, or a participant-owned `FabricParticipant` when passed
`--participant`. The participant path uses the local participant admin identity,
local peer endpoints from `FabricParticipant.status.localOrgStatus`, and the
imported orderer endpoint/TLS root declared under `spec.network.orderers`.
Participant manifests can also declare already-admitted remote peers under
`spec.network.peers`; invoke operations may then target both local and imported
peers explicitly for cross-cluster endorsement.

## Network And TLS Requirements

Every endpoint in the federated manifest must be reachable from the pods that
will use it. Kubernetes Service DNS names such as
`orderer0.fo-sample-orderer.svc.cluster.local:7050` are only valid inside the
cluster that owns the Service.

For a real multi-cluster join, decide these endpoints before applying the
manifests:

- founder orderer client endpoint: used by participant peers and lifecycle Jobs
- participant peer endpoint: published as the participant org anchor peer
- remote peer endpoints: optionally imported by participant clusters when
  participant-side invokes must endorse against already-admitted orgs
- optional orderer admin endpoint: only needed if a participant workflow is
  expected to call orderer participation APIs directly
- TLS root CAs: founder needs participant MSP/TLS roots; participant needs the
  orderer and imported peer TLS roots it will dial

The hostname in each TLS-enabled endpoint must match the certificate presented
by that Fabric component. The current FabricOps workload enrollment path issues
certificates for in-cluster Service DNS names plus `localhost`. Add external
addresses and any extra certificate names with `externalEndpoints` before those
workloads enroll:

```yaml
orgs:
  - organization:
      name: Orderer
    orderers:
      - groupName: group1
        prefix: orderer
        instances: 1
        externalEndpoints:
          - name: orderer0
            address: orderer0.bank-a.fabricops.io:7050
            tlsHosts:
              - orderer0.fabricops.io
  - organization:
      name: BankB
    peer:
      prefix: peer
      instances: 1
      externalEndpoints:
        - name: peer0
          address: peer0.bankb.fabricops.io:7051
```

FabricOps still uses in-cluster Services for its own local helper Jobs. The
external endpoint is advertised in status, connection profiles, join bundles,
peer gossip external endpoint, and channel config anchor/orderer entries.

For local two-cluster testing, a dial address may intentionally differ from the
certificate identity. Use `tlsHostnameOverride` for Fabric client and orderer
CLI flows:

```yaml
externalEndpoints:
  - name: orderer0
    address: host.docker.internal:8050
    tlsHostnameOverride: localhost
```

Participant imports have the same escape hatch:

```yaml
network:
  orderers:
    - org: OrdererOrg
      name: orderer0
      clientAddress: host.docker.internal:8050
      tlsHostnameOverride: localhost
  peers:
    - org: BankA
      name: peer0
      address: peer0.banka.fabricops.io:7051
      tlsRootCARef:
        configMapKeyRef:
          name: banka-peer0-artifacts
          key: tls-ca.pem
```

That override is useful for port-forward and host-port development smokes.
Production federated networks should use stable DNS names whose certificate SANs
match the advertised endpoints, especially for participant anchor peers, gossip
behavior, and imported remote peer endorsement targets. The Fabric peer CLI path
does not support a distinct TLS hostname override per endorsement peer, so
remote peer dial addresses should match the peer certificate SANs.

## Founder Runbook

1. Confirm the channel is healthy and the founder admin org named in
   `externalOrgs[].adminOrg`, or the first local channel org by default, can
   submit channel config updates.
2. Receive the participant Application org JSON and anchor peer endpoint from
   the joining organization.
3. Store the Application org JSON in the `FabricNetwork` namespace:

   ```bash
   kubectl create configmap bankb-application-org \
     -n default \
     --from-file=org.json=bankb-org.json
   ```

4. Add the participant to the founder `FabricNetwork` channel:

   ```yaml
   channels:
     - name: settlement
       externalOrgs:
         - name: BankB
           mspID: BankBMSP
           applicationOrgRef:
             configMapKeyRef:
               name: bankb-application-org
               key: org.json
           anchorPeers:
             - host: peer0.bankb.fabricops.io
               port: 7051
           requiredSignerMSPIDs:
             - BankAMSP
   ```

5. Wait for the founder network to report the external org as ready:

   ```bash
   fabricopsctl wait -n default --timeout 20m fabricnetwork-sample
   kubectl get fabricnetwork fabricnetwork-sample -n default \
     -o jsonpath='{.status.channelStatus[*].externalOrgs}'
   ```

6. Provide the participant with the channel block needed for peer join plus any
   orderer TLS roots not already present in the join bundle.

If the channel update policy needs signatures from multiple existing orgs, list
those founder-side MSPs under `requiredSignerMSPIDs`:

```yaml
externalOrgs:
  - name: BankB
    mspID: BankBMSP
    requiredSignerMSPIDs:
      - BankAMSP
      - BankCMSP
```

When the list contains more than the selected `adminOrg` MSP, FabricOps validates
the imported Application org JSON, copies the selected orderer TLS root into the
founder admin namespace, and reports that manual signatures are required instead
of launching a one-admin update Job. Use the unsigned script path from
`fabricopsctl join-bundle render-update` to compute, inspect, sign, and submit
the update until the operator has explicit multi-admin signature orchestration.

## Participant Runbook

1. Install FabricOps in the participant cluster.
2. Create ConfigMaps or Secrets for imported orderer TLS roots and channel
   blocks in the `FabricParticipant` namespace.
3. Apply or update a `FabricParticipant` resource with:

   - the participant-owned org shape under `spec.org`
   - reachable founder orderer endpoints under `spec.network.orderers`
   - optional imported remote peer endpoints under `spec.network.peers` when
     participant-side invokes need cross-org endorsement targets
   - imported channel block refs under `spec.channels[].blockRef`
   - externally reachable participant anchor peers under
     `spec.channels[].anchorPeers`
   - chaincode definitions under `spec.chaincodes`

4. Wait for local infrastructure, channel joins, and participant-side chaincode
   lifecycle to become ready:

   ```bash
   fabricopsctl wait --participant \
     -n default \
     --for=condition=Ready \
     --timeout=20m \
     bankb-participant
   ```

   During staged handoffs, the participant can also wait only for local CA,
   peer, and identity material before the founder has exported the channel
   block:

   ```bash
   fabricopsctl wait --participant \
     -n default \
     --for=condition=LocalInfrastructureReady \
     --timeout=20m \
     bankb-participant
   ```

5. Export the participant join bundle for the founder/coordinator:

   ```bash
   fabricopsctl join-bundle participant \
     -n default \
     --out bankb-join-bundle.json \
     bankb-participant
   ```

   The bundle carries public MSP roots, participant peer endpoints, declared
   anchor peers, membership policy/signer hints, chaincode definition
   expectations, and imported orderer TLS roots as inline PEM. It does not
   export private keys or enrollment material.

6. Invoke/query through the network using an org and peer that match the desired
   endorsement path. For participant-owned clusters:

   ```bash
   fabricopsctl query \
     --participant \
     -n default \
     --org BankB \
     --channel settlement \
     --chaincode settlement \
     --function readSettlement \
     --args '["settlement-001"]' \
     bankb-participant
   ```

   To submit from the participant cluster while collecting endorsements from a
   local peer and an imported founder peer, select both targets explicitly:

   ```bash
   fabricopsctl invoke \
     --participant \
     -n default \
     --org BankB \
     --peer BankB/peer0 \
     --peer BankA/peer0 \
     --channel settlement \
     --chaincode settlement \
     --function createSettlement \
     --args '["settlement-002","BankB","BankA","75","USD"]' \
     bankb-participant
   ```

## Two-Cluster E2E Acceptance Criteria

A meaningful federated e2e proof should use two isolated Kubernetes contexts and
prove the full handoff:

- founder cluster installs FabricOps and creates a channel without BankB
- participant cluster installs FabricOps and reconciles BankB-owned CA, admin,
  peer, and chaincode service material
- participant public MSP and anchor peer material is rendered into Application
  org JSON
- founder `FabricNetwork.spec.channels[].externalOrgs` admits BankB through a
  channel config update Job
- participant peers join using imported channel blocks and reachable orderer
  endpoints
- participant chaincode packages are installed and approved against the joined
  channel
- an invoke/query path exercises both founder and participant peers

That test should fail fast when endpoint DNS, TLS roots, or certificate SANs are
wrong, because those are the exact operational problems FabricOps is meant to
make explicit.

The current automated two-kind smoke covers the handoff through participant
channel join, participant-side chaincode approval, founder-side chaincode
commit, application traffic submitted through the founder peer then queried
through the participant peer, and a participant-side invoke that targets both
BankB's local peer and BankA's imported peer. It uses local kind NodePorts for
the founder orderer and founder peer so the participant cluster can reach the
remote Fabric endpoints. Production deployments should use the same explicit
endpoint contract with stable DNS and certificates whose SANs match the
advertised names.
