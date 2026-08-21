# Supported features

This document tracks FabricOps feature support against the upstream
[hyperledger-labs/fablo](https://github.com/hyperledger-labs/fablo) feature
matrix. Fablo remains the compatibility benchmark because FabricOps should
eventually cover as much of that practical Fabric-network surface area as makes
sense on Kubernetes.

The `Fablo v2` and `Fablo v3` columns describe upstream Fablo support for
Hyperledger Fabric 2.5.x and 3.1.x. The `FabricOps status` column describes the
current operator implementation in this repository.

The `CI tests` column may stay empty until a stable per-feature workflow link
exists. Feature notes call out kind e2e, Helm smoke, or envtest coverage when
that proof is already present.

## Status legend

| Status | Meaning |
|--------|---------|
| Supported | Implemented in FabricOps and validated through envtest, kind e2e, Helm smoke, OrbStack smoke, or focused build tests |
| Partial | Some support exists, but the behavior is incomplete or not broadly validated |
| Planned | Target feature, not implemented yet |
| Not applicable | Fablo feature does not map directly to the Kubernetes operator model |
| Not validated | Likely possible through configuration, but not verified yet |

## Version baseline

| Project | Fabric v2 baseline | Fabric v3 baseline | Notes |
|---------|--------------------|--------------------|-------|
| Fablo reference | 2.5.12 | 3.1.0 | Upstream Fablo supported-features baseline |
| FabricOps | Not validated yet | 3.1.0 | Current kind and Helm smokes use Fabric `3.1.0`; the operator accepts `spec.global.fabricVersion` |

## FabricOps operator features

| Feature | Fablo v2 | Fablo v3 | FabricOps status | Documented | CI tests | Notes |
|---------|----------|----------|------------------|------------|----------|-------|
| Declarative Kubernetes `FabricNetwork` CRD | n/a | n/a | Supported | README | | Main operator API at `fabricops.io/v1alpha1` |
| Per-org Kubernetes namespaces | n/a | n/a | Supported | README | | Uses compact `fo-<network>-<org>` names |
| Fabric CA deployment | n/a | n/a | Supported | README | | One CA Deployment per org |
| Fabric CA registrar bootstrap Secrets | n/a | n/a | Supported | README | | Deterministic bootstrap credential Secrets |
| Admin/orderer/peer registration and enrollment | n/a | n/a | Supported | README | | Fabric CA Jobs publish real MSP/TLS material into Secrets |
| MSP/TLS Secret validation | n/a | n/a | Supported | | | Invalid or missing material is surfaced in status |
| Persistent data for CAs/orderers/peers | n/a | n/a | Supported | README | | One PVC per Fabric component instance |
| Resource request/limit defaults | n/a | n/a | Supported | README | | Applies to Fabric workloads and helper Jobs |
| Succeeded helper Job cleanup | n/a | n/a | Supported | API | | Opt-in `spec.global.jobs.succeededHistoryTTLSeconds` cleans completed output-backed enrollment, channel block, orderer join, peer join, anchor peer update, and chaincode lifecycle Jobs while retaining failures |
| Status conditions | n/a | n/a | Supported | README | | `Ready`, `IdentityMaterialReady`, `ChannelsReady`, and `ObservabilityReady` |
| Endpoint discovery status | n/a | n/a | Supported | README, API | | Org status exposes CA, advertised orderer/peer, chaincode, and operations endpoint addresses |
| External endpoint advertising | n/a | n/a | Partial | API, docs/federated-join.md | | `externalEndpoints` on orderer groups and peer configs advertise remote-reachable endpoints, add TLS SAN hosts during enrollment, and support local-test `tlsHostnameOverride` for Fabric client/orderer CLI paths; production anchor peer/gossip paths still require DNS/SAN alignment |
| Federated join bundle export | n/a | n/a | Partial | README, CLI | | `fabricopsctl join-bundle --org` and `join-bundle participant` export public MSP config/root material, TLS roots, peer/orderer endpoints, anchor peer hints, membership policy/signer hints, and relevant chaincode definitions; `join-bundle validate` checks saved artifacts offline; `join-bundle plan` renders founder/participant next steps; `join-bundle render-org` emits Application org JSON for channel config updates; `join-bundle render-update` emits an unsigned config-update script |
| Federated founder admission | n/a | n/a | Partial | README, API, docs/federated-join.md | | `FabricNetwork.spec.channels[].externalOrgs` consumes rendered Application org JSON from ConfigMaps or Secrets, runs founder-admin channel config update Jobs, handles already-admitted orgs idempotently, reports durable admission status, and gates automatic submission when `requiredSignerMSPIDs` declares additional founder MSP signatures; full multi-admin signature orchestration is still planned |
| Federated participant API | n/a | n/a | Partial | API, sample, docs/federated-join.md | | Preview `FabricParticipant` CRD models participant-local orgs, imported orderer/peer/channel artifacts, membership policy hints, and chaincode lifecycle expectations; the controller reconciles participant-local CA/peer/admin material, consumes imported channel blocks, runs peer join Jobs, generates or imports CCaaS packages, installs packages on joined peers, deploys participant-local CCaaS workloads, approves definitions through imported orderers, and reports local/remote/channel/chaincode status; two-cluster kind e2e covers join-bundle handoff, founder admission, peer join, participant approval, founder commit, participant-scoped query traffic, and participant-origin invoke across local plus imported founder peer targets |
| Kubernetes Events | n/a | n/a | Supported | | | Ready and failure transitions emit events |
| Finalizer cleanup | n/a | n/a | Supported | README | | Deletes owned org namespaces after ownership-label checks |
| Fabric operations endpoints | n/a | n/a | Supported | README | | Peer/orderer `/healthz` and `/metrics` Services |
| Prometheus `ServiceMonitor` output | n/a | n/a | Supported | README | | Opt-in via `spec.global.observability.serviceMonitor`; requires Prometheus Operator CRDs |
| TLS certificate rotation | n/a | n/a | Planned | | | Production hardening follow-up |
| Operations endpoint TLS | n/a | n/a | Planned | | | Local-dev path currently uses HTTP operations endpoints |
| NetworkPolicy generation | n/a | n/a | Supported | README, API | | Opt-in org-boundary policies via `spec.global.networkPolicy.enabled` |
| Packaged install bundle | n/a | n/a | Supported | README | | `dist/install.yaml` is generated from `config/default` and published with releases |
| Helm chart distribution | n/a | n/a | Supported | README | | Release artifacts include the generated `dist/chart` archive |

## Network configuration

| Feature | Fablo v2 | Fablo v3 | FabricOps status | Documented | CI tests | Notes |
|---------|----------|----------|------------------|------------|----------|-------|
| BFT consensus | n/a | yes | Planned | | | Target for Fabric v3 |
| RAFT consensus | yes | yes | Supported | README | | Current sample joins two orderers to a channel using etcdraft config |
| SOLO consensus | yes | n/a | Planned | | | Legacy Fabric v2 target only |
| TLS | yes | yes | Supported | README | | Workload TLS and orderer admin TLS are enabled when `spec.global.tls=true` |
| Orderer groups | no | no | Partial | README | | CRD models groups; one group is validated in the sample |
| Peer dev mode | yes | no | Planned | | | Kubernetes workflow still undecided |
| Post-bootstrap peer scale-up | n/a | n/a | Supported | samples | | Kind e2e patches a ready network to add a new peer, joins it to declared channels, installs CCaaS packages, and invokes through the new peer |
| Safe peer scale-down | n/a | n/a | Supported | samples | | Removes stale peer and peer-specific CCaaS workload surfaces while retaining peer PVCs and identity material |
| Peer DB - LevelDB | yes | yes | Supported | README | | Default Fabric peer state database path; envtest guards that LevelDB peers do not receive CouchDB runtime wiring |
| Peer DB - CouchDB | yes | yes | Supported | README | | `spec.orgs[].peer.db: CouchDB` reconciles one CouchDB Deployment, Service, PVC, and credential Secret per peer, wires peer CouchDB env vars, gates peer readiness on CouchDB availability, and is covered by the sample rich-query kind e2e path |
| CA DB - SQLite | yes | yes | Supported | README | | Fabric CA default path used by current workloads |
| CA DB - Postgres | yes | yes | Planned | | | `spec.orgs[].ca.db` exists but external DB wiring is not implemented |
| CA DB - MySQL | yes | yes | Planned | | | External DB wiring is not implemented |
| Custom Fabric version | yes | yes | Partial | README | | `spec.global.fabricVersion` selects official Hyperledger image tags |
| Custom Fabric images | yes | yes | Partial | | | Arbitrary image repository overrides are not modelled yet |
| JSON/YAML config input | yes | yes | Supported | README | | Kubernetes CRs can be applied as YAML or JSON |

## Channels

| Feature | Fablo v2 | Fablo v3 | FabricOps status | Documented | CI tests | Notes |
|---------|----------|----------|------------------|------------|----------|-------|
| Channel config generation | yes | yes | Supported | README | | Generates `configtx.yaml` from `spec.channels` |
| Channel block generation | yes | yes | Supported | README | | Fabric tooling Job publishes channel block ConfigMaps |
| Orderer channel join | yes | yes | Supported | README | | Uses `osnadmin channel join` |
| Peer channel join | yes | yes | Supported | README | | Uses Fabric peer CLI Jobs per org namespace |
| Anchor peer updates | yes | yes | Supported | README | | Uses config fetch/patch/update flow |
| Channel readiness status | n/a | n/a | Supported | README | | `.status.channelStatus` and `ChannelsReady` |
| Multiple channels | yes | yes | Supported | samples | | The sample declares `settlement` and `audit`; kind e2e waits for all declared channels through `Ready=True` |
| Channel query scripts | yes | yes | Planned | | | Dedicated channel query helper scripts are not implemented yet |
| Channel config updates beyond anchor peers | yes | yes | Planned | | | Anchor peer updates are supported; broader config updates such as org membership changes need a dedicated flow |

## Chaincodes

| Feature | Fablo v2 | Fablo v3 | FabricOps status | Documented | CI tests | Notes |
|---------|----------|----------|------------------|------------|----------|-------|
| Node chaincode | yes | yes | Supported | README, samples | | Node CCaaS sample image is invoked successfully in kind e2e |
| Go chaincode | yes | yes | Supported | samples | | Go CCaaS sample image is built, installed, committed, and invoked successfully in kind e2e |
| Java chaincode | yes | yes | Supported | samples | | Java CCaaS sample image is built, installed, committed, and invoked successfully in kind e2e |
| Chaincode-as-a-Service (CCaaS) | yes | yes | Supported | README, samples | | Package metadata, install, approve, commit, and workloads are reconciled |
| CCaaS hot reload | yes | yes | Planned | | | Not modelled yet |
| Endorsement policies | yes | yes | Supported | samples | | `AND(...)` is validated by kind e2e; topology validation catches unknown, malformed, and outside-channel MSP references |
| Multi-org endorsements | yes | yes | Supported | samples | | Kind e2e invokes through BankA+BankB endorsement sets and queries both orgs |
| Private data collections | yes | yes | Supported | docs/private-data-collections.md, samples | | Kind e2e writes private data with transient input, confirms BankA can read it, confirms BankB cannot read the payload, and confirms BankB can query the private data hash |
| CouchDB index packaging | yes | yes | Supported | API, samples | | Public and private-collection JSON indexes are packaged with CCaaS install archives |
| Chaincode scripts: invoke/query | yes | yes | Supported | README, samples | | `fabricopsctl invoke/query` creates short-lived Fabric tools Jobs; the sample smoke invokes Node, Go, and Java CCaaS runtimes |
| Chaincode scripts: list | yes | yes | Planned | | | Not implemented |
| Chaincode install/upgrade commands | yes | yes | Supported | samples | | Kind e2e proves install/approve/commit, version+sequence upgrade, workload rollout, and post-upgrade invoke/query |
| Chaincode init-required lifecycle | yes | yes | Partial | API | | `initRequired` field exists; init flow is not smoke validated |
| State-based endorsement | n/a | n/a | Planned | | | Fabric-level feature; API shape and safe operation model still need study |

## Tools

| Feature | Fablo v2 | Fablo v3 | FabricOps status | Documented | CI tests | Notes |
|---------|----------|----------|------------------|------------|----------|-------|
| Fablo REST | yes | yes | Planned | | | Could become a Kubernetes-native gateway/helper workload later |
| Explorer | yes | no | Planned | | | Not implemented |
| Gateway client helper | n/a | n/a | Partial | README | | `fabricopsctl` can wait for named readiness conditions, read status, export generated connection profiles, and run invoke/query helper Jobs for `FabricNetwork` resources or `FabricParticipant` resources with `--participant`; SDK-specific Gateway wrappers are not implemented |
| Connection profiles | yes | yes | Supported | README, API | | Per-peer-org ConfigMaps contain JSON/YAML in-cluster Gateway/client profiles and status exposes the ConfigMap name |
| Export network topology to Mermaid | yes | yes | Planned | | | Not implemented |
| Other `init` options | n/a | n/a | Planned | | | Not implemented |

## Fablo commands and FabricOps equivalents

| Fablo command feature | Fablo v2 | Fablo v3 | FabricOps status | Documented | CI tests | Notes |
|-----------------------|----------|----------|------------------|------------|----------|-------|
| `generate` | yes | yes | Partial | README | | Operator generates channel config, channel blocks, and CCaaS packages during reconciliation |
| `up` | yes | yes | Partial | README | | Install bundle and Helm chart are supported; `fabricopsctl wait` can watch a generated `FabricNetwork`, but FabricOps does not provide a single Fablo-style `up` command |
| `start`, `stop`, `restart` | yes | yes | Planned | | | Workload lifecycle operations are not exposed as commands |
| `down`, `reset` | yes | yes | Partial | README | | Deleting a `FabricNetwork` cleans owned namespaces; reset semantics are not modelled |
| `prune`, `recreate` | yes | yes | Planned | | | Not implemented |
| `validate`, `extend-config` | yes | yes | Partial | API | | CRD schema and topology validation catch many invalid specs; Fablo-style config extension is not implemented |
| `version` | yes | yes | Planned | | | No FabricOps CLI version command yet |
| `init` (node, rest, dev) | yes | yes | Partial | samples | | Sample Node, Go, and Java chaincodes exist; no project generator command |
| `export-network-topology` to Mermaid | yes | yes | Planned | | | Not implemented |
| Other `init` options | n/a | n/a | Planned | | | Not implemented |

## Snapshot

| Feature | Fablo v2 | Fablo v3 | FabricOps status | Documented | CI tests | Notes |
|---------|----------|----------|------------------|------------|----------|-------|
| Create snapshot | yes | yes | Planned | | | Fabric snapshot operations are not automated yet |
| Restore snapshot | yes | yes | Planned | | | Restore workflow is not automated yet |

## Other features

| Feature | Fablo v2 | Fablo v3 | FabricOps status | Documented | CI tests | Notes |
|---------|----------|----------|------------------|------------|----------|-------|
| Peer dev mode | yes | no | Planned | | | Duplicated from Fablo's original matrix; kept as a target row |
| Hooks: post-generate | yes | yes | Planned | | | No hook system yet |
| Hooks: post-start | yes | yes | Planned | | | No hook system yet |
| Local Docker Compose network output | yes | yes | Not applicable | | | FabricOps targets Kubernetes resources instead |
| Terraform infrastructure examples | n/a | n/a | Supported | examples/terraform | | `examples/terraform/local-kind` provisions a local kind cluster for demos |
