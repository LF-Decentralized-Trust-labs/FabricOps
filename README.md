[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/13370/badge)](https://bestpractices.coreinfrastructure.org/projects/13370) [![Go Report Card](https://goreportcard.com/badge/github.com/LF-Decentralized-Trust-labs/FabricOps)](https://goreportcard.com/report/github.com/LF-Decentralized-Trust-labs/FabricOps) ![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/LF-Decentralized-Trust-labs/FabricOps?sort=semver)

![FabricOps](./fabricops-lockup-ink.png#gh-light-mode-only)
![FabricOps](./logo.png#gh-dark-mode-only)

[Documentation](https://fabricops.io) | [Releases](https://github.com/LF-Decentralized-Trust-labs/FabricOps/releases) | [Supported features](SUPPORTED_FEATURES.md)

FabricOps is an LF Decentralized Trust Lab and Kubernetes operator for
provisioning multi-organization Hyperledger Fabric networks from declarative
configuration.

FabricOps is community lab work, not a graduated Hyperledger Fabric project.
The project is intended to mature in the open while collaborating with adjacent
Fabric deployment and operations efforts.

The long-term goal is automated Fabric infrastructure on Kubernetes: Terraform for cluster/cloud infrastructure, a custom operator for Fabric CAs, orderers, peers, channels, and Chaincode-as-a-Service, plus TLS certificate lifecycle management and Prometheus-based health visibility.

## Installation

FabricOps installs into an existing Kubernetes cluster. End users only need `kubectl`; Helm is optional.

Requirements:

- Kubernetes cluster, such as EKS, kind, minikube, or OrbStack
- `kubectl` configured for the target cluster
- Default storage class for the sample network's persistent volumes
- Helm 3 or later, only if installing with Helm

### Install From Release Bundle

Install the latest published release bundle:

```bash
kubectl apply -f https://github.com/LF-Decentralized-Trust-labs/FabricOps/releases/download/v0.2.1/install.yaml
kubectl rollout status deployment/fabricops-controller-manager -n fabricops-system --timeout=120s
```

The bundle installs the `FabricNetwork` CRD, RBAC, ServiceAccount, manager Deployment, and metrics Service. The manager image is pinned to the release tag:

```text
ghcr.io/lf-decentralized-trust-labs/fabricops:0.2.0
```

### Install With Helm

Install the release chart directly from the GitHub release:

```bash
helm upgrade --install fabricops \
  https://github.com/LF-Decentralized-Trust-labs/FabricOps/releases/download/v0.2.1/fabricops-0.2.0.tgz \
  --namespace fabricops-system \
  --create-namespace \
  --wait
```

The chart installs CRDs, RBAC, the manager Deployment, and the metrics Service. By default it uses `ghcr.io/lf-decentralized-trust-labs/fabricops:<chart-appVersion>`.

Override the manager image if needed:

```bash
helm upgrade --install fabricops \
  https://github.com/LF-Decentralized-Trust-labs/FabricOps/releases/download/v0.2.1/fabricops-0.2.0.tgz \
  --namespace fabricops-system \
  --create-namespace \
  --set manager.image.repository=ghcr.io/lf-decentralized-trust-labs/fabricops \
  --set manager.image.tag=0.2.0 \
  --wait
```

### Render Then Apply

If you want to review the Kubernetes objects before applying them:

```bash
helm template fabricops \
  https://github.com/LF-Decentralized-Trust-labs/FabricOps/releases/download/v0.2.1/fabricops-0.2.0.tgz \
  --namespace fabricops-system > fabricops-install.yaml

kubectl apply -f fabricops-install.yaml
kubectl rollout status deployment/fabricops-controller-manager -n fabricops-system --timeout=120s
```

### Create A Sample Network

After installing the operator, apply the sample `FabricNetwork`:

```bash
kubectl apply -f https://raw.githubusercontent.com/LF-Decentralized-Trust-labs/FabricOps/v0.2.1/config/samples/fabricops_v1alpha1_fabricnetwork.yaml
kubectl wait fabricnetwork/fabricnetwork-sample -n default --for=condition=Ready --timeout=20m
```

Inspect the generated network:

```bash
kubectl get fabricnetwork fabricnetwork-sample -n default
kubectl get pods -n fo-sample-orderer
kubectl get pods -n fo-sample-banka
kubectl get pods -n fo-sample-bankb
```

FabricOps generates an in-cluster client connection profile for each peer org. For the sample BankA org:

```bash
kubectl get configmap fabricnetwork-sample-connection-profile -n fo-sample-banka -o jsonpath='{.data.connection\.yaml}'
kubectl get fabricnetwork fabricnetwork-sample -n default -o jsonpath='{.status.orgStatus[?(@.name=="BankA")].connectionProfileConfigMapName}'
kubectl get fabricnetwork fabricnetwork-sample -n default -o jsonpath='{.status.orgStatus[?(@.name=="BankA")].peerEndpoints}'
```

### Install fabricopsctl

`fabricopsctl` is optional, but it makes day-two operations easier by wrapping
status, readiness, connection profile lookup, membership bundle export, and
short-lived chaincode operation Jobs.

Install the CLI with Go:

```bash
go install github.com/LF-Decentralized-Trust-labs/FabricOps/cmd/fabricopsctl@latest
export PATH="$(go env GOPATH)/bin:$PATH"
fabricopsctl version
fabricopsctl status -n default fabricnetwork-sample
fabricopsctl wait -n default --timeout 20m fabricnetwork-sample
fabricopsctl status --participant -n default bankb-participant
fabricopsctl wait --participant -n default --for condition=LocalInfrastructureReady --timeout 20m bankb-participant
fabricopsctl connection-profile -n default --org BankA --format yaml fabricnetwork-sample
fabricopsctl join-bundle -n default --org BankA --out banka-join-bundle.json fabricnetwork-sample
fabricopsctl join-bundle validate banka-join-bundle.json
fabricopsctl join-bundle plan --channel settlement banka-join-bundle.json
fabricopsctl join-bundle render-org --channel settlement --out banka-org.json banka-join-bundle.json
fabricopsctl join-bundle render-update --channel settlement --out settlement-join-update.sh banka-join-bundle.json
fabricopsctl query -n default --org BankA \
  --channel settlement --chaincode settlement --function readSettlement \
  --args '["settlement-001"]' fabricnetwork-sample
fabricopsctl query --participant -n default --org BankB \
  --channel settlement --chaincode settlement --function readSettlement \
  --args '["settlement-001"]' bankb-participant
```

For reproducible installs, replace `@latest` with a release tag such as
`@v0.2.1`.

If `go install` succeeds but your shell cannot find `fabricopsctl`, make the
PATH export permanent in your shell profile, for example `~/.zshrc`.

When building from source:

```bash
make build-fabricopsctl
bin/fabricopsctl version
bin/fabricopsctl status -n default fabricnetwork-sample
bin/fabricopsctl wait -n default --timeout 20m fabricnetwork-sample
bin/fabricopsctl status --participant -n default bankb-participant
bin/fabricopsctl wait --participant -n default --for condition=Ready --timeout 20m bankb-participant
bin/fabricopsctl connection-profile -n default --org BankA --format yaml fabricnetwork-sample
bin/fabricopsctl join-bundle -n default --org BankA --out banka-join-bundle.json fabricnetwork-sample
bin/fabricopsctl join-bundle participant -n default --out bankb-join-bundle.json bankb-participant
bin/fabricopsctl join-bundle validate banka-join-bundle.json
bin/fabricopsctl join-bundle plan --channel settlement banka-join-bundle.json
bin/fabricopsctl join-bundle render-org --channel settlement --out banka-org.json banka-join-bundle.json
bin/fabricopsctl join-bundle render-update --channel settlement --out settlement-join-update.sh banka-join-bundle.json
bin/fabricopsctl query -n default --org BankA \
  --channel settlement --chaincode settlement --function readSettlement \
  --args '["settlement-001"]' fabricnetwork-sample
bin/fabricopsctl query --participant -n default --org BankB \
  --channel settlement --chaincode settlement --function readSettlement \
  --args '["settlement-001"]' bankb-participant
```

Local source builds report `fabricopsctl development`. Release builds use
`make build-fabricopsctl-release VERSION=<version>` to inject the release
version into the binary with Go linker flags.

Tools that render or apply FabricOps resources, including a future Fablo
Kubernetes engine, can use the same CLI surface after applying the
`FabricNetwork` or `FabricParticipant`: `wait` for readiness or a named
condition such as `LocalInfrastructureReady`, `status` for diagnostics,
`connection-profile` for client material, `join-bundle` for public membership
artifacts and signer hints, `join-bundle validate` for offline artifact checks,
`join-bundle plan` for founder/participant join planning,
`join-bundle participant` for participant-owned org exports,
`join-bundle render-org` for channel Application org JSON,
`join-bundle render-update` for an unsigned channel config update script, and
`invoke` or `query` for smoke checks, including `--participant` when the target
resource is a `FabricParticipant`.

### Federated Channel Joins

Founder/coordinator networks can admit a participant-owned organization by
referencing the rendered Application org JSON from a join bundle:

```bash
fabricopsctl join-bundle participant -n default --out bankb-join-bundle.json bankb-participant
fabricopsctl join-bundle render-org --channel settlement --out bankb-org.json bankb-join-bundle.json
kubectl create configmap bankb-application-org -n default --from-file=org.json=bankb-org.json
```

Then declare the external org on the founder `FabricNetwork` channel:

```yaml
channels:
  - name: settlement
    orgs:
      - name: BankA
        peers: ["peer0"]
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

FabricOps runs a founder-admin channel config update Job when the selected
founder admin MSP can satisfy the declared signer requirement by itself. If
`requiredSignerMSPIDs` names additional founder MSPs, FabricOps stages the
admission inputs and reports a manual signature handoff under
`.status.channelStatus[*].externalOrgs`.
See [Federated Join Operations](docs/federated-join.md) for the founder and
participant runbooks, DNS/TLS requirements, and two-cluster e2e acceptance
criteria.

### Uninstall

Delete `FabricNetwork` resources before removing the operator so FabricOps finalizers can clean up generated org namespaces:

```bash
kubectl delete fabricnetwork fabricnetwork-sample -n default --ignore-not-found
kubectl delete -f https://github.com/LF-Decentralized-Trust-labs/FabricOps/releases/download/v0.2.1/install.yaml
```

For Helm installs:

```bash
kubectl delete fabricnetwork fabricnetwork-sample -n default --ignore-not-found
helm uninstall fabricops -n fabricops-system
```

## Release Artifacts

Release `v0.2.1` publishes:

- `install.yaml`: single-file Kubernetes install bundle
- `fabricops-0.2.0.tgz`: Helm chart archive
- `ghcr.io/lf-decentralized-trust-labs/fabricops:0.2.0`: multi-platform manager image
- `ghcr.io/lf-decentralized-trust-labs/fabricops-node-settlement:0.2.0`: Node CCaaS sample
- `ghcr.io/lf-decentralized-trust-labs/fabricops-go-settlement:0.2.0`: Go CCaaS sample
- `ghcr.io/lf-decentralized-trust-labs/fabricops-java-settlement:0.2.0`: Java CCaaS sample

## Capabilities

FabricOps supports:

- A namespaced `FabricNetwork` CRD at `fabricops.io/v1alpha1`
- Per-org Kubernetes namespaces with compact network-scoped names
- Founder-side external org admission through channel config update Jobs
- Preview `FabricParticipant` CRD for federated-join imported artifacts, participant-local org reconciliation, peer channel joins, participant-side chaincode install/approval, and participant-scoped invoke/query with imported remote peer targets
- Fabric CA, orderer, peer, and CCaaS chaincode workloads
- Fabric CA registrar bootstrap, admin enrollment, and workload enrollment Secrets
- Fabric CA-backed MSP/TLS material for admins, orderers, and peers
- Certificate expiry inventory and Fabric CA renewal Jobs for admin, orderer, and peer leaf identities
- Persistent storage and resource defaults for Fabric workloads, CCaaS runtimes, and helper Jobs
- Declarative channel config generation, channel block generation, orderer joins, peer joins, and anchor peer updates
- CCaaS package metadata generation, install, approve, commit, and chaincode server workloads
- Per-peer-org client connection profile ConfigMaps for in-cluster Gateway/application clients
- Optional external endpoint advertising, TLS SAN enrollment hosts, and local-test TLS hostname overrides for federated peer/orderer access
- Endpoint discovery in status for Fabric CAs, peers, orderers, operations Services, and peer chaincode Services
- `fabricopsctl` helper commands for status, connection profile lookup, join bundle handoff, and chaincode invoke/query against `FabricNetwork` or `FabricParticipant` resources when built from source
- Kubernetes status conditions for component, identity, certificate lifecycle, channel, chaincode, and observability readiness
- Fabric peer/orderer operations endpoints and optional Prometheus Operator `ServiceMonitor` resources
- Optional org-boundary NetworkPolicies for FabricOps-managed pods
- Opt-in cleanup for successful helper Jobs whose outputs are stored in durable FabricOps resources
- Finalizer-based cleanup for generated org namespaces

See [SUPPORTED_FEATURES.md](SUPPORTED_FEATURES.md) for the detailed
compatibility matrix.

## Namespace Layout

Org namespaces use this convention:

```text
fo-<network>-<org>
```

For example, the sample `FabricNetwork` named `fabricnetwork-sample` creates:

```text
fo-sample-orderer
fo-sample-banka
```

If the `FabricNetwork` lives outside the `default` namespace, the control namespace is included to avoid cluster-wide namespace collisions:

```text
fo-<control-namespace>-<network>-<org>
```

## For Contributors

Use the [bug report](.github/ISSUE_TEMPLATE/bug_report.yml) or
[feature request](.github/ISSUE_TEMPLATE/feature_request.yml) issue form when
opening an issue. Pull requests should follow the
[pull request template](.github/PULL_REQUEST_TEMPLATE.md), including the related
issue, exact tests run, documentation and breaking-change impact, and DCO
sign-off checklist.

Contributor requirements:

- Go >= 1.23
- Kubebuilder >= 4.15.0
- Docker with buildx
- Helm 3 or later
- kind, OrbStack, minikube, or another Kubernetes cluster

### Local Controller Run

Install the CRD, run the controller locally, and apply the sample network:

```bash
make install
make run
kubectl apply -k config/samples
```

In another terminal, inspect the reconciled network:

```bash
kubectl get fabricnetwork fabricnetwork-sample -n default
kubectl get pods -n fo-sample-orderer
kubectl get pods -n fo-sample-banka
```

### Local In-Cluster Bundle

Build the manager image, render the install bundle, deploy the operator, and apply the sample network:

```bash
make docker-build IMG=controller:latest
make build-installer IMG=controller:latest
kubectl apply -f dist/install.yaml
kubectl apply -k config/samples
```

The generated manager Deployment uses `imagePullPolicy: IfNotPresent` so local development clusters such as OrbStack can use the locally built `controller:latest` image. For kind, load the image into the target cluster before applying the bundle:

```bash
kind load docker-image controller:latest --name <cluster-name>
```

Inspect the installed manager and sample:

```bash
kubectl get deploy -n fabricops-system fabricops-controller-manager
kubectl get fabricnetwork fabricnetwork-sample -n default
kubectl get pods -n fo-sample-orderer
kubectl get pods -n fo-sample-banka
```

For a clean kind-cluster demo:

```bash
kind create cluster --name fabricops-packaging
kind load docker-image controller:latest --name fabricops-packaging
kubectl apply -f dist/install.yaml
kubectl rollout status deployment/fabricops-controller-manager -n fabricops-system --timeout=120s
kubectl apply -k config/samples
make build-fabricopsctl
bin/fabricopsctl wait -n default --timeout 20m fabricnetwork-sample
bin/fabricopsctl invoke -n default --org BankA --peer BankA/peer0 --peer BankB/peer0 \
  --channel settlement --chaincode settlement --function createSettlement \
  --args '["demo-001","alice","bob","100","USD"]' fabricnetwork-sample
bin/fabricopsctl query -n default --org BankA --peer BankA/peer0 \
  --channel settlement --chaincode settlement --function readSettlement \
  --args '["demo-001"]' fabricnetwork-sample
```

### Local Helm Install

Use the generated chart in `dist/chart` for local chart development:

```bash
make docker-build IMG=controller:latest
kind load docker-image controller:latest --name <cluster-name>
make helm-deploy IMG=controller:latest
make helm-status
kubectl apply -k config/samples
kubectl wait fabricnetwork/fabricnetwork-sample -n default --for=condition=Ready --timeout=20m
```

Override local Helm settings with `HELM_RELEASE`, `HELM_NAMESPACE`, `HELM_CHART_DIR`, and `HELM_EXTRA_ARGS`.

### Manager Images

Local development uses `controller:latest` so OrbStack and kind can run the manager image without a registry. Published manager images should use immutable SemVer tags at:

```text
ghcr.io/lf-decentralized-trust-labs/fabricops:<version>
```

### Release Automation

Use the `Release` GitHub Actions workflow from the GitHub UI when publishing a
new release. Trigger it from `main` with a release tag such as `v<version>`.

The workflow validates the tag, updates release-version files, runs the Go test
and lint gates, builds and pushes the manager plus sample chaincode images,
generates `install.yaml` and the Helm chart package, verifies GHCR public
visibility, commits the release-prep changes, tags the commit, and creates the
GitHub release with the generated assets.

The workflow also builds `fabricopsctl` with the release version embedded and
verifies the binary through the normal build gate.

The local release helpers remain useful for debugging individual steps. For
example:

```bash
make docker-buildx-release VERSION=0.2.0
make build-installer-release VERSION=0.2.0
```

`docker-buildx-release` publishes the manager image for all configured `PLATFORMS`. For local single-platform sanity checks, use `docker-build-release` and `docker-push-release`.

Use that same tag for Helm installs:

```bash
make helm-deploy-release VERSION=0.2.0
```

The release helpers derive `RELEASE_IMG` from `IMAGE_REGISTRY`, `IMAGE_REPOSITORY`, and `VERSION`. Override those variables if the image moves to another registry or repository.

Before publishing release instructions, verify that the manager image and sample chaincode images are publicly pullable from GHCR:

```bash
make release-check-ghcr VERSION=0.2.0
```

See [docs/first-release-checklist.md](docs/first-release-checklist.md) for the full release checklist.

### Terraform Examples

Local development infrastructure examples live under [examples/terraform](examples/terraform). The first example provisions a single-node kind cluster for FabricOps demos:

```bash
make docker-build IMG=controller:latest
cd examples/terraform/local-kind
terraform init
terraform apply
```

### E2E Validation

Run the repeatable kind-based e2e proof with:

```bash
make test-e2e
```

The e2e target builds the local manager plus one selected settlement chaincode
runtime, loads those images into kind, installs the generated bundle, applies
the matching sample manifest, waits for `Ready=True`, and invokes/queries the
chaincode. CI runs the Node, Go, and Java lanes concurrently; the Node lane also
covers private data, peer scale changes, cleanup, and chaincode upgrade. See
[docs/e2e-validation.md](docs/e2e-validation.md) for kind, OrbStack, and
cleanup notes.

## Identity Secrets

FabricOps uses Fabric CA enrollment as the identity material path. The deterministic Secret contract is:

```text
<org>-ca-bootstrap: username, password, user-pass
<org>-ca-bootstrap-next-<hash>: username, password, user-pass
<org>-ca-bootstrap-previous: username, password, user-pass
<org>-admin-enrollment: username, password, user-pass
<workload>-enrollment: username, password, user-pass
<workload>-msp: config.yaml, cacert.pem, tlscacert.pem, signcert.pem, keystore.pem
<workload>-tls: ca.crt, server.crt, server.key
<org>-admin-msp: config.yaml, cacert.pem, tlscacert.pem, signcert.pem, keystore.pem
<org>-admin-tls: ca.crt, client.crt, client.key
```

Fabric CA pods receive `<org>-ca-bootstrap/user-pass` through `FABRIC_CA_SERVER_BOOTSTRAP_USER_PASS`. Admin enrollment Jobs use `<org>-admin-enrollment` to register and enroll the org admin identity, then publish enrolled MSP/TLS material to `<org>-admin-msp` and `<org>-admin-tls`. Workload enrollment Jobs use `<workload>-enrollment` to register and enroll orderer and peer identities, then publish enrolled material to `<workload>-msp` and `<workload>-tls`.

When `spec.orgs[].ca.registrar.rotation.requestID` is set, FabricOps stages a
new bootstrap registrar Secret, validates it through Fabric CA, promotes it
into the stable `<org>-ca-bootstrap` Secret only after the rotation Job
succeeds, and preserves the previous credential in
`<org>-ca-bootstrap-previous` for manual recovery.

Orderers mount identity Secrets at `/var/hyperledger/orderer/msp` and `/var/hyperledger/orderer/tls`. Peers mount them at `/etc/hyperledger/fabric/peer/msp` and `/etc/hyperledger/fabric/peer/tls`.

FabricOps inventories managed MSP/TLS certificates in status, starts Fabric CA
renewal Jobs for admin, orderer, and peer leaf identities inside the renewal
window, and rolls workloads when mounted identity material changes. See
[docs/certificate-lifecycle.md](docs/certificate-lifecycle.md) for recovery
paths and the boundary between leaf identity renewal, bootstrap registrar
rotation, and CA root rollover.

## Storage

Persistent data is configured through `spec.global.storage.ca`, `spec.global.storage.orderer`, and `spec.global.storage.peer`. Each component accepts a `size` and optional `storageClassName`.

Default sizes are:

- CA: `1Gi`
- Orderer: `5Gi`
- Peer: `10Gi`
- CouchDB peer state database: `10Gi`

CA pods mount persistent data at `/etc/hyperledger/fabric-ca-server`. Orderer and peer pods mount persistent data at `/var/hyperledger/production`. CouchDB pods mount persistent data at `/opt/couchdb/data` and use the peer storage size and storage class settings.

Fabric component instances run as singleton Deployments with `Recreate` rollout strategy and one PVC per instance.

## Peer State Databases

Peers use `spec.orgs[].peer.db` to select their state database backend. `LevelDB` keeps Fabric's built-in peer database behavior. `CouchDB` creates one CouchDB Deployment, Service, PVC, and credential Secret per peer, wires the peer's Fabric CouchDB environment variables, and gates peer readiness on the CouchDB Deployment becoming ready.

```yaml
spec:
  orgs:
    - organization:
        name: BankA
      peer:
        instances: 2
        db: CouchDB
        prefix: peer
```

Use CouchDB when chaincode needs rich queries or CouchDB indexes. Existing LevelDB peers remain unchanged unless `peer.db` is set to CouchDB.

## Job Cleanup

FabricOps keeps failed Jobs for diagnostics. To remove successful helper Jobs whose results are already stored in durable resources, set:

```yaml
spec:
  global:
    jobs:
      succeededHistoryTTLSeconds: 600
```

This currently applies to enrollment Jobs, certificate renewal Jobs, channel block generation Jobs, orderer join Jobs, peer join Jobs, anchor peer update Jobs, chaincode install Jobs, chaincode approval Jobs, and chaincode commit Jobs.

The sample `FabricNetwork` opts into a 10-minute successful helper Job history window so local runs stay inspectable without accumulating every completed output-backed Job forever.

If the field is omitted, FabricOps retains successful helper Jobs. This is
intentional for diagnostics, but tools that generate `FabricNetwork` manifests
can opt into cleanup by rendering `spec.global.jobs.succeededHistoryTTLSeconds`.

## Observability

Peer and orderer workloads expose Fabric operations endpoints through separate `*-operations` Services. Prometheus metrics are enabled on those operations endpoints.

Prometheus Operator `ServiceMonitor` output is opt-in:

```yaml
spec:
  global:
    observability:
      serviceMonitor:
        enabled: true
        interval: 30s
        scrapeTimeout: 10s
        labels:
          release: prometheus
```

The `monitoring.coreos.com/v1` ServiceMonitor CRD must already be installed before enabling ServiceMonitor output.

## Network Policy

Org-boundary NetworkPolicies are opt-in:

```yaml
spec:
  global:
    networkPolicy:
      enabled: true
```

When enabled, FabricOps creates one `fabricops-org-boundary` NetworkPolicy per generated org namespace. The policy selects FabricOps-managed pods for that org, allows ingress and egress among pods in namespaces that belong to the same `FabricNetwork`, and keeps DNS plus Kubernetes API egress open for helper Jobs. When disabled, FabricOps removes its owned `fabricops-org-boundary` policies and leaves unowned same-name policies untouched.

Your cluster CNI must support Kubernetes NetworkPolicy enforcement. If Prometheus or other operational tools run outside the generated Fabric namespaces, add a separate allow policy for those clients.

## Cleanup

FabricOps adds a finalizer to each `FabricNetwork`. When a `FabricNetwork` is deleted, the controller deletes generated org namespaces after confirming their FabricOps ownership labels.

## Example Resource

```yaml
apiVersion: fabricops.io/v1alpha1
kind: FabricNetwork
metadata:
  name: fabricnetwork-sample
spec:
  global:
    fabricVersion: "3.1.0"
    tls: true
    networkPolicy:
      enabled: false
    storage:
      ca:
        size: 1Gi
      orderer:
        size: 5Gi
      peer:
        size: 10Gi
  orgs:
    - organization:
        name: Orderer
        domain: orderer.example.com
        mspName: OrdererMSP
      ca:
        db: sqlite
      orderers:
        - groupName: group1
          type: raft
          instances: 2
          prefix: orderer
    - organization:
        name: BankA
        domain: banka.example.com
        mspName: BankAMSP
      ca:
        db: sqlite
      peer:
        instances: 2
        db: LevelDB
        prefix: peer
  channels:
    - name: settlement
      orgs:
        - name: BankA
          peers: ["peer0", "peer1"]
  chaincodes:
    - name: settlement
      version: "0.0.1"
      channel: settlement
      image: ghcr.io/lf-decentralized-trust-labs/fabricops-node-settlement:0.2.0
      sequence: 1
      ccaas:
        replicas: 1
        containerPort: 7052
        servicePort: 7052
        dialTimeout: 10s
        imagePullPolicy: IfNotPresent
```

## References

FabricOps uses [hyperledger-labs/fablo](https://github.com/hyperledger-labs/fablo) as an implementation reference for Fabric network bootstrapping behavior, especially around generated config, Docker Compose topology, CA enrollment flow, peer/orderer configuration, and CCaaS support.

## License

This project is licensed under the Apache License 2.0. See the LICENSE file for details.
