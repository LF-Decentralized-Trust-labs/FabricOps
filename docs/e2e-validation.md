# E2E Validation

FabricOps e2e validation provisions a real kind cluster, installs the in-cluster
manager, applies a runtime-specific sample `FabricNetwork` manifest, waits for
`Ready=True`, invokes the selected settlement CCaaS chaincode runtime, and
queries the written state by key plus CouchDB rich query.

## Kind

Run the default Node local e2e proof from the repository root:

```bash
make test-e2e
```

Run another runtime by selecting its manifest:

```bash
make test-e2e \
  E2E_CHAINCODE_RUNTIME=go \
  KIND_CLUSTER=fabricops-e2e-go
```

Supported runtime manifests are:

- `config/samples/e2e/node/fabricnetwork.yaml`
- `config/samples/e2e/go/fabricnetwork.yaml`
- `config/samples/e2e/java/fabricnetwork.yaml`

The default cluster name is `fabricops-test-e2e`. Override it with:

```bash
make test-e2e KIND_CLUSTER=fabricops-e2e
```

By default, the target deletes the kind cluster after the test run. Keep the cluster for inspection with:

```bash
make test-e2e E2E_SKIP_CLEANUP=true
```

When the test completes, it has validated:

- manager image build and kind image load
- selected settlement chaincode image build and kind image load
- generated install bundle deployment
- runtime-specific sample manifest reconciliation to `Ready=True`
- channel bootstrap, chaincode lifecycle, and CCaaS workload readiness
- committed settlement chaincode invokes, key reads, and CouchDB rich queries through BankA and BankB endorsement sets
- Node lane private data collection lifecycle wiring, transient private write, authorized private read, non-member private read rejection, and non-member private hash query
- Node lane peer scale changes, helper Job cleanup, and declarative sequence upgrade

Clean up a retained e2e cluster with:

```bash
make cleanup-test-e2e KIND_CLUSTER=fabricops-e2e
```

## OrbStack

For fast local smoke testing against OrbStack:

```bash
kubectl config use-context orbstack
make docker-build IMG=controller:latest
make build-installer IMG=controller:latest
kubectl apply -f dist/install.yaml
kubectl rollout status deployment/fabricops-controller-manager -n fabricops-system --timeout=120s
kubectl apply -k config/samples
make build-fabricopsctl
bin/fabricopsctl wait -n default --timeout 20m fabricnetwork-sample
bin/fabricopsctl invoke -n default --org BankA --peer BankA/peer0 --peer BankB/peer0 \
  --channel settlement --chaincode settlement --function createSettlement \
  --args '["orbstack-001","alice","bob","100","USD"]' fabricnetwork-sample
bin/fabricopsctl query -n default --org BankA --peer BankA/peer0 \
  --channel settlement --chaincode settlement --function readSettlement \
  --args '["orbstack-001"]' fabricnetwork-sample
```

Enable the private-data smoke after loading a Node settlement image that includes the private settlement transactions:

```bash
PRIVATE_SMOKE_ENABLED=true config/samples/chaincodes/node_settlement/invoke_smoke.sh
```

Remove the sample resources from the current cluster with:

```bash
make cleanup-sample
```

## GitHub Actions

The repository has two kind-backed CI smokes:

- `.github/workflows/test-e2e.yml` runs `make test-e2e` as concurrent Node, Go, and Java matrix lanes against the generated install bundle.
- `.github/workflows/test-chart.yml` installs the Helm chart, applies the Node sample manifest, waits for `Ready=True` with `fabricopsctl`, and invokes/queries the Node settlement chaincode with `fabricopsctl`.

The unit/envtest workflow also runs `go mod tidy` and generated-file drift checks before the repository is ready to release.
