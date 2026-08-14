# Release Checklist

Use this checklist before publishing a FabricOps release tag or pointing users at release install commands.

## Automated Release

Use the `Release` GitHub Actions workflow from the GitHub UI.

- Run it from the `main` branch.
- Enter the intended release tag, for example `v0.2.0`.
- The workflow creates the release-prep commit, pushes the release tag, and
  publishes the GitHub release assets.

The workflow performs these release gates:

- Validate the release tag and ensure the release does not already exist.
- Update release-version files.
- Run Go module tidy verification, unit/envtest tests, lint, and binary builds.
- Build `fabricopsctl` with the release version embedded through Go linker flags.
- Build and push the multi-platform manager image.
- Build and push sample chaincode images.
- Build `dist/install.yaml` and `dist/fabricops-<version>.tgz`.
- Verify GHCR images are publicly pullable before creating the release.

## Manual Sanity Checks

The commands below mirror the automated workflow and are useful for local
debugging or release dry runs.

## Verify The CLI Version

Build the CLI with the same linker-injected version used by the release workflow:

```bash
make build-fabricopsctl-release VERSION=0.2.0
test "$(bin/fabricopsctl version)" = "fabricopsctl 0.2.0"
```

## Build And Publish Images

```bash
make docker-buildx-release VERSION=0.2.0

config/samples/chaincodes/node_settlement/build_and_push.sh
config/samples/chaincodes/go_settlement/build_and_push.sh
config/samples/chaincodes/java_settlement/build_and_push.sh
```

`docker-buildx-release` is the preferred manager image release path because it publishes all configured `PLATFORMS`. Use `docker-build-release` and `docker-push-release` only for a local single-platform sanity build when needed.

The published release image names are:

- `ghcr.io/lf-decentralized-trust-labs/fabricops:<version>`
- `ghcr.io/lf-decentralized-trust-labs/fabricops-node-settlement:<version>`
- `ghcr.io/lf-decentralized-trust-labs/fabricops-go-settlement:<version>`
- `ghcr.io/lf-decentralized-trust-labs/fabricops-java-settlement:<version>`

## Verify Public GHCR Visibility

Run the unauthenticated registry check after pushing images:

```bash
make release-check-ghcr VERSION=0.2.0
```

This check asks GHCR for anonymous pull tokens and then reads image manifests without Docker credentials. It should pass for the manager image and all sample chaincode images before release docs, bundles, or charts reference those tags.

If a newly published GHCR package is still private, open the package settings on GitHub and change its visibility to public, then rerun the check. GitHub documents the package visibility flow in [Configuring a package's access control and visibility](https://docs.github.com/en/packages/learn-github-packages/configuring-a-packages-access-control-and-visibility): personal-account packages are private on first publish, and public container packages allow anonymous pulls.

## Generate Release Artifacts

```bash
make build-installer-release VERSION=0.2.0
helm lint dist/chart
helm template fabricops dist/chart --namespace fabricops-system >/tmp/fabricops-chart.yaml
```

Confirm the generated bundle uses the public manager image:

```bash
grep 'image: ghcr.io/lf-decentralized-trust-labs/fabricops:0.2.0' dist/install.yaml
```

Commit the generated `dist/install.yaml` changes for the release tag.

## Fresh Cluster Proof

Validate both distribution paths on clean kind clusters:

```bash
kind create cluster --name fabricops-release-bundle
kubectl apply -f dist/install.yaml
kubectl rollout status deployment/fabricops-controller-manager -n fabricops-system --timeout=120s
kubectl apply -k config/samples
make build-fabricopsctl
bin/fabricopsctl wait -n default --timeout 20m fabricnetwork-sample
bin/fabricopsctl invoke -n default --org BankA --peer BankA/peer0 --peer BankB/peer0 \
  --channel settlement --chaincode settlement --function createSettlement \
  --args '["release-bundle-001","alice","bob","100","USD"]' fabricnetwork-sample
bin/fabricopsctl query -n default --org BankA --peer BankA/peer0 \
  --channel settlement --chaincode settlement --function readSettlement \
  --args '["release-bundle-001"]' fabricnetwork-sample
```

```bash
kind create cluster --name fabricops-release-helm
make helm-deploy-release VERSION=0.2.0
kubectl apply -k config/samples
make build-fabricopsctl
bin/fabricopsctl wait -n default --timeout 20m fabricnetwork-sample
bin/fabricopsctl invoke -n default --org BankA --peer BankA/peer0 --peer BankB/peer0 \
  --channel settlement --chaincode settlement --function createSettlement \
  --args '["release-helm-001","alice","bob","100","USD"]' fabricnetwork-sample
bin/fabricopsctl query -n default --org BankA --peer BankA/peer0 \
  --channel settlement --chaincode settlement --function readSettlement \
  --args '["release-helm-001"]' fabricnetwork-sample
```

## Publish

- Prefer the `Release` workflow for final publication.
- If publishing manually, create and push the release tag, create the GitHub
  release, upload `install.yaml` and `fabricops-<version>.tgz`, then rerun
  `make release-check-ghcr VERSION=<version>` after the release is visible.
