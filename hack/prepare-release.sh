#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 v<major>.<minor>.<patch>" >&2
}

release_tag="${1:-}"
if [[ ! "$release_tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  usage
  exit 1
fi

release_version="${release_tag#v}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

export RELEASE_TAG="$release_tag"
export RELEASE_VERSION="$release_version"

perl_replace() {
  perl -0pi -e "$1" "${@:2}"
}

perl_replace 's/^VERSION \?= .+$/VERSION ?= $ENV{RELEASE_VERSION}/m' Makefile

perl_replace \
  's/^version: .+$/version: $ENV{RELEASE_VERSION}/m; s/^appVersion: .+$/appVersion: "$ENV{RELEASE_VERSION}"/m' \
  dist/chart/Chart.yaml

perl_replace '
  s{releases/download/v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?/install\.yaml}{releases/download/$ENV{RELEASE_TAG}/install.yaml}g;
  s{releases/download/v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?/fabricops-[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?\.tgz}{releases/download/$ENV{RELEASE_TAG}/fabricops-$ENV{RELEASE_VERSION}.tgz}g;
  s{raw.githubusercontent.com/LF-Decentralized-Trust-labs/FabricOps/v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?/}{raw.githubusercontent.com/LF-Decentralized-Trust-labs/FabricOps/$ENV{RELEASE_TAG}/}g;
  s{ghcr.io/lf-decentralized-trust-labs/fabricops:([0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?)}{ghcr.io/lf-decentralized-trust-labs/fabricops:$ENV{RELEASE_VERSION}}g;
  s{ghcr.io/lf-decentralized-trust-labs/fabricops-(node|go|java)-settlement:[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?}{ghcr.io/lf-decentralized-trust-labs/fabricops-$1-settlement:$ENV{RELEASE_VERSION}}g;
  s{fabricops-[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?\.tgz}{fabricops-$ENV{RELEASE_VERSION}.tgz}g;
  s{Release `v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?`}{Release `$ENV{RELEASE_TAG}`}g;
  s{VERSION=[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?}{VERSION=$ENV{RELEASE_VERSION}}g;
  s{\@v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?}{\@$ENV{RELEASE_TAG}}g;
' README.md

perl_replace '
  s{v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?}{$ENV{RELEASE_TAG}}g;
  s{(?<![A-Za-z0-9_.-])[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?![A-Za-z0-9_.-])}{$ENV{RELEASE_VERSION}}g;
' \
  docs/first-release-checklist.md

perl_replace \
  's{ghcr.io/lf-decentralized-trust-labs/fabricops-(node|go|java)-settlement:[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?}{ghcr.io/lf-decentralized-trust-labs/fabricops-$1-settlement:$ENV{RELEASE_VERSION}}g' \
  docs/private-data-collections.md \
  config/samples/fabricops_v1alpha1_fabricnetwork.yaml \
  config/samples/fabricops_v1alpha1_fabricparticipant.yaml \
  config/samples/e2e/node/fabricnetwork.yaml \
  config/samples/e2e/go/fabricnetwork.yaml \
  config/samples/e2e/java/fabricnetwork.yaml \
  config/samples/chaincodes/README.md \
  config/samples/chaincodes/node_settlement/build_and_push.sh \
  config/samples/chaincodes/go_settlement/build_and_push.sh \
  config/samples/chaincodes/java_settlement/build_and_push.sh \
  test/e2e/federated/e2e_suite_test.go \
  cmd/fabricopsctl/join_bundle_test.go \
  internal/controller/fabricnetwork_controller_test.go \
  internal/controller/fabricparticipant_controller_test.go

perl_replace 's/"version": "[^"]+"/"version": "$ENV{RELEASE_VERSION}"/' \
  config/samples/chaincodes/node_settlement/package.json

perl_replace "s/^version = '[^']+'/version = '\$ENV{RELEASE_VERSION}'/m" \
  config/samples/chaincodes/java_settlement/build.gradle

perl_replace 's/version = "[^"]+"/version = "$ENV{RELEASE_VERSION}"/' \
  config/samples/chaincodes/java_settlement/src/main/java/io/fabricops/samples/settlement/SettlementContract.java

echo "Prepared FabricOps release ${RELEASE_TAG} (${RELEASE_VERSION})"
