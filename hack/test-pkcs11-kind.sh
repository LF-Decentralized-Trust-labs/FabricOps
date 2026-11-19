#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
for tool in docker kind kubectl go; do command -v "$tool" >/dev/null; done
work=$(mktemp -d "${TMPDIR:-/tmp}/fabricops-pkcs11-kind.XXXXXX")
cluster="fabricops-pkcs11-proof-$(date +%s)-$$"
created=false
cleanup() {
  if [ "$created" = true ]; then kind delete cluster --name "$cluster"; fi
  rm -rf "$work"
}
trap cleanup EXIT
export KUBECONFIG="$work/kubeconfig"
for target in smoke enrollment peer; do
  docker build --target "$target" --tag "fabricops-pkcs11-kind:$target" "$root/config/images/pkcs11"
done
if kind get clusters | grep -Fxq "$cluster"; then echo 'Refusing an existing cluster' >&2; exit 1; fi
created=true
kind create cluster --name "$cluster" --kubeconfig "$KUBECONFIG" --wait 120s
kind load docker-image --name "$cluster" fabricops-pkcs11-kind:smoke fabricops-pkcs11-kind:enrollment fabricops-pkcs11-kind:peer
kubectl --context "kind-$cluster" apply -f "$root/config/crd/bases"
kubectl --context "kind-$cluster" wait --for=condition=Established --timeout=60s -f "$root/config/crd/bases"
cd "$root"
FABRICOPS_PKCS11_KIND_CLUSTER="$cluster" go test -tags=integration ./internal/controller \
  -run '^TestPKCS11KindRuntimeProof$' -count=1 -v -timeout 25m
