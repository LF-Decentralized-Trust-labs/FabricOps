#!/usr/bin/env bash
set -euo pipefail

kind_bin="${KIND:-kind}"
kubectl_bin="${KUBECTL:-kubectl}"
cluster=fabricops-restore-e2e
target="kind-${cluster}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

clusters="$("$kind_bin" get clusters)"
if printf '%s\n' "$clusters" | grep -Fxq "$cluster"; then
  echo "Cluster ${cluster} already exists; refusing to reuse or delete it" >&2
  exit 1
fi

workdir="$(mktemp -d "${TMPDIR:-/tmp}/fabricops-restore.XXXXXX")"
export KUBECONFIG="${workdir}/kubeconfig"
export KUBECTL_KUBERC=false
created=false
cleanup() {
  status=$?
  trap - EXIT
  if [ "$created" = true ]; then
    if ! "$kind_bin" delete cluster --name "$cluster" --kubeconfig "$KUBECONFIG"; then
      echo "Cleanup failed; kubeconfig retained at ${KUBECONFIG}" >&2
      exit 1
    fi
  fi
  rm -f "$KUBECONFIG"
  rmdir "$workdir"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# A failed create may leave a partial cluster; never delete a cluster we did not successfully create.
"$kind_bin" create cluster --name "$cluster" --kubeconfig "$KUBECONFIG" --wait 120s
created=true
"$kubectl_bin" --context "$target" apply -f "${repo_root}/config/crd/bases"
"$kubectl_bin" --context "$target" wait --for=condition=Established \
  crd/fabricnetworks.fabricops.io crd/fabricparticipants.fabricops.io --timeout=60s

cd "$repo_root"
FABRICOPS_RESTORE_KIND_CONTEXT="$target" go test -tags=restore_e2e ./cmd/fabricopsctl \
  -run '^TestMetadataRestoreKind$' -count=1 -v -timeout 5m
