#!/usr/bin/env bash
set -euo pipefail

accept_header="application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json"

version="${VERSION:-0.1.0}"
image_registry="${IMAGE_REGISTRY:-ghcr.io/lf-decentralized-trust-labs}"
image_repository="${IMAGE_REPOSITORY:-fabricops}"

default_images=(
  "${image_registry}/${image_repository}:${version}"
  "${image_registry}/fabricops-node-settlement:${version}"
  "${image_registry}/fabricops-go-settlement:${version}"
  "${image_registry}/fabricops-java-settlement:${version}"
)

if [ "$#" -eq 0 ]; then
  set -- "${default_images[@]}"
fi

failed=0

parse_image() {
  local image="$1"
  local registry remainder last repo ref

  registry="${image%%/*}"
  remainder="${image#*/}"

  if [ -z "$registry" ] || [ "$registry" = "$image" ]; then
    echo "Image must include a registry: ${image}" >&2
    return 1
  fi

  if [[ "$remainder" == *@* ]]; then
    repo="${remainder%@*}"
    ref="${remainder#*@}"
  else
    last="${remainder##*/}"
    if [[ "$last" == *:* ]]; then
      repo="${remainder%:*}"
      ref="${remainder##*:}"
    else
      repo="$remainder"
      ref="latest"
    fi
  fi

  printf '%s\n%s\n%s\n' "$registry" "$repo" "$ref"
}

for image in "$@"; do
  if ! parsed="$(parse_image "$image")"; then
    failed=1
    continue
  fi

  registry="$(printf '%s\n' "$parsed" | sed -n '1p')"
  repo="$(printf '%s\n' "$parsed" | sed -n '2p')"
  ref="$(printf '%s\n' "$parsed" | sed -n '3p')"

  if [ "$registry" != "ghcr.io" ]; then
    echo "FAIL ${image}: expected a ghcr.io image"
    failed=1
    continue
  fi

  token_url="https://${registry}/token?service=${registry}&scope=repository:${repo}:pull"
  manifest_url="https://${registry}/v2/${repo}/manifests/${ref}"

  if ! token_json="$(curl -fsSL "$token_url")"; then
    echo "FAIL ${image}: could not obtain an unauthenticated pull token"
    failed=1
    continue
  fi

  token="$(printf '%s' "$token_json" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')"
  if [ -z "$token" ]; then
    echo "FAIL ${image}: registry did not return an unauthenticated token"
    failed=1
    continue
  fi

  status="$(
    curl -sS -o /dev/null -w '%{http_code}' \
      -H "Authorization: Bearer ${token}" \
      -H "Accept: ${accept_header}" \
      "$manifest_url" || true
  )"

  case "$status" in
    200)
      echo "OK   ${image}"
      ;;
    401|403)
      echo "FAIL ${image}: not publicly pullable without credentials (HTTP ${status})"
      failed=1
      ;;
    404)
      echo "FAIL ${image}: manifest not found (HTTP 404)"
      failed=1
      ;;
    *)
      echo "FAIL ${image}: unexpected registry response HTTP ${status}"
      failed=1
      ;;
  esac
done

exit "$failed"
