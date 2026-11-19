#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
image=${PKCS11_SMOKE_IMAGE:-fabricops-pkcs11:smoke}
volume=
container=
network=
cleanup() {
  if [ -n "$container" ]; then docker rm -f "$container" > /dev/null 2>&1 || true; fi
  if [ -n "$network" ]; then docker network rm "$network" > /dev/null; fi
  if [ -n "$volume" ]; then docker volume rm "$volume" > /dev/null; fi
}
trap cleanup EXIT

docker build --target smoke --tag "$image" "$root/config/images/pkcs11"
volume=$(docker volume create --label fabricops.io/test=pkcs11)
container="fabricops-pkcs11-$volume"
network="$container"
docker network create --internal --label fabricops.io/test=pkcs11 "$network" > /dev/null
for scenario in enroll peer peer wrong-pin missing-module missing-token missing-key; do
  docker run --rm --name "$container" --network "$network" --tmpfs /run/fabricops:rw,noexec,nosuid,size=1m,mode=0700 \
    --mount "type=volume,source=$volume,target=/var/lib/fabricops-hsm" \
    "$image" "$scenario"
done
echo 'PKCS11 image smoke passed; this does not enable the operator backend or prove hardware isolation'
