#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
CHAINCODE_DIR="${CHAINCODE_DIR:-$SCRIPT_DIR}"

IMAGE=${IMAGE:-ghcr.io/dpereowei/fabricops-node-settlement:0.1.3}
PLATFORM=${PLATFORM:-linux/amd64}
PUSH=${PUSH:-false}

OUTPUT_FLAG=--load
if [ "$PUSH" = "true" ]; then
  OUTPUT_FLAG=--push
fi

docker buildx build \
  --platform "$PLATFORM" \
  --tag "$IMAGE" \
  "$OUTPUT_FLAG" \
  "$CHAINCODE_DIR"