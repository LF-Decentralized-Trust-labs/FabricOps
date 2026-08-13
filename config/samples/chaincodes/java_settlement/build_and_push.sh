#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
IMAGE=${IMAGE:-ghcr.io/lf-decentralized-trust-labs/fabricops-java-settlement:0.2.0}
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
  "$SCRIPT_DIR"
