FROM ubuntu:22.04

ARG FABRIC_VERSION=3.1.0
ARG TARGETARCH

ENV FABRIC_CFG_PATH=/etc/hyperledger/fabric

LABEL org.opencontainers.image.source="https://github.com/LF-Decentralized-Trust-labs/FabricOps" \
      org.opencontainers.image.description="FabricOps helper image with Hyperledger Fabric CLI binaries"

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates curl gzip jq tar \
    && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64|arm64) fabric_arch="${TARGETARCH}" ;; \
      *) echo "unsupported TARGETARCH ${TARGETARCH}; Fabric binary releases support linux-amd64 and linux-arm64" >&2; exit 1 ;; \
    esac; \
    curl -fsSL --show-error --retry 5 --retry-all-errors --connect-timeout 20 --max-time 600 -o /tmp/fabric.tgz \
      "https://github.com/hyperledger/fabric/releases/download/v${FABRIC_VERSION}/hyperledger-fabric-linux-${fabric_arch}-${FABRIC_VERSION}.tar.gz"; \
    tar -xzf /tmp/fabric.tgz -C /tmp; \
    mkdir -p "${FABRIC_CFG_PATH}"; \
    mv /tmp/bin/* /usr/local/bin/; \
    mv /tmp/config/* "${FABRIC_CFG_PATH}/"; \
    rm -rf /tmp/fabric.tgz /tmp/bin /tmp/config; \
    test -s "${FABRIC_CFG_PATH}/core.yaml"; \
    test -s "${FABRIC_CFG_PATH}/configtx.yaml"; \
    configtxgen --version; \
    peer version; \
    peer channel --help >/dev/null

ENTRYPOINT ["/bin/bash"]
