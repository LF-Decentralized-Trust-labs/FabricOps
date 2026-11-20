FROM golang:1.26.0-bookworm@sha256:2a0ba12e116687098780d3ce700f9ce3cb340783779646aafbabed748fa6677c AS source

# Fabric v3.1.0 and Fabric CA v1.5.15, matching the current sample/runtime line.
ARG FABRIC_REVISION=8f08391d840c1b105c6b62e8917687e81937695e
ARG FABRIC_CA_REVISION=256fdbeecf05c19b5cdf30e083d0e61d5ebd1c57

RUN mkdir -p /src/fabric /src/fabric-ca /out \
    && curl -fsSL --retry 5 "https://codeload.github.com/hyperledger/fabric/tar.gz/${FABRIC_REVISION}" \
       | tar -xz --strip-components=1 -C /src/fabric \
    && curl -fsSL --retry 5 "https://codeload.github.com/hyperledger/fabric-ca/tar.gz/${FABRIC_CA_REVISION}" \
       | tar -xz --strip-components=1 -C /src/fabric-ca

FROM source AS config-build
WORKDIR /src/fabric
COPY configure-provider.go /tmp/configure-provider.go
RUN go build -mod=vendor -buildvcs=false -o /out/fabricops-pkcs11-config /tmp/configure-provider.go

FROM source AS readiness-build
WORKDIR /src/fabric
COPY readiness.go readiness_test.go /tmp/
RUN go test -mod=vendor -tags pkcs11 /tmp/readiness.go /tmp/readiness_test.go \
    && CGO_ENABLED=1 go build -mod=vendor -tags pkcs11 -buildvcs=false -o /out/fabricops-pkcs11-ready /tmp/readiness.go

FROM source AS peer-build
WORKDIR /src/fabric
RUN CGO_ENABLED=1 go build -mod=vendor -tags pkcs11 -buildvcs=false -o /out/peer ./cmd/peer \
    && go version -m /out/peer | grep 'tags=pkcs11'

FROM source AS ca-build
WORKDIR /src/fabric-ca
RUN CGO_ENABLED=1 go build -mod=vendor -tags pkcs11 -buildvcs=false -o /out/fabric-ca-client ./cmd/fabric-ca-client \
    && CGO_ENABLED=1 go build -mod=vendor -buildvcs=false -o /out/fabric-ca-server ./cmd/fabric-ca-server \
    && go version -m /out/fabric-ca-client | grep 'tags=pkcs11'

FROM source AS ccaas-build
WORKDIR /src/fabric/ccaas_builder
RUN mkdir -p /out/ccaas \
    && go build -mod=readonly -buildvcs=false -o /out/ccaas/ ./cmd/detect ./cmd/build ./cmd/release

FROM source AS proof-build
WORKDIR /src/fabric
COPY endorsement-proof.go endorsement-proof_test.go /tmp/
RUN go test -mod=vendor /tmp/endorsement-proof.go /tmp/endorsement-proof_test.go \
    && go build -mod=vendor -buildvcs=false -o /out/fabricops-pkcs11-endorsement-proof /tmp/endorsement-proof.go \
    && go build -mod=vendor -buildvcs=false -o /out/configtxgen ./cmd/configtxgen

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates softhsm2 \
    && rm -rf /var/lib/apt/lists/*
ENV SOFTHSM2_CONF=/var/lib/fabricops-hsm/softhsm2.conf
LABEL org.opencontainers.image.source="https://github.com/LF-Decentralized-Trust-labs/FabricOps" \
      org.opencontainers.image.description="Experimental PKCS11 capability proof, not an enabled FabricOps backend"

FROM runtime AS enrollment
COPY --from=ca-build /out/fabric-ca-client /usr/local/bin/fabric-ca-client
COPY --from=config-build /out/fabricops-pkcs11-config /usr/local/bin/fabricops-pkcs11-config
COPY --chmod=0555 enroll-peer.sh /usr/local/bin/fabricops-enroll-peer
ENTRYPOINT ["/usr/local/bin/fabricops-enroll-peer"]

FROM runtime AS peer
ENV FABRIC_CFG_PATH=/etc/hyperledger/fabric
COPY --from=peer-build /out/peer /usr/local/bin/peer
COPY --from=readiness-build /out/fabricops-pkcs11-ready /usr/local/bin/fabricops-pkcs11-ready
COPY --from=config-build /out/fabricops-pkcs11-config /usr/local/bin/fabricops-pkcs11-config
COPY --from=peer-build /src/fabric/sampleconfig/core.yaml /etc/hyperledger/fabric/core.yaml
COPY --from=ccaas-build /out/ccaas/ /opt/hyperledger/ccaas_builder/bin/
ENTRYPOINT ["peer"]

FROM peer AS smoke
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl opensc openssl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=ca-build /out/fabric-ca-client /out/fabric-ca-server /usr/local/bin/
COPY --from=proof-build /out/fabricops-pkcs11-endorsement-proof /out/configtxgen /usr/local/bin/
COPY --chmod=0555 enroll-peer.sh /usr/local/bin/fabricops-enroll-peer
COPY smoke.sh /usr/local/lib/fabricops/pkcs11-smoke.sh
ENTRYPOINT ["bash", "/usr/local/lib/fabricops/pkcs11-smoke.sh"]
