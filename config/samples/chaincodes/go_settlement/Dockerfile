FROM golang:1.25-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/settlement-chaincode .

FROM alpine:3.21
LABEL org.opencontainers.image.source="https://github.com/LF-Decentralized-Trust-labs/FabricOps" \
      org.opencontainers.image.description="FabricOps Go settlement CCaaS sample chaincode" \
      org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /chaincode
COPY --from=build /out/settlement-chaincode /chaincode/settlement-chaincode

EXPOSE 7052
ENV CHAINCODE_SERVER_ADDRESS=0.0.0.0:7052

ENTRYPOINT ["/chaincode/settlement-chaincode"]
