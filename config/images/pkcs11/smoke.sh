#!/usr/bin/env bash
set -euo pipefail
umask 077

state=/var/lib/fabricops-hsm
module=/usr/lib/softhsm/libsofthsm2.so
token=fabricops-peers
pid=
cleanup() {
  if [ -n "$pid" ]; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

report_failure() {
  echo "PKCS11 smoke command failed at line $1" >&2
  local pin_value so_pin_value log
  pin_value=$(cat "$state/pin" 2>/dev/null || printf 'unset-pin')
  so_pin_value=$(cat "$state/so-pin" 2>/dev/null || printf 'unset-so-pin')
  for log in "$state"/*.log; do
    test -f "$log" || continue
    grep -Ei 'error|fatal|panic|bccsp|GetKey' "$log" \
      | sed -e "s/$pin_value/[REDACTED]/g" -e "s/$so_pin_value/[REDACTED]/g" \
        -e 's/adminpw/[REDACTED]/g' -e 's/peer0pw/[REDACTED]/g' \
      | tail -n 8 >&2 || true
  done
}
trap 'report_failure "$LINENO"' ERR

fail() {
  echo "PKCS11 smoke failed: $1" >&2
  report_failure "${BASH_LINENO[0]}"
  exit 1
}
no_msp_key() {
  test -z "$(find "$state/peer/msp/keystore" -type f -print -quit)" \
    || fail "peer MSP contains a software private key"
}

case "${1:-}" in
  enroll)
    mkdir -p "$state/tokens" "$state/peer/msp/keystore"
    printf 'directories.tokendir = %s/tokens\nobjectstore.backend = file\nlog.level = ERROR\n' "$state" > "$SOFTHSM2_CONF"
    openssl rand -hex 16 > "$state/pin"
    openssl rand -hex 16 > "$state/so-pin"
    pin=$(cat "$state/pin")
    softhsm2-util --init-token --free --label "$token" --pin "$pin" \
      --so-pin "$(cat "$state/so-pin")" > "$state/token-init.log" 2>&1

    fabric-ca-server start -b admin:adminpw --home "$state/ca" > "$state/ca.log" 2>&1 &
    pid=$!
    ready=false
    for _ in $(seq 1 60); do
      if curl -fsS http://127.0.0.1:7054/cainfo > /dev/null 2>&1; then ready=true; break; fi
      kill -0 "$pid" 2>/dev/null || fail "test CA exited before readiness"
      sleep 1
    done
    test "$ready" = true || fail "test CA readiness timed out"

    export FABRIC_CA_CLIENT_HOME="$state/client"
    fabric-ca-client enroll -u http://admin:adminpw@127.0.0.1:7054 --mspdir "$state/admin/msp" > "$state/admin.log" 2>&1
    fabric-ca-client register --url http://127.0.0.1:7054 --mspdir "$state/admin/msp" \
      --id.name peer0 --id.secret peer0pw --id.type peer > "$state/register.log" 2>&1
    cat > "$state/hsm-client.yaml" <<'PROVIDER'
bccsp:
  default: PKCS11
  pkcs11:
    Library: /usr/lib/softhsm/libsofthsm2.so
    Label: fabricops-peers
    Pin: ""
    Hash: SHA2
    Security: 256
PROVIDER
    FABRIC_CA_CLIENT_BCCSP_DEFAULT=PKCS11 \
    FABRIC_CA_CLIENT_BCCSP_PKCS11_LIBRARY="$module" \
    FABRIC_CA_CLIENT_BCCSP_PKCS11_LABEL="$token" \
    FABRIC_CA_CLIENT_BCCSP_PKCS11_PIN="$pin" \
      fabric-ca-client enroll --config "$state/hsm-client.yaml" \
        -u http://peer0:peer0pw@127.0.0.1:7054 --mspdir "$state/peer/msp" > "$state/enroll.log" 2>&1
    cat > "$state/peer/msp/config.yaml" <<'MSP'
NodeOUs:
  Enable: true
  ClientOUIdentifier:
    OrganizationalUnitIdentifier: client
  PeerOUIdentifier:
    OrganizationalUnitIdentifier: peer
  AdminOUIdentifier:
    OrganizationalUnitIdentifier: admin
  OrdererOUIdentifier:
    OrganizationalUnitIdentifier: orderer
MSP
    no_msp_key
    pkcs11-tool --module "$module" --token-label "$token" --login --pin "$pin" \
      --list-objects --type privkey > "$state/objects.log" 2>&1
    grep -q 'Private Key Object' "$state/objects.log" || fail "token contains no private key"
    grep -q 'never extractable' "$state/objects.log" || fail "private key is not marked never extractable"
    grep -q 'sensitive' "$state/objects.log" || fail "private key is not marked sensitive"
    echo 'PASS: CA enrollment uses a sensitive, non-extractable token key; MSP keystore is empty'
    ;;
  peer|wrong-pin|missing-module|missing-token|missing-key)
    pin=$(cat "$state/pin")
    expected=
    case "$1" in
      wrong-pin) pin=deliberately-invalid; expected='CKR_PIN_INCORRECT|CKR_PIN_LOCKED' ;;
      missing-module) module=/nonexistent/provider.so; expected='library|provider.so' ;;
      missing-token) token=nonexistent-token; expected='could not find token with label' ;;
      missing-key)
        expected='KeyMaterial not found|private key.*not found|key.*not found'
        token=empty-token
        softhsm2-util --init-token --free --label "$token" --pin "$pin" \
          --so-pin "$(cat "$state/so-pin")" > "$state/empty-token.log" 2>&1
        ;;
    esac
    export CORE_PEER_BCCSP_DEFAULT=PKCS11 CORE_PEER_BCCSP_PKCS11_LIBRARY="$module"
    export CORE_PEER_BCCSP_PKCS11_LABEL="$token" CORE_PEER_BCCSP_PKCS11_PIN="$pin"
    export CORE_PEER_BCCSP_PKCS11_HASH=SHA2 CORE_PEER_BCCSP_PKCS11_SECURITY=256
    export CORE_PEER_MSPCONFIGPATH="$state/peer/msp" CORE_PEER_LOCALMSPID=Org1MSP
    export CORE_PEER_FILESYSTEMPATH="$state/ledger" CORE_PEER_ID=peer0
    export CORE_PEER_ADDRESS=127.0.0.1:7051 CORE_PEER_LISTENADDRESS=127.0.0.1:7051
    export CORE_PEER_CHAINCODELISTENADDRESS=127.0.0.1:7052 CORE_PEER_CHAINCODEADDRESS=127.0.0.1:7052
    export CORE_PEER_TLS_ENABLED=false CORE_OPERATIONS_LISTENADDRESS=127.0.0.1:9443
    export CORE_OPERATIONS_TLS_ENABLED=false CORE_VM_ENDPOINT=
    # Upstream provider debug logging includes the PIN; never enable it here.
    export FABRIC_LOGGING_SPEC=warning
    fabricops-peer-config /etc/hyperledger/fabric/core.yaml /run/fabricops/core.yaml
    export FABRIC_CFG_PATH=/run/fabricops
    peer node start > "$state/$1.log" 2>&1 &
    pid=$!
    ready=false
    for _ in $(seq 1 45); do
      if curl -fsS http://127.0.0.1:9443/healthz > /dev/null 2>&1; then ready=true; break; fi
      kill -0 "$pid" 2>/dev/null || break
      sleep 1
    done
    no_msp_key
    if [ "$1" = peer ]; then
      test "$ready" = true || fail "PKCS11 peer failed readiness"
      echo 'PASS: peer starts with certificate-bound token key and no MSP key file'
    else
      test "$ready" = false || fail "$1 silently reached readiness"
      if kill -0 "$pid" 2>/dev/null; then fail "$1 did not exit promptly"; fi
      if wait "$pid"; then fail "$1 exited successfully"; fi
      pid=
      grep -Eiq "$expected" "$state/$1.log" || fail "$1 failed for an unrelated reason"
      echo "PASS: $1 fails closed without an MSP key file"
    fi
    ;;
  *) fail 'expected enroll, peer, wrong-pin, missing-module, missing-token, or missing-key' ;;
esac
if grep -Fq -f "$state/pin" -f "$state/so-pin" "$state"/*.log; then
  fail 'provider PIN appeared in diagnostic logs'
fi
