#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob
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
  for log in "$state"/*.log /run/fabricops/provider/*.log; do
    test -f "$log" || continue
    grep -Ei 'error|fatal|panic|bccsp|GetKey|PKCS11 peer enrollment|failed|required' "$log" \
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
  test -z "$(find "${1:-$state/peer/msp}/keystore" -type f -print -quit)" \
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

    export FABRICOPS_ENROLLMENT_DIR=/run/fabricops/enrollment
    export FABRICOPS_PKCS11_CONFIG_DIR=/run/fabricops/provider
    export FABRICOPS_PKCS11_LIBRARY="$module" FABRICOPS_PKCS11_TOKEN_LABEL="$token" FABRICOPS_PKCS11_PIN="$pin"
    export FABRICOPS_CA_ADDRESS=127.0.0.1:7054 FABRICOPS_CA_BOOTSTRAP_USER_PASS=admin:adminpw
    export FABRICOPS_WORKLOAD_USERNAME=peer0 FABRICOPS_WORKLOAD_PASSWORD=peer0pw FABRICOPS_WORKLOAD_TYPE=peer
    export FABRICOPS_WORKLOAD_CSR_HOSTS=localhost,127.0.0.1 FABRICOPS_TLS_ENABLED=true
    fabricops-enroll-peer > "$state/enroll.log" 2>&1
    output="$FABRICOPS_ENROLLMENT_DIR/output"
    test ! -e "$output/msp/keystore.pem" || fail "enrollment exported an MSP key"
    test -n "$(find "$FABRICOPS_ENROLLMENT_DIR/bootstrap/msp/keystore" -type f -print -quit)" || fail "registrar did not retain software signing"
    mkdir -p "$state/peer/msp/cacerts" "$state/peer/msp/signcerts" "$state/peer/msp/tlscacerts" "$state/tls"
    cp "$output/msp/config.yaml" "$state/peer/msp/config.yaml"
    cp "$output/msp/cacert.pem" "$state/peer/msp/cacerts/ca.pem"
    cp "$output/msp/signcert.pem" "$state/peer/msp/signcerts/cert.pem"
    cp "$output/msp/tlscacert.pem" "$state/peer/msp/tlscacerts/tlsca.pem"
    cp "$output/tls/"* "$state/tls/"
    openssl pkey -in "$state/tls/server.key" -noout > /dev/null 2>&1
    tls_cert_hash=$(openssl x509 -in "$state/tls/server.crt" -noout -pubkey | openssl pkey -pubin -outform DER | openssl dgst -sha256)
    tls_key_hash=$(openssl pkey -in "$state/tls/server.key" -pubout -outform DER | openssl dgst -sha256)
    test "$tls_cert_hash" = "$tls_key_hash" || fail "TLS certificate and software key do not match"
    test "$(stat -c %a "$FABRICOPS_PKCS11_CONFIG_DIR/client-pkcs11.yaml")" = 600 || fail "provider config permissions are too broad"
    test "$(stat -c %a "$output/tls/server.key")" = 640 || fail "published TLS key permissions are too broad"
    no_msp_key
    pkcs11-tool --module "$module" --token-label "$token" --login --pin "$pin" \
      --list-objects --type privkey > "$state/objects.log" 2>&1
    grep -q 'Private Key Object' "$state/objects.log" || fail "token contains no private key"
    grep -q 'never extractable' "$state/objects.log" || fail "private key is not marked never extractable"
    grep -q 'sensitive' "$state/objects.log" || fail "private key is not marked sensitive"
    test "$(grep -c 'Private Key Object' "$state/objects.log")" = 1 || fail "registrar or TLS operations used the peer token"
    if fabricops-enroll-peer > "$state/repeat.log" 2>&1; then fail "existing enrollment was silently replaced"; fi
    grep -q 'existing' "$state/repeat.log" || fail "repeat enrollment failed for an unrelated reason"
    if FABRICOPS_ENROLLMENT_DIR=/run/fabricops/recovery FABRICOPS_PKCS11_CONFIG_DIR=/run/fabricops/recovery-config \
      fabricops-enroll-peer > "$state/recovery.log" 2>&1; then fail "registered identity was silently reenrolled"; fi
    grep -q 'register failed' "$state/recovery.log" || fail "registered identity rejection failed for an unrelated reason"
    for rejection in wrong-pin missing-module missing-token non-memory missing-pin missing-hosts; do
      helper_work="/run/fabricops/reject-$rejection"
      helper_config="/run/fabricops/config-$rejection"
      helper_module="$module"; helper_token="$token"; helper_pin="$pin"; helper_hosts=localhost,127.0.0.1
      case "$rejection" in
        wrong-pin) helper_pin=deliberately-invalid; expected='token-backed MSP enrollment failed' ;;
        missing-module) helper_module=/nonexistent/provider.so; expected='provider module is unavailable' ;;
        missing-token) helper_token=nonexistent-token; expected='token-backed MSP enrollment failed' ;;
        non-memory) helper_config="$state/rejected-config"; expected='require memory-backed storage' ;;
        missing-pin) helper_pin=; expected='provider PIN is required' ;;
        missing-hosts) helper_hosts=; expected='TLS CSR hosts are required' ;;
      esac
      if FABRICOPS_ENROLLMENT_DIR="$helper_work" FABRICOPS_PKCS11_CONFIG_DIR="$helper_config" \
        FABRICOPS_WORKLOAD_USERNAME="peer-$rejection" FABRICOPS_WORKLOAD_CSR_HOSTS="$helper_hosts" \
        FABRICOPS_PKCS11_LIBRARY="$helper_module" FABRICOPS_PKCS11_TOKEN_LABEL="$helper_token" FABRICOPS_PKCS11_PIN="$helper_pin" \
        fabricops-enroll-peer > "$state/enroll-$rejection.log" 2>&1; then fail "enrollment accepted $rejection"; fi
      grep -q "$expected" "$state/enroll-$rejection.log" || fail "enrollment $rejection failed for an unrelated reason"
      test ! -e "$helper_work/output" || fail "failed enrollment exposed publishable output"
    done
    pkcs11-tool --module "$module" --token-label "$token" --login --pin "$pin" --list-objects --type privkey > "$state/objects-after.log" 2>&1
    test "$(grep -c 'Private Key Object' "$state/objects-after.log")" = 1 || fail "rejected enrollment generated additional token keys"
    FABRICOPS_ENROLLMENT_DIR=/run/fabricops/second FABRICOPS_PKCS11_CONFIG_DIR=/run/fabricops/second-config \
      FABRICOPS_WORKLOAD_USERNAME=peer1 FABRICOPS_TLS_ENABLED=false FABRICOPS_WORKLOAD_CSR_HOSTS='' \
      fabricops-enroll-peer > "$state/second.log" 2>&1
    second=/run/fabricops/second/output
    test ! -e "$second/tls" && test ! -e "$second/msp/tlscacert.pem" || fail "TLS-disabled enrollment created TLS output"
    mkdir -p "$state/peer1/msp/cacerts" "$state/peer1/msp/signcerts" "$state/peer1/msp/keystore"
    cp "$second/msp/config.yaml" "$state/peer1/msp/config.yaml"
    cp "$second/msp/cacert.pem" "$state/peer1/msp/cacerts/ca.pem"
    cp "$second/msp/signcert.pem" "$state/peer1/msp/signcerts/cert.pem"
    no_msp_key "$state/peer1/msp"
    pkcs11-tool --module "$module" --token-label "$token" --login --pin "$pin" --list-objects --type privkey > "$state/two-peers.log" 2>&1
    test "$(grep -c 'Private Key Object' "$state/two-peers.log")" = 2 || fail "peers did not receive distinct token keys"
    echo 'PASS: enrollment rejects provider errors, non-memory storage, missing inputs, and implicit reenrollment'
    echo 'PASS: two peers enroll distinct token keys with independently scoped software TLS'
    echo 'PASS: CA enrollment uses a sensitive, non-extractable token key; MSP keystore is empty'
    ;;
  peer|peer-second|wrong-pin|missing-module|missing-token|missing-key)
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
    export FABRICOPS_PKCS11_LIBRARY="$module" FABRICOPS_PKCS11_TOKEN_LABEL="$token" FABRICOPS_PKCS11_PIN="$pin"
    export CORE_PEER_MSPCONFIGPATH="$state/peer/msp" CORE_PEER_LOCALMSPID=Org1MSP
    export CORE_PEER_FILESYSTEMPATH="$state/ledger" CORE_PEER_ID=peer0
    if [ "$1" = peer-second ]; then
      export CORE_PEER_MSPCONFIGPATH="$state/peer1/msp" CORE_PEER_FILESYSTEMPATH="$state/ledger-peer1" CORE_PEER_ID=peer1
    fi
    export CORE_PEER_ADDRESS=127.0.0.1:7051 CORE_PEER_LISTENADDRESS=127.0.0.1:7051
    export CORE_PEER_CHAINCODELISTENADDRESS=127.0.0.1:7052 CORE_PEER_CHAINCODEADDRESS=127.0.0.1:7052
    export CORE_PEER_TLS_ENABLED=false CORE_OPERATIONS_LISTENADDRESS=127.0.0.1:9443
    export CORE_OPERATIONS_TLS_ENABLED=false CORE_VM_ENDPOINT=
    # Upstream provider debug logging includes the PIN; never enable it here.
    export FABRIC_LOGGING_SPEC=warning
    fabricops-pkcs11-config peer /etc/hyperledger/fabric/core.yaml /run/fabricops/core.yaml
    export FABRIC_CFG_PATH=/run/fabricops
    peer node start > "$state/$1.log" 2>&1 &
    pid=$!
    ready=false
    for _ in $(seq 1 45); do
      if curl -fsS http://127.0.0.1:9443/healthz > /dev/null 2>&1; then ready=true; break; fi
      kill -0 "$pid" 2>/dev/null || break
      sleep 1
    done
    no_msp_key "$CORE_PEER_MSPCONFIGPATH"
    if [ "$1" = peer ] || [ "$1" = peer-second ]; then
      test "$ready" = true || fail "PKCS11 peer failed readiness"
      FABRIC_LOGGING_SPEC=debug fabricops-pkcs11-ready > "$state/readiness.log" 2>&1 || fail 'provider-aware readiness failed'
      if FABRICOPS_PKCS11_TOKEN_LABEL=nonexistent-token fabricops-pkcs11-ready > "$state/readiness-failure.log" 2>&1; then
        fail 'readiness accepted a missing token while the peer was running'
      fi
      grep -qx 'PKCS11 peer is not ready' "$state/readiness-failure.log" || fail 'readiness exposed provider diagnostics'
      fabricops-pkcs11-ready || fail 'readiness did not recover'
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
  *) fail 'expected enroll, peer, peer-second, wrong-pin, missing-module, missing-token, or missing-key' ;;
esac
logs=("$state"/*.log /run/fabricops/*/*.log)
if grep -Fq -f "$state/pin" -f "$state/so-pin" "${logs[@]}"; then
  fail 'provider PIN appeared in diagnostic logs'
fi
