#!/bin/sh
set +x
set -eu
umask 077

fail() { echo "PKCS11 peer enrollment: $1" >&2; exit 1; }
: "${FABRICOPS_CA_ADDRESS:?CA address is required}"
: "${FABRICOPS_CA_BOOTSTRAP_USER_PASS:?registrar credentials are required}"
: "${FABRICOPS_WORKLOAD_USERNAME:?peer username is required}"
: "${FABRICOPS_WORKLOAD_PASSWORD:?peer password is required}"
: "${FABRICOPS_PKCS11_PIN:?provider PIN is required}"
: "${FABRICOPS_PKCS11_LIBRARY:?provider module is required}"
: "${FABRICOPS_PKCS11_TOKEN_LABEL:?provider token is required}"
[ "${FABRICOPS_WORKLOAD_TYPE:-}" = peer ] || fail 'only peer MSP enrollment is supported'
case "${FABRICOPS_TLS_ENABLED:-}" in true|false) ;; *) fail 'TLS mode must be explicit' ;; esac
if [ "$FABRICOPS_TLS_ENABLED" = true ]; then
  : "${FABRICOPS_WORKLOAD_CSR_HOSTS:?TLS CSR hosts are required}"
fi
[ -r "$FABRICOPS_PKCS11_LIBRARY" ] || fail 'provider module is unavailable'

work=${FABRICOPS_ENROLLMENT_DIR:-/fabricops/enrollment}
config_dir=${FABRICOPS_PKCS11_CONFIG_DIR:-/run/fabricops}
mkdir -p "$work" "$config_dir"
[ "$(stat -f -c %T "$work")" = tmpfs ] && [ "$(stat -f -c %T "$config_dir")" = tmpfs ] \
  || fail 'working material and provider configuration require memory-backed storage'
[ ! -e "$work/output" ] && [ ! -e "$work/bootstrap" ] && [ ! -e "$work/peer-msp" ] \
  || fail 'refusing existing enrollment material; explicit recovery is required'
export FABRIC_CA_CLIENT_HOME="$work/client"
export FABRIC_LOGGING_SPEC=warning FABRIC_CA_CLIENT_DEBUG=false
mkdir -p "$work/bootstrap" "$work/peer-msp"
fabricops-pkcs11-config client "$config_dir/client-pkcs11.yaml"
cat > "$config_dir/client-sw.yaml" <<'SOFTWARE'
bccsp:
  default: SW
  sw:
    hash: SHA2
    security: 256
debug: false
loglevel: warning
SOFTWARE

sw_client() {
  stage=$1
  subcommand=$2
  shift 2
  if ! FABRIC_CA_CLIENT_BCCSP_DEFAULT=SW fabric-ca-client "$subcommand" --config "$config_dir/client-sw.yaml" \
    --loglevel warning "$@" > "$config_dir/$stage.log" 2>&1; then
    fail "$stage failed; existing identities require explicit recovery"
  fi
}
copy_one() {
  source_dir=$1
  destination=$2
  set -- "$source_dir/"*
  [ "$#" = 1 ] && [ -s "$1" ] || fail 'expected exactly one certificate or TLS key artifact'
  cp "$1" "$destination"
}

sw_client bootstrap enroll -u "http://${FABRICOPS_CA_BOOTSTRAP_USER_PASS}@${FABRICOPS_CA_ADDRESS}" --mspdir "$work/bootstrap/msp"
# Do not treat an already-registered identity as permission to generate a new key.
sw_client register register --url "http://${FABRICOPS_CA_ADDRESS}" --mspdir "$work/bootstrap/msp" \
  --id.name "$FABRICOPS_WORKLOAD_USERNAME" --id.secret "$FABRICOPS_WORKLOAD_PASSWORD" --id.type peer
if ! FABRIC_CA_CLIENT_BCCSP_DEFAULT=PKCS11 fabric-ca-client enroll --config "$config_dir/client-pkcs11.yaml" \
  --loglevel warning -u "http://${FABRICOPS_WORKLOAD_USERNAME}:${FABRICOPS_WORKLOAD_PASSWORD}@${FABRICOPS_CA_ADDRESS}" \
  --csr.keyrequest.algo ecdsa --csr.keyrequest.size 256 --mspdir "$work/peer-msp/msp" > "$config_dir/msp.log" 2>&1; then
  fail 'token-backed MSP enrollment failed; software fallback is disabled'
fi
[ -z "$(find "$work/peer-msp/msp/keystore" -type f -print -quit)" ] \
  || fail 'provider enrollment produced a software MSP key'

# Assemble privately, then expose output only after all enrollment stages pass.
mkdir -p "$work/staged/msp"
copy_one "$work/peer-msp/msp/cacerts" "$work/staged/msp/cacert.pem"
copy_one "$work/peer-msp/msp/signcerts" "$work/staged/msp/signcert.pem"
cat > "$work/staged/msp/config.yaml" <<'MSP'
NodeOUs:
  Enable: true
  ClientOUIdentifier:
    Certificate: cacerts/ca.pem
    OrganizationalUnitIdentifier: client
  PeerOUIdentifier:
    Certificate: cacerts/ca.pem
    OrganizationalUnitIdentifier: peer
  AdminOUIdentifier:
    Certificate: cacerts/ca.pem
    OrganizationalUnitIdentifier: admin
  OrdererOUIdentifier:
    Certificate: cacerts/ca.pem
    OrganizationalUnitIdentifier: orderer
MSP

if [ "$FABRICOPS_TLS_ENABLED" = true ]; then
  sw_client tls enroll -u "http://${FABRICOPS_WORKLOAD_USERNAME}:${FABRICOPS_WORKLOAD_PASSWORD}@${FABRICOPS_CA_ADDRESS}" \
    --enrollment.profile tls --csr.hosts "$FABRICOPS_WORKLOAD_CSR_HOSTS" --mspdir "$work/peer-tls/tls"
  mkdir -p "$work/staged/tls"
  copy_one "$work/peer-tls/tls/tlscacerts" "$work/staged/msp/tlscacert.pem"
  cp "$work/staged/msp/tlscacert.pem" "$work/staged/tls/ca.crt"
  copy_one "$work/peer-tls/tls/signcerts" "$work/staged/tls/server.crt"
  copy_one "$work/peer-tls/tls/keystore" "$work/staged/tls/server.key"
fi
if grep -Eq 'PRIVATE KEY-----' "$work/staged/msp/"*; then fail 'private key PEM found in public MSP output'; fi
# A future publisher shares only output through the pod's explicit group, not
# registrar keys or provider configuration. No world-readable key material.
# Kubernetes owns an EmptyDir mount root even with fsGroup configured. Do not
# chmod that root as a non-root actor; require its existing group traversal.
if [ "$(stat -c %u "$work")" = "$(id -u)" ]; then
  chmod g+rx "$work"
else
  [ "$(stat -c %g "$work")" = "$(id -g)" ] \
    && [ "$((0$(stat -c %a "$work") & 0010))" -ne 0 ] \
    || fail 'working mount does not permit publisher group traversal'
fi
chmod -R g+rX "$work/staged"
mv "$work/staged" "$work/output"
echo 'PKCS11 peer enrollment output is ready; MSP signing key remains in the token'
