# Certificate Lifecycle

FabricOps inventories the certificate-bearing data in every managed MSP and TLS
Secret for a `FabricNetwork`. The status includes the Secret name, key, owning
workload, certificate subject/issuer, validity window, renewal time, state, and
renewal Job name when the operator starts renewal.

## What FabricOps Tracks

For each org admin, orderer, and peer, FabricOps tracks:

- MSP CA roots in `cacert.pem`
- MSP signing certificates in `signcert.pem`
- TLS CA roots in `tlscacert.pem` and `ca.crt` when TLS is enabled
- Admin TLS client certificates in `client.crt`
- Orderer and peer TLS server certificates in `server.crt`

Leaf certificates enrolled through Fabric CA are renewable by FabricOps. CA root
material is surfaced in status so operators can see expiry early, but root CA
rollover is a separate operational procedure and is not rewritten by a leaf
identity renewal Job.

## Renewal Behavior

FabricOps starts renewal when a renewable certificate is expired or enters the
default 30-day renewal window before `NotAfter`.

Renewal Jobs reuse the Fabric CA enrollment path and keep the same Secret names:

- `<org>-admin-msp`
- `<org>-admin-tls`
- `<workload>-msp`
- `<workload>-tls`

The Job name includes a digest of the certificate material that triggered the
renewal, for example `peer0-renew-2f3a0e19bd`. This lets a failed renewal remain
available for logs while a later certificate revision can produce a distinct
retry Job.

When a renewal Job publishes updated Secret data, FabricOps updates the orderer
or peer Deployment pod template with `fabricops.io/identity-revision`. The
annotation is based on the mounted MSP/TLS Secret contents, so Kubernetes rolls
the workload without recreating the network.

## Status Signals

`FabricNetwork.status.orgStatus[].certificates[]` contains the detailed
inventory. The top-level `CertificateLifecycleReady` condition reports:

- `True / CertificateLifecycleReady` when all tracked certificates are valid
- `False / CertificateRenewalRequired` when renewable certificates are inside
  the renewal window
- `False / CertificateRenewalRunning` while renewal Jobs are running
- `False / CertificateRenewalFailed` when a renewal Job failed
- `False / CertificateExpired`, `CertificateMaterialMissing`, or
  `CertificateMaterialInvalid` when material needs intervention

The plain `fabricopsctl status` output prints a compact per-org certificate
summary. Use JSON or YAML output for the full inventory:

```bash
fabricopsctl status -n default -o yaml fabricnetwork-sample
```

## Recovery Paths

If enrollment or renewal fails, inspect the retained Job:

```bash
kubectl logs -n <org-namespace> job/<renewal-job-name> --all-containers
kubectl describe job -n <org-namespace> <renewal-job-name>
```

After fixing the Fabric CA or credentials, delete the failed renewal Job. The
operator will recreate a renewal Job for the same still-expiring certificate
revision:

```bash
kubectl delete job -n <org-namespace> <renewal-job-name>
```

If a certificate is missing or malformed, confirm that the deterministic MSP/TLS
Secret still exists and is labeled as Fabric CA-managed:

```bash
kubectl get secret -n <org-namespace> <workload>-msp -o yaml
kubectl get secret -n <org-namespace> <workload>-tls -o yaml
```

If CA root material is near expiry or expired, plan a CA rollover and refresh the
affected enrollment output. FabricOps reports the condition, but it does not yet
perform root CA migration or channel MSP config updates automatically.

Fabric CA bootstrap registrar credential rotation is also intentionally separate
from leaf certificate renewal. Rotate the registrar only after validating the CA
database state and any admin enrollment Jobs that still depend on those
credentials.
