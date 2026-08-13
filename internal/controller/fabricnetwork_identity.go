/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

const (
	secretKindCABootstrap    = "ca-bootstrap"
	secretKindAdminEnroll    = "admin-enrollment"
	secretKindWorkloadEnroll = "workload-enrollment"
	secretKindAdminMSP       = "admin-msp"
	secretKindAdminTLS       = "admin-tls"

	caBootstrapUsername     = "admin"
	bootstrapPasswordLength = 32
)

func (r *FabricNetworkReconciler) reconcileIdentityMaterial(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) error {
	if err := r.ensureCABootstrapSecret(ctx, net, org, namespace); err != nil {
		return err
	}
	if err := r.ensureAdminEnrollmentSecret(ctx, net, org, namespace); err != nil {
		return err
	}

	for _, group := range org.Orderers {
		for i := 0; i < group.Instances; i++ {
			name := sanitizeName(fmt.Sprintf("%s%d", group.Prefix, i))
			if err := r.ensureEnrollmentCredentialSecret(ctx, net, org, namespace, name, componentOrderer, secretKindWorkloadEnroll); err != nil {
				return err
			}
		}
	}

	if org.Peer == nil {
		return nil
	}

	for i := 0; i < org.Peer.Instances; i++ {
		name := sanitizeName(fmt.Sprintf("%s%d", org.Peer.Prefix, i))
		if err := r.ensureEnrollmentCredentialSecret(ctx, net, org, namespace, name, componentPeer, secretKindWorkloadEnroll); err != nil {
			return err
		}
	}

	return nil
}

func caBootstrapSecretName(org fabricopsv1alpha1.Org) string {
	return sanitizeName(org.Organization.Name + "-ca-bootstrap")
}

func adminIdentityName(org fabricopsv1alpha1.Org) string {
	return sanitizeName(org.Organization.Name + "-admin")
}

func adminEnrollmentSecretName(org fabricopsv1alpha1.Org) string {
	return identityEnrollmentSecretName(adminIdentityName(org))
}

func identityEnrollmentSecretName(identityName string) string {
	return sanitizeName(identityName + "-enrollment")
}

func (r *FabricNetworkReconciler) ensureCABootstrapSecret(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) error {
	desired, err := buildCABootstrapSecret(net, org, namespace)
	if err != nil {
		return err
	}

	err = r.ensureSecret(ctx, desired, func(secret corev1.Secret) string {
		return identitySecretValidationError(secret, secretKindCABootstrap, net.Spec.Global.TLS)
	})
	return err
}

func buildCABootstrapSecret(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) (*corev1.Secret, error) {
	password, err := generateBootstrapPassword()
	if err != nil {
		return nil, err
	}

	name := caBootstrapSecretName(org)
	userPass := caBootstrapUsername + ":" + password

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: identityLabels(net, org, componentCA, name, map[string]string{
				labelIdentityKind: secretKindCABootstrap,
			}),
			Annotations: resourceAnnotations(net, org),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			caBootstrapUsernameKey: []byte(caBootstrapUsername),
			caBootstrapPasswordKey: []byte(password),
			caBootstrapUserPassKey: []byte(userPass),
		},
	}, nil
}

func (r *FabricNetworkReconciler) ensureAdminEnrollmentSecret(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) error {
	return r.ensureEnrollmentCredentialSecret(ctx, net, org, namespace, adminIdentityName(org), componentAdmin, secretKindAdminEnroll)
}

func (r *FabricNetworkReconciler) ensureEnrollmentCredentialSecret(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	identityName string,
	component string,
	kind string,
) error {
	desired, err := buildEnrollmentCredentialSecret(net, org, namespace, identityName, component, kind)
	if err != nil {
		return err
	}

	err = r.ensureSecret(ctx, desired, func(secret corev1.Secret) string {
		return identitySecretValidationError(secret, kind, net.Spec.Global.TLS)
	})
	return err
}

func buildEnrollmentCredentialSecret(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	identityName string,
	component string,
	kind string,
) (*corev1.Secret, error) {
	password, err := generateBootstrapPassword()
	if err != nil {
		return nil, err
	}

	name := identityEnrollmentSecretName(identityName)
	userPass := identityName + ":" + password

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: identityLabels(net, org, component, identityName, map[string]string{
				labelIdentityKind: kind,
			}),
			Annotations: resourceAnnotations(net, org),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			caBootstrapUsernameKey: []byte(identityName),
			caBootstrapPasswordKey: []byte(password),
			caBootstrapUserPassKey: []byte(userPass),
		},
	}, nil
}

func (r *FabricNetworkReconciler) ensureSecret(
	ctx context.Context,
	desired *corev1.Secret,
	validationError func(corev1.Secret) string,
) error {
	var existing corev1.Secret
	key := client.ObjectKeyFromObject(desired)

	err := r.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		log := logf.FromContext(ctx)
		log.Info("Creating Secret", "name", desired.Name, "namespace", desired.Namespace)
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	if err := r.updateObjectWithRetry(ctx, desired, func(object client.Object) (bool, error) {
		existing := object.(*corev1.Secret)
		changed := false
		if existing.Labels == nil {
			existing.Labels = map[string]string{}
			changed = true
		}
		for key, value := range desired.Labels {
			if existing.Labels[key] != value {
				existing.Labels[key] = value
				changed = true
			}
		}
		if mergeAnnotations(&existing.Annotations, desired.Annotations) {
			changed = true
		}

		if validationError != nil {
			if reason := validationError(*existing); reason != "" {
				existing.Data = desired.Data
				existing.Type = desired.Type
				changed = true
			}
		}

		if !changed {
			return false, nil
		}

		log := logf.FromContext(ctx)
		log.Info("Updating Secret", "name", existing.Name, "namespace", existing.Namespace)
		return true, nil
	}); err != nil {
		return err
	}

	return nil
}

func identityLabels(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	component string,
	workloadName string,
	extra map[string]string,
) map[string]string {
	labels := orgLabels(net, org, component)
	labels[labelWorkload] = workloadName

	maps.Copy(labels, extra)

	return labels
}

func generateBootstrapPassword() (string, error) {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	out := make([]byte, bootstrapPasswordLength)
	for i := range out {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		if err != nil {
			return "", err
		}
		out[i] = alphabet[n.Int64()]
	}
	return string(out), nil
}

func workloadDNSNames(name, namespace string) []string {
	return []string{
		name,
		fmt.Sprintf("%s.%s", name, namespace),
		fmt.Sprintf("%s.%s.svc", name, namespace),
		fmt.Sprintf("%s.%s.svc.cluster.local", name, namespace),
		"localhost",
	}
}

func identitySecretValidationError(secret corev1.Secret, kind string, tlsEnabled bool) string {
	switch kind {
	case secretKindCABootstrap:
		return caBootstrapSecretValidationError(secret)
	case secretKindAdminEnroll:
		return enrollmentCredentialSecretValidationError(secret)
	case secretKindWorkloadEnroll:
		return enrollmentCredentialSecretValidationError(secret)
	case secretKindMSP:
		return mspIdentitySecretValidationError(secret, tlsEnabled)
	case secretKindTLS:
		return tlsIdentitySecretValidationError(secret)
	case secretKindAdminMSP:
		return mspIdentitySecretValidationError(secret, tlsEnabled)
	case secretKindAdminTLS:
		return adminTLSIdentitySecretValidationError(secret)
	default:
		return ""
	}
}

func caBootstrapSecretValidationError(secret corev1.Secret) string {
	return enrollmentCredentialSecretValidationError(secret)
}

func enrollmentCredentialSecretValidationError(secret corev1.Secret) string {
	missing := missingSecretKeys(secret, caBootstrapSecretKeys())
	if len(missing) > 0 {
		return "missing keys: " + strings.Join(missing, ",")
	}

	username := strings.TrimSpace(string(secret.Data[caBootstrapUsernameKey]))
	password := strings.TrimSpace(string(secret.Data[caBootstrapPasswordKey]))
	userPass := strings.TrimSpace(string(secret.Data[caBootstrapUserPassKey]))
	if username == "" {
		return "empty enrollment username"
	}
	if password == "" {
		return "empty enrollment password"
	}
	if userPass != username+":"+password {
		return "enrollment user-pass does not match username/password"
	}

	return ""
}

func mspIdentitySecretValidationError(secret corev1.Secret, tlsEnabled bool) string {
	missing := missingSecretKeys(secret, mspSecretKeys(tlsEnabled))
	if len(missing) > 0 {
		return "missing keys: " + strings.Join(missing, ",")
	}

	if strings.TrimSpace(string(secret.Data[mspConfigKey])) == "" {
		return "empty MSP config"
	}
	if _, err := parsePEMCertificate(secret.Data[mspCACertKey]); err != nil {
		return "invalid MSP CA certificate"
	}
	if tlsEnabled {
		if _, err := parsePEMCertificate(secret.Data[mspTLSCACertKey]); err != nil {
			return "invalid TLS CA certificate"
		}
	}
	signCert, err := parsePEMCertificate(secret.Data[mspSignCertKey])
	if err != nil {
		return "invalid signing certificate"
	}
	if len(signCert.ExtKeyUsage) > 0 {
		return "signing certificate has incompatible extended key usage"
	}
	if err := parsePEMPrivateKey(secret.Data[mspKeyStoreKey]); err != nil {
		return "invalid private key"
	}

	return ""
}

func tlsIdentitySecretValidationError(secret corev1.Secret) string {
	missing := missingSecretKeys(secret, tlsSecretKeys())
	if len(missing) > 0 {
		return "missing keys: " + strings.Join(missing, ",")
	}

	if _, err := parsePEMCertificate(secret.Data[tlsCACertKey]); err != nil {
		return "invalid TLS CA certificate"
	}
	if _, err := parsePEMCertificate(secret.Data[tlsServerCertKey]); err != nil {
		return "invalid TLS server certificate"
	}
	if err := parsePEMPrivateKey(secret.Data[tlsServerKeyKey]); err != nil {
		return "invalid TLS server key"
	}

	return ""
}

func adminTLSIdentitySecretValidationError(secret corev1.Secret) string {
	missing := missingSecretKeys(secret, []string{
		tlsCACertKey,
		tlsClientCertKey,
		tlsClientKeyKey,
	})
	if len(missing) > 0 {
		return "missing keys: " + strings.Join(missing, ",")
	}

	if _, err := parsePEMCertificate(secret.Data[tlsCACertKey]); err != nil {
		return "invalid TLS CA certificate"
	}
	cert, err := parsePEMCertificate(secret.Data[tlsClientCertKey])
	if err != nil {
		return "invalid TLS client certificate"
	}
	if !hasExtKeyUsage(cert, x509.ExtKeyUsageClientAuth) {
		return "TLS client certificate missing client auth usage"
	}
	if err := parsePEMPrivateKey(secret.Data[tlsClientKeyKey]); err != nil {
		return "invalid TLS client key"
	}

	return ""
}

func hasExtKeyUsage(cert *x509.Certificate, usage x509.ExtKeyUsage) bool {
	return slices.Contains(cert.ExtKeyUsage, usage)
}

func parsePEMCertificate(data []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("missing certificate PEM block")
	}

	return x509.ParseCertificate(block.Bytes)
}

func parsePEMPrivateKey(data []byte) error {
	block, _ := pem.Decode(data)
	if block == nil {
		return fmt.Errorf("missing private key PEM block")
	}

	var err error
	switch block.Type {
	case "EC PRIVATE KEY":
		_, err = x509.ParseECPrivateKey(block.Bytes)
	case "PRIVATE KEY":
		_, err = x509.ParsePKCS8PrivateKey(block.Bytes)
	case "RSA PRIVATE KEY":
		_, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	default:
		return fmt.Errorf("unsupported private key PEM block %q", block.Type)
	}
	return err
}

func mspConfigYAML() string {
	return `NodeOUs:
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
`
}
