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
	"reflect"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestChannelPublicArtifactsDoNotCopyPrivateKeys(t *testing.T) {
	for _, consensus := range []string{"raft", "bft"} {
		t.Run(consensus, func(t *testing.T) {
			net, channel, _, _, orderer := channelConfigOperationFixture()
			net.UID = "owner"
			net.Spec.Global.FabricVersion = "3.1.0"
			net.Spec.Orgs[0].Orderers[0].Type = consensus
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := batchv1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			var objects []client.Object
			for _, org := range net.Spec.Orgs {
				data := map[string][]byte{}
				for _, key := range mspSecretKeys(true) {
					data[key] = []byte(key)
				}
				data["provider-pin"] = []byte("must-not-copy")
				objects = append(objects, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
					Name: identitySecretName(adminIdentityName(org), secretKindMSP), Namespace: orgNamespaceName(net, org)}, Data: data})
			}
			objects = append(objects,
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "orderer0-tls", Namespace: orderer.namespace},
					Data: map[string][]byte{tlsCACertKey: []byte("root"), tlsServerCertKey: []byte("cert"),
						tlsServerKeyKey: []byte("private"), "provider-pin": []byte("must-not-copy")}},
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "orderer-admin-tls", Namespace: orderer.namespace},
					Data: map[string][]byte{tlsCACertKey: []byte("root"), tlsClientCertKey: []byte("cert"),
						tlsClientKeyKey: []byte("client-private")}},
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "orderer0-msp", Namespace: orderer.namespace},
					Data: copySecretData(objects[0].(*corev1.Secret).Data)},
			)
			// Simulate a replica made by the previous operator version.
			legacy := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: channelOrgMSPSecretName(channel.Name, net.Spec.Orgs[1]),
				Namespace: orderer.namespace, UID: "replica", Annotations: resourceAnnotations(net, net.Spec.Orgs[0])},
				Data: copySecretData(objects[1].(*corev1.Secret).Data)}
			objects = append(objects, legacy)
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			r := &FabricNetworkReconciler{Client: c, Scheme: scheme}
			legacyJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "legacy-generator", Namespace: orderer.namespace},
				Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{{Name: "msp", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
						SecretName: legacy.Name, Items: []corev1.KeyToPath{{Key: mspKeyStoreKey, Path: "keystore/key.pem"}}}}}},
				}}}}
			if err := c.Create(t.Context(), legacyJob); err != nil {
				t.Fatal(err)
			}
			if err := r.ensureChannelCryptoSecrets(t.Context(), net, channel, net.Spec.Orgs[0], orderer.namespace); err == nil {
				t.Error("must wait for a legacy Job before removing its required key projection")
			}
			var held corev1.Secret
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(legacy), &held); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(held.Data, legacy.Data) {
				t.Error("active legacy Job lost its existing material")
			}
			if err := c.Delete(t.Context(), legacyJob); err != nil {
				t.Fatal(err)
			}
			if err := r.ensureChannelCryptoSecrets(t.Context(), net, channel, net.Spec.Orgs[0], orderer.namespace); err != nil {
				t.Fatal(err)
			}
			publicNames := []string{channelOrgMSPSecretName(channel.Name, net.Spec.Orgs[0]),
				channelOrgMSPSecretName(channel.Name, net.Spec.Orgs[1]), channelOrdererTLSSecretName(channel.Name, orderer.name)}
			if consensus == "bft" {
				publicNames = append(publicNames, channelOrdererMSPSecretName(channel.Name, orderer.name))
			}
			for _, name := range publicNames {
				var secret corev1.Secret
				if err := c.Get(t.Context(), client.ObjectKey{Namespace: orderer.namespace, Name: name}, &secret); err != nil {
					t.Fatal(err)
				}
				for _, key := range []string{mspKeyStoreKey, tlsServerKeyKey, tlsClientKeyKey, "provider-pin"} {
					if _, present := secret.Data[key]; present {
						t.Errorf("public replica %s contains %s", name, key)
					}
				}
			}
			var adminTLS corev1.Secret
			if err := c.Get(t.Context(), client.ObjectKey{Namespace: orderer.namespace,
				Name: channelOrdererAdminTLSSecretName(channel.Name, net.Spec.Orgs[0])}, &adminTLS); err != nil {
				t.Fatal(err)
			}
			if string(adminTLS.Data[tlsClientKeyKey]) != "client-private" {
				t.Fatal("osnadmin still needs its client signing key")
			}
			for _, object := range objects[:len(objects)-1] {
				var actual corev1.Secret
				if err := c.Get(t.Context(), client.ObjectKeyFromObject(object), &actual); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(actual.Data, object.(*corev1.Secret).Data) {
					t.Errorf("source identity %s changed", object.GetName())
				}
			}
			blockJob := buildChannelBlockJob(net, channel, net.Spec.Orgs[0], orderer.namespace)
			for _, volume := range blockJob.Spec.Template.Spec.Volumes {
				if volume.Secret == nil {
					continue
				}
				for _, item := range volume.Secret.Items {
					if item.Key == mspKeyStoreKey || item.Key == tlsServerKeyKey || item.Key == tlsClientKeyKey {
						t.Errorf("configtxgen volume %s requests private key %s", volume.Name, item.Key)
					}
				}
			}
		})
	}
}

func TestCertificateOnlySourcesAndStrictReplicationAllowlist(t *testing.T) {
	for _, test := range []struct {
		name      string
		items     []corev1.KeyToPath
		wantError bool
	}{
		{"public certificates without a private key", tlsPublicSecretItems(), false},
		{"no allowlist", nil, true},
		{"missing requested key", []corev1.KeyToPath{{Key: "missing", Path: "missing"}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			source := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "source", Namespace: "org"},
				Data: map[string][]byte{tlsCACertKey: []byte("root"), tlsServerCertKey: []byte("cert"),
					"provider-pin": []byte("sensitive-value")}}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(source).Build()
			r := &FabricNetworkReconciler{Client: c, Scheme: scheme}
			err := r.ensureCopiedSecret(t.Context(), client.ObjectKeyFromObject(source), "org", "copy", nil, nil, test.items)
			if (err != nil) != test.wantError {
				t.Fatalf("unexpected replication result: %v", err)
			}
			if err != nil && strings.Contains(err.Error(), "sensitive-value") {
				t.Fatal("diagnostic leaked secret data")
			}
			var actual corev1.Secret
			getErr := c.Get(t.Context(), client.ObjectKey{Namespace: "org", Name: "copy"}, &actual)
			if test.wantError {
				if !apierrors.IsNotFound(getErr) {
					t.Fatalf("failed validation wrote a replica: %v", getErr)
				}
				return
			}
			if getErr != nil {
				t.Fatal(getErr)
			}
			if len(actual.Data) != 2 || string(actual.Data[tlsCACertKey]) != "root" || string(actual.Data[tlsServerCertKey]) != "cert" {
				t.Errorf("incorrect certificate-only data: keys=%v", reflect.ValueOf(actual.Data).MapKeys())
			}
		})
	}
}

func TestLegacySecretConsumerDetection(t *testing.T) {
	removed := map[string]bool{mspKeyStoreKey: true}
	for _, test := range []struct {
		name string
		spec corev1.PodSpec
		want bool
	}{
		{"required private key", corev1.PodSpec{Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: "copy", Items: mspSecretItems(true)}}}}}, true},
		{"whole Secret", corev1.PodSpec{Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: "copy"}}}}}, true},
		{"certificate-only projection", corev1.PodSpec{Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: "copy", Items: mspPublicSecretItems(true)}}}}}, false},
		{"unrelated Secret", corev1.PodSpec{Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: "other"}}}}}, false},
		{"projected volume", corev1.PodSpec{Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{Secret: &corev1.SecretProjection{
				LocalObjectReference: corev1.LocalObjectReference{Name: "copy"}}}}}}}}}, true},
		{"init environment", corev1.PodSpec{InitContainers: []corev1.Container{{Env: []corev1.EnvVar{{ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "copy"}, Key: mspKeyStoreKey}}}}}}}, true},
		{"environment import", corev1.PodSpec{Containers: []corev1.Container{{EnvFrom: []corev1.EnvFromSource{{
			SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "copy"}}}}}}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := podRequiresRemovedSecretKeys(test.spec, "copy", removed); got != test.want {
				t.Errorf("consumer detection=%v, want %v", got, test.want)
			}
		})
	}
}

func TestSecretPruningWaitsForConsumersToFinish(t *testing.T) {
	for _, test := range []struct {
		name         string
		jobCondition batchv1.JobConditionType
		podPhase     corev1.PodPhase
		wantBlocked  bool
	}{
		{"pending job", "", "", true},
		{"terminating failed job", batchv1.JobFailureTarget, "", true},
		{"completed job", batchv1.JobComplete, "", false},
		{"failed job", batchv1.JobFailed, "", false},
		{"pending pod", batchv1.JobComplete, corev1.PodPending, true},
		{"running pod", batchv1.JobComplete, corev1.PodRunning, true},
		{"completed pod", batchv1.JobComplete, corev1.PodSucceeded, false},
		{"failed pod", batchv1.JobComplete, corev1.PodFailed, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := batchv1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			spec := corev1.PodSpec{Volumes: []corev1.Volume{{Name: "identity", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: "copy", Items: mspSecretItems(true)}}}}}
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: "org"},
				Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: spec}}}
			if test.jobCondition != "" {
				job.Status.Conditions = []batchv1.JobCondition{{Type: test.jobCondition, Status: corev1.ConditionTrue}}
			}
			objects := []client.Object{job}
			if test.podPhase != "" {
				objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "legacy-pod", Namespace: "org"},
					Spec: spec, Status: corev1.PodStatus{Phase: test.podPhase}})
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			r := &FabricNetworkReconciler{Client: c, Scheme: scheme}
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "copy", Namespace: "org"},
				Data: map[string][]byte{mspKeyStoreKey: []byte("private")}}
			err := r.ensureSecretKeyRemovalSafe(t.Context(), secret, map[string][]byte{})
			if (err != nil) != test.wantBlocked {
				t.Errorf("blocked=%v, want %v: %v", err != nil, test.wantBlocked, err)
			}
		})
	}
}

func TestSigningProjectionKeepsExistingOrder(t *testing.T) {
	for _, tlsEnabled := range []bool{false, true} {
		want := []corev1.KeyToPath{{Key: mspConfigKey, Path: "config.yaml"},
			{Key: mspCACertKey, Path: "cacerts/ca.pem"}, {Key: mspSignCertKey, Path: "signcerts/cert.pem"},
			{Key: mspKeyStoreKey, Path: "keystore/key.pem"}}
		if tlsEnabled {
			want = append(want, corev1.KeyToPath{Key: mspTLSCACertKey, Path: "tlscacerts/tlsca.pem"})
		}
		if got := mspSecretItems(tlsEnabled); !reflect.DeepEqual(got, want) {
			t.Errorf("signing projection changed for tls=%v; would trigger unnecessary workload rollouts", tlsEnabled)
		}
	}
}
