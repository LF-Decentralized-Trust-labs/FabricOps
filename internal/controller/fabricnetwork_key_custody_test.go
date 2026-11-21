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
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"
)

func TestManagerSecretCleanupRBACIsScoped(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "config", "rbac", "role.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var role rbacv1.ClusterRole
	if err := yaml.Unmarshal(data, &role); err != nil {
		t.Fatal(err)
	}
	secretDelete := false
	podReads := map[string]bool{}
	for _, rule := range role.Rules {
		if slices.Contains(rule.APIGroups, "") && slices.Contains(rule.Resources, "pods") {
			for _, verb := range rule.Verbs {
				if verb != "get" && verb != "list" && verb != "watch" {
					t.Errorf("consumer inspection must not grant Pod verb %s", verb)
				}
				podReads[verb] = true
			}
		}
		if (!slices.Contains(rule.APIGroups, "") && !slices.Contains(rule.APIGroups, "*")) ||
			(!slices.Contains(rule.Verbs, "delete") && !slices.Contains(rule.Verbs, "*")) {
			continue
		}
		if slices.Contains(rule.Resources, "secrets") {
			secretDelete = true
		}
		for _, protected := range []string{"*", "configmaps", "persistentvolumeclaims", "serviceaccounts"} {
			if slices.Contains(rule.Resources, protected) {
				t.Errorf("credential cleanup must not grant delete on %s", protected)
			}
		}
	}
	if !secretDelete {
		t.Fatal("manager cannot retire obsolete credential Secrets")
	}
	for _, verb := range []string{"get", "list", "watch"} {
		if !podReads[verb] {
			t.Errorf("manager cannot %s Pods to check unfinished consumers", verb)
		}
	}
}

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
			if err := r.ensureChannelPublicCryptoSecrets(t.Context(), net, channel, net.Spec.Orgs[0], orderer.namespace); err == nil {
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
			adminSource := objects[3].(*corev1.Secret).DeepCopy()
			if err := c.Delete(t.Context(), adminSource); err != nil {
				t.Fatal(err)
			}
			if err := r.ensureChannelPublicCryptoSecrets(t.Context(), net, channel, net.Spec.Orgs[0], orderer.namespace); err != nil {
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
			adminTLSKey := client.ObjectKey{Namespace: orderer.namespace,
				Name: channelOrdererAdminTLSSecretName(channel.Name, net.Spec.Orgs[0])}
			if err := c.Get(t.Context(), adminTLSKey, &adminTLS); !apierrors.IsNotFound(err) {
				t.Fatalf("public channel preparation copied admin TLS credentials: %v", err)
			}
			adminSource.ResourceVersion = ""
			if err := c.Create(t.Context(), adminSource); err != nil {
				t.Fatal(err)
			}
			if _, _, err := r.reconcileOrdererJoins(t.Context(), net, channel, net.Spec.Orgs[0], orderer.namespace); err != nil {
				t.Fatal(err)
			}
			if err := c.Get(t.Context(), adminTLSKey, &adminTLS); err != nil {
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

func TestOrdererJoinRejectsNonHostCredentialDestination(t *testing.T) {
	net, channel, adminOrg, _, orderer := channelConfigOperationFixture()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &FabricNetworkReconciler{Client: c, Scheme: scheme}
	namespace := orgNamespaceName(net, adminOrg)
	if _, _, err := r.reconcileOrdererJoins(t.Context(), net, channel, adminOrg, namespace); err == nil {
		t.Fatal("orderer join accepted an application-org namespace")
	}
	var secret corev1.Secret
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: namespace,
		Name: channelOrdererAdminTLSSecretName(channel.Name, orderer.org)}, &secret); !apierrors.IsNotFound(err) {
		t.Fatalf("rejected destination received credentials: %v", err)
	}
	var jobs batchv1.JobList
	if err := c.List(t.Context(), &jobs); err != nil || len(jobs.Items) != 0 {
		t.Fatalf("rejected destination received Jobs: %v", err)
	}
}

var _ = Describe("Orderer administration credential boundaries", func() {
	It("retires only current-UID replicas after legacy consumers drain through the API server", func() {
		net, channel, adminOrg, _, orderer := channelConfigOperationFixture()
		net.Name, net.UID = "custody-scope-api", "current-network"
		net.Spec.Channels = []fabricopsv1alpha1.Channel{channel}
		namespace := orgNamespaceName(net, adminOrg)
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, ns))).To(Succeed()) })
		name := channelOrdererAdminTLSSecretName(channel.Name, orderer.org)
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace,
			Labels: channelLabels(net, adminOrg, channel.Name), Annotations: resourceAnnotations(net, adminOrg)},
			Data: map[string][]byte{tlsClientKeyKey: []byte("private-test-material")}}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "legacy-reader", Namespace: namespace}, Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "hold", Image: "busybox:1.36",
				VolumeMounts: []corev1.VolumeMount{{Name: "admin", MountPath: "/admin", ReadOnly: true}}}},
			Volumes: []corev1.Volume{{Name: "admin", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: name}}}},
		}}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		r := &FabricNetworkReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := r.reconcileChannels(ctx, net, nil)
		Expect(err).To(MatchError(ContainSubstring("waiting for Pod")))
		actual := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(secret), actual)).To(Succeed())
		Expect(actual.Data).To(Equal(secret.Data))
		Expect(k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0))).To(Succeed())
		_, err = r.reconcileChannels(ctx, net, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(secret), actual))).To(BeTrue())
		foreign := secret.DeepCopy()
		foreign.UID, foreign.ResourceVersion = "", ""
		foreign.Annotations[annotationFabricNetworkUID] = "previous-network"
		Expect(k8sClient.Create(ctx, foreign)).To(Succeed())
		_, err = r.reconcileChannels(ctx, net, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(foreign), actual)).To(Succeed())
		Expect(actual.UID).To(Equal(foreign.UID))
	})
})

func TestChannelReconciliationRetiresOnlyOwnedNonHostAdminTLSReplicas(t *testing.T) {
	for _, test := range []struct {
		name                                                         string
		foreignUID, unowned, sourceIdentity, activePod, wrongChannel bool
	}{
		{name: "obsolete replica"}, {name: "previous network UID", foreignUID: true},
		{name: "unowned object", unowned: true}, {name: "source identity", sourceIdentity: true},
		{name: "active legacy consumer", activePod: true}, {name: "another channel", wrongChannel: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			net, channel, adminOrg, _, orderer := channelConfigOperationFixture()
			net.UID = "owner"
			net.Spec.Channels = []fabricopsv1alpha1.Channel{channel}
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := batchv1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			name := channelOrdererAdminTLSSecretName(channel.Name, orderer.org)
			data := map[string][]byte{tlsCACertKey: []byte("root"), tlsClientCertKey: []byte("cert"), tlsClientKeyKey: []byte("private")}
			legacy := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: orgNamespaceName(net, adminOrg),
				UID: "replica", Labels: channelLabels(net, adminOrg, channel.Name), Annotations: resourceAnnotations(net, adminOrg)}, Data: data}
			if test.foreignUID {
				legacy.Annotations[annotationFabricNetworkUID] = "previous"
			}
			if test.unowned {
				legacy.Annotations = nil
			}
			if test.sourceIdentity {
				legacy.Labels[labelIdentityKind] = secretKindAdminTLS
			}
			if test.wrongChannel {
				legacy.Labels[labelChannel] = "other"
			}
			hostCopy := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: orderer.namespace,
				UID: "host-copy", Labels: channelLabels(net, orderer.org, channel.Name), Annotations: resourceAnnotations(net, orderer.org)}, Data: copySecretData(data)}
			objects := []client.Object{legacy, hostCopy}
			if test.activePod {
				objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "legacy-reader", Namespace: legacy.Namespace},
					Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "admin", VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{SecretName: name}}}}}})
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			r := &FabricNetworkReconciler{Client: c, Scheme: scheme}
			_, err := r.reconcileChannels(t.Context(), net, nil)
			if (err != nil) != test.activePod {
				t.Fatalf("unexpected cleanup result: %v", err)
			}
			var actual corev1.Secret
			err = c.Get(t.Context(), client.ObjectKeyFromObject(legacy), &actual)
			preserve := test.foreignUID || test.unowned || test.sourceIdentity || test.activePod || test.wrongChannel
			if preserve {
				if err != nil || !reflect.DeepEqual(actual.Data, legacy.Data) {
					t.Fatalf("protected Secret was changed: %v", err)
				}
			} else if !apierrors.IsNotFound(err) {
				t.Fatalf("obsolete admin credentials remain: %v", err)
			}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(hostCopy), &actual); err != nil {
				t.Fatal("channel-host credentials were deleted", err)
			}
			if test.activePod {
				if err := c.Delete(t.Context(), objects[2]); err != nil {
					t.Fatal(err)
				}
				if _, err := r.reconcileChannels(t.Context(), net, nil); err != nil {
					t.Fatal(err)
				}
				if err := c.Get(t.Context(), client.ObjectKeyFromObject(legacy), &actual); !apierrors.IsNotFound(err) {
					t.Fatalf("drained credentials remain: %v", err)
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
