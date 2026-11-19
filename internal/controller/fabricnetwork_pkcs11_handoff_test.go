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
	"strings"
	"testing"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPKCS11PeerWaitsForEnrollmentProof(t *testing.T) {
	net, ns, r := pkcs11RetentionFixture(t)
	org := net.Spec.Orgs[0]
	org.Peer.Instances = 1
	org.Peer.MSPSigning.PKCS11.Runtime.StateClaims = org.Peer.MSPSigning.PKCS11.Runtime.StateClaims[:1]
	if err := appsv1.AddToScheme(r.Scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(r.Scheme); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(t.Context(), preparedPKCS11Claim(ns.Name, net.UID)); err != nil {
		t.Fatal(err)
	}
	status, err := r.reconcilePeers(t.Context(), net, org, ns.Name)
	if err != nil {
		t.Fatal(err)
	}
	if status.Desired != 1 || status.Ready != 0 {
		t.Fatal("waiting peer was omitted from desired workload status")
	}
	var deployments appsv1.DeploymentList
	if err := r.List(t.Context(), &deployments, client.InNamespace(ns.Name)); err != nil {
		t.Fatal(err)
	}
	if len(deployments.Items) != 0 {
		t.Fatal("peer scheduled without completed enrollment proof")
	}
}

func pkcs11HandoffFixture(t *testing.T) (*fabricopsv1alpha1.FabricNetwork, *corev1.Namespace, *FabricNetworkReconciler, *batchv1.Job) {
	t.Helper()
	net, ns, r := pkcs11RetentionFixture(t)
	org := &net.Spec.Orgs[0]
	org.Peer.Instances, org.Peer.DB = 1, "LevelDB"
	org.Peer.MSPSigning.PKCS11.Runtime.StateClaims = org.Peer.MSPSigning.PKCS11.Runtime.StateClaims[:1]
	for _, register := range []func() error{
		func() error { return batchv1.AddToScheme(r.Scheme) },
		func() error { return appsv1.AddToScheme(r.Scheme) },
		func() error { return rbacv1.AddToScheme(r.Scheme) },
	} {
		if err := register(); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Create(t.Context(), preparedPKCS11Claim(ns.Name, net.UID)); err != nil {
		t.Fatal(err)
	}
	authority, err := generateTestIdentityAuthority(*org)
	if err != nil {
		t.Fatal(err)
	}
	msp, err := buildTestMSPSecret(net, *org, ns.Name, "peer0", componentPeer, secretKindMSP, authority)
	if err != nil {
		t.Fatal(err)
	}
	delete(msp.Data, mspKeyStoreKey)
	msp.UID, msp.Annotations = "public-msp-uid", resourceAnnotations(net, *org)
	msp.Labels[labelIdentitySource] = identitySourceFabricCA
	if err := r.Create(t.Context(), msp); err != nil {
		t.Fatal(err)
	}
	tls, err := buildTestWorkloadTLSSecret(net, *org, ns.Name, "peer0", componentPeer, nil, authority)
	if err != nil {
		t.Fatal(err)
	}
	tls.UID, tls.Annotations = "tls-uid", resourceAnnotations(net, *org)
	tls.Labels[labelIdentitySource] = identitySourceFabricCA
	if err := r.Create(t.Context(), tls); err != nil {
		t.Fatal(err)
	}
	job := buildWorkloadEnrollmentJob(net, *org, ns.Name, "peer0", componentPeer, peerWorkloadCSRHosts(*org, "peer0", ns.Name))
	job.UID = "enrollment-job-uid"
	job.Status.Succeeded = 1
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	now := metav1.Now()
	job.Status.CompletionTime = &now
	if err := r.Create(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	return net, ns, r, job
}

func TestPKCS11HandoffWaitsForJobAndPodDrain(t *testing.T) {
	net, ns, r, job := pkcs11HandoffFixture(t)
	org := net.Spec.Orgs[0]
	ready := func() bool {
		t.Helper()
		ok, err := r.pkcs11PeerEnrollmentReady(t.Context(), net, org, ns.Name, "peer0")
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if ready() {
		t.Fatal("receipt creation skipped Job/Pod drain")
	}
	if err := r.Get(t.Context(), client.ObjectKeyFromObject(job), &batchv1.Job{}); err != nil {
		t.Fatal("Job was deleted before handoff persistence")
	}
	for _, phase := range []corev1.PodPhase{corev1.PodPending, corev1.PodRunning, corev1.PodSucceeded, corev1.PodFailed} {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "token-holder-" + string(phase), Namespace: ns.Name},
			Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "token", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "token-peer0"}}}}}, Status: corev1.PodStatus{Phase: phase}}
		if err := r.Create(t.Context(), pod); err != nil {
			t.Fatal(err)
		}
		if ready() {
			t.Fatal("peer started while a Pod still referenced its token claim")
		}
		if err := r.Get(t.Context(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
			t.Fatal("handoff deleted a Pod directly")
		}
		if err := r.Delete(t.Context(), pod); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Get(t.Context(), client.ObjectKeyFromObject(job), &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Fatal("completed enrollment Job was not retired")
	}
	if !ready() {
		t.Fatal("peer did not advance after Job and Pod drain")
	}
	if err := r.recordPKCS11StateRetention(t.Context(), net); err != nil {
		t.Fatal(err)
	}
	if !ready() {
		t.Fatal("ordinary retention refresh erased handoff evidence")
	}
	status, err := r.reconcilePeers(t.Context(), net, org, ns.Name)
	if err != nil || status.Desired != 1 {
		t.Fatal("peer could not reconcile after durable handoff:", err)
	}
	if err := r.Get(t.Context(), client.ObjectKey{Namespace: ns.Name, Name: "peer0"}, &appsv1.Deployment{}); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "running-peer", Namespace: ns.Name}, Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "token", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "token-peer0"}}}}}}
	if err := r.Create(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	if !ready() {
		t.Fatal("a running peer invalidated its already-drained handoff")
	}
}

func TestPKCS11HandoffRequiresStrictOwnedCompletion(t *testing.T) {
	for _, test := range []struct {
		name      string
		mutate    func(*batchv1.Job)
		wantError bool
	}{
		{"counter only", func(j *batchv1.Job) { j.Status.Conditions = nil }, false},
		{"still active", func(j *batchv1.Job) { j.Status.Active = 1 }, false},
		{"failed", func(j *batchv1.Job) {
			j.Status.Conditions = append(j.Status.Conditions, batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue})
		}, true},
		{"foreign", func(j *batchv1.Job) { j.Annotations[annotationFabricNetworkUID] = "foreign" }, true},
		{"changed plan", func(j *batchv1.Job) { j.Spec.Template.Spec.InitContainers[0].Image = "unexpected:image" }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			net, ns, r, job := pkcs11HandoffFixture(t)
			// Seed through the fake client so status and immutable-template mutations
			// model stale/foreign objects without weakening real API validation.
			if err := r.Delete(t.Context(), job); err != nil {
				t.Fatal(err)
			}
			test.mutate(job)
			job.ResourceVersion = ""
			if err := r.Create(t.Context(), job); err != nil {
				t.Fatal(err)
			}
			ready, err := r.pkcs11PeerEnrollmentReady(t.Context(), net, net.Spec.Orgs[0], ns.Name, "peer0")
			if ready || (err != nil) != test.wantError {
				t.Fatalf("unexpected readiness decision: ready=%v error=%v", ready, err)
			}
			if err := r.Get(t.Context(), client.ObjectKeyFromObject(job), &batchv1.Job{}); err != nil {
				t.Fatal("incomplete or foreign Job was deleted")
			}
		})
	}
}

func TestPKCS11HandoffRejectsChangedMaterialAndReplays(t *testing.T) {
	for _, change := range []string{"image", "MSP UID", "MSP data", "missing MSP", "Job UID"} {
		t.Run(change, func(t *testing.T) {
			net, ns, r, job := pkcs11HandoffFixture(t)
			if _, err := r.pkcs11PeerEnrollmentReady(t.Context(), net, net.Spec.Orgs[0], ns.Name, "peer0"); err != nil {
				t.Fatal(err)
			}
			if err := r.Delete(t.Context(), job); err != nil {
				t.Fatal(err)
			}
			var msp corev1.Secret
			key := client.ObjectKey{Namespace: ns.Name, Name: "peer0-msp"}
			if err := r.Get(t.Context(), key, &msp); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "image":
				net.Spec.Orgs[0].Peer.MSPSigning.PKCS11.Runtime.EnrollmentImage = "changed:image"
			case "MSP UID":
				msp.UID = "replacement-msp"
				if err := r.Update(t.Context(), &msp); err != nil {
					t.Fatal(err)
				}
			case "MSP data":
				msp.Data[mspConfigKey] = append(msp.Data[mspConfigKey], '\n')
				if err := r.Update(t.Context(), &msp); err != nil {
					t.Fatal(err)
				}
			case "missing MSP":
				if err := r.Delete(t.Context(), &msp); err != nil {
					t.Fatal(err)
				}
			case "Job UID":
				job.UID, job.ResourceVersion = "replacement-job", ""
				if err := r.Create(t.Context(), job); err != nil {
					t.Fatal(err)
				}
			}
			if ready, err := r.pkcs11PeerEnrollmentReady(t.Context(), net, net.Spec.Orgs[0], ns.Name, "peer0"); ready || err == nil {
				t.Fatal("changed handoff evidence was accepted")
			}
			if change == "missing MSP" {
				if err := r.reconcileWorkloadEnrollment(t.Context(), net, net.Spec.Orgs[0], ns.Name, "peer0", componentPeer, nil); err == nil {
					t.Fatal("fresh enrollment was allowed after handoff")
				}
				if err := r.Get(t.Context(), client.ObjectKeyFromObject(job), &batchv1.Job{}); !apierrors.IsNotFound(err) {
					t.Fatal("lost identity material recreated an enrollment Job")
				}
			}
		})
	}
}

func TestPKCS11EnrollmentBypassesGeneralHistoryCleanup(t *testing.T) {
	net, ns, r, job := pkcs11HandoffFixture(t)
	zero := int32(0)
	net.Spec.Global.Jobs = &fabricopsv1alpha1.JobCleanupConfig{SucceededHistoryTTLSeconds: &zero}
	job.Annotations[annotationSucceededJobCleanup] = "true"
	if err := r.Update(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	if _, err := r.cleanupSucceededJobs(t.Context(), net); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(t.Context(), client.ObjectKey{Namespace: ns.Name, Name: job.Name}, &batchv1.Job{}); err != nil {
		t.Fatal("history cleanup deleted enrollment evidence before handoff")
	}
}

func TestPKCS11PublishedMaterialCannotReplaceMissingJobProof(t *testing.T) {
	net, ns, r, job := pkcs11HandoffFixture(t)
	if err := r.Delete(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	if ready, err := r.pkcs11PeerEnrollmentReady(t.Context(), net, net.Spec.Orgs[0], ns.Name, "peer0"); ready || err == nil {
		t.Fatal("Secret existence replaced missing enrollment proof")
	}
}

func TestParticipantHandoffEventsUseUIDAndSyntheticOwnerName(t *testing.T) {
	_, _, r := pkcs11RetentionFixture(t)
	if err := fabricopsv1alpha1.AddToScheme(r.Scheme); err != nil {
		t.Fatal(err)
	}
	participant := fabricParticipantFixture("participant-" + strings.Repeat("long-name", 9))
	participant.UID = "participant-uid"
	if err := r.Create(t.Context(), participant); err != nil {
		t.Fatal(err)
	}
	local := participantLocalFabricNetwork(participant)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: resourceAnnotations(local, participant.Spec.Org)}}
	pr := &FabricParticipantReconciler{Client: r.Client, Scheme: r.Scheme}
	requests := pr.participantRequestsForObject(t.Context(), pod)
	if len(requests) != 1 || requests[0].NamespacedName != client.ObjectKeyFromObject(participant) {
		t.Fatal("participant handoff event was not mapped through its hashed local owner name")
	}
	pod.Annotations[annotationFabricNetworkUID] = "foreign-uid"
	if len(pr.participantRequestsForObject(t.Context(), pod)) != 0 {
		t.Fatal("foreign participant event was accepted")
	}
}

var _ = Describe("PKCS11 enrollment handoff", func() {
	It("persists publication evidence before retiring the Job and waits for Pod disappearance", func() {
		net, _, _, _, _ := channelConfigOperationFixture()
		net.Name, net.UID = "pkcs-handoff-api", "handoff-network-uid"
		org := pkcs11RuntimeOrgFixture(GinkgoT())
		org.Peer.Instances, org.Peer.DB = 1, "LevelDB"
		org.Peer.MSPSigning.PKCS11.Runtime.StateClaims = org.Peer.MSPSigning.PKCS11.Runtime.StateClaims[:1]
		net.Spec.Orgs = []fabricopsv1alpha1.Org{org}
		ns := buildOrgNamespace(net, org)
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, ns)).To(Succeed()) })
		claim := preparedPKCS11Claim(ns.Name, net.UID)
		claim.UID, claim.Status = "", corev1.PersistentVolumeClaimStatus{}
		Expect(k8sClient.Create(ctx, claim)).To(Succeed())
		claim.Status.Phase = corev1.ClaimBound
		Expect(k8sClient.Status().Update(ctx, claim)).To(Succeed())
		authority, err := generateTestIdentityAuthority(org)
		Expect(err).NotTo(HaveOccurred())
		msp, err := buildTestMSPSecret(net, org, ns.Name, "peer0", componentPeer, secretKindMSP, authority)
		Expect(err).NotTo(HaveOccurred())
		delete(msp.Data, mspKeyStoreKey)
		tls, err := buildTestWorkloadTLSSecret(net, org, ns.Name, "peer0", componentPeer, nil, authority)
		Expect(err).NotTo(HaveOccurred())
		for _, secret := range []*corev1.Secret{msp, tls} {
			secret.Annotations = resourceAnnotations(net, org)
			secret.Labels[labelIdentitySource] = identitySourceFabricCA
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		}
		account, _, _ := pkcs11PublisherRBAC(net, org, ns.Name, "peer0")
		Expect(k8sClient.Create(ctx, account)).To(Succeed())
		job := buildWorkloadEnrollmentJob(net, org, ns.Name, "peer0", componentPeer, peerWorkloadCSRHosts(org, "peer0", ns.Name))
		Expect(k8sClient.Create(ctx, job)).To(Succeed())
		markJobComplete(ctx, ns.Name, job.Name)
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(job), job)).To(Succeed())
		controller := true
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "enrollment-holder", Namespace: ns.Name,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: &controller}}}, Spec: job.Spec.Template.Spec}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		r := &FabricNetworkReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		ready, err := r.pkcs11PeerEnrollmentReady(ctx, net, org, ns.Name, "peer0")
		Expect(err).NotTo(HaveOccurred())
		Expect(ready).To(BeFalse())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(job), &batchv1.Job{})).To(Succeed())
		_, binding, err := r.pkcs11PeerBinding(ctx, net, org, ns.Name, "peer0")
		Expect(err).NotTo(HaveOccurred())
		Expect(binding.Enrollment).NotTo(BeNil())
		Expect(binding.Enrollment.JobUID).To(Equal(job.UID))
		Expect(binding.Enrollment.Drained).To(BeFalse())
		ready, err = r.pkcs11PeerEnrollmentReady(ctx, net, org, ns.Name, "peer0")
		Expect(err).NotTo(HaveOccurred())
		Expect(ready).To(BeFalse())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(job), &batchv1.Job{}))).To(BeTrue())
		status, err := r.reconcilePeers(ctx, net, org, ns.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(status.Desired).To(Equal(int32(1)))
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "peer0"}, &appsv1.Deployment{}))).To(BeTrue())
		Expect(k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0))).To(Succeed())
		status, err = r.reconcilePeers(ctx, net, org, ns.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(status.Desired).To(Equal(int32(1)))
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "peer0"}, &appsv1.Deployment{})).To(Succeed())
	})
})
