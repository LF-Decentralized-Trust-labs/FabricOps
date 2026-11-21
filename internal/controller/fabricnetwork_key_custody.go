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
	"fmt"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func (r *FabricNetworkReconciler) cleanupUnusedOrdererAdminTLS(
	ctx context.Context, net *fabricopsv1alpha1.FabricNetwork, channel fabricopsv1alpha1.Channel, hostNamespace string,
) error {
	if net.UID == "" {
		return nil
	}
	names := map[string]bool{}
	for _, orderer := range desiredOrdererInstances(net) {
		names[channelOrdererAdminTLSSecretName(channel.Name, orderer.org)] = true
	}
	for _, org := range net.Spec.Orgs {
		namespace := orgNamespaceName(net, org)
		if namespace == hostNamespace {
			continue
		}
		var secrets corev1.SecretList
		if err := r.List(ctx, &secrets, client.InNamespace(namespace),
			client.MatchingLabels(channelLabels(net, org, channel.Name))); err != nil {
			return err
		}
		for i := range secrets.Items {
			secret := &secrets.Items[i]
			annotations := secret.Annotations
			if !names[secret.Name] || secret.Labels[labelIdentityKind] != "" || annotations[annotationFabricNetwork] != net.Name ||
				annotations[annotationFabricNetworkNamespace] != net.Namespace ||
				annotations[annotationFabricNetworkUID] != string(net.UID) {
				continue
			}
			if err := r.ensureSecretKeyRemovalSafe(ctx, secret, nil); err != nil {
				return err
			}
			logf.FromContext(ctx).Info("Deleting unused orderer admin TLS Secret", "namespace", namespace, "name", secret.Name)
			if err := client.IgnoreNotFound(r.Delete(ctx, secret,
				client.Preconditions{UID: &secret.UID, ResourceVersion: &secret.ResourceVersion})); err != nil {
				return err
			}
		}
	}
	return nil
}

// Old Job templates are immutable and may still require key files that newer
// certificate-only templates omit. Let these consumers finish before pruning.
func (r *FabricNetworkReconciler) ensureSecretKeyRemovalSafe(
	ctx context.Context, secret *corev1.Secret, desired map[string][]byte,
) error {
	removed := map[string]bool{}
	for key := range secret.Data {
		if _, keep := desired[key]; !keep {
			removed[key] = true
		}
	}
	if len(removed) == 0 {
		return nil
	}
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.InNamespace(secret.Namespace)); err != nil {
		return err
	}
	for _, job := range jobs.Items {
		terminal := false
		for _, condition := range job.Status.Conditions {
			if condition.Status == corev1.ConditionTrue && (condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed) {
				terminal = true
			}
		}
		if !terminal && podRequiresRemovedSecretKeys(job.Spec.Template.Spec, secret.Name, removed) {
			return fmt.Errorf("waiting for Job %s/%s before removing obsolete keys from Secret %s", secret.Namespace, job.Name, secret.Name)
		}
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(secret.Namespace)); err != nil {
		return err
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed &&
			podRequiresRemovedSecretKeys(pod.Spec, secret.Name, removed) {
			return fmt.Errorf("waiting for Pod %s/%s before removing obsolete keys from Secret %s", secret.Namespace, pod.Name, secret.Name)
		}
	}
	return nil
}

func podRequiresRemovedSecretKeys(spec corev1.PodSpec, name string, removed map[string]bool) bool {
	projects := func(source string, items []corev1.KeyToPath) bool {
		if source != name {
			return false
		}
		if len(items) == 0 {
			return true
		}
		for _, item := range items {
			if removed[item.Key] {
				return true
			}
		}
		return false
	}
	for _, volume := range spec.Volumes {
		if volume.Secret != nil && projects(volume.Secret.SecretName, volume.Secret.Items) {
			return true
		}
		if volume.Projected != nil {
			for _, source := range volume.Projected.Sources {
				if source.Secret != nil && projects(source.Secret.Name, source.Secret.Items) {
					return true
				}
			}
		}
	}
	for _, containers := range [][]corev1.Container{spec.InitContainers, spec.Containers} {
		for _, container := range containers {
			for _, env := range container.Env {
				if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
					ref := env.ValueFrom.SecretKeyRef
					if ref.Name == name && removed[ref.Key] {
						return true
					}
				}
			}
			for _, env := range container.EnvFrom {
				if env.SecretRef != nil && env.SecretRef.Name == name {
					return true
				}
			}
		}
	}
	return false
}
