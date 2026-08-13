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
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

const (
	annotationSucceededJobCleanup = "fabricops.io/succeeded-job-cleanup"
)

func succeededJobCleanupAnnotations(annotations map[string]string) map[string]string {
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[annotationSucceededJobCleanup] = "true"
	return annotations
}

func succeededJobCleanupTTL(net *fabricopsv1alpha1.FabricNetwork) (time.Duration, bool) {
	if net.Spec.Global.Jobs == nil || net.Spec.Global.Jobs.SucceededHistoryTTLSeconds == nil {
		return 0, false
	}
	return time.Duration(*net.Spec.Global.Jobs.SucceededHistoryTTLSeconds) * time.Second, true
}

func (r *FabricNetworkReconciler) cleanupSucceededJobs(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
) (time.Duration, error) {
	ttl, enabled := succeededJobCleanupTTL(net)
	if !enabled {
		return 0, nil
	}

	now := time.Now()
	propagation := metav1.DeletePropagationBackground
	var nextCleanup time.Duration
	for _, org := range net.Spec.Orgs {
		namespace := orgNamespaceName(net, org)
		expectedOwner := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   namespace,
				Labels:      orgLabels(net, org, componentNetwork),
				Annotations: resourceAnnotations(net, org),
			},
		}

		var jobs batchv1.JobList
		if err := r.List(ctx, &jobs, client.InNamespace(namespace), fabricNetworkSelector(net)); err != nil {
			return 0, err
		}

		for i := range jobs.Items {
			job := &jobs.Items[i]
			if !succeededJobCleanupEligible(job) || !jobSucceeded(*job) || job.Status.CompletionTime == nil {
				continue
			}

			age := now.Sub(job.Status.CompletionTime.Time)
			if age < ttl {
				remaining := ttl - age
				if nextCleanup == 0 || remaining < nextCleanup {
					nextCleanup = remaining
				}
				continue
			}

			expectedOwner.Name = job.Name
			if err := r.deleteOwnedObject(ctx, job, expectedOwner, &client.DeleteOptions{PropagationPolicy: &propagation}); err != nil {
				return 0, err
			}
		}
	}

	return nextCleanup, nil
}

func succeededJobCleanupEligible(job *batchv1.Job) bool {
	return job.Annotations[annotationSucceededJobCleanup] == "true"
}

func fabricNetworkSelector(net *fabricopsv1alpha1.FabricNetwork) client.MatchingLabels {
	return client.MatchingLabels{
		labelFabricNetwork:          sanitizeName(net.Name),
		labelFabricNetworkNamespace: sanitizeName(net.Namespace),
	}
}

func resultWithCleanupRequeue(result ctrl.Result, cleanupAfter time.Duration) ctrl.Result {
	if cleanupAfter <= 0 {
		return result
	}
	if result.RequeueAfter == 0 || cleanupAfter < result.RequeueAfter {
		result.RequeueAfter = cleanupAfter
	}
	return result
}
