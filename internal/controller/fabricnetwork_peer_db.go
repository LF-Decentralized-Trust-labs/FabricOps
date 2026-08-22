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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

type peerDatabase string

const (
	peerDatabaseLevelDB peerDatabase = "LevelDB"
	peerDatabaseCouchDB peerDatabase = "CouchDB"

	couchDBUsernameKey = "username"
	couchDBPasswordKey = "password"
)

func couchDBStartupArgs() []string {
	return []string{"+S", "2:2", "+SDcpu", "2", "+SDio", "2"}
}

func normalizePeerDatabase(db string) (peerDatabase, bool) {
	switch strings.ToLower(strings.TrimSpace(db)) {
	case "leveldb", "level-db", "level_db":
		return peerDatabaseLevelDB, true
	case "couchdb", "couch-db", "couch_db":
		return peerDatabaseCouchDB, true
	default:
		return "", false
	}
}

func peerDatabaseStatus(org fabricopsv1alpha1.Org) string {
	if org.Peer == nil {
		return ""
	}
	if db, ok := normalizePeerDatabase(org.Peer.DB); ok {
		return string(db)
	}
	return strings.TrimSpace(org.Peer.DB)
}

func peerUsesCouchDB(org fabricopsv1alpha1.Org) bool {
	if org.Peer == nil {
		return false
	}
	db, ok := normalizePeerDatabase(org.Peer.DB)
	return ok && db == peerDatabaseCouchDB
}

func couchDBName(peerName string) string {
	return sanitizeName(peerName + "-couchdb")
}

func couchDBSecretName(peerName string) string {
	return sanitizeName(peerName + "-couchdb-auth")
}

func couchDBAddress(peerName, namespace string) string {
	return serviceDNS(couchDBName(peerName), namespace, couchDBPort)
}

func couchDBUsername(peerName string) string {
	return sanitizeName(peerName + "-admin")
}

func couchDBSecretValidationError(secret corev1.Secret) string {
	if strings.TrimSpace(string(secret.Data[couchDBUsernameKey])) == "" {
		return couchDBUsernameKey + " is required"
	}
	if strings.TrimSpace(string(secret.Data[couchDBPasswordKey])) == "" {
		return couchDBPasswordKey + " is required"
	}
	return ""
}

func buildCouchDBSecret(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	peerName string,
) (*corev1.Secret, error) {
	labels := orgLabels(net, org, componentCouchDB)
	labels[labelWorkload] = peerName
	password, err := generateBootstrapPassword()
	if err != nil {
		return nil, err
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        couchDBSecretName(peerName),
			Namespace:   namespace,
			Labels:      labels,
			Annotations: resourceAnnotations(net, org),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			couchDBUsernameKey: []byte(couchDBUsername(peerName)),
			couchDBPasswordKey: []byte(password),
		},
	}, nil
}

func buildCouchDBDeployment(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	peerName string,
	namespace string,
) *appsv1.Deployment {
	name := couchDBName(peerName)
	replicas := int32(1)
	selector := map[string]string{
		labelFabricNetwork:          sanitizeName(net.Name),
		labelFabricNetworkNamespace: sanitizeName(net.Namespace),
		labelOrg:                    sanitizeName(org.Organization.Name),
		labelComponent:              componentCouchDB,
		labelWorkload:               peerName,
	}
	labels := mergeMap(orgLabels(net, org, componentCouchDB), map[string]string{
		labelWorkload: peerName,
	})

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: resourceAnnotations(net, org),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RecreateDeploymentStrategyType,
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: selector,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: resourceAnnotations(net, org),
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						dataVolume(name),
					},
					Containers: []corev1.Container{
						{
							Name:            containerCouchDB,
							Image:           defaultCouchDBImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Args:            couchDBStartupArgs(),
							Env: []corev1.EnvVar{
								secretKeyEnvVar("COUCHDB_USER", couchDBSecretName(peerName), couchDBUsernameKey),
								secretKeyEnvVar("COUCHDB_PASSWORD", couchDBSecretName(peerName), couchDBPasswordKey),
							},
							Ports: []corev1.ContainerPort{
								{ContainerPort: couchDBPort, Name: "couchdb", Protocol: corev1.ProtocolTCP},
							},
							ReadinessProbe: tcpReadinessProbe(couchDBPort),
							LivenessProbe:  tcpLivenessProbe(couchDBPort),
							Resources:      componentResourceRequirements(componentCouchDB),
							VolumeMounts: []corev1.VolumeMount{
								dataVolumeMount(couchDBDataPath),
							},
						},
					},
				},
			},
		},
	}
}

func buildCouchDBService(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	peerName string,
	namespace string,
) *corev1.Service {
	selector := map[string]string{
		labelFabricNetwork:          sanitizeName(net.Name),
		labelFabricNetworkNamespace: sanitizeName(net.Namespace),
		labelOrg:                    sanitizeName(org.Organization.Name),
		labelComponent:              componentCouchDB,
		labelWorkload:               peerName,
	}
	labels := mergeMap(orgLabels(net, org, componentCouchDB), map[string]string{
		labelWorkload: peerName,
	})

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        couchDBName(peerName),
			Namespace:   namespace,
			Labels:      labels,
			Annotations: resourceAnnotations(net, org),
		},
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Ports: []corev1.ServicePort{
				{
					Name:       "couchdb",
					Port:       couchDBPort,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt32(couchDBPort),
				},
			},
		},
	}
}

func couchDBPeerEnv(peerName, namespace string) []corev1.EnvVar {
	secretName := couchDBSecretName(peerName)
	return []corev1.EnvVar{
		{Name: "CORE_LEDGER_STATE_STATEDATABASE", Value: string(peerDatabaseCouchDB)},
		{Name: "CORE_LEDGER_STATE_COUCHDBCONFIG_COUCHDBADDRESS", Value: couchDBAddress(peerName, namespace)},
		secretKeyEnvVar("CORE_LEDGER_STATE_COUCHDBCONFIG_USERNAME", secretName, couchDBUsernameKey),
		secretKeyEnvVar("CORE_LEDGER_STATE_COUCHDBCONFIG_PASSWORD", secretName, couchDBPasswordKey),
		{Name: "CORE_LEDGER_STATE_COUCHDBCONFIG_MAXRETRIES", Value: "10"},
		{Name: "CORE_LEDGER_STATE_COUCHDBCONFIG_MAXRETRIESONSTARTUP", Value: "10"},
		{Name: "CORE_LEDGER_STATE_COUCHDBCONFIG_REQUESTTIMEOUT", Value: "35s"},
	}
}

func secretKeyEnvVar(name, secretName, key string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: secretName,
				},
				Key: key,
			},
		},
	}
}
