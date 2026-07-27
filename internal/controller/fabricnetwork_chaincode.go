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
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fabricopsv1alpha1 "github.com/dpereowei/fabricops/api/v1alpha1"
)

const (
	chaincodeMetadataKey        = "metadata.json"
	chaincodeConnectionKey      = "connection.json"
	chaincodePackageLabelKey    = "packageLabel"
	chaincodePackageFileKey     = "packageFile"
	chaincodeConnectionAddrKey  = "address"
	chaincodeCollectionsKey     = "collections.json"
	chaincodePackageMetadataDir = "metadata/META-INF"

	chaincodePackageIDKey       = "packageID"
	chaincodeChaincodeIDKey     = "chaincodeID"
	chaincodePackageHashKey     = "packageHash"
	chaincodeQueryInstalledKey  = "queryinstalled.json"
	chaincodeQueryApprovedKey   = "queryapproved.json"
	chaincodeQueryCommittedKey  = "querycommitted.json"
	chaincodePackageIDFile      = "package-id"
	chaincodeChaincodeIDFile    = "chaincode-id"
	chaincodePackageHashFile    = "package-hash"
	chaincodeQueryInstalledFile = "queryinstalled.json"
	chaincodeQueryApprovedFile  = "queryapproved.json"
	chaincodeQueryCommittedFile = "querycommitted.json"
	chaincodePackageArchiveMode = 0o644

	chaincodeWorkDir         = "/fabricops/chaincode"
	chaincodePackageInputDir = chaincodeWorkDir + "/package"
	chaincodePackageBuildDir = chaincodeWorkDir + "/build"
	chaincodeOutputDir       = chaincodeWorkDir + "/output"
	chaincodeAdminMSPPath    = chaincodeWorkDir + "/crypto/msp"
	chaincodeAdminTLSPath    = chaincodeWorkDir + "/crypto/tls"
	chaincodeCollectionsDir  = chaincodeWorkDir + "/collections"
	chaincodeCollectionsPath = chaincodeCollectionsDir + "/" + chaincodeCollectionsKey

	chaincodePackageVolumeName = "chaincode-package"
	chaincodeOutputVolumeName  = "chaincode-output"
	chaincodeAdminMSPVolume    = "admin-msp"
	chaincodeAdminTLSVolume    = "admin-tls"
	chaincodeCollectionsVolume = "chaincode-collections"

	installChaincodeContainer        = "install-chaincode-package"
	publishChaincodeInstallContainer = "publish-chaincode-package-id"
	approveChaincodeContainer        = "approve-chaincode-definition"
	commitChaincodeContainer         = "commit-chaincode-definition"
	chaincodeServerContainer         = "chaincode"

	envChaincodePackageIDConfigMap       = "FABRICOPS_CHAINCODE_PACKAGE_ID_CONFIGMAP"
	envChaincodeLifecycleResultConfigMap = "FABRICOPS_CHAINCODE_LIFECYCLE_RESULT_CONFIGMAP"
	envChaincodeLifecycleResultKey       = "FABRICOPS_CHAINCODE_LIFECYCLE_RESULT_KEY"
	envChaincodeLifecycleResultFile      = "FABRICOPS_CHAINCODE_LIFECYCLE_RESULT_FILE"
	envChaincodeChannel                  = "FABRICOPS_CHAINCODE_CHANNEL"
	envChaincodeName                     = "FABRICOPS_CHAINCODE_NAME"
	envChaincodePeer                     = "FABRICOPS_CHAINCODE_PEER"
	envCCAASChaincodeID                  = "CHAINCODE_ID"
	envCCAASCoreChaincodeIDName          = "CORE_CHAINCODE_ID_NAME"
	envCCAASChaincodeServerAddress       = "CHAINCODE_SERVER_ADDRESS"
	envCCAASCoreChaincodeAddress         = "CORE_CHAINCODE_ADDRESS"

	chaincodePeerHostnameTemplate    = "{{.peer_hostname}}"
	chaincodePeerHostnamePlaceholder = "fabricops-peer-hostname"
	annotationCollectionConfigHash   = "fabricops.io/collection-config-hash"
)

type chaincodePackageMetadata struct {
	Type  string `json:"type"`
	Label string `json:"label"`
}

type chaincodeConnection struct {
	Address     string `json:"address"`
	DialTimeout string `json:"dial_timeout"`
	TLSRequired bool   `json:"tls_required"`
}

type chaincodeCollectionConfig struct {
	Name              string                                      `json:"name"`
	Policy            string                                      `json:"policy"`
	RequiredPeerCount int32                                       `json:"requiredPeerCount"`
	MaxPeerCount      int32                                       `json:"maxPeerCount"`
	BlockToLive       int64                                       `json:"blockToLive"`
	MemberOnlyRead    bool                                        `json:"memberOnlyRead"`
	MemberOnlyWrite   bool                                        `json:"memberOnlyWrite"`
	EndorsementPolicy *chaincodeCollectionEndorsementPolicyConfig `json:"endorsementPolicy,omitempty"`
}

type chaincodeCollectionEndorsementPolicyConfig struct {
	SignaturePolicy     string `json:"signaturePolicy,omitempty"`
	ChannelConfigPolicy string `json:"channelConfigPolicy,omitempty"`
}

type chaincodeCouchDBIndexConfig struct {
	Index          chaincodeCouchDBIndexFields `json:"index"`
	DesignDocument string                      `json:"ddoc,omitempty"`
	Name           string                      `json:"name"`
	Type           string                      `json:"type"`
}

type chaincodeCouchDBIndexFields struct {
	Fields []string `json:"fields"`
}

type chaincodeLifecyclePeer struct {
	org       fabricopsv1alpha1.Org
	peerName  string
	namespace string
	address   string
	tlsRootCA []byte
}

func (r *FabricNetworkReconciler) reconcileChaincodes(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channelStatuses []fabricopsv1alpha1.ChannelStatus,
) ([]fabricopsv1alpha1.ChaincodeStatus, error) {
	statuses := make([]fabricopsv1alpha1.ChaincodeStatus, 0, len(net.Spec.Chaincodes))
	channels := channelsByName(net)
	orgs := orgsByName(net)
	seen := map[string]struct{}{}

	for _, chaincode := range net.Spec.Chaincodes {
		status := fabricopsv1alpha1.ChaincodeStatus{
			Name:         chaincode.Name,
			Channel:      chaincode.Channel,
			Version:      chaincode.Version,
			PackageLabel: chaincodePackageLabel(chaincode),
			Sequence:     chaincodeSequence(chaincode),
		}
		messages := []string{}
		lifecycleMessage := ""
		approvalPeers := map[string]string{}
		channelReady := false
		collectionConfigJSON := ""
		collectionConfigHash := ""
		desiredWorkloads := map[string]struct{}{}

		key := chaincode.Channel + "/" + chaincode.Name
		if _, ok := seen[key]; ok {
			messages = append(messages, "Chaincode name must be unique within a channel")
		}
		seen[key] = struct{}{}

		if strings.TrimSpace(chaincode.Name) == "" {
			messages = append(messages, "Chaincode name is required")
		}
		if strings.TrimSpace(chaincode.Version) == "" {
			messages = append(messages, "Chaincode version is required")
		}
		if strings.TrimSpace(chaincode.Image) == "" {
			messages = append(messages, "Chaincode image is required for CCaaS")
		}

		channel, ok := channels[chaincode.Channel]
		if strings.TrimSpace(chaincode.Channel) == "" {
			messages = append(messages, "Chaincode channel is required")
		} else if !ok {
			messages = append(messages, "Unknown channel")
		} else {
			channelReady = channelReadyForChaincode(channelStatuses, chaincode.Channel)
			approvalPeers = chaincodeApprovalPeers(channel)
			if len(chaincode.PrivateData) > 0 {
				var err error
				collectionConfigJSON, collectionConfigHash, err = renderChaincodeCollectionConfig(net, channel, chaincode)
				if err != nil {
					return statuses, err
				}
				status.CollectionConfigMap = chaincodeCollectionConfigMapName(chaincode)
				status.CollectionConfigHash = collectionConfigHash
			}
		}

		if ok {
			for _, channelOrg := range channel.Orgs {
				org, orgKnown := orgs[channelOrg.Name]
				for _, peerName := range channelOrg.Peers {
					target := fabricopsv1alpha1.ChaincodeTargetStatus{
						OrgName:  channelOrg.Name,
						PeerName: peerName,
					}
					status.PackageMetadata.Desired++

					if !orgKnown {
						target.Message = "Unknown org"
						status.Targets = append(status.Targets, target)
						continue
					}

					target.Namespace = orgNamespaceName(net, org)
					target.WorkloadName = chaincodeServiceName(chaincode, org, peerName)
					target.Workload.Desired = chaincodeReplicas(chaincode)
					target.ServiceName = chaincodeServiceName(chaincode, org, peerName)
					target.Address = chaincodeConnectionAddress(target.ServiceName, target.Namespace, chaincode)
					target.PackageConfigMapName = chaincodePackageConfigMapName(chaincode, org)
					target.PackageIDConfigMapName = chaincodePackageIDConfigMapName(chaincode, org, peerName)
					target.InstallJobName = chaincodeInstallJobName(chaincode, org, peerName)

					if !peerDeclared(org, peerName) {
						target.Message = "Unknown peer"
						status.Targets = append(status.Targets, target)
						continue
					}
					desiredWorkloads[target.WorkloadName] = struct{}{}
					if len(messages) > 0 {
						target.Message = "Waiting for valid chaincode configuration"
						status.Targets = append(status.Targets, target)
						continue
					}
					status.Workloads.Desired += target.Workload.Desired

					configMap, err := buildChaincodePackageConfigMap(net, chaincode, org)
					if err != nil {
						return statuses, err
					}
					if err := r.ensureConfigMap(ctx, configMap); err != nil {
						return statuses, err
					}
					if len(chaincode.PrivateData) > 0 {
						collectionConfigMap := buildChaincodeCollectionConfigMap(
							net,
							chaincode,
							org,
							collectionConfigJSON,
							collectionConfigHash,
						)
						if err := r.ensureConfigMap(ctx, collectionConfigMap); err != nil {
							return statuses, err
						}
					}

					target.PackageMetadataReady = true
					target.Message = "Package metadata generated"
					status.PackageMetadata.Ready++

					status.Installed.Desired++
					if channelReady {
						if err := r.ensureChaincodeInstallRBAC(ctx, net, org, chaincode, peerName); err != nil {
							return statuses, err
						}

						installed, packageID, chaincodeID, message, err := r.chaincodeInstallReadiness(
							ctx,
							target.Namespace,
							target.PackageIDConfigMapName,
							target.InstallJobName,
						)
						if err != nil {
							return statuses, err
						}

						target.PackageID = packageID
						target.ChaincodeID = chaincodeID
						target.Installed = installed
						target.Message = message

						if installed {
							status.Installed.Ready++
							if err := r.ensureChaincodeWorkload(ctx, net, chaincode, org, peerName, chaincodeID); err != nil {
								return statuses, err
							}
							workload, workloadReady, workloadMessage, err := r.chaincodeWorkloadReadiness(ctx, target.Namespace, target.WorkloadName)
							if err != nil {
								return statuses, err
							}
							target.Workload = workload
							target.WorkloadReady = workloadReady
							status.Workloads.Ready += workload.Ready
							if !workloadReady && workloadMessage != "" {
								target.Message = workloadMessage
							}
						} else {
							if err := r.ensureJob(ctx, buildChaincodeInstallJob(net, chaincode, org, peerName)); err != nil {
								return statuses, err
							}
						}

						if approvalPeers[channelOrg.Name] == peerName {
							status.Approved.Desired++
							if installed {
								target.ApproveJobName = chaincodeApproveJobName(chaincode, org, packageID)

								orderer, ok := chaincodeLifecycleOrderer(net)
								if !ok {
									lifecycleMessage = "Waiting for an orderer before lifecycle approval"
								} else {
									if err := r.ensureChaincodeApproveInputs(ctx, net, channel, chaincode, org, target.Namespace, orderer); err != nil {
										return statuses, err
									}

									approved, message, err := r.chaincodeApproveReadiness(
										ctx,
										target.Namespace,
										chaincodeApproveResultConfigMapName(chaincode, org, packageID),
										target.ApproveJobName,
										org,
										chaincodeSequence(chaincode),
									)
									if err != nil {
										return statuses, err
									}
									if approved {
										target.Approved = true
										target.Message = "Chaincode definition approved"
										status.Approved.Ready++
									} else {
										if message != "" {
											target.Message = message
										}
										if err := r.ensureJob(ctx, buildChaincodeApproveJob(net, channel, chaincode, org, peerName, packageID, orderer)); err != nil {
											return statuses, err
										}
									}
								}
							}
						}
					}
					status.Targets = append(status.Targets, target)
				}
			}
		}

		if err := r.cleanupRemovedChaincodeWorkloads(ctx, net, chaincode, ok && len(messages) == 0, desiredWorkloads); err != nil {
			return statuses, err
		}

		status.PackageMetadataReady = status.PackageMetadata.Desired > 0 &&
			status.PackageMetadata.Ready >= status.PackageMetadata.Desired
		status.InstalledReady = status.Installed.Desired > 0 &&
			status.Installed.Ready >= status.Installed.Desired
		status.WorkloadsReady = status.Workloads.Desired > 0 &&
			status.Workloads.Ready >= status.Workloads.Desired
		status.ApprovedReady = status.Approved.Desired > 0 &&
			status.Approved.Ready >= status.Approved.Desired
		if ok && len(messages) == 0 && status.ApprovedReady {
			packageID := firstChaincodePackageID(status.Targets)
			if packageID == "" {
				lifecycleMessage = "Waiting for chaincode package ID before lifecycle commit"
			} else {
				status.CommitJobName = chaincodeCommitJobName(chaincode, packageID)
				committed, message, err := r.reconcileChaincodeCommit(ctx, net, channel, chaincode, packageID)
				if err != nil {
					return statuses, err
				}
				status.Committed = committed
				if message != "" {
					lifecycleMessage = message
				}
			}
		}
		status.Ready = status.Committed && status.WorkloadsReady
		status.Message = chaincodeStatusMessage(status, messages, channelReady, lifecycleMessage)
		statuses = append(statuses, status)
	}

	return statuses, nil
}

func (r *FabricNetworkReconciler) cleanupRemovedChaincodeWorkloads(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	chaincode fabricopsv1alpha1.Chaincode,
	enabled bool,
	desiredWorkloads map[string]struct{},
) error {
	if !enabled {
		return nil
	}

	for _, org := range net.Spec.Orgs {
		namespace := orgNamespaceName(net, org)
		selector := client.MatchingLabels{
			labelFabricNetwork:          sanitizeName(net.Name),
			labelFabricNetworkNamespace: sanitizeName(net.Namespace),
			labelOrg:                    sanitizeName(org.Organization.Name),
			labelComponent:              componentChaincode,
			labelChannel:                sanitizeName(chaincode.Channel),
			labelChaincode:              sanitizeName(chaincode.Name),
		}
		expectedOwner := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   namespace,
				Labels:      chaincodeLabels(net, org, chaincode.Channel, chaincode.Name),
				Annotations: resourceAnnotations(net, org),
			},
		}

		var deployments appsv1.DeploymentList
		if err := r.List(ctx, &deployments, client.InNamespace(namespace), selector); err != nil {
			return err
		}
		for i := range deployments.Items {
			deployment := &deployments.Items[i]
			if _, ok := desiredWorkloads[deployment.Name]; ok {
				continue
			}
			expectedOwner.Name = deployment.Name
			if err := r.deleteOwnedObject(ctx, deployment, expectedOwner); err != nil {
				return err
			}
		}

		var services corev1.ServiceList
		if err := r.List(ctx, &services, client.InNamespace(namespace), selector); err != nil {
			return err
		}
		for i := range services.Items {
			service := &services.Items[i]
			if _, ok := desiredWorkloads[service.Name]; ok {
				continue
			}
			expectedOwner.Name = service.Name
			if err := r.deleteOwnedObject(ctx, service, expectedOwner); err != nil {
				return err
			}
		}
	}

	return nil
}

func buildChaincodePackageConfigMap(
	net *fabricopsv1alpha1.FabricNetwork,
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
) (*corev1.ConfigMap, error) {
	label := chaincodePackageLabel(chaincode)
	address, err := chaincodeConnectionAddressTemplate(chaincode, org, orgNamespaceName(net, org))
	if err != nil {
		return nil, err
	}

	metadataJSON, err := marshalChaincodeJSON(chaincodePackageMetadata{
		Type:  "ccaas",
		Label: label,
	})
	if err != nil {
		return nil, err
	}

	connectionJSON, err := marshalChaincodeJSON(chaincodeConnection{
		Address:     address,
		DialTimeout: chaincodeDialTimeout(chaincode),
		TLSRequired: false,
	})
	if err != nil {
		return nil, err
	}
	packageFile := label + ".tar.gz"
	couchDBIndexFiles, err := renderChaincodeCouchDBIndexes(chaincode)
	if err != nil {
		return nil, err
	}
	packageArchive, err := buildChaincodePackageArchive(metadataJSON, connectionJSON, couchDBIndexFiles)
	if err != nil {
		return nil, err
	}

	labels := chaincodeLabels(net, org, chaincode.Channel, chaincode.Name)

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        chaincodePackageConfigMapName(chaincode, org),
			Namespace:   orgNamespaceName(net, org),
			Labels:      labels,
			Annotations: resourceAnnotations(net, org),
		},
		Data: map[string]string{
			chaincodeMetadataKey:       metadataJSON,
			chaincodeConnectionKey:     connectionJSON,
			chaincodePackageLabelKey:   label,
			chaincodePackageFileKey:    packageFile,
			chaincodeConnectionAddrKey: address,
		},
		BinaryData: map[string][]byte{
			packageFile: packageArchive,
		},
	}, nil
}

func buildChaincodeCollectionConfigMap(
	net *fabricopsv1alpha1.FabricNetwork,
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	collectionConfigJSON string,
	collectionConfigHash string,
) *corev1.ConfigMap {
	annotations := resourceAnnotations(net, org)
	annotations[annotationCollectionConfigHash] = collectionConfigHash

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        chaincodeCollectionConfigMapName(chaincode),
			Namespace:   orgNamespaceName(net, org),
			Labels:      chaincodeLabels(net, org, chaincode.Channel, chaincode.Name),
			Annotations: annotations,
		},
		Data: map[string]string{
			chaincodeCollectionsKey: collectionConfigJSON,
		},
	}
}

func renderChaincodeCollectionConfig(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	chaincode fabricopsv1alpha1.Chaincode,
) (string, string, error) {
	orgs := orgsByName(net)
	channelPeerCounts := channelPeerCountsByOrg(channel)
	collections := make([]chaincodeCollectionConfig, 0, len(chaincode.PrivateData))

	for _, collection := range chaincode.PrivateData {
		config, err := renderChaincodeCollection(collection, orgs, channelPeerCounts)
		if err != nil {
			return "", "", err
		}
		collections = append(collections, config)
	}

	collectionConfigJSON, err := marshalChaincodeJSON(collections)
	if err != nil {
		return "", "", err
	}

	return collectionConfigJSON, shortSHA256(collectionConfigJSON), nil
}

func renderChaincodeCollection(
	collection fabricopsv1alpha1.PrivateDataCollection,
	orgs map[string]fabricopsv1alpha1.Org,
	channelPeerCounts map[string]int,
) (chaincodeCollectionConfig, error) {
	policy := strings.TrimSpace(collection.Policy)
	if policy == "" {
		policyParts := make([]string, 0, len(collection.OrgNames))
		for _, orgName := range collection.OrgNames {
			orgName = strings.TrimSpace(orgName)
			org, ok := orgs[orgName]
			if !ok {
				return chaincodeCollectionConfig{}, fmt.Errorf("private data collection %q references unknown org %q", collection.Name, orgName)
			}
			policyParts = append(policyParts, fmt.Sprintf("'%s.member'", org.Organization.MSPName))
		}
		policy = fmt.Sprintf("OR(%s)", strings.Join(policyParts, ","))
	}

	authorizedPeers := 0
	for _, orgName := range collection.OrgNames {
		orgName = strings.TrimSpace(orgName)
		authorizedPeers += channelPeerCounts[orgName]
	}

	requiredPeerCount := int32(0)
	if collection.RequiredPeerCount != nil {
		requiredPeerCount = *collection.RequiredPeerCount
	}
	maxPeerCount := int32(max(authorizedPeers-1, 0))
	if collection.MaxPeerCount != nil {
		maxPeerCount = *collection.MaxPeerCount
	}
	blockToLive := int64(0)
	if collection.BlockToLive != nil {
		blockToLive = *collection.BlockToLive
	}
	memberOnlyRead := true
	if collection.MemberOnlyRead != nil {
		memberOnlyRead = *collection.MemberOnlyRead
	}
	memberOnlyWrite := true
	if collection.MemberOnlyWrite != nil {
		memberOnlyWrite = *collection.MemberOnlyWrite
	}

	config := chaincodeCollectionConfig{
		Name:              collection.Name,
		Policy:            policy,
		RequiredPeerCount: requiredPeerCount,
		MaxPeerCount:      maxPeerCount,
		BlockToLive:       blockToLive,
		MemberOnlyRead:    memberOnlyRead,
		MemberOnlyWrite:   memberOnlyWrite,
	}
	if collection.EndorsementPolicy != nil {
		signaturePolicy := strings.TrimSpace(collection.EndorsementPolicy.SignaturePolicy)
		channelConfigPolicy := strings.TrimSpace(collection.EndorsementPolicy.ChannelConfigPolicy)
		if signaturePolicy == "" && channelConfigPolicy == "" {
			return config, nil
		}
		config.EndorsementPolicy = &chaincodeCollectionEndorsementPolicyConfig{
			SignaturePolicy:     signaturePolicy,
			ChannelConfigPolicy: channelConfigPolicy,
		}
	}

	return config, nil
}

func renderChaincodeCouchDBIndexes(chaincode fabricopsv1alpha1.Chaincode) (map[string][]byte, error) {
	indexFiles := map[string][]byte{}
	for _, index := range chaincode.CouchDBIndexes {
		name := strings.TrimSpace(index.Name)
		if name == "" {
			return nil, fmt.Errorf("chaincode %q has a CouchDB index without a name", chaincode.Name)
		}
		if len(index.Fields) == 0 {
			return nil, fmt.Errorf("CouchDB index %q must declare at least one field", name)
		}

		fields := make([]string, 0, len(index.Fields))
		for _, field := range index.Fields {
			field = strings.TrimSpace(field)
			if field == "" {
				return nil, fmt.Errorf("CouchDB index %q has an empty field", name)
			}
			fields = append(fields, field)
		}

		indexJSON, err := marshalChaincodeJSON(chaincodeCouchDBIndexConfig{
			Index: chaincodeCouchDBIndexFields{
				Fields: fields,
			},
			DesignDocument: strings.TrimSpace(index.DesignDocument),
			Name:           name,
			Type:           "json",
		})
		if err != nil {
			return nil, err
		}

		path := chaincodeCouchDBIndexPath(index)
		if _, exists := indexFiles[path]; exists {
			return nil, fmt.Errorf("duplicate CouchDB index package path %q", path)
		}
		indexFiles[path] = []byte(indexJSON)
	}

	return indexFiles, nil
}

func buildChaincodePackageArchive(metadataJSON, connectionJSON string, couchDBIndexFiles map[string][]byte) ([]byte, error) {
	codeFiles := map[string][]byte{
		chaincodeConnectionKey: []byte(connectionJSON),
	}
	maps.Copy(codeFiles, couchDBIndexFiles)

	codeArchive, err := gzipTar(codeFiles)
	if err != nil {
		return nil, err
	}

	return gzipTar(map[string][]byte{
		chaincodeMetadataKey: []byte(metadataJSON),
		"code.tar.gz":        codeArchive,
	})
}

func chaincodeCouchDBIndexPath(index fabricopsv1alpha1.CouchDBIndex) string {
	fileName := sanitizeName(index.Name) + ".json"
	collection := strings.TrimSpace(index.Collection)
	if collection != "" {
		return fmt.Sprintf("%s/statedb/couchdb/collections/%s/indexes/%s", chaincodePackageMetadataDir, collection, fileName)
	}
	return fmt.Sprintf("%s/statedb/couchdb/indexes/%s", chaincodePackageMetadataDir, fileName)
}

func gzipTar(files map[string][]byte) ([]byte, error) {
	var buffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&buffer)
	gzipWriter.ModTime = time.Unix(0, 0).UTC()

	tarWriter := tar.NewWriter(gzipWriter)
	for _, name := range sortedKeys(files) {
		contents := files[name]
		header := &tar.Header{
			Name:    name,
			Mode:    chaincodePackageArchiveMode,
			Size:    int64(len(contents)),
			ModTime: time.Unix(0, 0).UTC(),
			Format:  tar.FormatUSTAR,
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			return nil, err
		}
		if _, err := tarWriter.Write(contents); err != nil {
			return nil, err
		}
	}
	if err := tarWriter.Close(); err != nil {
		return nil, err
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func sortedKeys(values map[string][]byte) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func (r *FabricNetworkReconciler) ensureChaincodeWorkload(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	peerName string,
	chaincodeID string,
) error {
	if err := r.ensureService(ctx, buildChaincodeService(net, chaincode, org, peerName)); err != nil {
		return err
	}
	return r.ensureDeployment(ctx, buildChaincodeDeployment(net, chaincode, org, peerName, chaincodeID))
}

func buildChaincodeDeployment(
	net *fabricopsv1alpha1.FabricNetwork,
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	peerName string,
	chaincodeID string,
) *appsv1.Deployment {
	namespace := orgNamespaceName(net, org)
	name := chaincodeServiceName(chaincode, org, peerName)
	replicas := chaincodeReplicas(chaincode)
	labels := chaincodePeerLabels(net, org, chaincode, peerName)
	selector := chaincodeWorkloadSelector(net, org, chaincode, peerName)
	containerPort := chaincodeContainerPort(chaincode)

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: resourceAnnotations(net, org),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: selector,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: resourceAnnotations(net, org),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            chaincodeServerContainer,
							Image:           chaincode.Image,
							ImagePullPolicy: chaincodeImagePullPolicy(chaincode),
							Env:             chaincodeContainerEnv(chaincode, chaincodeID),
							Ports: []corev1.ContainerPort{
								{
									Name:          "chaincode",
									ContainerPort: containerPort,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							ReadinessProbe: tcpReadinessProbe(containerPort),
							LivenessProbe:  tcpLivenessProbe(containerPort),
							Resources:      chaincodeResourceRequirements(chaincode),
						},
					},
				},
			},
		},
	}
}

func buildChaincodeService(
	net *fabricopsv1alpha1.FabricNetwork,
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	peerName string,
) *corev1.Service {
	name := chaincodeServiceName(chaincode, org, peerName)
	servicePort := chaincodeServicePort(chaincode)
	containerPort := chaincodeContainerPort(chaincode)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   orgNamespaceName(net, org),
			Labels:      chaincodePeerLabels(net, org, chaincode, peerName),
			Annotations: resourceAnnotations(net, org),
		},
		Spec: corev1.ServiceSpec{
			Selector: chaincodeWorkloadSelector(net, org, chaincode, peerName),
			Ports: []corev1.ServicePort{
				{
					Name:       "chaincode",
					Port:       servicePort,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt32(containerPort),
				},
			},
		},
	}
}

func (r *FabricNetworkReconciler) chaincodeWorkloadReadiness(
	ctx context.Context,
	namespace string,
	deploymentName string,
) (fabricopsv1alpha1.WorkloadStatus, bool, string, error) {
	workload, err := r.deploymentWorkloadStatus(ctx, namespace, deploymentName)
	if apierrors.IsNotFound(err) {
		return fabricopsv1alpha1.WorkloadStatus{}, false, "Waiting for chaincode workload Deployment", nil
	}
	if err != nil {
		return fabricopsv1alpha1.WorkloadStatus{}, false, "", err
	}
	if workload.Desired > 0 && workloadReady(workload) {
		return workload, true, "", nil
	}

	return workload, false, "Waiting for chaincode workload Deployment", nil
}

func (r *FabricNetworkReconciler) ensureChaincodeInstallRBAC(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	chaincode fabricopsv1alpha1.Chaincode,
	peerName string,
) error {
	namespace := orgNamespaceName(net, org)
	if err := r.ensureServiceAccount(ctx, buildChaincodeInstallServiceAccount(net, org, chaincode, peerName)); err != nil {
		return err
	}
	if err := r.ensureRole(ctx, buildChaincodeInstallRole(net, org, chaincode, peerName, namespace)); err != nil {
		return err
	}
	return r.ensureRoleBinding(ctx, buildChaincodeInstallRoleBinding(net, org, chaincode, peerName, namespace))
}

func buildChaincodeInstallServiceAccount(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	chaincode fabricopsv1alpha1.Chaincode,
	peerName string,
) *corev1.ServiceAccount {
	namespace := orgNamespaceName(net, org)
	name := chaincodeInstallerName(chaincode, org, peerName)

	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      chaincodePeerLabels(net, org, chaincode, peerName),
			Annotations: resourceAnnotations(net, org),
		},
	}
}

func buildChaincodeInstallRole(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	chaincode fabricopsv1alpha1.Chaincode,
	peerName string,
	namespace string,
) *rbacv1.Role {
	name := chaincodeInstallerName(chaincode, org, peerName)

	return &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      chaincodePeerLabels(net, org, chaincode, peerName),
			Annotations: resourceAnnotations(net, org),
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     []string{"get", "create", "update", "patch"},
			},
		},
	}
}

func buildChaincodeInstallRoleBinding(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	chaincode fabricopsv1alpha1.Chaincode,
	peerName string,
	namespace string,
) *rbacv1.RoleBinding {
	name := chaincodeInstallerName(chaincode, org, peerName)

	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      chaincodePeerLabels(net, org, chaincode, peerName),
			Annotations: resourceAnnotations(net, org),
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      name,
				Namespace: namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     name,
		},
	}
}

func buildChaincodeInstallJob(
	net *fabricopsv1alpha1.FabricNetwork,
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	peerName string,
) *batchv1.Job {
	namespace := orgNamespaceName(net, org)
	labels := chaincodePeerLabels(net, org, chaincode, peerName)
	annotations := resourceAnnotations(net, org)
	packageFile := chaincodePackageLabel(chaincode) + ".tar.gz"
	backoffLimit := int32(4)
	volumeMounts := []corev1.VolumeMount{
		{Name: chaincodePackageVolumeName, MountPath: chaincodePackageInputDir, ReadOnly: true},
		{Name: chaincodeOutputVolumeName, MountPath: chaincodeOutputDir},
		{Name: chaincodeAdminMSPVolume, MountPath: chaincodeAdminMSPPath, ReadOnly: true},
	}
	volumes := []corev1.Volume{
		{
			Name: chaincodePackageVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: chaincodePackageConfigMapName(chaincode, org),
					},
					Items: []corev1.KeyToPath{
						{Key: chaincodeMetadataKey, Path: chaincodeMetadataKey},
						{Key: chaincodeConnectionKey, Path: chaincodeConnectionKey},
						{Key: chaincodePackageLabelKey, Path: chaincodePackageLabelKey},
						{Key: chaincodePackageFileKey, Path: chaincodePackageFileKey},
						{Key: packageFile, Path: packageFile},
					},
				},
			},
		},
		{
			Name: chaincodeOutputVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		{
			Name: chaincodeAdminMSPVolume,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  identitySecretName(adminIdentityName(org), secretKindMSP),
					Items:       mspSecretItems(net.Spec.Global.TLS),
					DefaultMode: secretVolumeDefaultMode(),
				},
			},
		},
	}

	if net.Spec.Global.TLS {
		volumes = append(volumes, corev1.Volume{
			Name: chaincodeAdminTLSVolume,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  identitySecretName(adminIdentityName(org), secretKindTLS),
					Items:       adminTLSSecretItems(),
					DefaultMode: secretVolumeDefaultMode(),
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      chaincodeAdminTLSVolume,
			MountPath: chaincodeAdminTLSPath,
			ReadOnly:  true,
		})
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        chaincodeInstallJobName(chaincode, org, peerName),
			Namespace:   namespace,
			Labels:      labels,
			Annotations: succeededJobCleanupAnnotations(annotations),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: resourceAnnotations(net, org),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: chaincodeInstallerName(chaincode, org, peerName),
					RestartPolicy:      corev1.RestartPolicyNever,
					Volumes:            volumes,
					InitContainers: []corev1.Container{
						{
							Name:         installChaincodeContainer,
							Image:        fabricToolsImage(net.Spec.Global.FabricVersion),
							Command:      []string{"sh", "-ec", installChaincodePackageScript(org, peerName, namespace, net.Spec.Global.TLS)},
							Resources:    componentResourceRequirements(componentPeer),
							VolumeMounts: volumeMounts,
						},
					},
					Containers: []corev1.Container{
						{
							Name:      publishChaincodeInstallContainer,
							Image:     kubectlImage(),
							Command:   []string{"sh", "-ec", publishChaincodeInstallScript()},
							Env:       publishChaincodeInstallEnv(chaincode, org, peerName),
							Resources: componentResourceRequirements(componentKubectl),
							VolumeMounts: []corev1.VolumeMount{
								{Name: chaincodeOutputVolumeName, MountPath: chaincodeOutputDir},
							},
						},
					},
				},
			},
		},
	}
}

func (r *FabricNetworkReconciler) ensureChaincodeApproveInputs(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	namespace string,
	orderer ordererInstance,
) error {
	if len(chaincode.PrivateData) > 0 {
		collectionConfigJSON, collectionConfigHash, err := renderChaincodeCollectionConfig(net, channel, chaincode)
		if err != nil {
			return err
		}
		if err := r.ensureConfigMap(ctx, buildChaincodeCollectionConfigMap(net, chaincode, org, collectionConfigJSON, collectionConfigHash)); err != nil {
			return err
		}
	}

	if !net.Spec.Global.TLS {
		return nil
	}
	source := client.ObjectKey{
		Namespace: orderer.namespace,
		Name:      identitySecretName(orderer.name, secretKindTLS),
	}
	return r.ensureCopiedSecret(
		ctx,
		source,
		namespace,
		channelOrdererTLSSecretName(channel.Name, orderer.name),
		channelLabels(net, org, channel.Name),
		resourceAnnotations(net, org),
	)
}

func buildChaincodeApproveJob(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	peerName string,
	packageID string,
	orderer ordererInstance,
) *batchv1.Job {
	namespace := orgNamespaceName(net, org)
	labels := chaincodePeerLabels(net, org, chaincode, peerName)
	labels[labelWorkload] = sanitizeName(adminIdentityName(org))
	annotations := resourceAnnotations(net, org)
	backoffLimit := int32(4)
	volumeMounts := []corev1.VolumeMount{
		{Name: chaincodeAdminMSPVolume, MountPath: chaincodeAdminMSPPath, ReadOnly: true},
		{Name: chaincodeOutputVolumeName, MountPath: chaincodeOutputDir},
	}
	volumes := []corev1.Volume{
		{
			Name: chaincodeOutputVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		{
			Name: chaincodeAdminMSPVolume,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  identitySecretName(adminIdentityName(org), secretKindMSP),
					Items:       mspSecretItems(net.Spec.Global.TLS),
					DefaultMode: secretVolumeDefaultMode(),
				},
			},
		},
	}

	if len(chaincode.PrivateData) > 0 {
		volumes = append(volumes, corev1.Volume{
			Name: chaincodeCollectionsVolume,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: chaincodeCollectionConfigMapName(chaincode),
					},
					Items: []corev1.KeyToPath{
						{Key: chaincodeCollectionsKey, Path: chaincodeCollectionsKey},
					},
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      chaincodeCollectionsVolume,
			MountPath: chaincodeCollectionsDir,
			ReadOnly:  true,
		})
	}

	if net.Spec.Global.TLS {
		volumes = append(volumes,
			corev1.Volume{
				Name: chaincodeAdminTLSVolume,
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName:  identitySecretName(adminIdentityName(org), secretKindTLS),
						Items:       adminTLSSecretItems(),
						DefaultMode: secretVolumeDefaultMode(),
					},
				},
			},
			corev1.Volume{
				Name: channelOrdererTLSVolumeName(orderer.name),
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName:  channelOrdererTLSSecretName(channel.Name, orderer.name),
						Items:       tlsSecretItems(),
						DefaultMode: secretVolumeDefaultMode(),
					},
				},
			},
		)
		volumeMounts = append(volumeMounts,
			corev1.VolumeMount{Name: chaincodeAdminTLSVolume, MountPath: chaincodeAdminTLSPath, ReadOnly: true},
			corev1.VolumeMount{Name: channelOrdererTLSVolumeName(orderer.name), MountPath: chaincodeOrdererTLSPath(orderer.name), ReadOnly: true},
		)
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        chaincodeApproveJobName(chaincode, org, packageID),
			Namespace:   namespace,
			Labels:      labels,
			Annotations: succeededJobCleanupAnnotations(annotations),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: resourceAnnotations(net, org),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: chaincodeInstallerName(chaincode, org, peerName),
					RestartPolicy:      corev1.RestartPolicyNever,
					Volumes:            volumes,
					InitContainers: []corev1.Container{
						{
							Name:  approveChaincodeContainer,
							Image: fabricToolsImage(net.Spec.Global.FabricVersion),
							Command: []string{"sh", "-ec", approveChaincodeDefinitionScript(
								net,
								channel,
								chaincode,
								org,
								peerName,
								namespace,
								packageID,
								orderer,
							)},
							Resources:    componentResourceRequirements(componentPeer),
							VolumeMounts: volumeMounts,
						},
					},
					Containers: []corev1.Container{
						{
							Name:      publishChaincodeLifecycleResultContainerName(approveChaincodeContainer),
							Image:     kubectlImage(),
							Command:   []string{"sh", "-ec", publishChaincodeLifecycleResultScript()},
							Env:       publishChaincodeLifecycleResultEnv(chaincode, peerName, chaincodeApproveResultConfigMapName(chaincode, org, packageID), chaincodeQueryApprovedKey, chaincodeQueryApprovedFile),
							Resources: componentResourceRequirements(componentKubectl),
							VolumeMounts: []corev1.VolumeMount{
								{Name: chaincodeOutputVolumeName, MountPath: chaincodeOutputDir},
							},
						},
					},
				},
			},
		},
	}
}

func (r *FabricNetworkReconciler) chaincodeApproveReadiness(
	ctx context.Context,
	namespace string,
	resultConfigMapName string,
	approveJobName string,
	org fabricopsv1alpha1.Org,
	sequence int32,
) (bool, string, error) {
	resultReady, resultMessage, err := r.chaincodeLifecycleResultReadiness(
		ctx,
		namespace,
		resultConfigMapName,
		chaincodeQueryApprovedKey,
		sequence,
		"chaincode approve",
	)
	if err != nil || resultReady {
		return resultReady, resultMessage, err
	}

	var job batchv1.Job
	err = r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: approveJobName}, &job)
	if apierrors.IsNotFound(err) {
		return false, "Waiting for chaincode approve Job", nil
	}
	if err != nil {
		return false, "", err
	}
	if jobFailed(job) {
		return false, fmt.Sprintf("%s: chaincode approve Job failed", org.Organization.Name), nil
	}
	if jobSucceeded(job) {
		if resultMessage != "" {
			return false, resultMessage, nil
		}
		return false, "Waiting for chaincode approve result ConfigMap", nil
	}

	return false, "Waiting for chaincode approve Job", nil
}

func (r *FabricNetworkReconciler) reconcileChaincodeCommit(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	chaincode fabricopsv1alpha1.Chaincode,
	packageID string,
) (bool, string, error) {
	orderer, ok := chaincodeLifecycleOrderer(net)
	if !ok {
		return false, "Waiting for an orderer before lifecycle commit", nil
	}

	peers := chaincodeLifecyclePeers(net, channel)
	externalPeers, err := r.externalChaincodeLifecyclePeers(ctx, net, channel)
	if err != nil {
		return false, "", err
	}
	peers = append(peers, externalPeers...)
	if len(peers) == 0 {
		return false, "Waiting for target peers before lifecycle commit", nil
	}

	submitter := peers[0]
	namespace := submitter.namespace
	jobName := chaincodeCommitJobName(chaincode, packageID)
	resultConfigMapName := chaincodeCommitResultConfigMapName(chaincode, packageID)

	committed, message, err := r.chaincodeCommitReadiness(
		ctx,
		namespace,
		resultConfigMapName,
		jobName,
		chaincodeSequence(chaincode),
	)
	if err != nil {
		return false, "", err
	}
	if committed {
		return true, "", nil
	}
	if err := r.ensureChaincodeCommitInputs(ctx, net, channel, chaincode, submitter.org, namespace, orderer, peers); err != nil {
		return false, "", err
	}
	if err := r.ensureServiceAccount(ctx, buildChaincodeCommitServiceAccount(net, chaincode, submitter.org, namespace)); err != nil {
		return false, "", err
	}
	if err := r.ensureRole(ctx, buildChaincodeCommitRole(net, chaincode, submitter.org, namespace)); err != nil {
		return false, "", err
	}
	if err := r.ensureRoleBinding(ctx, buildChaincodeCommitRoleBinding(net, chaincode, submitter.org, namespace)); err != nil {
		return false, "", err
	}
	if err := r.ensureJob(ctx, buildChaincodeCommitJob(net, channel, chaincode, packageID, submitter, orderer, peers)); err != nil {
		return false, "", err
	}
	if message != "" {
		return false, message, nil
	}

	return false, "Waiting for chaincode commit Job", nil
}

func (r *FabricNetworkReconciler) ensureChaincodeCommitInputs(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	chaincode fabricopsv1alpha1.Chaincode,
	hostOrg fabricopsv1alpha1.Org,
	namespace string,
	orderer ordererInstance,
	peers []chaincodeLifecyclePeer,
) error {
	labels := chaincodeLabels(net, hostOrg, chaincode.Channel, chaincode.Name)
	annotations := resourceAnnotations(net, hostOrg)

	if len(chaincode.PrivateData) > 0 {
		collectionConfigJSON, collectionConfigHash, err := renderChaincodeCollectionConfig(net, channel, chaincode)
		if err != nil {
			return err
		}
		if err := r.ensureConfigMap(ctx, buildChaincodeCollectionConfigMap(net, chaincode, hostOrg, collectionConfigJSON, collectionConfigHash)); err != nil {
			return err
		}
	}

	if !net.Spec.Global.TLS {
		return nil
	}

	if err := r.ensureCopiedSecret(
		ctx,
		client.ObjectKey{
			Namespace: orderer.namespace,
			Name:      identitySecretName(orderer.name, secretKindTLS),
		},
		namespace,
		channelOrdererTLSSecretName(channel.Name, orderer.name),
		labels,
		annotations,
	); err != nil {
		return err
	}

	for _, peer := range peers {
		if len(peer.tlsRootCA) > 0 {
			desired := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:        chaincodePeerTLSSecretName(chaincode, peer.org, peer.peerName),
					Namespace:   namespace,
					Labels:      labels,
					Annotations: annotations,
				},
				Type: corev1.SecretTypeOpaque,
				Data: map[string][]byte{
					tlsCACertKey: peer.tlsRootCA,
				},
			}
			if err := r.ensureReplicatedSecret(ctx, desired); err != nil {
				return err
			}
			continue
		}
		if err := r.ensureCopiedSecret(
			ctx,
			client.ObjectKey{
				Namespace: peer.namespace,
				Name:      identitySecretName(peer.peerName, secretKindTLS),
			},
			namespace,
			chaincodePeerTLSSecretName(chaincode, peer.org, peer.peerName),
			labels,
			annotations,
		); err != nil {
			return err
		}
	}

	return nil
}

func buildChaincodeCommitServiceAccount(
	net *fabricopsv1alpha1.FabricNetwork,
	chaincode fabricopsv1alpha1.Chaincode,
	hostOrg fabricopsv1alpha1.Org,
	namespace string,
) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:        chaincodeCommitterName(chaincode),
			Namespace:   namespace,
			Labels:      chaincodeLabels(net, hostOrg, chaincode.Channel, chaincode.Name),
			Annotations: resourceAnnotations(net, hostOrg),
		},
	}
}

func buildChaincodeCommitRole(
	net *fabricopsv1alpha1.FabricNetwork,
	chaincode fabricopsv1alpha1.Chaincode,
	hostOrg fabricopsv1alpha1.Org,
	namespace string,
) *rbacv1.Role {
	name := chaincodeCommitterName(chaincode)

	return &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      chaincodeLabels(net, hostOrg, chaincode.Channel, chaincode.Name),
			Annotations: resourceAnnotations(net, hostOrg),
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     []string{"get", "create", "update", "patch"},
			},
		},
	}
}

func buildChaincodeCommitRoleBinding(
	net *fabricopsv1alpha1.FabricNetwork,
	chaincode fabricopsv1alpha1.Chaincode,
	hostOrg fabricopsv1alpha1.Org,
	namespace string,
) *rbacv1.RoleBinding {
	name := chaincodeCommitterName(chaincode)

	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      chaincodeLabels(net, hostOrg, chaincode.Channel, chaincode.Name),
			Annotations: resourceAnnotations(net, hostOrg),
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      name,
				Namespace: namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     name,
		},
	}
}

func buildChaincodeCommitJob(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	chaincode fabricopsv1alpha1.Chaincode,
	packageID string,
	submitter chaincodeLifecyclePeer,
	orderer ordererInstance,
	peers []chaincodeLifecyclePeer,
) *batchv1.Job {
	labels := chaincodeLabels(net, submitter.org, chaincode.Channel, chaincode.Name)
	labels[labelWorkload] = sanitizeName(adminIdentityName(submitter.org))
	annotations := resourceAnnotations(net, submitter.org)
	backoffLimit := int32(4)
	volumeMounts := []corev1.VolumeMount{
		{Name: chaincodeAdminMSPVolume, MountPath: chaincodeAdminMSPPath, ReadOnly: true},
		{Name: chaincodeOutputVolumeName, MountPath: chaincodeOutputDir},
	}
	volumes := []corev1.Volume{
		{
			Name: chaincodeOutputVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		{
			Name: chaincodeAdminMSPVolume,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  identitySecretName(adminIdentityName(submitter.org), secretKindMSP),
					Items:       mspSecretItems(net.Spec.Global.TLS),
					DefaultMode: secretVolumeDefaultMode(),
				},
			},
		},
	}

	if len(chaincode.PrivateData) > 0 {
		volumes = append(volumes, corev1.Volume{
			Name: chaincodeCollectionsVolume,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: chaincodeCollectionConfigMapName(chaincode),
					},
					Items: []corev1.KeyToPath{
						{Key: chaincodeCollectionsKey, Path: chaincodeCollectionsKey},
					},
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      chaincodeCollectionsVolume,
			MountPath: chaincodeCollectionsDir,
			ReadOnly:  true,
		})
	}

	if net.Spec.Global.TLS {
		volumes = append(volumes,
			corev1.Volume{
				Name: chaincodeAdminTLSVolume,
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName:  identitySecretName(adminIdentityName(submitter.org), secretKindTLS),
						Items:       adminTLSSecretItems(),
						DefaultMode: secretVolumeDefaultMode(),
					},
				},
			},
			corev1.Volume{
				Name: channelOrdererTLSVolumeName(orderer.name),
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName:  channelOrdererTLSSecretName(channel.Name, orderer.name),
						Items:       tlsSecretItems(),
						DefaultMode: secretVolumeDefaultMode(),
					},
				},
			},
		)
		volumeMounts = append(volumeMounts,
			corev1.VolumeMount{Name: chaincodeAdminTLSVolume, MountPath: chaincodeAdminTLSPath, ReadOnly: true},
			corev1.VolumeMount{Name: channelOrdererTLSVolumeName(orderer.name), MountPath: chaincodeOrdererTLSPath(orderer.name), ReadOnly: true},
		)

		for _, peer := range peers {
			volumeName := chaincodePeerTLSVolumeName(peer.org, peer.peerName)
			volumes = append(volumes, corev1.Volume{
				Name: volumeName,
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName:  chaincodePeerTLSSecretName(chaincode, peer.org, peer.peerName),
						Items:       chaincodePeerTLSSecretItems(peer),
						DefaultMode: secretVolumeDefaultMode(),
					},
				},
			})
			volumeMounts = append(volumeMounts, corev1.VolumeMount{
				Name:      volumeName,
				MountPath: chaincodePeerTLSPath(peer.org, peer.peerName),
				ReadOnly:  true,
			})
		}
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        chaincodeCommitJobName(chaincode, packageID),
			Namespace:   submitter.namespace,
			Labels:      labels,
			Annotations: succeededJobCleanupAnnotations(annotations),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: resourceAnnotations(net, submitter.org),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: chaincodeCommitterName(chaincode),
					RestartPolicy:      corev1.RestartPolicyNever,
					Volumes:            volumes,
					InitContainers: []corev1.Container{
						{
							Name:  commitChaincodeContainer,
							Image: fabricToolsImage(net.Spec.Global.FabricVersion),
							Command: []string{"sh", "-ec", commitChaincodeDefinitionScript(
								net,
								channel,
								chaincode,
								submitter,
								orderer,
								peers,
							)},
							Resources:    componentResourceRequirements(componentPeer),
							VolumeMounts: volumeMounts,
						},
					},
					Containers: []corev1.Container{
						{
							Name:      publishChaincodeLifecycleResultContainerName(commitChaincodeContainer),
							Image:     kubectlImage(),
							Command:   []string{"sh", "-ec", publishChaincodeLifecycleResultScript()},
							Env:       publishChaincodeLifecycleResultEnv(chaincode, submitter.peerName, chaincodeCommitResultConfigMapName(chaincode, packageID), chaincodeQueryCommittedKey, chaincodeQueryCommittedFile),
							Resources: componentResourceRequirements(componentKubectl),
							VolumeMounts: []corev1.VolumeMount{
								{Name: chaincodeOutputVolumeName, MountPath: chaincodeOutputDir},
							},
						},
					},
				},
			},
		},
	}
}

func (r *FabricNetworkReconciler) chaincodeCommitReadiness(
	ctx context.Context,
	namespace string,
	resultConfigMapName string,
	commitJobName string,
	sequence int32,
) (bool, string, error) {
	resultReady, resultMessage, err := r.chaincodeLifecycleResultReadiness(
		ctx,
		namespace,
		resultConfigMapName,
		chaincodeQueryCommittedKey,
		sequence,
		"chaincode commit",
	)
	if err != nil || resultReady {
		return resultReady, resultMessage, err
	}

	var job batchv1.Job
	err = r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: commitJobName}, &job)
	if apierrors.IsNotFound(err) {
		return false, "Waiting for chaincode commit Job", nil
	}
	if err != nil {
		return false, "", err
	}
	if jobFailed(job) {
		return false, "Chaincode commit Job failed", nil
	}
	if jobSucceeded(job) {
		if resultMessage != "" {
			return false, resultMessage, nil
		}
		return false, "Waiting for chaincode commit result ConfigMap", nil
	}

	return false, "Waiting for chaincode commit Job", nil
}

func (r *FabricNetworkReconciler) chaincodeLifecycleResultReadiness(
	ctx context.Context,
	namespace string,
	configMapName string,
	resultKey string,
	sequence int32,
	operation string,
) (bool, string, error) {
	var result corev1.ConfigMap
	err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: configMapName}, &result)
	if apierrors.IsNotFound(err) {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}

	raw := strings.TrimSpace(result.Data[resultKey])
	if raw == "" {
		return false, fmt.Sprintf("Waiting for %s result ConfigMap data", operation), nil
	}
	if !chaincodeLifecycleResultSequenceMatches(raw, sequence) {
		return false, fmt.Sprintf("Waiting for %s result ConfigMap sequence %d", operation, sequence), nil
	}

	return true, "", nil
}

func chaincodeLifecycleResultSequenceMatches(raw string, sequence int32) bool {
	var payload struct {
		Sequence any `json:"sequence"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return false
	}

	switch value := payload.Sequence.(type) {
	case float64:
		return value == float64(sequence)
	case string:
		return value == fmt.Sprintf("%d", sequence)
	default:
		return false
	}
}

func (r *FabricNetworkReconciler) chaincodeInstallReadiness(
	ctx context.Context,
	namespace string,
	packageIDConfigMapName string,
	installJobName string,
) (bool, string, string, string, error) {
	var result corev1.ConfigMap
	err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: packageIDConfigMapName}, &result)
	if err == nil {
		packageID := strings.TrimSpace(result.Data[chaincodePackageIDKey])
		chaincodeID := strings.TrimSpace(result.Data[chaincodeChaincodeIDKey])
		if chaincodeID == "" {
			chaincodeID = packageID
		}
		if packageID != "" {
			return true, packageID, chaincodeID, "Chaincode package installed", nil
		}
		return false, packageID, chaincodeID, "Waiting for package ID ConfigMap", nil
	}
	if !apierrors.IsNotFound(err) {
		return false, "", "", "", err
	}

	var job batchv1.Job
	err = r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: installJobName}, &job)
	if apierrors.IsNotFound(err) {
		return false, "", "", "Waiting for chaincode install Job", nil
	}
	if err != nil {
		return false, "", "", "", err
	}
	if jobFailed(job) {
		return false, "", "", "Chaincode install Job failed", nil
	}
	if jobSucceeded(job) {
		return false, "", "", "Waiting for package ID ConfigMap", nil
	}

	return false, "", "", "Waiting for chaincode install Job", nil
}

func marshalChaincodeJSON(value any) (string, error) {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return "", err
	}
	return string(encoded) + "\n", nil
}

func installChaincodePackageScript(
	org fabricopsv1alpha1.Org,
	peerName string,
	namespace string,
	tlsEnabled bool,
) string {
	tlsEnv := "export CORE_PEER_TLS_ENABLED=false"
	if tlsEnabled {
		tlsEnv = fmt.Sprintf(`ADMIN_TLS_DIR=%q
export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_TLS_CERT_FILE="$ADMIN_TLS_DIR/client.crt"
export CORE_PEER_TLS_KEY_FILE="$ADMIN_TLS_DIR/client.key"
export CORE_PEER_TLS_ROOTCERT_FILE="$ADMIN_TLS_DIR/ca.crt"`, chaincodeAdminTLSPath)
	}

	return fmt.Sprintf(`set -eu

PACKAGE_INPUT_DIR=%q
OUTPUT_DIR=%q
PACKAGE_LABEL="$(cat "$PACKAGE_INPUT_DIR/%s")"
PACKAGE_ARCHIVE="$(cat "$PACKAGE_INPUT_DIR/%s")"
PACKAGE_FILE="$PACKAGE_INPUT_DIR/$PACKAGE_ARCHIVE"
QUERY_FILE="$OUTPUT_DIR/%s"
PACKAGE_ID_FILE="$OUTPUT_DIR/%s"
CHAINCODE_ID_FILE="$OUTPUT_DIR/%s"
PACKAGE_HASH_FILE="$OUTPUT_DIR/%s"

export CORE_PEER_LOCALMSPID=%q
export CORE_PEER_ADDRESS=%q
export CORE_PEER_MSPCONFIGPATH=%q
%s

mkdir -p "$OUTPUT_DIR"
test -f "$PACKAGE_FILE"

query_installed() {
  peer lifecycle chaincode queryinstalled --output json > "$QUERY_FILE"
}

extract_package_id() {
  PACKAGE_ID="$(jq -r --arg package_label "$PACKAGE_LABEL" '.installed_chaincodes[]? | select(.label == $package_label) | .package_id' "$QUERY_FILE" | tail -n 1)"
  if [ -z "$PACKAGE_ID" ] || [ "$PACKAGE_ID" = "null" ]; then
    echo "Package label $PACKAGE_LABEL was not found in queryinstalled output" >&2
    return 1
  fi

  PACKAGE_HASH="${PACKAGE_ID#*:}"
  printf '%%s\n' "$PACKAGE_ID" > "$PACKAGE_ID_FILE"
  printf '%%s\n' "$PACKAGE_ID" > "$CHAINCODE_ID_FILE"
  printf '%%s\n' "$PACKAGE_HASH" > "$PACKAGE_HASH_FILE"
}

if query_installed 2>/tmp/fabricops-chaincode-queryinstalled.err && extract_package_id; then
  echo "Chaincode package $PACKAGE_LABEL is already installed"
  exit 0
fi

peer lifecycle chaincode install "$PACKAGE_FILE"
query_installed
extract_package_id
`, chaincodePackageInputDir,
		chaincodeOutputDir,
		chaincodePackageLabelKey,
		chaincodePackageFileKey,
		chaincodeQueryInstalledFile,
		chaincodePackageIDFile,
		chaincodeChaincodeIDFile,
		chaincodePackageHashFile,
		org.Organization.MSPName,
		serviceDNS(peerName, namespace, peerPort),
		chaincodeAdminMSPPath,
		tlsEnv,
	)
}

func approveChaincodeDefinitionScript(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	peerName string,
	namespace string,
	packageID string,
	orderer ordererInstance,
) string {
	return approveChaincodeDefinitionScriptForOrderer(
		net,
		channel,
		chaincode,
		org,
		peerName,
		namespace,
		packageID,
		ordererClientAddress(orderer),
		chaincodeOrdererTLSPath(orderer.name)+"/ca.crt",
		"",
		chaincodeEndorsementPolicy(net, channel, chaincode),
		chaincodeCollectionsConfigPath(chaincode),
	)
}

func approveChaincodeDefinitionScriptForOrderer(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	peerName string,
	namespace string,
	packageID string,
	ordererAddress string,
	ordererTLSCAPath string,
	ordererTLSHostnameOverride string,
	endorsementPolicy string,
	collectionsConfigPath string,
) string {
	tlsEnv := "export CORE_PEER_TLS_ENABLED=false"
	tlsArgs := ""
	if net.Spec.Global.TLS {
		tlsEnv = fmt.Sprintf(`ADMIN_TLS_DIR=%q
export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_TLS_CERT_FILE="$ADMIN_TLS_DIR/client.crt"
export CORE_PEER_TLS_KEY_FILE="$ADMIN_TLS_DIR/client.key"
export CORE_PEER_TLS_ROOTCERT_FILE="$ADMIN_TLS_DIR/ca.crt"`, chaincodeAdminTLSPath)
		tlsArgs = fmt.Sprintf(`set -- "$@" --tls --cafile %q`, ordererTLSCAPath)
		if strings.TrimSpace(ordererTLSHostnameOverride) != "" {
			tlsArgs += fmt.Sprintf("\nset -- \"$@\" --ordererTLSHostnameOverride %q", strings.TrimSpace(ordererTLSHostnameOverride))
		}
	}

	return fmt.Sprintf(`set -eu

CHANNEL_ID=%q
CHAINCODE_NAME=%q
CHAINCODE_VERSION=%q
PACKAGE_ID=%q
SEQUENCE=%d
ORDERER_ADDRESS=%q
ENDORSEMENT_POLICY=%q
INIT_REQUIRED=%q
COLLECTIONS_CONFIG=%q
OUTPUT_DIR=%q
QUERY_APPROVED_FILE="$OUTPUT_DIR/%s"

export CORE_PEER_LOCALMSPID=%q
export CORE_PEER_ADDRESS=%q
export CORE_PEER_MSPCONFIGPATH=%q
%s

mkdir -p "$OUTPUT_DIR"

if peer lifecycle chaincode queryapproved \
  --channelID "$CHANNEL_ID" \
  --name "$CHAINCODE_NAME" \
  --output json > "$QUERY_APPROVED_FILE" 2>/tmp/fabricops-queryapproved.err; then
  if jq -e --argjson sequence "$SEQUENCE" '(.sequence | tonumber) == $sequence' "$QUERY_APPROVED_FILE" >/dev/null; then
    echo "Chaincode $CHAINCODE_NAME sequence $SEQUENCE is already approved for $CORE_PEER_LOCALMSPID"
    exit 0
  fi
fi

set -- peer lifecycle chaincode approveformyorg \
  -o "$ORDERER_ADDRESS" \
  --channelID "$CHANNEL_ID" \
  --name "$CHAINCODE_NAME" \
  --version "$CHAINCODE_VERSION" \
  --package-id "$PACKAGE_ID" \
  --sequence "$SEQUENCE"

%s

if [ -n "$ENDORSEMENT_POLICY" ]; then
  set -- "$@" --signature-policy "$ENDORSEMENT_POLICY"
fi
if [ "$INIT_REQUIRED" = "true" ]; then
  set -- "$@" --init-required
fi
if [ -n "$COLLECTIONS_CONFIG" ]; then
  set -- "$@" --collections-config "$COLLECTIONS_CONFIG"
fi

"$@"

peer lifecycle chaincode queryapproved \
  --channelID "$CHANNEL_ID" \
  --name "$CHAINCODE_NAME" \
  --output json > "$QUERY_APPROVED_FILE"
`, channel.Name,
		chaincode.Name,
		chaincode.Version,
		packageID,
		chaincodeSequence(chaincode),
		ordererAddress,
		endorsementPolicy,
		boolString(chaincode.InitRequired),
		collectionsConfigPath,
		chaincodeOutputDir,
		chaincodeQueryApprovedFile,
		org.Organization.MSPName,
		serviceDNS(peerName, namespace, peerPort),
		chaincodeAdminMSPPath,
		tlsEnv,
		tlsArgs,
	)
}

func commitChaincodeDefinitionScript(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	chaincode fabricopsv1alpha1.Chaincode,
	submitter chaincodeLifecyclePeer,
	orderer ordererInstance,
	peers []chaincodeLifecyclePeer,
) string {
	tlsEnv := "export CORE_PEER_TLS_ENABLED=false"
	tlsArgs := ""
	peerArgs := ""
	if net.Spec.Global.TLS {
		tlsEnv = fmt.Sprintf(`ADMIN_TLS_DIR=%q
export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_TLS_CERT_FILE="$ADMIN_TLS_DIR/client.crt"
export CORE_PEER_TLS_KEY_FILE="$ADMIN_TLS_DIR/client.key"
export CORE_PEER_TLS_ROOTCERT_FILE="$ADMIN_TLS_DIR/ca.crt"`, chaincodeAdminTLSPath)
		tlsArgs = fmt.Sprintf(`set -- "$@" --tls --cafile %q`, chaincodeOrdererTLSPath(orderer.name)+"/ca.crt")

		var peerBuilder strings.Builder
		for _, peer := range peers {
			fmt.Fprintf(&peerBuilder,
				"set -- \"$@\" --peerAddresses %q --tlsRootCertFiles %q\n",
				chaincodeLifecyclePeerAddress(peer),
				chaincodePeerTLSPath(peer.org, peer.peerName)+"/ca.crt",
			)
		}
		peerArgs = peerBuilder.String()
	}

	return fmt.Sprintf(`set -eu

CHANNEL_ID=%q
CHAINCODE_NAME=%q
CHAINCODE_VERSION=%q
SEQUENCE=%d
ORDERER_ADDRESS=%q
ENDORSEMENT_POLICY=%q
INIT_REQUIRED=%q
COLLECTIONS_CONFIG=%q
OUTPUT_DIR=%q
QUERY_COMMITTED_FILE="$OUTPUT_DIR/%s"

export CORE_PEER_LOCALMSPID=%q
export CORE_PEER_ADDRESS=%q
export CORE_PEER_MSPCONFIGPATH=%q
%s

mkdir -p "$OUTPUT_DIR"

if peer lifecycle chaincode querycommitted \
  --channelID "$CHANNEL_ID" \
  --name "$CHAINCODE_NAME" \
  --output json > "$QUERY_COMMITTED_FILE" 2>/tmp/fabricops-querycommitted.err; then
  if jq -e --argjson sequence "$SEQUENCE" '(.sequence | tonumber) == $sequence' "$QUERY_COMMITTED_FILE" >/dev/null; then
    echo "Chaincode $CHAINCODE_NAME sequence $SEQUENCE is already committed on $CHANNEL_ID"
    exit 0
  fi
fi

set -- peer lifecycle chaincode commit \
  -o "$ORDERER_ADDRESS" \
  --channelID "$CHANNEL_ID" \
  --name "$CHAINCODE_NAME" \
  --version "$CHAINCODE_VERSION" \
  --sequence "$SEQUENCE"

%s
%s

if [ -n "$ENDORSEMENT_POLICY" ]; then
  set -- "$@" --signature-policy "$ENDORSEMENT_POLICY"
fi
if [ "$INIT_REQUIRED" = "true" ]; then
  set -- "$@" --init-required
fi
if [ -n "$COLLECTIONS_CONFIG" ]; then
  set -- "$@" --collections-config "$COLLECTIONS_CONFIG"
fi

"$@"

peer lifecycle chaincode querycommitted \
  --channelID "$CHANNEL_ID" \
  --name "$CHAINCODE_NAME" \
  --output json > "$QUERY_COMMITTED_FILE"
`, channel.Name,
		chaincode.Name,
		chaincode.Version,
		chaincodeSequence(chaincode),
		ordererClientAddress(orderer),
		chaincodeEndorsementPolicy(net, channel, chaincode),
		boolString(chaincode.InitRequired),
		chaincodeCollectionsConfigPath(chaincode),
		chaincodeOutputDir,
		chaincodeQueryCommittedFile,
		submitter.org.Organization.MSPName,
		chaincodeLifecyclePeerAddress(submitter),
		chaincodeAdminMSPPath,
		tlsEnv,
		tlsArgs,
		peerArgs,
	)
}

func publishChaincodeInstallScript() string {
	return `set -eu

kubectl -n "$POD_NAMESPACE" create configmap "$FABRICOPS_CHAINCODE_PACKAGE_ID_CONFIGMAP" \
  --from-file="` + chaincodePackageIDKey + `=` + chaincodeOutputDir + `/` + chaincodePackageIDFile + `" \
  --from-file="` + chaincodeChaincodeIDKey + `=` + chaincodeOutputDir + `/` + chaincodeChaincodeIDFile + `" \
  --from-file="` + chaincodePackageHashKey + `=` + chaincodeOutputDir + `/` + chaincodePackageHashFile + `" \
  --from-file="` + chaincodeQueryInstalledKey + `=` + chaincodeOutputDir + `/` + chaincodeQueryInstalledFile + `" \
  --dry-run=client -o yaml | kubectl -n "$POD_NAMESPACE" apply -f -

kubectl -n "$POD_NAMESPACE" label configmap "$FABRICOPS_CHAINCODE_PACKAGE_ID_CONFIGMAP" \
  fabricops.io/component=chaincode \
  fabricops.io/channel="$FABRICOPS_CHAINCODE_CHANNEL" \
  fabricops.io/chaincode="$FABRICOPS_CHAINCODE_NAME" \
  fabricops.io/workload="$FABRICOPS_CHAINCODE_PEER" \
  app.kubernetes.io/component=chaincode \
  --overwrite
`
}

func publishChaincodeInstallEnv(
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	peerName string,
) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: envChaincodePackageIDConfigMap, Value: chaincodePackageIDConfigMapName(chaincode, org, peerName)},
		{Name: envChaincodeChannel, Value: sanitizeName(chaincode.Channel)},
		{Name: envChaincodeName, Value: sanitizeName(chaincode.Name)},
		{Name: envChaincodePeer, Value: sanitizeName(peerName)},
		{
			Name: envPodNamespace,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		},
	}
}

func publishChaincodeLifecycleResultContainerName(operation string) string {
	return sanitizeName("publish-" + operation + "-result")
}

func publishChaincodeLifecycleResultScript() string {
	return `set -eu

kubectl -n "$POD_NAMESPACE" create configmap "$FABRICOPS_CHAINCODE_LIFECYCLE_RESULT_CONFIGMAP" \
  --from-file="$FABRICOPS_CHAINCODE_LIFECYCLE_RESULT_KEY=` + chaincodeOutputDir + `/$FABRICOPS_CHAINCODE_LIFECYCLE_RESULT_FILE" \
  --dry-run=client -o yaml | kubectl -n "$POD_NAMESPACE" apply -f -

kubectl -n "$POD_NAMESPACE" label configmap "$FABRICOPS_CHAINCODE_LIFECYCLE_RESULT_CONFIGMAP" \
  fabricops.io/component=chaincode \
  fabricops.io/channel="$FABRICOPS_CHAINCODE_CHANNEL" \
  fabricops.io/chaincode="$FABRICOPS_CHAINCODE_NAME" \
  fabricops.io/workload="$FABRICOPS_CHAINCODE_PEER" \
  app.kubernetes.io/component=chaincode \
  --overwrite
`
}

func publishChaincodeLifecycleResultEnv(
	chaincode fabricopsv1alpha1.Chaincode,
	peerName string,
	configMapName string,
	resultKey string,
	resultFile string,
) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: envChaincodeLifecycleResultConfigMap, Value: configMapName},
		{Name: envChaincodeLifecycleResultKey, Value: resultKey},
		{Name: envChaincodeLifecycleResultFile, Value: resultFile},
		{Name: envChaincodeChannel, Value: sanitizeName(chaincode.Channel)},
		{Name: envChaincodeName, Value: sanitizeName(chaincode.Name)},
		{Name: envChaincodePeer, Value: sanitizeName(peerName)},
		{
			Name: envPodNamespace,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		},
	}
}

func chaincodeStatusMessage(
	status fabricopsv1alpha1.ChaincodeStatus,
	messages []string,
	channelReady bool,
	lifecycleMessage string,
) string {
	if len(messages) > 0 {
		return strings.Join(messages, "; ")
	}
	if lifecycleMessage != "" {
		return lifecycleMessage
	}
	if status.PackageMetadata.Desired == 0 {
		return "No target peers found"
	}
	if status.PackageMetadataReady && !channelReady {
		return "Package metadata generated; waiting for channel bootstrap before lifecycle install"
	}
	if status.Ready {
		return "Chaincode committed and workload ready"
	}
	if status.Committed && !status.WorkloadsReady {
		return "Chaincode committed; waiting for chaincode workload Deployment"
	}
	if status.ApprovedReady {
		return "Chaincode approved; waiting for lifecycle commit Job"
	}
	if status.InstalledReady && !status.WorkloadsReady {
		return "Chaincode package installed; waiting for chaincode workloads and lifecycle approval Jobs"
	}
	if status.InstalledReady {
		return "Chaincode package installed; waiting for lifecycle approval Jobs"
	}
	if status.PackageMetadataReady && channelReady {
		return "Package metadata generated; waiting for chaincode install Jobs"
	}
	return "Waiting for package metadata generation"
}

func chaincodeStatusesEqual(a, b []fabricopsv1alpha1.ChaincodeStatus) bool {
	return reflect.DeepEqual(a, b)
}

func allChaincodesReady(statuses []fabricopsv1alpha1.ChaincodeStatus) bool {
	for _, status := range statuses {
		if !status.Ready {
			return false
		}
	}

	return true
}

func channelsByName(net *fabricopsv1alpha1.FabricNetwork) map[string]fabricopsv1alpha1.Channel {
	channels := map[string]fabricopsv1alpha1.Channel{}
	for _, channel := range net.Spec.Channels {
		channels[channel.Name] = channel
	}
	return channels
}

func channelReadyForChaincode(statuses []fabricopsv1alpha1.ChannelStatus, channelName string) bool {
	for _, status := range statuses {
		if status.Name == channelName {
			return status.Ready
		}
	}
	return false
}

func chaincodeApprovalPeers(channel fabricopsv1alpha1.Channel) map[string]string {
	peers := map[string]string{}
	for _, org := range channel.Orgs {
		if len(org.Peers) == 0 {
			continue
		}
		peers[org.Name] = org.Peers[0]
	}

	return peers
}

func chaincodeLifecycleOrderer(net *fabricopsv1alpha1.FabricNetwork) (ordererInstance, bool) {
	orderers := desiredOrdererInstances(net)
	if len(orderers) == 0 {
		return ordererInstance{}, false
	}

	return orderers[0], true
}

func chaincodeLifecyclePeers(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
) []chaincodeLifecyclePeer {
	orgs := orgsByName(net)
	peers := []chaincodeLifecyclePeer{}

	for _, channelOrg := range channel.Orgs {
		org, ok := orgs[channelOrg.Name]
		if !ok {
			continue
		}

		namespace := orgNamespaceName(net, org)
		for _, peerName := range channelOrg.Peers {
			if !peerDeclared(org, peerName) {
				continue
			}
			peers = append(peers, chaincodeLifecyclePeer{
				org:       org,
				peerName:  peerName,
				namespace: namespace,
			})
		}
	}

	return peers
}

func (r *FabricNetworkReconciler) externalChaincodeLifecyclePeers(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
) ([]chaincodeLifecyclePeer, error) {
	if len(channel.ExternalOrgs) == 0 {
		return nil, nil
	}

	peers := []chaincodeLifecyclePeer{}
	for _, externalOrg := range channel.ExternalOrgs {
		raw, ready, message, err := r.channelArtifactBytes(ctx, net.Namespace, &externalOrg.ApplicationOrgRef)
		if err != nil {
			return nil, err
		}
		if !ready {
			return nil, fmt.Errorf("%s: %s", externalOrg.Name, message)
		}

		_, parsedAnchorPeers, err := parseApplicationOrgJSON(raw)
		if err != nil {
			return nil, fmt.Errorf("%s application org JSON is invalid: %w", externalOrg.Name, err)
		}
		anchorPeers := externalOrg.AnchorPeers
		if len(anchorPeers) == 0 {
			anchorPeers = parsedAnchorPeers
		}
		if len(anchorPeers) == 0 {
			continue
		}

		var tlsRootCA []byte
		if net.Spec.Global.TLS {
			tlsRootCA, err = applicationOrgTLSRootCA(raw)
			if err != nil {
				return nil, fmt.Errorf("%s application org TLS root is invalid: %w", externalOrg.Name, err)
			}
			if len(tlsRootCA) == 0 {
				return nil, fmt.Errorf("%s application org JSON is missing tls_root_certs", externalOrg.Name)
			}
		}

		org := fabricopsv1alpha1.Org{
			Organization: fabricopsv1alpha1.OrgMeta{
				Name:    externalOrg.Name,
				MSPName: externalOrg.MSPID,
			},
		}
		for i, anchorPeer := range anchorPeers {
			host := strings.TrimSpace(anchorPeer.Host)
			if host == "" || anchorPeer.Port <= 0 {
				continue
			}
			peers = append(peers, chaincodeLifecyclePeer{
				org:       org,
				peerName:  sanitizeName(fmt.Sprintf("external-peer-%d", i)),
				address:   fmt.Sprintf("%s:%d", host, anchorPeer.Port),
				tlsRootCA: tlsRootCA,
			})
		}
	}

	return peers, nil
}

func chaincodeLifecyclePeerAddress(peer chaincodeLifecyclePeer) string {
	if strings.TrimSpace(peer.address) != "" {
		return strings.TrimSpace(peer.address)
	}
	return serviceDNS(peer.peerName, peer.namespace, peerPort)
}

func chaincodePeerTLSSecretItems(peer chaincodeLifecyclePeer) []corev1.KeyToPath {
	if len(peer.tlsRootCA) > 0 {
		return []corev1.KeyToPath{
			{Key: tlsCACertKey, Path: "ca.crt"},
		}
	}
	return tlsSecretItems()
}

func firstChaincodePackageID(targets []fabricopsv1alpha1.ChaincodeTargetStatus) string {
	for _, target := range targets {
		if strings.TrimSpace(target.PackageID) != "" {
			return target.PackageID
		}
	}

	return ""
}

func chaincodeEndorsementPolicy(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	chaincode fabricopsv1alpha1.Chaincode,
) string {
	if strings.TrimSpace(chaincode.EndorsementPolicy) != "" {
		return chaincode.EndorsementPolicy
	}

	orgs := channelPeerOrganizations(net, channel)
	if len(orgs) == 0 {
		return ""
	}

	identities := make([]string, 0, len(orgs))
	for _, org := range orgs {
		identities = append(identities, fmt.Sprintf("'%s.member'", org.Organization.MSPName))
	}

	return fmt.Sprintf("OR(%s)", strings.Join(identities, ","))
}

func peerDeclared(org fabricopsv1alpha1.Org, peerName string) bool {
	if org.Peer == nil {
		return false
	}

	for i := 0; i < org.Peer.Instances; i++ {
		if sanitizeName(fmt.Sprintf("%s%d", org.Peer.Prefix, i)) == peerName {
			return true
		}
	}
	return false
}

func chaincodePackageLabel(chaincode fabricopsv1alpha1.Chaincode) string {
	if strings.TrimSpace(chaincode.PackageLabel) != "" {
		return chaincode.PackageLabel
	}
	return fmt.Sprintf("%s_%s_%s", chaincode.Channel, chaincode.Name, chaincode.Version)
}

func chaincodeSequence(chaincode fabricopsv1alpha1.Chaincode) int32 {
	if chaincode.Sequence > 0 {
		return chaincode.Sequence
	}
	return 1
}

func chaincodeDialTimeout(chaincode fabricopsv1alpha1.Chaincode) string {
	if chaincode.CCAAS != nil && strings.TrimSpace(chaincode.CCAAS.DialTimeout) != "" {
		return chaincode.CCAAS.DialTimeout
	}
	return "10s"
}

func chaincodeReplicas(chaincode fabricopsv1alpha1.Chaincode) int32 {
	if chaincode.CCAAS != nil && chaincode.CCAAS.Replicas > 0 {
		return chaincode.CCAAS.Replicas
	}
	return 1
}

func chaincodeContainerPort(chaincode fabricopsv1alpha1.Chaincode) int32 {
	if chaincode.CCAAS != nil && chaincode.CCAAS.ContainerPort > 0 {
		return chaincode.CCAAS.ContainerPort
	}
	return peerChaincodePort
}

func chaincodeServicePort(chaincode fabricopsv1alpha1.Chaincode) int32 {
	if chaincode.CCAAS != nil && chaincode.CCAAS.ServicePort > 0 {
		return chaincode.CCAAS.ServicePort
	}
	return peerChaincodePort
}

func chaincodeImagePullPolicy(chaincode fabricopsv1alpha1.Chaincode) corev1.PullPolicy {
	if chaincode.CCAAS != nil && chaincode.CCAAS.ImagePullPolicy != "" {
		return chaincode.CCAAS.ImagePullPolicy
	}
	return corev1.PullIfNotPresent
}

func chaincodeContainerEnv(chaincode fabricopsv1alpha1.Chaincode, chaincodeID string) []corev1.EnvVar {
	required := map[string]string{
		envCCAASChaincodeID:            chaincodeID,
		envCCAASCoreChaincodeIDName:    chaincodeID,
		envCCAASChaincodeServerAddress: fmt.Sprintf("0.0.0.0:%d", chaincodeContainerPort(chaincode)),
		envCCAASCoreChaincodeAddress:   fmt.Sprintf("0.0.0.0:%d", chaincodeContainerPort(chaincode)),
	}
	protected := map[string]struct{}{}
	env := []corev1.EnvVar{}

	for name := range required {
		protected[name] = struct{}{}
	}
	if chaincode.CCAAS != nil {
		for _, item := range chaincode.CCAAS.Env {
			if _, ok := protected[item.Name]; ok {
				continue
			}
			env = append(env, item)
		}
	}
	for _, name := range []string{
		envCCAASChaincodeID,
		envCCAASCoreChaincodeIDName,
		envCCAASChaincodeServerAddress,
		envCCAASCoreChaincodeAddress,
	} {
		env = append(env, corev1.EnvVar{Name: name, Value: required[name]})
	}

	return env
}

func chaincodeResourceRequirements(chaincode fabricopsv1alpha1.Chaincode) corev1.ResourceRequirements {
	if chaincode.CCAAS != nil && chaincode.CCAAS.Resources != nil {
		return *chaincode.CCAAS.Resources.DeepCopy()
	}
	return componentResourceRequirements(componentPeer)
}

func chaincodeServiceName(chaincode fabricopsv1alpha1.Chaincode, org fabricopsv1alpha1.Org, peerName string) string {
	return sanitizeName(fmt.Sprintf("%s-%s-%s-%s-ccaas", chaincode.Channel, chaincode.Name, org.Organization.Name, peerName))
}

func chaincodeConnectionAddress(serviceName string, namespace string, chaincode fabricopsv1alpha1.Chaincode) string {
	return serviceDNS(serviceName, namespace, chaincodeServicePort(chaincode))
}

func chaincodeConnectionAddressTemplate(
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	namespace string,
) (string, error) {
	serviceName := chaincodeServiceName(chaincode, org, chaincodePeerHostnamePlaceholder)
	if !strings.Contains(serviceName, chaincodePeerHostnamePlaceholder) {
		return "", fmt.Errorf(
			"chaincode service name for %q on channel %q in org %q is too long to template peer-specific CCaaS addresses",
			chaincode.Name,
			chaincode.Channel,
			org.Organization.Name,
		)
	}

	serviceName = strings.ReplaceAll(serviceName, chaincodePeerHostnamePlaceholder, chaincodePeerHostnameTemplate)
	return chaincodeConnectionAddress(serviceName, namespace, chaincode), nil
}

func chaincodePackageConfigMapName(
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
) string {
	return sanitizeName(fmt.Sprintf("%s-%s-%s-package", chaincode.Channel, chaincode.Name, org.Organization.Name))
}

func chaincodeCollectionConfigMapName(chaincode fabricopsv1alpha1.Chaincode) string {
	return sanitizeName(fmt.Sprintf("%s-%s-collections", chaincode.Channel, chaincode.Name))
}

func chaincodePackageIDConfigMapName(
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	peerName string,
) string {
	return sanitizeName(fmt.Sprintf("%s-%s-%s-package-id", chaincodePackageLabel(chaincode), org.Organization.Name, peerName))
}

func chaincodeApproveJobName(
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	packageID string,
) string {
	parts := []string{chaincodePackageLabel(chaincode), chaincodePackageHash(packageID)}
	parts = append(parts, chaincodeDefinitionNameHash(chaincode))
	parts = append(parts, org.Organization.Name, "approve")
	return sanitizeName(strings.Join(parts, "-"))
}

func chaincodeApproveResultConfigMapName(
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	packageID string,
) string {
	return sanitizeName(chaincodeApproveJobName(chaincode, org, packageID) + "-result")
}

func chaincodeCommitJobName(chaincode fabricopsv1alpha1.Chaincode, packageID string) string {
	parts := []string{chaincodePackageLabel(chaincode), chaincodePackageHash(packageID)}
	parts = append(parts, chaincodeDefinitionNameHash(chaincode))
	parts = append(parts, "commit")
	return sanitizeName(strings.Join(parts, "-"))
}

func chaincodeCommitResultConfigMapName(chaincode fabricopsv1alpha1.Chaincode, packageID string) string {
	return sanitizeName(chaincodeCommitJobName(chaincode, packageID) + "-result")
}

func chaincodeInstallJobName(
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	peerName string,
) string {
	return sanitizeName(fmt.Sprintf("%s-%s-%s-install", chaincodePackageLabel(chaincode), org.Organization.Name, peerName))
}

func chaincodeInstallerName(
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	peerName string,
) string {
	return sanitizeName(fmt.Sprintf("%s-%s-%s-installer", chaincodePackageLabel(chaincode), org.Organization.Name, peerName))
}

func chaincodeCommitterName(chaincode fabricopsv1alpha1.Chaincode) string {
	return sanitizeName(fmt.Sprintf("%s-committer", chaincodePackageLabel(chaincode)))
}

func chaincodeOrdererTLSPath(ordererName string) string {
	return chaincodeWorkDir + "/crypto/orderers/" + sanitizeName(ordererName) + "/tls"
}

func chaincodePeerTLSVolumeName(org fabricopsv1alpha1.Org, peerName string) string {
	return sanitizeName("peer-tls-" + org.Organization.Name + "-" + peerName)
}

func chaincodePeerTLSSecretName(
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	peerName string,
) string {
	return sanitizeName(fmt.Sprintf("%s-%s-%s-tls", chaincodePackageLabel(chaincode), org.Organization.Name, peerName))
}

func chaincodePeerTLSPath(org fabricopsv1alpha1.Org, peerName string) string {
	return chaincodeWorkDir + "/crypto/peers/" + sanitizeName(org.Organization.Name) + "/" + sanitizeName(peerName) + "/tls"
}

func chaincodePackageHash(packageID string) string {
	parts := strings.SplitN(packageID, ":", 2)
	if len(parts) == 2 && strings.TrimSpace(parts[1]) != "" {
		return parts[1]
	}
	return "pending"
}

func chaincodeDefinitionNameHash(chaincode fabricopsv1alpha1.Chaincode) string {
	payload := struct {
		Sequence          int32                                     `json:"sequence"`
		EndorsementPolicy string                                    `json:"endorsementPolicy,omitempty"`
		InitRequired      bool                                      `json:"initRequired,omitempty"`
		PrivateData       []fabricopsv1alpha1.PrivateDataCollection `json:"privateData,omitempty"`
	}{
		Sequence:          chaincodeSequence(chaincode),
		EndorsementPolicy: strings.TrimSpace(chaincode.EndorsementPolicy),
		InitRequired:      chaincode.InitRequired,
		PrivateData:       chaincode.PrivateData,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "definition"
	}

	return shortSHA256(string(encoded))
}

func chaincodeCollectionsConfigPath(chaincode fabricopsv1alpha1.Chaincode) string {
	if len(chaincode.PrivateData) == 0 {
		return ""
	}
	return chaincodeCollectionsPath
}

func shortSHA256(value string) string {
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", sum)[:12]
}

func chaincodeLabels(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	channelName string,
	chaincodeName string,
) map[string]string {
	return mergeMap(orgLabels(net, org, componentChaincode), map[string]string{
		labelChannel:   sanitizeName(channelName),
		labelChaincode: sanitizeName(chaincodeName),
	})
}

func chaincodePeerLabels(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	chaincode fabricopsv1alpha1.Chaincode,
	peerName string,
) map[string]string {
	return mergeMap(chaincodeLabels(net, org, chaincode.Channel, chaincode.Name), map[string]string{
		labelWorkload: sanitizeName(peerName),
	})
}

func chaincodeWorkloadSelector(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	chaincode fabricopsv1alpha1.Chaincode,
	peerName string,
) map[string]string {
	return map[string]string{
		labelFabricNetwork:          sanitizeName(net.Name),
		labelFabricNetworkNamespace: sanitizeName(net.Namespace),
		labelOrg:                    sanitizeName(org.Organization.Name),
		labelComponent:              componentChaincode,
		labelChannel:                sanitizeName(chaincode.Channel),
		labelChaincode:              sanitizeName(chaincode.Name),
		labelWorkload:               sanitizeName(peerName),
	}
}
