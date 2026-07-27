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
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	fabricopsv1alpha1 "github.com/dpereowei/fabricops/api/v1alpha1"
)

const (
	componentConnectionProfile = "connection-profile"

	connectionProfileJSONKey = "connection.json"
	connectionProfileYAMLKey = "connection.yaml"
)

type connectionProfileTLSRoots struct {
	peers    map[string]string
	orderers map[string]string
}

type fabricConnectionProfile struct {
	Name                   string                                         `json:"name"`
	Version                string                                         `json:"version"`
	Client                 fabricConnectionProfileClient                  `json:"client"`
	Channels               map[string]fabricConnectionProfileChannel      `json:"channels,omitempty"`
	Organizations          map[string]fabricConnectionProfileOrganization `json:"organizations"`
	Orderers               map[string]fabricConnectionProfileGRPCEndpoint `json:"orderers,omitempty"`
	Peers                  map[string]fabricConnectionProfileGRPCEndpoint `json:"peers,omitempty"`
	CertificateAuthorities map[string]fabricConnectionProfileCA           `json:"certificateAuthorities,omitempty"`
}

type fabricConnectionProfileClient struct {
	Organization string `json:"organization"`
}

type fabricConnectionProfileChannel struct {
	Orderers []string                                      `json:"orderers,omitempty"`
	Peers    map[string]fabricConnectionProfileChannelPeer `json:"peers,omitempty"`
}

type fabricConnectionProfileChannelPeer struct {
	EndorsingPeer  bool `json:"endorsingPeer"`
	ChaincodeQuery bool `json:"chaincodeQuery"`
	LedgerQuery    bool `json:"ledgerQuery"`
	EventSource    bool `json:"eventSource"`
}

type fabricConnectionProfileOrganization struct {
	MSPID                  string   `json:"mspid"`
	Peers                  []string `json:"peers,omitempty"`
	Orderers               []string `json:"orderers,omitempty"`
	CertificateAuthorities []string `json:"certificateAuthorities,omitempty"`
}

type fabricConnectionProfileGRPCEndpoint struct {
	URL         string                          `json:"url"`
	GRPCOptions map[string]string               `json:"grpcOptions,omitempty"`
	TLSCACerts  *fabricConnectionProfileTLSCert `json:"tlsCACerts,omitempty"`
}

type fabricConnectionProfileTLSCert struct {
	PEM string `json:"pem"`
}

type fabricConnectionProfileCA struct {
	URL    string `json:"url"`
	CAName string `json:"caName"`
}

func (r *FabricNetworkReconciler) reconcileConnectionProfiles(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	statuses []fabricopsv1alpha1.OrgStatus,
) ([]fabricopsv1alpha1.OrgStatus, error) {
	if !identityMaterialReady(statuses) {
		return statuses, nil
	}

	tlsRoots, err := r.loadConnectionProfileTLSRoots(ctx, net)
	if err != nil {
		return statuses, err
	}

	nextStatuses := append([]fabricopsv1alpha1.OrgStatus(nil), statuses...)
	for i := range nextStatuses {
		org, ok := orgsByName(net)[nextStatuses[i].Name]
		if !ok || org.Peer == nil {
			continue
		}

		profile, err := buildConnectionProfileConfigMap(net, org, tlsRoots)
		if err != nil {
			return nextStatuses, err
		}
		if err := r.ensureConfigMap(ctx, profile); err != nil {
			return nextStatuses, err
		}

		nextStatuses[i].ConnectionProfileConfigMapName = profile.Name
	}

	return nextStatuses, nil
}

func (r *FabricNetworkReconciler) loadConnectionProfileTLSRoots(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
) (connectionProfileTLSRoots, error) {
	roots := connectionProfileTLSRoots{
		peers:    map[string]string{},
		orderers: map[string]string{},
	}
	if !net.Spec.Global.TLS {
		return roots, nil
	}

	for _, peer := range desiredPeerInstances(net) {
		pem, err := r.loadTLSCACert(ctx, peer.namespace, identitySecretName(peer.name, secretKindTLS))
		if err != nil {
			return roots, err
		}
		roots.peers[connectionProfilePeerName(peer.org, peer.name)] = pem
	}

	for _, orderer := range desiredOrdererInstances(net) {
		pem, err := r.loadTLSCACert(ctx, orderer.namespace, identitySecretName(orderer.name, secretKindTLS))
		if err != nil {
			return roots, err
		}
		roots.orderers[connectionProfileOrdererName(orderer.org, orderer.name)] = pem
	}

	return roots, nil
}

func (r *FabricNetworkReconciler) loadTLSCACert(ctx context.Context, namespace, secretName string) (string, error) {
	var secret corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: secretName}, &secret); err != nil {
		return "", err
	}

	pem := strings.TrimSpace(string(secret.Data[tlsCACertKey]))
	if pem == "" {
		return "", fmt.Errorf("secret %s/%s is missing %s", namespace, secretName, tlsCACertKey)
	}

	return pem, nil
}

func buildConnectionProfileConfigMap(
	net *fabricopsv1alpha1.FabricNetwork,
	clientOrg fabricopsv1alpha1.Org,
	tlsRoots connectionProfileTLSRoots,
) (*corev1.ConfigMap, error) {
	profile := buildConnectionProfile(net, clientOrg, tlsRoots)
	jsonProfile, err := json.MarshalIndent(profile, "", "  ")
	if err != nil {
		return nil, err
	}
	yamlProfile, err := yaml.JSONToYAML(jsonProfile)
	if err != nil {
		return nil, err
	}

	namespace := orgNamespaceName(net, clientOrg)
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        connectionProfileConfigMapName(net),
			Namespace:   namespace,
			Labels:      orgLabels(net, clientOrg, componentConnectionProfile),
			Annotations: resourceAnnotations(net, clientOrg),
		},
		Data: map[string]string{
			connectionProfileJSONKey: string(jsonProfile),
			connectionProfileYAMLKey: string(yamlProfile),
		},
	}, nil
}

func buildConnectionProfile(
	net *fabricopsv1alpha1.FabricNetwork,
	clientOrg fabricopsv1alpha1.Org,
	tlsRoots connectionProfileTLSRoots,
) fabricConnectionProfile {
	orderers := desiredOrdererInstances(net)
	peers := desiredPeerInstances(net)
	ordererNames := make([]string, 0, len(orderers))

	profile := fabricConnectionProfile{
		Name:    net.Name,
		Version: "1.0.0",
		Client: fabricConnectionProfileClient{
			Organization: clientOrg.Organization.Name,
		},
		Channels:               map[string]fabricConnectionProfileChannel{},
		Organizations:          map[string]fabricConnectionProfileOrganization{},
		Orderers:               map[string]fabricConnectionProfileGRPCEndpoint{},
		Peers:                  map[string]fabricConnectionProfileGRPCEndpoint{},
		CertificateAuthorities: map[string]fabricConnectionProfileCA{},
	}

	for _, orderer := range orderers {
		name := connectionProfileOrdererName(orderer.org, orderer.name)
		ordererNames = append(ordererNames, name)
		profile.Orderers[name] = connectionProfileGRPCEndpoint(ordererAdvertisedClientAddress(orderer), ordererHost(orderer), tlsRoots.orderers[name])
	}

	for _, peer := range peers {
		name := connectionProfilePeerName(peer.org, peer.name)
		profile.Peers[name] = connectionProfileGRPCEndpoint(peerAdvertisedAddress(peer), peerHost(peer), tlsRoots.peers[name])
	}

	for _, org := range net.Spec.Orgs {
		orgProfile := fabricConnectionProfileOrganization{
			MSPID: org.Organization.MSPName,
			CertificateAuthorities: []string{
				connectionProfileCAName(org),
			},
		}
		for _, orderer := range orderers {
			if orderer.org.Organization.Name == org.Organization.Name {
				orgProfile.Orderers = append(orgProfile.Orderers, connectionProfileOrdererName(org, orderer.name))
			}
		}
		for _, peer := range peers {
			if peer.org.Organization.Name == org.Organization.Name {
				orgProfile.Peers = append(orgProfile.Peers, connectionProfilePeerName(org, peer.name))
			}
		}
		profile.Organizations[org.Organization.Name] = orgProfile
		profile.CertificateAuthorities[connectionProfileCAName(org)] = fabricConnectionProfileCA{
			URL:    "http://" + serviceDNS(sanitizeName(org.Organization.Name+"-ca"), orgNamespaceName(net, org), caPort),
			CAName: sanitizeName(org.Organization.Name),
		}
	}

	for _, channel := range net.Spec.Channels {
		channelProfile := fabricConnectionProfileChannel{
			Orderers: ordererNames,
			Peers:    map[string]fabricConnectionProfileChannelPeer{},
		}
		for _, channelOrg := range channel.Orgs {
			org, ok := orgsByName(net)[channelOrg.Name]
			if !ok {
				continue
			}
			for _, peerName := range channelOrg.Peers {
				channelProfile.Peers[connectionProfilePeerName(org, peerName)] = fabricConnectionProfileChannelPeer{
					EndorsingPeer:  true,
					ChaincodeQuery: true,
					LedgerQuery:    true,
					EventSource:    true,
				}
			}
		}
		profile.Channels[channel.Name] = channelProfile
	}

	return profile
}

func connectionProfileGRPCEndpoint(address, host, tlsCACert string) fabricConnectionProfileGRPCEndpoint {
	scheme := "grpc"
	var tlsCert *fabricConnectionProfileTLSCert
	grpcOptions := map[string]string{}
	if tlsCACert != "" {
		scheme = "grpcs"
		tlsCert = &fabricConnectionProfileTLSCert{PEM: tlsCACert}
		grpcOptions["ssl-target-name-override"] = host
		grpcOptions["hostnameOverride"] = host
	}

	return fabricConnectionProfileGRPCEndpoint{
		URL:         scheme + "://" + address,
		GRPCOptions: grpcOptions,
		TLSCACerts:  tlsCert,
	}
}

func desiredPeerInstances(net *fabricopsv1alpha1.FabricNetwork) []peerInstance {
	instances := []peerInstance{}
	for _, org := range net.Spec.Orgs {
		if org.Peer == nil {
			continue
		}
		namespace := orgNamespaceName(net, org)
		for i := 0; i < org.Peer.Instances; i++ {
			instances = append(instances, peerInstance{
				org:       org,
				name:      sanitizeName(fmt.Sprintf("%s%d", org.Peer.Prefix, i)),
				namespace: namespace,
			})
		}
	}

	return instances
}

func connectionProfileConfigMapName(net *fabricopsv1alpha1.FabricNetwork) string {
	return sanitizeName(net.Name + "-connection-profile")
}

func connectionProfileOrdererName(org fabricopsv1alpha1.Org, ordererName string) string {
	return sanitizeName(ordererName) + "." + sanitizeName(org.Organization.Name)
}

func connectionProfilePeerName(org fabricopsv1alpha1.Org, peerName string) string {
	return sanitizeName(peerName) + "." + sanitizeName(org.Organization.Name)
}

func connectionProfileCAName(org fabricopsv1alpha1.Org) string {
	return sanitizeName(org.Organization.Name + "-ca")
}

func ordererHost(orderer ordererInstance) string {
	return ordererConnectionProfileTLSHost(orderer)
}

func peerHost(peer peerInstance) string {
	return peerConnectionProfileTLSHost(peer)
}
