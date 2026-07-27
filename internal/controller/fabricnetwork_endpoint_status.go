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
	"fmt"

	fabricopsv1alpha1 "github.com/dpereowei/fabricops/api/v1alpha1"
)

func orgEndpointStatus(
	org fabricopsv1alpha1.Org,
	namespace string,
) (string, []fabricopsv1alpha1.OrdererEndpointStatus, []fabricopsv1alpha1.PeerEndpointStatus) {
	return caEndpointStatus(org, namespace), ordererEndpointStatuses(org, namespace), peerEndpointStatuses(org, namespace)
}

func caEndpointStatus(org fabricopsv1alpha1.Org, namespace string) string {
	return "http://" + serviceDNS(sanitizeName(org.Organization.Name+"-ca"), namespace, caPort)
}

func ordererEndpointStatuses(org fabricopsv1alpha1.Org, namespace string) []fabricopsv1alpha1.OrdererEndpointStatus {
	endpoints := []fabricopsv1alpha1.OrdererEndpointStatus{}
	for _, group := range org.Orderers {
		for i := 0; i < group.Instances; i++ {
			name := sanitizeName(fmt.Sprintf("%s%d", group.Prefix, i))
			orderer := ordererInstance{
				org:       org,
				group:     group,
				name:      name,
				namespace: namespace,
			}
			endpoints = append(endpoints, fabricopsv1alpha1.OrdererEndpointStatus{
				Name:                name,
				Namespace:           namespace,
				ClientAddress:       ordererAdvertisedClientAddress(orderer),
				TLSHostnameOverride: ordererTLSHostnameOverride(orderer),
				AdminAddress:        serviceDNS(name, namespace, ordererAdminPort),
				OperationsAddress:   "http://" + serviceDNS(operationsServiceName(name), namespace, ordererOpsPort),
			})
		}
	}

	return endpoints
}

func peerEndpointStatuses(org fabricopsv1alpha1.Org, namespace string) []fabricopsv1alpha1.PeerEndpointStatus {
	if org.Peer == nil {
		return nil
	}

	endpoints := []fabricopsv1alpha1.PeerEndpointStatus{}
	for i := 0; i < org.Peer.Instances; i++ {
		name := sanitizeName(fmt.Sprintf("%s%d", org.Peer.Prefix, i))
		peer := peerInstance{org: org, name: name, namespace: namespace}
		endpoints = append(endpoints, fabricopsv1alpha1.PeerEndpointStatus{
			Name:                name,
			Address:             peerAdvertisedAddress(peer),
			TLSHostnameOverride: peerTLSHostnameOverride(peer),
			ChaincodeAddress:    serviceDNS(name, namespace, peerChaincodePort),
			OperationsAddress:   "http://" + serviceDNS(operationsServiceName(name), namespace, peerOpsPort),
		})
	}

	return endpoints
}
