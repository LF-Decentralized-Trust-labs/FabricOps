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
	stdlibnet "net"
	"strconv"
	"strings"

	fabricopsv1alpha1 "github.com/dpereowei/fabricops/api/v1alpha1"
)

func ordererAdvertisedClientAddress(orderer ordererInstance) string {
	return workloadExternalAddress(ordererClientAddress(orderer), orderer.group.ExternalEndpoints, orderer.name)
}

func ordererAdvertisedHost(orderer ordererInstance) string {
	return endpointHost(ordererAdvertisedClientAddress(orderer))
}

func ordererAdvertisedPort(orderer ordererInstance) int32 {
	return endpointPort(ordererAdvertisedClientAddress(orderer), ordererPort)
}

func ordererTLSHostnameOverride(orderer ordererInstance) string {
	return workloadTLSHostnameOverride(orderer.group.ExternalEndpoints, orderer.name)
}

func ordererConnectionProfileTLSHost(orderer ordererInstance) string {
	if override := ordererTLSHostnameOverride(orderer); override != "" {
		return override
	}
	return ordererAdvertisedHost(orderer)
}

func peerAdvertisedAddress(peer peerInstance) string {
	if peer.org.Peer == nil {
		return peerAddress(peer)
	}
	return workloadExternalAddress(peerAddress(peer), peer.org.Peer.ExternalEndpoints, peer.name)
}

func peerAdvertisedPort(peer peerInstance) int32 {
	return endpointPort(peerAdvertisedAddress(peer), peerPort)
}

func peerAdvertisedHost(peer peerInstance) string {
	return endpointHost(peerAdvertisedAddress(peer))
}

func peerTLSHostnameOverride(peer peerInstance) string {
	if peer.org.Peer == nil {
		return ""
	}
	return workloadTLSHostnameOverride(peer.org.Peer.ExternalEndpoints, peer.name)
}

func peerConnectionProfileTLSHost(peer peerInstance) string {
	if override := peerTLSHostnameOverride(peer); override != "" {
		return override
	}
	return peerAdvertisedHost(peer)
}

func ordererWorkloadCSRHosts(group fabricopsv1alpha1.OrdererGroup, workloadName string, namespace string) []string {
	return workloadCSRHosts(workloadName, namespace, group.ExternalEndpoints)
}

func peerWorkloadCSRHosts(org fabricopsv1alpha1.Org, workloadName string, namespace string) []string {
	if org.Peer == nil {
		return workloadDNSNames(workloadName, namespace)
	}
	return workloadCSRHosts(workloadName, namespace, org.Peer.ExternalEndpoints)
}

func workloadExternalAddress(
	defaultAddress string,
	endpoints []fabricopsv1alpha1.ExternalEndpoint,
	workloadName string,
) string {
	endpoint, ok := externalEndpointForWorkload(endpoints, workloadName)
	if !ok || strings.TrimSpace(endpoint.Address) == "" {
		return defaultAddress
	}
	return strings.TrimSpace(endpoint.Address)
}

func workloadCSRHosts(
	workloadName string,
	namespace string,
	endpoints []fabricopsv1alpha1.ExternalEndpoint,
) []string {
	hosts := workloadDNSNames(workloadName, namespace)
	endpoint, ok := externalEndpointForWorkload(endpoints, workloadName)
	if !ok {
		return hosts
	}
	if host := endpointHost(endpoint.Address); host != "" {
		hosts = append(hosts, host)
	}
	hosts = append(hosts, endpoint.TLSHosts...)
	hosts = append(hosts, endpoint.TLSHostnameOverride)
	return uniqueTrimmedStrings(hosts)
}

func workloadTLSHostnameOverride(
	endpoints []fabricopsv1alpha1.ExternalEndpoint,
	workloadName string,
) string {
	endpoint, ok := externalEndpointForWorkload(endpoints, workloadName)
	if !ok {
		return ""
	}
	return strings.TrimSpace(endpoint.TLSHostnameOverride)
}

func externalEndpointForWorkload(
	endpoints []fabricopsv1alpha1.ExternalEndpoint,
	workloadName string,
) (fabricopsv1alpha1.ExternalEndpoint, bool) {
	for _, endpoint := range endpoints {
		if strings.TrimSpace(endpoint.Name) == workloadName {
			return endpoint, true
		}
	}
	return fabricopsv1alpha1.ExternalEndpoint{}, false
}

func endpointHost(address string) string {
	host, _, err := stdlibnet.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return ""
	}
	return strings.Trim(host, "[]")
}

func endpointPort(address string, defaultPort int32) int32 {
	_, portValue, err := stdlibnet.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return defaultPort
	}
	port, err := strconv.Atoi(portValue)
	if err != nil || port <= 0 || port > 65535 {
		return defaultPort
	}
	return int32(port)
}

func uniqueTrimmedStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
