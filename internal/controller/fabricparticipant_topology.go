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
	stdlibnet "net"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	fabricopsv1alpha1 "github.com/dpereowei/fabricops/api/v1alpha1"
)

func validateFabricParticipantTopology(participant *fabricopsv1alpha1.FabricParticipant) []string {
	problems := []string{}
	spec := participant.Spec

	if strings.TrimSpace(spec.Global.FabricVersion) == "" {
		problems = append(problems, "spec.global.fabricVersion is required")
	}
	if len(spec.Channels) > 0 && !spec.Global.TLS {
		problems = append(problems, "spec.global.tls must be true when participant channels are declared")
	}

	problems = append(problems, validateFabricParticipantOrg(spec.Org)...)
	problems = append(problems, validateParticipantNetwork(spec.Global.TLS, spec.Network)...)
	problems = append(problems, validateParticipantChannels(spec.Org, spec.Channels)...)
	problems = append(problems, validateParticipantChaincodes(spec.Channels, spec.Chaincodes)...)

	return problems
}

func validateFabricParticipantOrg(org fabricopsv1alpha1.Org) []string {
	problems := []string{}
	if strings.TrimSpace(org.Organization.Name) == "" {
		problems = append(problems, "spec.org.organization.name is required")
	}
	if strings.TrimSpace(org.Organization.Domain) == "" {
		problems = append(problems, "spec.org.organization.domain is required")
	}
	if strings.TrimSpace(org.Organization.MSPName) == "" {
		problems = append(problems, "spec.org.organization.mspName is required")
	}
	if len(org.Orderers) > 0 {
		problems = append(
			problems,
			"spec.org.orderers is not supported for participants; import remote orderers under spec.network.orderers",
		)
	}
	if org.Peer == nil || org.Peer.Instances == 0 {
		problems = append(problems, "spec.org.peer must declare at least one participant peer")
	}
	return problems
}

func validateParticipantNetwork(tls bool, network fabricopsv1alpha1.ParticipantNetwork) []string {
	problems := []string{}
	if strings.TrimSpace(network.Name) == "" {
		problems = append(problems, "spec.network.name is required")
	}
	if len(network.Orderers) == 0 {
		problems = append(problems, "spec.network.orderers must include at least one reachable orderer")
	}
	for i, orderer := range network.Orderers {
		path := fmt.Sprintf("spec.network.orderers[%d]", i)
		if strings.TrimSpace(orderer.Org) == "" {
			problems = append(problems, path+".org is required")
		}
		if strings.TrimSpace(orderer.Name) == "" {
			problems = append(problems, path+".name is required")
		}
		problems = append(problems, validateParticipantEndpoint(path+".clientAddress", orderer.ClientAddress, true)...)
		problems = append(problems, validateParticipantEndpoint(path+".adminAddress", orderer.AdminAddress, false)...)
		if tls {
			problems = append(problems, validateParticipantArtifactRef(path+".tlsRootCARef", orderer.TLSRootCARef, true)...)
		} else if orderer.TLSRootCARef != nil {
			problems = append(problems, validateParticipantArtifactRef(path+".tlsRootCARef", orderer.TLSRootCARef, false)...)
		}
	}

	seenPeers := map[string]struct{}{}
	for i, peer := range network.Peers {
		path := fmt.Sprintf("spec.network.peers[%d]", i)
		orgName := strings.TrimSpace(peer.Org)
		peerName := strings.TrimSpace(peer.Name)
		if orgName == "" {
			problems = append(problems, path+".org is required")
		}
		if peerName == "" {
			problems = append(problems, path+".name is required")
		}
		if orgName != "" && peerName != "" {
			key := strings.ToLower(orgName + "/" + peerName)
			if _, ok := seenPeers[key]; ok {
				problems = append(problems, fmt.Sprintf("imported peer %q is declared more than once", orgName+"/"+peerName))
			}
			seenPeers[key] = struct{}{}
		}
		problems = append(problems, validateParticipantEndpoint(path+".address", peer.Address, true)...)
		if tls {
			problems = append(problems, validateParticipantArtifactRef(path+".tlsRootCARef", peer.TLSRootCARef, true)...)
		} else if peer.TLSRootCARef != nil {
			problems = append(problems, validateParticipantArtifactRef(path+".tlsRootCARef", peer.TLSRootCARef, false)...)
		}
	}
	return problems
}

func validateParticipantChannels(
	org fabricopsv1alpha1.Org,
	channels []fabricopsv1alpha1.ParticipantChannel,
) []string {
	problems := []string{}
	seenChannels := map[string]struct{}{}
	for i, channel := range channels {
		path := fmt.Sprintf("spec.channels[%d]", i)
		channelName := strings.TrimSpace(channel.Name)
		if channelName == "" {
			problems = append(problems, path+".name is required")
		} else if _, ok := seenChannels[channelName]; ok {
			problems = append(problems, fmt.Sprintf("participant channel %q is declared more than once", channelName))
		}
		seenChannels[channelName] = struct{}{}

		problems = append(problems, validateParticipantArtifactRef(path+".blockRef", &channel.BlockRef, true)...)
		if len(channel.Peers) == 0 {
			problems = append(problems, fmt.Sprintf("participant channel %q must include at least one peer", channelName))
		} else if unknownPeers := unknownChannelPeers(org, channel.Peers); len(unknownPeers) > 0 {
			problems = append(
				problems,
				fmt.Sprintf(
					"participant channel %q references unknown local peers: %s",
					channelName,
					strings.Join(unknownPeers, ", "),
				),
			)
		}

		for j, anchor := range channel.AnchorPeers {
			anchorPath := fmt.Sprintf("%s.anchorPeers[%d]", path, j)
			if strings.TrimSpace(anchor.Name) == "" {
				problems = append(problems, anchorPath+".name is required")
			}
			if strings.TrimSpace(anchor.Host) == "" {
				problems = append(problems, anchorPath+".host is required")
			}
			if anchor.Port <= 0 || anchor.Port > 65535 {
				problems = append(problems, anchorPath+".port must be between 1 and 65535")
			}
		}
		if channel.Membership != nil {
			for j, mspID := range channel.Membership.RequiredSignerMSPIDs {
				if strings.TrimSpace(mspID) == "" {
					problems = append(
						problems,
						fmt.Sprintf("%s.membership.requiredSignerMSPIDs[%d] is required", path, j),
					)
				}
			}
		}
	}
	return problems
}

func validateParticipantChaincodes(
	channels []fabricopsv1alpha1.ParticipantChannel,
	chaincodes []fabricopsv1alpha1.ParticipantChaincode,
) []string {
	problems := []string{}
	channelNames := map[string]struct{}{}
	for _, channel := range channels {
		channelNames[channel.Name] = struct{}{}
	}

	seenChaincodes := map[string]struct{}{}
	for i, chaincode := range chaincodes {
		path := fmt.Sprintf("spec.chaincodes[%d]", i)
		chaincodeName := strings.TrimSpace(chaincode.Name)
		channelName := strings.TrimSpace(chaincode.Channel)
		if chaincodeName == "" {
			problems = append(problems, path+".name is required")
		}
		if channelName == "" {
			problems = append(problems, path+".channel is required")
		} else if _, ok := channelNames[channelName]; !ok {
			problems = append(
				problems,
				fmt.Sprintf("participant chaincode %q references unknown channel %q", chaincodeName, channelName),
			)
		}
		if chaincodeName != "" && channelName != "" {
			key := channelName + "/" + chaincodeName
			if _, ok := seenChaincodes[key]; ok {
				problems = append(
					problems,
					fmt.Sprintf("participant chaincode %q is declared more than once on channel %q", chaincodeName, channelName),
				)
			}
			seenChaincodes[key] = struct{}{}
		}
		if strings.TrimSpace(chaincode.Version) == "" {
			problems = append(problems, path+".version is required")
		}
		if strings.TrimSpace(chaincode.PackageLabel) == "" {
			problems = append(problems, path+".packageLabel is required")
		}
		if chaincode.Sequence <= 0 {
			problems = append(problems, path+".sequence must be greater than zero")
		}
		if chaincode.PackageRef == nil && strings.TrimSpace(chaincode.Image) == "" {
			problems = append(problems, path+".packageRef or image is required")
		}
		if chaincode.PackageRef != nil {
			problems = append(problems, validateParticipantArtifactRef(path+".packageRef", chaincode.PackageRef, false)...)
		}
	}
	return problems
}

func validateParticipantArtifactRef(
	path string,
	ref *fabricopsv1alpha1.ParticipantArtifactKeyRef,
	required bool,
) []string {
	if ref == nil {
		if required {
			return []string{path + " is required"}
		}
		return nil
	}

	hasConfigMap := ref.ConfigMapKeyRef != nil
	hasSecret := ref.SecretKeyRef != nil
	if hasConfigMap == hasSecret {
		return []string{path + " must set exactly one of configMapKeyRef or secretKeyRef"}
	}
	if hasConfigMap {
		return validateConfigMapKeySelector(path+".configMapKeyRef", *ref.ConfigMapKeyRef)
	}
	return validateSecretKeySelector(path+".secretKeyRef", *ref.SecretKeyRef)
}

func validateConfigMapKeySelector(path string, ref corev1.ConfigMapKeySelector) []string {
	problems := []string{}
	if strings.TrimSpace(ref.Name) == "" {
		problems = append(problems, path+".name is required")
	}
	if strings.TrimSpace(ref.Key) == "" {
		problems = append(problems, path+".key is required")
	}
	return problems
}

func validateSecretKeySelector(path string, ref corev1.SecretKeySelector) []string {
	problems := []string{}
	if strings.TrimSpace(ref.Name) == "" {
		problems = append(problems, path+".name is required")
	}
	if strings.TrimSpace(ref.Key) == "" {
		problems = append(problems, path+".key is required")
	}
	return problems
}

func validateParticipantEndpoint(path string, address string, required bool) []string {
	address = strings.TrimSpace(address)
	if address == "" {
		if required {
			return []string{path + " is required"}
		}
		return nil
	}
	host, portValue, err := stdlibnet.SplitHostPort(address)
	if err != nil {
		return []string{fmt.Sprintf("%s is invalid: %v", path, err)}
	}
	if strings.TrimSpace(host) == "" {
		return []string{path + " host is required"}
	}
	port, err := strconv.Atoi(portValue)
	if err != nil || port <= 0 || port > 65535 {
		return []string{path + " port must be between 1 and 65535"}
	}
	return nil
}
