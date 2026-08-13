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
	"regexp"
	"strings"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

var signaturePolicyPrincipalPattern = regexp.MustCompile(`'([^']+)'`)

func validateFabricNetworkTopology(net *fabricopsv1alpha1.FabricNetwork) []string {
	problems := []string{}
	orgs := map[string]fabricopsv1alpha1.Org{}
	ordererCount := 0

	if len(net.Spec.Orgs) == 0 {
		problems = append(problems, "spec.orgs must include at least one organization")
	}

	for _, org := range net.Spec.Orgs {
		orgName := strings.TrimSpace(org.Organization.Name)
		if orgName == "" {
			continue
		}
		if _, ok := orgs[orgName]; ok {
			problems = append(problems, fmt.Sprintf("org %q is declared more than once", orgName))
		}
		orgs[orgName] = org

		for _, group := range org.Orderers {
			ordererCount += group.Instances
		}
		problems = append(problems, validateOrgExternalEndpoints(org)...)
	}

	if ordererCount == 0 {
		problems = append(problems, "at least one orderer instance is required")
	}
	if len(net.Spec.Channels) > 0 && !net.Spec.Global.TLS {
		problems = append(problems, "spec.global.tls must be true when channels are declared")
	}

	channels := map[string]fabricopsv1alpha1.Channel{}
	for _, channel := range net.Spec.Channels {
		channelName := strings.TrimSpace(channel.Name)
		if channelName == "" {
			continue
		}
		if _, ok := channels[channelName]; ok {
			problems = append(problems, fmt.Sprintf("channel %q is declared more than once", channelName))
		}
		channels[channelName] = channel

		if len(channel.Orgs) == 0 {
			problems = append(problems, fmt.Sprintf("channel %q must include at least one peer org", channelName))
		}
		localChannelOrgNames := map[string]struct{}{}
		for _, channelOrg := range channel.Orgs {
			orgName := strings.TrimSpace(channelOrg.Name)
			if orgName != "" {
				localChannelOrgNames[orgName] = struct{}{}
			}
			org, ok := orgs[orgName]
			if !ok {
				problems = append(problems, fmt.Sprintf("channel %q references unknown org %q", channelName, orgName))
				continue
			}
			if org.Peer == nil || org.Peer.Instances == 0 {
				problems = append(problems, fmt.Sprintf("channel %q org %q has no peer instances", channelName, orgName))
				continue
			}
			if len(channelOrg.Peers) == 0 {
				problems = append(problems, fmt.Sprintf("channel %q org %q must include at least one peer", channelName, orgName))
				continue
			}
			if unknownPeers := unknownChannelPeers(org, channelOrg.Peers); len(unknownPeers) > 0 {
				problems = append(
					problems,
					fmt.Sprintf("channel %q org %q references unknown peers: %s", channelName, orgName, strings.Join(unknownPeers, ", ")),
				)
			}
		}
		problems = append(problems, validateChannelExternalOrgs(channel, orgs, localChannelOrgNames, net)...)
	}

	seenChaincodes := map[string]struct{}{}
	seenChaincodePackageLabels := map[string]string{}
	for _, chaincode := range net.Spec.Chaincodes {
		chaincodeName := strings.TrimSpace(chaincode.Name)
		channelName := strings.TrimSpace(chaincode.Channel)
		if chaincodeName == "" || channelName == "" {
			continue
		}

		key := channelName + "/" + chaincodeName
		if _, ok := seenChaincodes[key]; ok {
			problems = append(problems, fmt.Sprintf("chaincode %q is declared more than once on channel %q", chaincodeName, channelName))
		}
		seenChaincodes[key] = struct{}{}

		channel, channelKnown := channels[channelName]
		if !channelKnown {
			problems = append(problems, fmt.Sprintf("chaincode %q references unknown channel %q", chaincodeName, channelName))
		}

		packageLabel := chaincodePackageLabel(chaincode)
		chaincodeRef := channelName + "/" + chaincodeName
		if previous, ok := seenChaincodePackageLabels[packageLabel]; ok {
			problems = append(
				problems,
				fmt.Sprintf("chaincode package label %q is used by both %q and %q", packageLabel, previous, chaincodeRef),
			)
		}
		seenChaincodePackageLabels[packageLabel] = chaincodeRef

		if channelKnown {
			problems = append(problems, validateChaincodeEndorsementPolicyTopology(chaincode, channel, orgs)...)
			problems = append(problems, validateChaincodePrivateDataTopology(chaincode, channel, orgs)...)
			problems = append(problems, validateChaincodeCouchDBIndexes(chaincode)...)
		}
	}

	return problems
}

func validateChannelExternalOrgs(
	channel fabricopsv1alpha1.Channel,
	orgs map[string]fabricopsv1alpha1.Org,
	localChannelOrgNames map[string]struct{},
	net *fabricopsv1alpha1.FabricNetwork,
) []string {
	problems := []string{}
	seenNames := map[string]struct{}{}
	seenMSPIDs := map[string]struct{}{}
	localMSPs := orgMSPNames(orgs)
	localChannelMSPs := map[string]struct{}{}
	for orgName := range localChannelOrgNames {
		org, ok := orgs[orgName]
		if !ok {
			continue
		}
		localChannelMSPs[org.Organization.MSPName] = struct{}{}
	}

	for i, externalOrg := range channel.ExternalOrgs {
		path := fmt.Sprintf("channel %q externalOrgs[%d]", channel.Name, i)
		name := strings.TrimSpace(externalOrg.Name)
		mspID := strings.TrimSpace(externalOrg.MSPID)
		if name == "" {
			problems = append(problems, path+" name is required")
		} else {
			if _, ok := seenNames[name]; ok {
				problems = append(problems, fmt.Sprintf("channel %q external org %q is declared more than once", channel.Name, name))
			}
			seenNames[name] = struct{}{}
			if _, ok := orgs[name]; ok {
				problems = append(problems, fmt.Sprintf("channel %q external org %q is already a local org", channel.Name, name))
			}
		}
		if mspID == "" {
			problems = append(problems, path+" mspID is required")
		} else {
			if _, ok := seenMSPIDs[mspID]; ok {
				problems = append(problems, fmt.Sprintf("channel %q external MSP %q is declared more than once", channel.Name, mspID))
			}
			seenMSPIDs[mspID] = struct{}{}
			if _, ok := localMSPs[mspID]; ok {
				problems = append(problems, fmt.Sprintf("channel %q external MSP %q is already managed by a local org", channel.Name, mspID))
			}
		}

		problems = append(problems, validateChannelArtifactRef(path+".applicationOrgRef", &externalOrg.ApplicationOrgRef)...)
		if adminOrg := strings.TrimSpace(externalOrg.AdminOrg); adminOrg != "" {
			if _, ok := orgs[adminOrg]; !ok {
				problems = append(problems, fmt.Sprintf("%s adminOrg %q is not a local org", path, adminOrg))
			} else if _, ok := localChannelOrgNames[adminOrg]; !ok {
				problems = append(problems, fmt.Sprintf("%s adminOrg %q is not a local org on channel %q", path, adminOrg, channel.Name))
			}
		}
		seenRequiredSigners := map[string]struct{}{}
		for j, signerMSPID := range externalOrg.RequiredSignerMSPIDs {
			signerPath := fmt.Sprintf("%s.requiredSignerMSPIDs[%d]", path, j)
			signerMSPID = strings.TrimSpace(signerMSPID)
			if signerMSPID == "" {
				problems = append(problems, signerPath+" is required")
				continue
			}
			if _, ok := seenRequiredSigners[signerMSPID]; ok {
				problems = append(
					problems,
					fmt.Sprintf("%s required signer MSP %q is declared more than once", path, signerMSPID),
				)
			}
			seenRequiredSigners[signerMSPID] = struct{}{}
			if _, ok := localChannelMSPs[signerMSPID]; !ok {
				problems = append(
					problems,
					fmt.Sprintf("%s required signer MSP %q is not a local channel org MSP", path, signerMSPID),
				)
			}
		}
		if externalOrg.Orderer != nil {
			if !channelOrdererRefMatchesAny(net, *externalOrg.Orderer) {
				problems = append(problems, fmt.Sprintf("%s orderer does not match a local orderer instance", path))
			}
		}
		for j, anchorPeer := range externalOrg.AnchorPeers {
			anchorPath := fmt.Sprintf("%s.anchorPeers[%d]", path, j)
			if strings.TrimSpace(anchorPeer.Host) == "" {
				problems = append(problems, anchorPath+".host is required")
			}
			if anchorPeer.Port <= 0 || anchorPeer.Port > 65535 {
				problems = append(problems, anchorPath+".port must be between 1 and 65535")
			}
		}
	}

	return problems
}

func validateOrgExternalEndpoints(org fabricopsv1alpha1.Org) []string {
	problems := []string{}
	orgName := strings.TrimSpace(org.Organization.Name)

	for i, group := range org.Orderers {
		knownOrderers := map[string]struct{}{}
		for instance := 0; instance < group.Instances; instance++ {
			knownOrderers[sanitizeName(fmt.Sprintf("%s%d", group.Prefix, instance))] = struct{}{}
		}
		path := fmt.Sprintf("org %q orderers[%d].externalEndpoints", orgName, i)
		problems = append(problems, validateExternalEndpoints(path, group.ExternalEndpoints, knownOrderers)...)
	}

	if org.Peer != nil {
		problems = append(
			problems,
			validateExternalEndpoints(fmt.Sprintf("org %q peer.externalEndpoints", orgName), org.Peer.ExternalEndpoints, desiredPeerNames(org))...,
		)
	}

	return problems
}

func validateExternalEndpoints(
	path string,
	endpoints []fabricopsv1alpha1.ExternalEndpoint,
	knownWorkloads map[string]struct{},
) []string {
	problems := []string{}
	seen := map[string]struct{}{}
	for i, endpoint := range endpoints {
		endpointPath := fmt.Sprintf("%s[%d]", path, i)
		name := strings.TrimSpace(endpoint.Name)
		if name == "" {
			problems = append(problems, endpointPath+".name is required")
		} else {
			if _, ok := seen[name]; ok {
				problems = append(problems, fmt.Sprintf("%s workload %q is declared more than once", path, name))
			}
			seen[name] = struct{}{}
			if _, ok := knownWorkloads[name]; !ok {
				problems = append(problems, fmt.Sprintf("%s references unknown workload %q", endpointPath, name))
			}
		}
		problems = append(problems, validateParticipantEndpoint(endpointPath+".address", endpoint.Address, true)...)
		for j, host := range endpoint.TLSHosts {
			if strings.TrimSpace(host) == "" {
				problems = append(problems, fmt.Sprintf("%s.tlsHosts[%d] is required", endpointPath, j))
			}
		}
	}
	return problems
}

func validateChannelArtifactRef(path string, ref *fabricopsv1alpha1.ChannelArtifactKeyRef) []string {
	if ref == nil {
		return []string{path + " is required"}
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

func channelOrdererRefMatchesAny(
	net *fabricopsv1alpha1.FabricNetwork,
	ref fabricopsv1alpha1.ChannelOrdererRef,
) bool {
	if strings.TrimSpace(ref.Org) == "" && strings.TrimSpace(ref.Name) == "" {
		return false
	}
	for _, orderer := range desiredOrdererInstances(net) {
		if strings.TrimSpace(ref.Org) != "" && ref.Org != orderer.org.Organization.Name {
			continue
		}
		if strings.TrimSpace(ref.Name) != "" && ref.Name != orderer.name {
			continue
		}
		return true
	}
	return false
}

func validateChaincodeEndorsementPolicyTopology(
	chaincode fabricopsv1alpha1.Chaincode,
	channel fabricopsv1alpha1.Channel,
	orgs map[string]fabricopsv1alpha1.Org,
) []string {
	policy := strings.TrimSpace(chaincode.EndorsementPolicy)
	if policy == "" {
		return nil
	}

	return validateSignaturePolicyMSPReferences(
		policy,
		fmt.Sprintf("chaincode %q endorsementPolicy", chaincode.Name),
		channel,
		orgs,
	)
}

func validateChaincodePrivateDataTopology(
	chaincode fabricopsv1alpha1.Chaincode,
	channel fabricopsv1alpha1.Channel,
	orgs map[string]fabricopsv1alpha1.Org,
) []string {
	problems := []string{}
	seenCollections := map[string]struct{}{}
	channelPeerCounts := channelPeerCountsByOrg(channel)

	for _, collection := range chaincode.PrivateData {
		collectionName := strings.TrimSpace(collection.Name)
		if collectionName == "" {
			problems = append(problems, fmt.Sprintf("chaincode %q private data collection name is required", chaincode.Name))
			continue
		}
		if strings.HasPrefix(collectionName, "_") {
			problems = append(
				problems,
				fmt.Sprintf("chaincode %q private data collection %q must not start with _", chaincode.Name, collectionName),
			)
		}
		if _, ok := seenCollections[collectionName]; ok {
			problems = append(
				problems,
				fmt.Sprintf("chaincode %q private data collection %q is declared more than once", chaincode.Name, collectionName),
			)
		}
		seenCollections[collectionName] = struct{}{}

		if len(collection.OrgNames) == 0 {
			problems = append(
				problems,
				fmt.Sprintf("chaincode %q private data collection %q must include at least one org", chaincode.Name, collectionName),
			)
		}

		authorizedPeers := 0
		for _, orgName := range collection.OrgNames {
			orgName = strings.TrimSpace(orgName)
			if _, ok := orgs[orgName]; !ok {
				problems = append(
					problems,
					fmt.Sprintf("chaincode %q private data collection %q references unknown org %q", chaincode.Name, collectionName, orgName),
				)
				continue
			}
			peerCount, ok := channelPeerCounts[orgName]
			if !ok {
				problems = append(
					problems,
					fmt.Sprintf(
						"chaincode %q private data collection %q references org %q outside channel %q",
						chaincode.Name,
						collectionName,
						orgName,
						channel.Name,
					),
				)
				continue
			}
			authorizedPeers += peerCount
		}

		maxPeerCount := int32(max(authorizedPeers-1, 0))
		if collection.MaxPeerCount != nil {
			maxPeerCount = *collection.MaxPeerCount
			if maxPeerCount > int32(max(authorizedPeers-1, 0)) {
				problems = append(
					problems,
					fmt.Sprintf(
						"chaincode %q private data collection %q maxPeerCount %d exceeds available authorized peers %d",
						chaincode.Name,
						collectionName,
						maxPeerCount,
						max(authorizedPeers-1, 0),
					),
				)
			}
		}
		requiredPeerCount := int32(0)
		if collection.RequiredPeerCount != nil {
			requiredPeerCount = *collection.RequiredPeerCount
		}
		if requiredPeerCount > maxPeerCount {
			problems = append(
				problems,
				fmt.Sprintf(
					"chaincode %q private data collection %q requiredPeerCount %d exceeds maxPeerCount %d",
					chaincode.Name,
					collectionName,
					requiredPeerCount,
					maxPeerCount,
				),
			)
		}

		if collection.EndorsementPolicy != nil &&
			strings.TrimSpace(collection.EndorsementPolicy.SignaturePolicy) != "" &&
			strings.TrimSpace(collection.EndorsementPolicy.ChannelConfigPolicy) != "" {
			problems = append(
				problems,
				fmt.Sprintf(
					"chaincode %q private data collection %q must use only one endorsementPolicy field",
					chaincode.Name,
					collectionName,
				),
			)
		}
	}

	return problems
}

func validateChaincodeCouchDBIndexes(chaincode fabricopsv1alpha1.Chaincode) []string {
	problems := []string{}
	collections := map[string]struct{}{}
	for _, collection := range chaincode.PrivateData {
		collectionName := strings.TrimSpace(collection.Name)
		if collectionName != "" {
			collections[collectionName] = struct{}{}
		}
	}

	seenPaths := map[string]struct{}{}
	for _, index := range chaincode.CouchDBIndexes {
		indexName := strings.TrimSpace(index.Name)
		if indexName == "" {
			problems = append(problems, fmt.Sprintf("chaincode %q CouchDB index name is required", chaincode.Name))
			continue
		}
		if len(index.Fields) == 0 {
			problems = append(problems, fmt.Sprintf("chaincode %q CouchDB index %q must include at least one field", chaincode.Name, indexName))
		}
		for _, field := range index.Fields {
			if strings.TrimSpace(field) == "" {
				problems = append(problems, fmt.Sprintf("chaincode %q CouchDB index %q has an empty field", chaincode.Name, indexName))
			}
		}

		collectionName := strings.TrimSpace(index.Collection)
		if collectionName != "" {
			if _, ok := collections[collectionName]; !ok {
				problems = append(
					problems,
					fmt.Sprintf("chaincode %q CouchDB index %q references unknown private data collection %q", chaincode.Name, indexName, collectionName),
				)
			}
		}

		path := chaincodeCouchDBIndexPath(index)
		if _, ok := seenPaths[path]; ok {
			problems = append(problems, fmt.Sprintf("chaincode %q CouchDB index package path %q is declared more than once", chaincode.Name, path))
		}
		seenPaths[path] = struct{}{}
	}

	return problems
}

func validateSignaturePolicyMSPReferences(
	policy string,
	context string,
	channel fabricopsv1alpha1.Channel,
	orgs map[string]fabricopsv1alpha1.Org,
) []string {
	problems := []string{}
	allMSPs := orgMSPNames(orgs)
	for _, externalOrg := range channel.ExternalOrgs {
		mspName := strings.TrimSpace(externalOrg.MSPID)
		if mspName != "" {
			allMSPs[mspName] = struct{}{}
		}
	}
	channelMSPs := channelMSPNames(channel, orgs)
	matches := signaturePolicyPrincipalPattern.FindAllStringSubmatch(policy, -1)
	if len(matches) == 0 {
		return append(problems, fmt.Sprintf("%s must reference at least one MSP principal", context))
	}

	seenProblems := map[string]struct{}{}
	for _, match := range matches {
		principal := strings.TrimSpace(match[1])
		parts := strings.SplitN(principal, ".", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			key := "principal:" + principal
			if _, ok := seenProblems[key]; ok {
				continue
			}
			seenProblems[key] = struct{}{}
			problems = append(problems, fmt.Sprintf("%s principal %q must use MSP.role format", context, principal))
			continue
		}

		mspName := strings.TrimSpace(parts[0])
		if _, ok := allMSPs[mspName]; !ok {
			key := "unknown:" + mspName
			if _, duplicate := seenProblems[key]; duplicate {
				continue
			}
			seenProblems[key] = struct{}{}
			problems = append(problems, fmt.Sprintf("%s references unknown MSP %q", context, mspName))
			continue
		}
		if _, ok := channelMSPs[mspName]; !ok {
			key := "outside:" + mspName
			if _, duplicate := seenProblems[key]; duplicate {
				continue
			}
			seenProblems[key] = struct{}{}
			problems = append(problems, fmt.Sprintf("%s references MSP %q outside channel %q", context, mspName, channel.Name))
		}
	}

	return problems
}

func channelPeerCountsByOrg(channel fabricopsv1alpha1.Channel) map[string]int {
	counts := map[string]int{}
	for _, org := range channel.Orgs {
		counts[org.Name] = len(org.Peers)
	}
	return counts
}

func orgMSPNames(orgs map[string]fabricopsv1alpha1.Org) map[string]struct{} {
	mspNames := map[string]struct{}{}
	for _, org := range orgs {
		mspName := strings.TrimSpace(org.Organization.MSPName)
		if mspName != "" {
			mspNames[mspName] = struct{}{}
		}
	}
	return mspNames
}

func channelMSPNames(
	channel fabricopsv1alpha1.Channel,
	orgs map[string]fabricopsv1alpha1.Org,
) map[string]struct{} {
	mspNames := map[string]struct{}{}
	for _, channelOrg := range channel.Orgs {
		org, ok := orgs[strings.TrimSpace(channelOrg.Name)]
		if !ok {
			continue
		}
		mspName := strings.TrimSpace(org.Organization.MSPName)
		if mspName != "" {
			mspNames[mspName] = struct{}{}
		}
	}
	for _, externalOrg := range channel.ExternalOrgs {
		mspName := strings.TrimSpace(externalOrg.MSPID)
		if mspName != "" {
			mspNames[mspName] = struct{}{}
		}
	}
	return mspNames
}
