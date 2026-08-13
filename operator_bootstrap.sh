#!/bin/bash

kubebuilder init --domain fabricops.io --repo github.com/LF-Decentralized-Trust-labs/FabricOps
kubebuilder create api --group fabricops --version v1alpha1 --kind FabricNetwork