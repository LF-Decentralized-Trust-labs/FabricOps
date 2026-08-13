# Image URL to use for local building/deployment targets.
LOCAL_IMG ?= controller:latest
IMG ?= $(LOCAL_IMG)
# Canonical manager image location for release builds.
IMAGE_REGISTRY ?= ghcr.io/lf-decentralized-trust-labs
IMAGE_REPOSITORY ?= fabricops
VERSION ?= 0.2.0
RELEASE_IMG ?= $(IMAGE_REGISTRY)/$(IMAGE_REPOSITORY):$(VERSION)
SAMPLE_CHAINCODE_IMAGES ?= $(IMAGE_REGISTRY)/fabricops-node-settlement:$(VERSION) $(IMAGE_REGISTRY)/fabricops-go-settlement:$(VERSION) $(IMAGE_REGISTRY)/fabricops-java-settlement:$(VERSION)
RELEASE_CHECK_IMAGES ?= $(RELEASE_IMG) $(SAMPLE_CHAINCODE_IMAGES)
# YEAR defines the year value used for substituting the YEAR placeholder in the boilerplate header.
YEAR ?= $(shell date +%Y)

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	"$(CONTROLLER_GEN)" rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases output:rbac:artifacts:config=config/rbac

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	"$(CONTROLLER_GEN)" object:headerFile="hack/boilerplate.go.txt",year=$(YEAR) paths="./api/..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet setup-envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# TODO(user): To use a different vendor for e2e tests, modify the setup under 'tests/e2e'.
# The default setup assumes Kind is pre-installed and builds/loads the Manager Docker image locally.
# kubectl kuberc is disabled by default for test isolation; enable with:
# - KUBECTL_KUBERC=true
# CertManager is installed by default; skip with:
# - CERT_MANAGER_INSTALL_SKIP=true
KIND_CLUSTER ?= fabricops-test-e2e
E2E_SKIP_CLEANUP ?= false
E2E_GO_TEST_TIMEOUT ?= 75m
E2E_CHAINCODE_RUNTIME ?= node
E2E_SAMPLE_MANIFEST ?= config/samples/e2e/$(E2E_CHAINCODE_RUNTIME)/fabricnetwork.yaml
KIND_FEDERATED_FOUNDER_CLUSTER ?= fabricops-fed-founder
KIND_FEDERATED_PARTICIPANT_CLUSTER ?= fabricops-fed-participant
E2E_FEDERATED_GO_TEST_TIMEOUT ?= 90m
E2E_FEDERATED_ORDERER_NODE_PORT ?= 30050
E2E_FEDERATED_PEER_NODE_PORT ?= 30051
E2E_FEDERATED_FOUNDER_PEER_NODE_PORT ?= 30052

.PHONY: setup-test-e2e
setup-test-e2e: ## Set up a Kind cluster for e2e tests if it does not exist
	@command -v $(KIND) >/dev/null 2>&1 || { \
		echo "Kind is not installed. Please install Kind manually."; \
		exit 1; \
	}
	@case "$$($(KIND) get clusters)" in \
		*"$(KIND_CLUSTER)"*) \
			echo "Kind cluster '$(KIND_CLUSTER)' already exists. Skipping creation." ;; \
		*) \
			echo "Creating Kind cluster '$(KIND_CLUSTER)'..."; \
			$(KIND) create cluster --name $(KIND_CLUSTER) ;; \
	esac
	@$(KUBECTL) config use-context kind-$(KIND_CLUSTER)

.PHONY: test-e2e
test-e2e: setup-test-e2e manifests generate fmt vet ## Run the e2e tests. Expected an isolated environment using Kind.
	@status=0; \
	KIND=$(KIND) KUBECTL=$(KUBECTL) KIND_CLUSTER=$(KIND_CLUSTER) IMG=$(IMG) E2E_CHAINCODE_RUNTIME=$(E2E_CHAINCODE_RUNTIME) E2E_SAMPLE_MANIFEST=$(E2E_SAMPLE_MANIFEST) go test -tags=e2e ./test/e2e/ -v -ginkgo.v -timeout $(E2E_GO_TEST_TIMEOUT) || status=$$?; \
	if [ "$(E2E_SKIP_CLEANUP)" = "true" ]; then \
		echo "Keeping Kind cluster '$(KIND_CLUSTER)' because E2E_SKIP_CLEANUP=true"; \
	else \
		$(KIND) delete cluster --name $(KIND_CLUSTER); \
	fi; \
	exit $$status

.PHONY: setup-test-e2e-federated
setup-test-e2e-federated: ## Set up two Kind clusters for federated e2e tests if they do not exist
	@command -v $(KIND) >/dev/null 2>&1 || { \
		echo "Kind is not installed. Please install Kind manually."; \
		exit 1; \
	}
	@for cluster in $(KIND_FEDERATED_FOUNDER_CLUSTER) $(KIND_FEDERATED_PARTICIPANT_CLUSTER); do \
		case "$$($(KIND) get clusters)" in \
			*"$$cluster"*) \
				echo "Kind cluster '$$cluster' already exists. Skipping creation." ;; \
			*) \
				echo "Creating Kind cluster '$$cluster'..."; \
				$(KIND) create cluster --name "$$cluster" ;; \
		esac; \
	done

.PHONY: test-e2e-federated
test-e2e-federated: setup-test-e2e-federated manifests generate fmt vet ## Run the two-cluster federated join e2e test.
	@status=0; \
	KIND=$(KIND) KUBECTL=$(KUBECTL) KIND_FEDERATED_FOUNDER_CLUSTER=$(KIND_FEDERATED_FOUNDER_CLUSTER) KIND_FEDERATED_PARTICIPANT_CLUSTER=$(KIND_FEDERATED_PARTICIPANT_CLUSTER) E2E_FEDERATED_ORDERER_NODE_PORT=$(E2E_FEDERATED_ORDERER_NODE_PORT) E2E_FEDERATED_PEER_NODE_PORT=$(E2E_FEDERATED_PEER_NODE_PORT) E2E_FEDERATED_FOUNDER_PEER_NODE_PORT=$(E2E_FEDERATED_FOUNDER_PEER_NODE_PORT) IMG=$(IMG) go test -tags=e2e ./test/e2e/federated/ -v -ginkgo.v -timeout $(E2E_FEDERATED_GO_TEST_TIMEOUT) || status=$$?; \
	if [ "$(E2E_SKIP_CLEANUP)" = "true" ]; then \
		echo "Keeping Kind clusters '$(KIND_FEDERATED_FOUNDER_CLUSTER)' and '$(KIND_FEDERATED_PARTICIPANT_CLUSTER)' because E2E_SKIP_CLEANUP=true"; \
	else \
		$(KIND) delete cluster --name $(KIND_FEDERATED_FOUNDER_CLUSTER); \
		$(KIND) delete cluster --name $(KIND_FEDERATED_PARTICIPANT_CLUSTER); \
	fi; \
	exit $$status

.PHONY: cleanup-test-e2e
cleanup-test-e2e: ## Tear down the Kind cluster used for e2e tests
	@$(KIND) delete cluster --name $(KIND_CLUSTER)

.PHONY: cleanup-test-e2e-federated
cleanup-test-e2e-federated: ## Tear down the Kind clusters used for federated e2e tests
	@$(KIND) delete cluster --name $(KIND_FEDERATED_FOUNDER_CLUSTER)
	@$(KIND) delete cluster --name $(KIND_FEDERATED_PARTICIPANT_CLUSTER)

.PHONY: cleanup-sample
cleanup-sample: ## Delete the sample FabricNetwork and generated sample namespaces from the current cluster.
	-$(KUBECTL) delete fabricnetwork fabricnetwork-sample -n default --ignore-not-found
	-$(KUBECTL) delete namespace fo-sample-orderer fo-sample-banka fo-sample-bankb --ignore-not-found

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	"$(GOLANGCI_LINT)" run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	"$(GOLANGCI_LINT)" run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	"$(GOLANGCI_LINT)" config verify

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: build-fabricopsctl
build-fabricopsctl: fmt vet ## Build fabricopsctl helper binary.
	go build -o bin/fabricopsctl ./cmd/fabricopsctl

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/main.go

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

.PHONY: docker-build-release
docker-build-release: ## Build the manager image with the canonical release tag.
	$(MAKE) docker-build IMG=$(RELEASE_IMG)

.PHONY: docker-push-release
docker-push-release: ## Push the manager image with the canonical release tag.
	$(MAKE) docker-push IMG=$(RELEASE_IMG)

.PHONY: release-check-ghcr
release-check-ghcr: ## Verify release manager and sample chaincode images are publicly pullable from GHCR.
	VERSION="$(VERSION)" IMAGE_REGISTRY="$(IMAGE_REGISTRY)" IMAGE_REPOSITORY="$(IMAGE_REPOSITORY)" hack/check-ghcr-public.sh $(RELEASE_CHECK_IMAGES)

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert the build platform directive into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	builder=fabricops-builder; \
	cleanup() { $(CONTAINER_TOOL) buildx rm "$$builder" >/dev/null 2>&1 || true; rm -f Dockerfile.cross; }; \
	trap cleanup EXIT; \
	$(CONTAINER_TOOL) buildx create --name "$$builder" >/dev/null 2>&1 || true; \
	$(CONTAINER_TOOL) buildx build --builder "$$builder" --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .

.PHONY: docker-buildx-release
docker-buildx-release: ## Build and push the manager image with the canonical release tag for all configured platforms.
	$(MAKE) docker-buildx IMG=$(RELEASE_IMG)

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	tmp="$$(mktemp)"; \
	cp config/manager/kustomization.yaml "$$tmp"; \
	trap 'cp "$$tmp" config/manager/kustomization.yaml; rm -f "$$tmp"' EXIT; \
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}; \
	cd ../..; \
	"$(KUSTOMIZE)" build config/default > dist/install.yaml

.PHONY: build-installer-release
build-installer-release: ## Generate install bundle with the canonical release image tag.
	$(MAKE) build-installer IMG=$(RELEASE_IMG)

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" apply -f -; else echo "No CRDs to install; skipping."; fi

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -; else echo "No CRDs to delete; skipping."; fi

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
KUSTOMIZE_VERSION ?= v5.8.1
CONTROLLER_TOOLS_VERSION ?= v0.21.0

#ENVTEST_VERSION is the controller-runtime version to use for setup-envtest, derived from go.mod
ENVTEST_VERSION ?= $(shell v='$(call gomodver,sigs.k8s.io/controller-runtime)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_VERSION manually (controller-runtime replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v")

#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell v='$(call gomodver,k8s.io/api)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_K8S_VERSION manually (k8s.io/api replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?[0-9]+\.([0-9]+).*/1.\1/')

GOLANGCI_LINT_VERSION ?= v2.12.2
.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))
	@test -f .custom-gcl.yml && { \
		echo "Building custom golangci-lint with plugins..." && \
		$(GOLANGCI_LINT) custom --destination $(LOCALBIN) --name golangci-lint-custom && \
		mv -f $(LOCALBIN)/golangci-lint-custom $(GOLANGCI_LINT); \
	} || true

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
GOBIN="$(LOCALBIN)" go install $${package} ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

define gomodver
$(shell go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' $(1) 2>/dev/null)
endef

##@ Helm Deployment

## Helm binary to use for deploying the chart
HELM ?= helm
## Namespace to deploy the Helm release
HELM_NAMESPACE ?= fabricops-system
## Name of the Helm release
HELM_RELEASE ?= fabricops
## Path to the Helm chart directory
HELM_CHART_DIR ?= dist/chart
## Additional arguments to pass to helm commands
HELM_EXTRA_ARGS ?=

.PHONY: install-helm
install-helm: ## Install the latest version of Helm.
	@command -v $(HELM) >/dev/null 2>&1 || { \
		echo "Installing Helm..." && \
		curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-4 | bash; \
	}

.PHONY: helm-deploy
helm-deploy: install-helm ## Deploy manager to the K8s cluster via Helm. Specify an image with IMG.
	IMG="$(IMG)"; \
	$(HELM) upgrade --install $(HELM_RELEASE) $(HELM_CHART_DIR) \
		--namespace $(HELM_NAMESPACE) \
		--create-namespace \
		--set manager.image.repository=$${IMG%:*} \
		--set manager.image.tag=$${IMG##*:} \
		--wait \
		--timeout 5m \
		$(HELM_EXTRA_ARGS)

.PHONY: helm-deploy-release
helm-deploy-release: ## Deploy manager via Helm with the canonical release image tag.
	$(MAKE) helm-deploy IMG=$(RELEASE_IMG)

.PHONY: helm-uninstall
helm-uninstall: ## Uninstall the Helm release from the K8s cluster.
	$(HELM) uninstall $(HELM_RELEASE) --namespace $(HELM_NAMESPACE)

.PHONY: helm-status
helm-status: ## Show Helm release status.
	$(HELM) status $(HELM_RELEASE) --namespace $(HELM_NAMESPACE)

.PHONY: helm-history
helm-history: ## Show Helm release history.
	$(HELM) history $(HELM_RELEASE) --namespace $(HELM_NAMESPACE)

.PHONY: helm-rollback
helm-rollback: ## Rollback to previous Helm release.
	$(HELM) rollback $(HELM_RELEASE) --namespace $(HELM_NAMESPACE)
