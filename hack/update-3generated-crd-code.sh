#!/bin/bash
#
# Copyright the Velero contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

# this script expects to be run from the root of the Velero repo.

if [[ -z "${GOPATH}" ]]; then
  GOPATH=~/go
fi

if [[ ! -d "${GOPATH}/src/k8s.io/code-generator" ]]; then
  echo "k8s.io/code-generator missing from GOPATH"
  exit 1
fi

if ! command -v controller-gen > /dev/null; then
  echo "controller-gen is missing"
  exit 1
fi

${GOPATH}/src/k8s.io/code-generator/generate-groups.sh \
  all \
  github.com/vmware-tanzu/velero/pkg/generated \
  github.com/vmware-tanzu/velero/pkg/apis \
  "velero:v1,v1alpha1" \
  --go-header-file ./hack/boilerplate.go.txt \
  --output-base ../../.. \
  $@

# Generate apiextensions.k8s.io/v1

# Generate CRD for v1.
controller-gen \
  crd:crdVersions=v1 \
  paths=./pkg/apis/velero/v1/... \
  paths=./pkg/controller/... \
  output:crd:artifacts:config=config/crd/v1/bases \
  object \
  paths=./pkg/apis/velero/v1/...

# Generate CRD for v1alpha1.
controller-gen \
  crd:crdVersions=v1 \
  paths=./pkg/apis/velero/v1alpha1/... \
  paths=./pkg/controller/... \
  output:crd:artifacts:config=config/crd/v1alpha1/bases \
  object \
  paths=./pkg/apis/velero/v1alpha1/...

# Generate RBAC.
controller-gen \
  paths=./pkg/apis/velero/v1/... \
  paths=./pkg/apis/velero/v1alpha1/... \
  paths=./pkg/controller/... \
  rbac:roleName=velero-perms

go generate ./config/crd/v1/crds

go generate ./config/crd/v1alpha1/crds
