#!/bin/bash
# Copyright 2026 The Volcano Authors.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Remove CEL validation rules (x-kubernetes-validations) from generated CRD YAML files.
# CEL is GA only since Kubernetes 1.29; on older clusters (e.g. 1.19) the apiserver silently
# drops the field, so the rules are not enforced. Run this only for such deployments, through
# `make manifests CRD_STRIP_CEL=true`. See docs/user-guide/k8s_1.19_crd_validation.md.
#
# Usage: hack/strip-cel-validations.sh <crd-yaml-file>...

set -o errexit
set -o nounset
set -o pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <crd-yaml-file>..." >&2
  exit 1
fi

for f in "$@"; do
  if [ ! -f "$f" ]; then
    echo "ERROR: $f does not exist" >&2
    exit 1
  fi
  # Drop each "x-kubernetes-validations:" block: the key line, every deeper-indented line,
  # and "- " items at the key's own indentation. The block ends at the next sibling key.
  perl -i -ne '
    if (!$skip && /^(\s*)x-kubernetes-validations:\s*$/) { $ind = length($1); $skip = 1; next }
    if ($skip) {
      if (/^(\s*)\S/ && (length($1) > $ind || (length($1) == $ind && /^\s*-\s/))) { next }
      $skip = 0
    }
    print
  ' "$f"
  if grep -q "x-kubernetes-validations" "$f"; then
    echo "ERROR: x-kubernetes-validations still present in $f" >&2
    exit 1
  fi
  echo "stripped x-kubernetes-validations from $f"
done
