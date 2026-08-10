#!/bin/bash

# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Install the packages named by ACTIONS_EXTRA_PACKAGES, so that a custom action can
# bring its own dependencies. docker/profiles/docker-compose.actions.yml passes this
# variable to every datahub-actions service.
#
# The locked image variant removes uv and points the package index at a dead address
# on purpose (see docker/datahub-actions/Dockerfile). Runtime installation is refused
# there with an explicit error rather than skipped quietly.

set -euo pipefail

extra_packages="${ACTIONS_EXTRA_PACKAGES:-}"

if [ -z "$extra_packages" ]; then
  exit 0
fi

if ! command -v uv >/dev/null 2>&1; then
  # printf rather than cat: this path must still report itself in an image that has
  # been stripped down.
  printf '%s\n' \
    "ACTIONS_EXTRA_PACKAGES is set to \"${extra_packages}\", but this image cannot install" \
    "packages at runtime: no package manager is present. The locked datahub-actions image" \
    "strips uv and blocks its package index by design." \
    "" \
    "Use the full or slim datahub-actions image, or build an image that already contains" \
    "these packages." >&2
  exit 1
fi

echo "Installing ACTIONS_EXTRA_PACKAGES: ${extra_packages}"
# Unquoted on purpose: the variable holds a whitespace-separated list of packages.
# shellcheck disable=SC2086
uv pip install $extra_packages
