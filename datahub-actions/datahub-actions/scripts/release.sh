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

#!/bin/bash
set -euxo pipefail

ROOT=../..

if [[ ! ${RELEASE_SKIP_TEST:-} ]] && [[ ! ${RELEASE_SKIP_INSTALL:-} ]]; then
    ${ROOT}/gradlew build  # also runs tests
elif [[ ! ${RELEASE_SKIP_INSTALL:-} ]]; then
    ${ROOT}/gradlew install
fi

# Check packaging constraint.
python -c 'import setuptools; where="./src"; assert setuptools.find_packages(where) == setuptools.find_namespace_packages(where), "you seem to be missing or have extra __init__.py files"'

if [[ ${RELEASE_VERSION:-} ]]; then
    # Replace version with RELEASE_VERSION env variable
    sed -i.bak "s/__version__ = \"0.0.0.dev0\"/__version__ = \"$RELEASE_VERSION\"/" src/datahub_actions/__init__.py
else
    vim src/datahub_actions/__init__.py
fi

rm -rf build dist || true
python -m build
if [[ ! ${RELEASE_SKIP_UPLOAD:-} ]]; then
    python -m twine upload 'dist/*' --verbose
fi
git restore src/datahub_actions/__init__.py