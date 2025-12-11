#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.


set -euxo pipefail

pip install -e 'git+https://github.com/acryldata/avro_gen#egg=avro-gen3'
pip install -e 'git+https://github.com/acryldata/PyHive#egg=acryl-pyhive[hive]'
pip install -e '.[dev]'
