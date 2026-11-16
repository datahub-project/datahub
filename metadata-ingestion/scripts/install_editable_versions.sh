#!/bin/bash

set -euxo pipefail

pip install -e 'git+https://github.com/acryldata/avro_gen#egg=avro-gen3'
pip install -e '.[dev]'
