# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

FROM 795586375822.dkr.ecr.us-west-2.amazonaws.com/datahub-executor:v0.3.8.2-acryl

COPY setup.py custom_transformer/setup.py
COPY custom_transform_example.py custom_transformer/custom_transform_example.py
COPY owners.json custom_transformer/owners.json
COPY recipe.dhub.yaml custom_transformer/recipe.dhub.yaml

USER root

RUN chown -R root:root custom_transformer/* && \
    chmod -R u+rw custom_transformer && \
    python3 -m venv venv && \
    . venv/bin/activate && \
    pip install uv && \
    uv pip install 'acryl-datahub' && \
    uv pip install -e 'file:///datahub-executor/custom_transformer'

USER datahub