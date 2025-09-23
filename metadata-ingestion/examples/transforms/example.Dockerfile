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