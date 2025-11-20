import time

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

dataset_urn = builder.make_dataset_urn(
    platform="bigquery", name="project.dataset.transactions", env="PROD"
)

field_urn = builder.make_schema_field_urn(
    parent_urn=dataset_urn, field_path="transaction_amount"
)

current_docs = graph.get_aspect(
    entity_urn=field_urn, aspect_type=models.DocumentationClass
)

documentation_text = (
    "The monetary value of the transaction in USD. "
    "This field is calculated from the base currency amount "
    "using the exchange rate at transaction time."
)

attribution = models.MetadataAttributionClass(
    time=int(time.time() * 1000),
    actor=builder.make_user_urn("data_steward"),
    source=builder.make_data_platform_urn("manual"),
)

new_doc = models.DocumentationAssociationClass(
    documentation=documentation_text,
    attribution=attribution,
)

if current_docs and current_docs.documentations:
    source_exists = False
    for i, doc in enumerate(current_docs.documentations):
        if doc.attribution and doc.attribution.source == attribution.source:
            current_docs.documentations[i] = new_doc
            source_exists = True
            break
    if not source_exists:
        current_docs.documentations.append(new_doc)
else:
    current_docs = models.DocumentationClass(documentations=[new_doc])

emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=field_urn,
        aspect=current_docs,
    )
)
