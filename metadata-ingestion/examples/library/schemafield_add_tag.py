import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

dataset_urn = builder.make_dataset_urn(
    platform="postgres", name="public.users", env="PROD"
)

field_urn = builder.make_schema_field_urn(
    parent_urn=dataset_urn, field_path="email_address"
)

current_tags = graph.get_aspect(
    entity_urn=field_urn, aspect_type=models.GlobalTagsClass
)

tag_to_add = builder.make_tag_urn("PII")
tag_association = models.TagAssociationClass(tag=tag_to_add)

if current_tags and current_tags.tags:
    if tag_to_add not in [tag.tag for tag in current_tags.tags]:
        current_tags.tags.append(tag_association)
else:
    current_tags = models.GlobalTagsClass(tags=[tag_association])

emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=field_urn,
        aspect=current_tags,
    )
)
