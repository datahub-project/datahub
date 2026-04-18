# metadata-ingestion/examples/library/application_add_tag.py
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass


def make_application_urn(application_id: str) -> str:
    """Create a DataHub application URN."""
    return f"urn:li:application:{application_id}"


def make_tag_urn(tag_name: str) -> str:
    """Create a DataHub tag URN."""
    return f"urn:li:tag:{tag_name}"


emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

application_urn = make_application_urn("customer-analytics-service")

tag_to_add = make_tag_urn("production")

tags = GlobalTagsClass(
    tags=[
        TagAssociationClass(tag=tag_to_add),
    ]
)

metadata_event = MetadataChangeProposalWrapper(
    entityUrn=application_urn,
    aspect=tags,
)
emitter.emit(metadata_event)

print(f"Added tag {tag_to_add} to application {application_urn}")
