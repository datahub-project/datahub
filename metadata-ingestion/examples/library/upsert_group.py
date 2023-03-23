import logging

from datahub.emitter.mce_builder import make_group_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Imports for metadata model classes
from datahub.metadata.schema_classes import CorpGroupInfoClass

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

group_urn = make_group_urn("foogroup@acryl.io")
event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=group_urn,
    aspect=CorpGroupInfoClass(
        admins=["urn:li:corpuser:datahub"],
        members=["urn:li:corpuser:bar@acryl.io", "urn:li:corpuser:joe@acryl.io"],
        groups=[],
        displayName="Foo Group",
        email="foogroup@acryl.io",
        description="Software engineering team",
        slack="@foogroup",
    ),
)

# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)
log.info(f"Upserted group {group_urn}")
