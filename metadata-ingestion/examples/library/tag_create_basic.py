# metadata-ingestion/examples/library/tag_create_basic.py
import logging
import os

from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import TagPropertiesClass

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create a tag URN
tag_urn = make_tag_urn("pii")

# Define tag properties
tag_properties = TagPropertiesClass(
    name="Personally Identifiable Information",
    description="This tag indicates that the asset contains PII data and should be handled according to data privacy regulations.",
    colorHex="#FF0000",
)

# Create the metadata change proposal
event = MetadataChangeProposalWrapper(
    entityUrn=tag_urn,
    aspect=tag_properties,
)

# Emit to DataHub
rest_emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)
rest_emitter.emit(event)
log.info(f"Created tag {tag_urn}")
