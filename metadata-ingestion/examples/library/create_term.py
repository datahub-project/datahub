import logging

from datahub.emitter.mce_builder import make_term_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Imports for metadata model classes
from datahub.metadata.schema_classes import GlossaryTermInfoClass

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

term_urn = make_term_urn("rateofreturn")
term_properties_aspect = GlossaryTermInfoClass(
    definition="A rate of return (RoR) is the net gain or loss of an investment over a specified time period.",
    name="Rate of Return",
    termSource="",
)

event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=term_urn,
    aspect=term_properties_aspect,
)

# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)
log.info(f"Created term {term_urn}")
