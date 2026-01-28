import logging
import os

from datahub.emitter.mce_builder import make_domain_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DomainPropertiesClass

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

domain_urn = make_domain_urn("marketing")
domain_properties_aspect = DomainPropertiesClass(
    name="Verticals",
    description="Entities related to the verticals sub-domain",
    parentDomain="urn:li:domain:marketing",
)

event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=domain_urn,
    aspect=domain_properties_aspect,
)

# Get DataHub connection details from environment
gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")

rest_emitter = DatahubRestEmitter(gms_server=gms_server, token=token)
rest_emitter.emit(event)
log.info(f"Created domain {domain_urn}")
