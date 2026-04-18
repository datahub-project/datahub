# metadata-ingestion/examples/library/application_create.py
import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import ApplicationPropertiesClass


def make_application_urn(application_id: str) -> str:
    """Create a DataHub application URN."""
    return f"urn:li:application:{application_id}"


gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
emitter = DatahubRestEmitter(gms_server=gms_server, token=token)

application_urn = make_application_urn("customer-analytics-service")

application_properties = ApplicationPropertiesClass(
    name="Customer Analytics Service",
    description="A microservice that processes customer events and generates analytics insights for the marketing team",
    customProperties={
        "team": "data-platform",
        "language": "python",
        "repository": "https://github.com/company/customer-analytics",
    },
    externalUrl="https://wiki.company.com/customer-analytics",
)

metadata_event = MetadataChangeProposalWrapper(
    entityUrn=application_urn,
    aspect=application_properties,
)
emitter.emit(metadata_event)

print(f"Created application: {application_urn}")
