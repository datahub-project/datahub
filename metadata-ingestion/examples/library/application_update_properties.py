# metadata-ingestion/examples/library/application_update_properties.py
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import ApplicationPropertiesClass


def make_application_urn(application_id: str) -> str:
    """Create a DataHub application URN."""
    return f"urn:li:application:{application_id}"


emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

application_urn = make_application_urn("customer-analytics-service")

updated_properties = ApplicationPropertiesClass(
    name="Customer Analytics Service v2",
    description="Updated: A microservice that processes customer events and generates real-time analytics insights. Now includes ML-based predictions.",
    customProperties={
        "team": "data-platform",
        "language": "python",
        "repository": "https://github.com/company/customer-analytics",
        "version": "2.0.0",
        "deployment": "kubernetes",
    },
    externalUrl="https://wiki.company.com/customer-analytics-v2",
)

metadata_event = MetadataChangeProposalWrapper(
    entityUrn=application_urn,
    aspect=updated_properties,
)
emitter.emit(metadata_event)

print(f"Updated properties for application: {application_urn}")
