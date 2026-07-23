# metadata-ingestion/examples/library/application_add_domain.py
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DomainsClass


def make_application_urn(application_id: str) -> str:
    """Create a DataHub application URN."""
    return f"urn:li:application:{application_id}"


def make_domain_urn(domain_id: str) -> str:
    """Create a DataHub domain URN."""
    return f"urn:li:domain:{domain_id}"


emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

application_urn = make_application_urn("customer-analytics-service")

domain_to_add = make_domain_urn("marketing")

domains = DomainsClass(domains=[domain_to_add])

metadata_event = MetadataChangeProposalWrapper(
    entityUrn=application_urn,
    aspect=domains,
)
emitter.emit(metadata_event)

print(f"Added domain {domain_to_add} to application {application_urn}")
