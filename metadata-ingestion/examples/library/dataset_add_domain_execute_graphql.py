# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.emitter.mce_builder import make_dataset_urn, make_domain_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DomainsClass, ChangeTypeClass

# Define the dataset URN
dataset_urn = make_dataset_urn(platform="hive", name="realestate_db.sales", env="PROD")

gms_endpoint = "http://localhost:8080"
rest_emitter = DatahubRestEmitter(gms_server=gms_endpoint)

# Define the domain URN
domain_urn = make_domain_urn("marketing")

# Create the Domains aspect
domains_aspect = DomainsClass(domains=[domain_urn])

# Create the MetadataChangeProposalWrapper event
event = MetadataChangeProposalWrapper(
    entityUrn=dataset_urn,
    aspect=domains_aspect,
    changeType=ChangeTypeClass.UPSERT
)

# Create the REST emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

# Emit the event
rest_emitter.emit(event)
