from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import ChangeTypeClass, DomainPropertiesClass

gms_endpoint = "http://localhost:8080"

graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))
new_urn = "urn:li:domain:mydomain"


event = MetadataChangeProposalWrapper(
    entityType="domain",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=new_urn,
    aspectName="domainProperties",
    aspect=DomainPropertiesClass(
        name="myname",
        description="blah",
    ),
)
graph.emit(event)
