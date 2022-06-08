from datahub.emitter.mcp import MetadataChangeProposalWrapper

# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    ContainerClass,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
)

gms_endpoint = "http://localhost:8080"
token = "xxx-your-token-here-xxx"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint, token=token))

# Code below creates a new container, "man-123-bin" with its associated 
# properties AND link it to the parent container new_urn.

new_urn = "urn:li:container:csv_container2"

new_urn2 = "urn:li:container:man-123-bin"
event = MetadataChangeProposalWrapper(
    entityType="container",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=new_urn2,
    aspectName="containerProperties",
    aspect=ContainerPropertiesClass(
        name="man-123-bin",
        description="some platformless container",
    ),
)
graph.emit(event)
event = MetadataChangeProposalWrapper(
    entityType="container",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=new_urn2,
    aspectName="container",
    aspect=ContainerClass(container=new_urn),
)

graph.emit(event)
event = MetadataChangeProposalWrapper(
    entityType="container",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=new_urn2,
    aspectName="dataPlatformInstance",
    aspect=DataPlatformInstanceClass(platform="urn:li:dataPlatform:csv"),
)
graph.emit(event)
