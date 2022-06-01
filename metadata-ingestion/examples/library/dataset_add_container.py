import logging
from typing import Optional

from datahub.emitter.mce_builder import make_dataset_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    ContainerClass,  
    ChangeTypeClass  
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
token='token here'

# Inputs -> owner, ownership_type, dataset
dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,datahub.metadata_aspect_v3,PROD)"


# Some objects to help with conditional pathways later
container_instance = ContainerClass(container = "")


event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityType="dataset",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=dataset_urn,
    aspectName="container",
    aspect=container_instance,
)
gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint, token=token))


graph.emit(event)
