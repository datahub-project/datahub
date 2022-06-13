import time

from datahub.emitter.mcp import MetadataChangeProposalWrapper

# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    DatasetPropertiesClass,
    InstitutionalMemoryMetadataClass,
)

# Inputs -> owner, ownership_type, dataset
documentation_to_add = "## The Real Estate Sales Dataset\nThis is a really important Dataset that contains all the relevant information about sales that have happened organized by address.\n"
link_to_add = "https://wikipedia.com/real_estate"
link_description = "This is the definition of what real estate means"
dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"

# Some helpful variables to fill out objects later
now = int(time.time() * 1000)  # milliseconds since epoch
current_timestamp = AuditStampClass(time=now, actor="urn:li:corpuser:ingestion")
institutional_memory_element = InstitutionalMemoryMetadataClass(
    url=link_to_add,
    description=link_description,
    createStamp=current_timestamp,
)


# First we get the current owners
gms_endpoint = "http://172.19.0.1:8080"
token = "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRlbW8iLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMSIsImV4cCI6MTY1MzU1MjQ4MSwianRpIjoiZTNiMmNiYzItMzcxOC00YmUzLTk3MWQtMmQ2MzkzNjJiNDQxIiwic3ViIjoiZGVtbyIsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.bSYl_K-6NPYeuRDaAyZ0NWK3Mo4bHE53OO-w5C53KXE"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint, token=token))

current_properties = graph.get_aspect_v2(
    entity_urn=dataset_urn,
    aspect="datasetProperties",
    aspect_type=DatasetPropertiesClass,
)
new_properties = DatasetPropertiesClass(description="# backend blah \n")


event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityType="dataset",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=dataset_urn,
    aspectName="datasetProperties",
    aspect=new_properties,
)
graph.emit(event)
