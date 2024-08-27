from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import MLModelGroupPropertiesClass

# First we get the current owners
gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

urn = "urn:li:mlModelGroup:(urn:li:dataPlatform:science,my-model-group,PROD)"
result = graph.get_aspect(entity_urn=urn, aspect_type=MLModelGroupPropertiesClass)

print(result)
