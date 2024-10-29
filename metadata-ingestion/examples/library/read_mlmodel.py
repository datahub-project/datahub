from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import MLModelPropertiesClass

# First we get the current owners
gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

urn = "urn:li:mlModel:(urn:li:dataPlatform:science,scienceModel,PROD)"
result = graph.get_aspect(entity_urn=urn, aspect_type=MLModelPropertiesClass)

print(result)
