from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import UpstreamLineageClass

# Get the current lineage for a dataset
gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

urn = "urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)"
result = graph.get_aspect(entity_urn=urn, aspect_type=UpstreamLineageClass)

print(result)
