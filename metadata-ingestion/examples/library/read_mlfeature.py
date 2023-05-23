from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import MLFeaturePropertiesClass

# First we get the current owners
gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

urn = "urn:li:mlFeature:(test_feature_table_all_feature_dtypes,test_BOOL_feature)"
result = graph.get_aspect(entity_urn=urn, aspect_type=MLFeaturePropertiesClass)

print(result)
