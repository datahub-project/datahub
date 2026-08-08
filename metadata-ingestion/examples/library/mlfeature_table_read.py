from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import MLFeatureTablePropertiesClass
from datahub.metadata.urns import MlFeatureTableUrn

graph = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))

# Or get this from the UI (share -> copy urn) and use MlFeatureTableUrn.from_string(...)
mlfeature_table_urn = MlFeatureTableUrn(
    "feast", "test_feature_table_all_feature_dtypes"
)

mlfeature_table_properties = graph.get_aspect(
    entity_urn=str(mlfeature_table_urn),
    aspect_type=MLFeatureTablePropertiesClass,
)

print("MLFeature Table name:", mlfeature_table_urn.name)
print("MLFeature Table platform:", mlfeature_table_urn.platform)
if mlfeature_table_properties is not None:
    print("MLFeature Table description:", mlfeature_table_properties.description)
    print("MLFeature Table features:", mlfeature_table_properties.mlFeatures)
else:
    print(f"MLFeature Table {mlfeature_table_urn} not found")
