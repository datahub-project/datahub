from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import MLFeaturePropertiesClass
from datahub.metadata.urns import MlFeatureUrn

graph = get_default_graph()

# Or get this from the UI (share -> copy urn) and use MlFeatureUrn.from_string(...)
mlfeature_urn = MlFeatureUrn("users_feature_table", "user_signup_date")

mlfeature_properties = graph.get_aspect(
    entity_urn=str(mlfeature_urn),
    aspect_type=MLFeaturePropertiesClass,
)

print("MLFeature name:", mlfeature_urn.name)
print("MLFeature namespace:", mlfeature_urn.feature_namespace)
if mlfeature_properties is None:
    raise SystemExit(f"MLFeature not found: {mlfeature_urn}")

print("MLFeature description:", mlfeature_properties.description)
print("MLFeature data type:", mlfeature_properties.dataType)
print("MLFeature sources:", mlfeature_properties.sources)
