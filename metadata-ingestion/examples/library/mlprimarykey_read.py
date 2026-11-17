from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import MLPrimaryKeyPropertiesClass
from datahub.metadata.urns import MlPrimaryKeyUrn

graph = get_default_graph()

# Or get this from the UI (share -> copy urn) and use MlPrimaryKeyUrn.from_string(...)
mlprimarykey_urn = MlPrimaryKeyUrn("users_feature_table", "user_id")

mlprimarykey_properties = graph.get_aspect(
    entity_urn=str(mlprimarykey_urn),
    aspect_type=MLPrimaryKeyPropertiesClass,
)

print("MLPrimaryKey name:", mlprimarykey_urn.name)
print("MLPrimaryKey namespace:", mlprimarykey_urn.feature_namespace)
if mlprimarykey_properties is None:
    raise SystemExit(f"MLPrimaryKey not found: {mlprimarykey_urn}")

print("MLPrimaryKey description:", mlprimarykey_properties.description)
print("MLPrimaryKey data type:", mlprimarykey_properties.dataType)
print("MLPrimaryKey sources:", mlprimarykey_properties.sources)
