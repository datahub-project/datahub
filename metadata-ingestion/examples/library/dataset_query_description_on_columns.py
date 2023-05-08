from datahub.emitter.mce_builder import make_dataset_urn

# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import SchemaMetadataClass

dataset_urn = make_dataset_urn(platform="hive", name="fct_users_created", env="PROD")

gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

# Query multiple aspects from entity
result = graph.get_aspects_for_entity(
    entity_urn=dataset_urn,
    aspects=["schemaMetadata"],
    aspect_types=[SchemaMetadataClass],
)["schemaMetadata"]

if result:
    column_descriptions = [
        {field.fieldPath: field.description} for field in result.fields
    ]
    print(column_descriptions)
