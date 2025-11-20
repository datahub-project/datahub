from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.urns import MlFeatureUrn

graph = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))

feature_urn = MlFeatureUrn(
    feature_namespace="user_features",
    name="age",
)

relationships = graph.get_related_entities(
    entity_urn=str(feature_urn),
    relationship_types=["Contains"],
    direction=DataHubGraph.RelationshipDirection.INCOMING,
)

if relationships:
    feature_table_urns = [rel.urn for rel in relationships]
    print(f"Feature {feature_urn} is contained in tables:")
    for table_urn in feature_table_urns:
        print(f"  - {table_urn}")
else:
    print(f"Feature {feature_urn} is not associated with any feature table")
