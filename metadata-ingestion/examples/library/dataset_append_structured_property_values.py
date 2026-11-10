from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.graph.client import get_default_graph

# Uses the instance from `datahub init`, or DATAHUB_GMS_URL / DATAHUB_GMS_TOKEN.
graph = get_default_graph()

dataset_urn = make_dataset_urn(platform="hive", name="fct_users_created", env="PROD")

# Keeps the values the property already has and appends any that are missing.
graph.add_structured_property_values(
    entity_urn=dataset_urn,
    property="io.acryl.privacy.dataOwners",
    values=["team-a", "team-b"],
)
