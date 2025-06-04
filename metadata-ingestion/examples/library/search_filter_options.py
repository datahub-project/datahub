from datahub.sdk import DatahubClient, FilterDsl as F

client = DatahubClient(gms_server="<your_server>", token="<your_token>")

# Search for entities that are in PROD environment
prod_results = client.search.get_urns(filter=F.env("PROD"))

# Search for entities that are dashboards
dashboards = client.search.get_urns(filter=F.entity_type("dashboard"))

# Search for datasets that are on snowflake platform
snowflake_datasets = client.search.get_urns(
    filter=F.and_(F.entity_type("dataset"), F.platform("snowflake"))
)


print(list(prod_results))
print(list(dashboards))
print(list(snowflake_datasets))
