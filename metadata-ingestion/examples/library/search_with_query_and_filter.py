from datahub.sdk import DataHubClient, FilterDsl as F

client = DataHubClient(server="<your_server>", token="<your_token>")

# Search snowflake datasets that have "forecast" in the metadata
results = client.search.get_urns(
    query="forecast", filter=F.and_(F.platform("snowflake"), F.entity_type("dataset"))
)
print(list(results))
