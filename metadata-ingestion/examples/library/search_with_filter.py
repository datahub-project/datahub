from datahub.sdk import DataHubClient, FilterDsl as F

client = DataHubClient(server="<your_server>", token="<your_token>")

# Search for entities that are on snowflake platform
results = client.search.get_urns(filter=F.platform("snowflake"))

print(list(results))
