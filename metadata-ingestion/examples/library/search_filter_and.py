from datahub.sdk import DataHubClient, FilterDsl as F

client = DataHubClient(server="<your_server>", token="<your_token>")

# Search snowflake datasets that are in PROD environment
results = client.search.get_urns(filter=F.and_(F.platform("snowflake"), F.env("PROD")))

print(list(results))
