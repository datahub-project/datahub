from datahub.sdk import DatahubClient, FilterDsl as F

client = DatahubClient(gms_server="<your_server>", token="<your_token>")

# Search for entities that are in PROD environment and not a chart
results = client.search.get_urns(
    filter=F.and_(F.env("PROD"), F.not_(F.entity_type("chart")))
)

print(list(results))
