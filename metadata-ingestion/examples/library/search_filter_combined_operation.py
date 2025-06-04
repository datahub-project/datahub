from datahub.sdk import DatahubClient, FilterDsl as F

client = DatahubClient(gms_server="<your_server>", token="<your_token>")

# Search for charts or snowflake datasets
results = client.search.get_urns(
    filter=F.or_(
        F.entity_type("chart"),
        F.and_(F.platform("snowflake"), F.entity_type("dataset")),
    )
)

print(list(results))
