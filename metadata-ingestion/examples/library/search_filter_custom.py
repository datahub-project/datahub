from datahub.sdk import DataHubClient, FilterDsl as F

client = DataHubClient(server="<your_server>", token="<your_token>")

# Search for datasets that have "example_dataset" in the urn
results = client.search.get_urns(
    filter=F.custom_filter(field="urn", condition="CONTAIN", values=["example_dataset"])
)

print(list(results))
