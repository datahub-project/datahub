from datahub.sdk import DataHubClient

client = DataHubClient(server="<your_server>", token="<your_token>")

# Search for entities with "sales" in the metadata
results = client.search.get_urns(query="sales")

print(list(results))
