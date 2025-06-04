from datahub.sdk import DataHubClient

client = DataHubClient(server="<your_server>", token="<your_token>")

# Search for datasets with "forecast" in the metadata
results = client.search.get_urns(query="forecast")

print(list(results))
