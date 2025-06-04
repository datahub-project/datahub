from datahub.sdk import DatahubClient

client = DatahubClient(gms_server="<your_server>", token="<your_token>")

# Search for datasets with "forecast" in the metadata
results = client.search.get_urns(query="forecast")

print(list(results))
