from datahub.sdk import DataHubClient
from datahub.sdk.search_filters import FilterDsl as F

# search for all assets having user urn:li:corpuser:jdoe as owner
client = DataHubClient(server="<your_server>", token="<your_token>")
results = client.search.get_urns(filter=F.owner("urn:li:corpuser:jdoe"))
