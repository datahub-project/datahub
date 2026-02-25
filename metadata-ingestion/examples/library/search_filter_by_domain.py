from datahub.sdk import DataHubClient
from datahub.sdk.search_filters import FilterDsl as F

# search for all assets in the marketing domain
client = DataHubClient.from_env()
results = client.search.get_urns(filter=F.domain("urn:li:domain:marketing"))
