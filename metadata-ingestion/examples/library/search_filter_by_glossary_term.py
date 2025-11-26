from datahub.sdk import DataHubClient
from datahub.sdk.search_filters import FilterDsl as F

# search for all assets associated with a specific glossary term
client = DataHubClient(server="<your_server>", token="<your_token>")
results = client.search.get_urns(
    filter=F.glossary_term("urn:li:glossaryTerm:AccountBalance")
)
