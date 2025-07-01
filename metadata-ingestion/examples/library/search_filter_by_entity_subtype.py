from datahub.sdk import DataHubClient
from datahub.sdk.search_filters import FilterDsl as F

# search for all mlflow assets of subtype "ML Experiment"
client = DataHubClient(server="<your_server>", token="<your_token>")
results = client.search.get_urns(
    filter=F.and_(F.platform("mlflow"), F.entity_subtype("ML Experiment"))
)
