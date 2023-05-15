import logging

from datahub.cli import delete_cli
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

graph = DataHubGraph(
    config=DatahubClientConfig(
        server="http://localhost:8080",
    )
)

dataset_urn = make_dataset_urn(name="fct_users_created", platform="hive")

delete_cli._delete_one_urn(graph, urn=dataset_urn, soft=True)

log.info(f"Deleted dataset {dataset_urn}")
