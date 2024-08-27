import json
import pathlib

import cachetools
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

from datahub_integrations.gen_ai.cached_graph import make_cached_graph

current_dir = pathlib.Path(__file__).parent.resolve()
graph_credentials = json.loads((current_dir / "graph_credentials.json").read_text())
assert "acryl" in graph_credentials


@cachetools.cached(cache=cachetools.Cache(maxsize=1000))
def create_datahub_graph(key: str) -> DataHubGraph:
    """Create a DataHub client based on the graph credentials."""

    creds = graph_credentials[key]
    graph = DataHubGraph(
        DatahubClientConfig(
            **creds,
        )
    )
    graph.test_connection()

    cached_graph = make_cached_graph(graph, ttl_sec=None)
    cached_graph.test_connection()
    return cached_graph


if __name__ == "__main__":
    # Test the graph helper.

    assert create_datahub_graph("acryl")
    _test_entity_1 = create_datahub_graph("longtailcompanions").get_entity_semityped(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.pet_profiles,PROD)"
    )
    assert isinstance(_test_entity_1, dict)
    _test_entity_2 = create_datahub_graph("longtailcompanions").get_entity_semityped(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,datahub_community.datahub_slack.message,PROD)"
    )
    assert isinstance(_test_entity_2, dict)
    assert _test_entity_1["datasetKey"] != _test_entity_2["datasetKey"]  # type: ignore
    del _test_entity_1, _test_entity_2
