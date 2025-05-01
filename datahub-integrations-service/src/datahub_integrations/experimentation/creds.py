import json

import cachetools
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

from datahub_integrations.app import ROOT_DIR
from datahub_integrations.gen_ai.cached_graph import make_cached_graph
from datahub_integrations.util.serialized import serialized

_graph_credentials = json.loads(
    (ROOT_DIR / "experiments/graph_credentials.json").read_text()
)
assert "acryl" in _graph_credentials


@serialized
@cachetools.cached(cache=cachetools.Cache(maxsize=1000))
def create_uncached_datahub_graph(key: str) -> DataHubGraph:
    creds = _graph_credentials[key]
    graph = DataHubGraph(
        DatahubClientConfig(
            **creds,
        )
    )
    graph.test_connection()

    return graph


@serialized
@cachetools.cached(cache=cachetools.Cache(maxsize=1000))
def create_cached_datahub_graph(key: str) -> DataHubGraph:
    graph = create_uncached_datahub_graph(key)
    return make_cached_graph(graph, ttl_sec=None)


if __name__ == "__main__":
    # Test the graph helper.

    assert create_cached_datahub_graph("acryl")
    _test_entity_1 = create_cached_datahub_graph(
        "longtailcompanions"
    ).get_entity_semityped(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.pet_profiles,PROD)"
    )
    assert isinstance(_test_entity_1, dict)
    _test_entity_2 = create_cached_datahub_graph(
        "longtailcompanions"
    ).get_entity_semityped(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,datahub_community.datahub_slack.message,PROD)"
    )
    assert isinstance(_test_entity_2, dict)
    assert _test_entity_1["datasetKey"] != _test_entity_2["datasetKey"]  # type: ignore
    del _test_entity_1, _test_entity_2
