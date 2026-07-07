import pytest

from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.run.pipeline import Pipeline
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import get_kafka_broker_url, get_kafka_schema_registry

# Reuse the existing delete-test sample data + its dataset URN.
DATA_FILE = "tests/delete/cli_test_data.json"
DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-delete,PROD)"


@pytest.fixture(autouse=True)
def _clear_default_graph_cache():
    # get_default_graph is cached; clear before and after so env changes in this
    # module take effect and don't leak a stale graph into other smoke tests.
    get_default_graph.cache_clear()
    yield
    get_default_graph.cache_clear()


def _create_default_sink_pipeline(auth_session, monkeypatch, default_sink):
    """Build a no-sink pipeline (the shape UI ingestion uses) whose default sink
    is chosen by env. `default_sink` is "rest" or "kafka"."""
    # Point the default graph / REST (fallback) at the running stack.
    monkeypatch.setenv("DATAHUB_GMS_URL", auth_session.gms_url())
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", auth_session.gms_token())
    if default_sink == "kafka":
        # Both markers required to enable the managed Kafka default.
        monkeypatch.setenv("DATAHUB_INGESTION_DEFAULT_SINK", "kafka")
        monkeypatch.setenv("DATAHUB_EXECUTOR_MANAGED", "true")
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVER", get_kafka_broker_url())
        monkeypatch.setenv("KAFKA_SCHEMAREGISTRY_URL", get_kafka_schema_registry())
    get_default_graph.cache_clear()
    return Pipeline.create(
        {
            "source": {"type": "file", "config": {"filename": DATA_FILE}},
            "pipeline_name": f"default_sink_smoke_{default_sink}",
        }
    )


@pytest.mark.parametrize(
    "default_sink,expected_sink_type",
    [("rest", "datahub-rest"), ("kafka", "datahub-kafka")],
)
def test_default_sink_ingests_end_to_end(
    auth_session, graph_client, monkeypatch, default_sink, expected_sink_type
):
    """Same no-sink recipe run under the REST default and the managed Kafka
    default: each must select the expected sink and still land metadata in GMS.

    The Kafka arm exercises the exact flow UI ingestion uses; only the two env
    markers differ from the REST arm.
    """
    # Clean slate so a passing existence check proves this run delivered.
    graph_client.hard_delete_entity(DATASET_URN)
    wait_for_writes_to_sync()

    pipeline = _create_default_sink_pipeline(auth_session, monkeypatch, default_sink)
    assert pipeline.sink_type == expected_sink_type
    # ctx.graph must be wired either way so stateful ingestion (checkpoints,
    # stale-entity soft-deletes) keeps working.
    assert pipeline.ctx.graph is not None

    pipeline.run()
    pipeline.raise_from_status()
    wait_for_writes_to_sync()

    assert graph_client.exists(DATASET_URN)
