import pathlib
import time
from typing import Any, Iterator

import pytest
import requests

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

_resources_dir = pathlib.Path(__file__).parent

pytestmark = pytest.mark.integration_batch_2

ELASTICSEARCH_PORT = 29200
OPENSEARCH_PORT = 29201

# Both backends must produce identical metadata, so the two tests share one golden; a
# divergence (e.g. a server version serializing mappings differently) fails the comparison.
_GOLDEN = "elasticsearch_mces_golden.json"

_INDICES = {
    "my_index": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "title": {"type": "text"},
                "views": {"type": "long"},
                "created_at": {"type": "date"},
                "author": {
                    "properties": {
                        "name": {"type": "keyword"},
                        "id": {"type": "long"},
                    }
                },
            }
        },
    },
    "my_other_index": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "sku": {"type": "keyword"},
                "price": {"type": "double"},
                "in_stock": {"type": "boolean"},
            }
        },
    },
}

_COMPOSABLE_TEMPLATE = {
    "index_patterns": ["my_log_*"],
    "template": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "message": {"type": "text"},
                "level": {"type": "keyword"},
                "timestamp": {"type": "date"},
            }
        },
    },
}


def _seed(base_url: str, timeout: float = 240.0) -> None:
    """Wait for the cluster, then create indices + a composable template.

    Uses the REST API directly (identical for Elasticsearch and OpenSearch) so seeding
    is client-library-agnostic.
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            resp = requests.get(
                f"{base_url}/_cluster/health",
                params={"wait_for_status": "yellow", "timeout": "5s"},
                timeout=10,
            )
            if resp.status_code == 200:
                break
        except requests.RequestException:
            pass
        if time.monotonic() > deadline:
            raise TimeoutError(f"cluster at {base_url} did not become healthy")
        time.sleep(2)

    for name, body in _INDICES.items():
        resp = requests.put(f"{base_url}/{name}", json=body, timeout=30)
        resp.raise_for_status()

    resp = requests.put(
        f"{base_url}/_index_template/my_template", json=_COMPOSABLE_TEMPLATE, timeout=30
    )
    resp.raise_for_status()


@pytest.fixture(scope="module")
def elasticsearch_runner(docker_compose_runner: Any) -> Iterator[None]:
    with docker_compose_runner(
        _resources_dir / "docker-compose.elasticsearch.yml", "elasticsearch"
    ):
        _seed(f"http://localhost:{ELASTICSEARCH_PORT}")
        yield


@pytest.fixture(scope="module")
def opensearch_runner(docker_compose_runner: Any) -> Iterator[None]:
    with docker_compose_runner(
        _resources_dir / "docker-compose.opensearch.yml", "opensearch"
    ):
        _seed(f"http://localhost:{OPENSEARCH_PORT}")
        yield


def _run_pipeline(port: int, run_id: str, output_path: str) -> None:
    pipeline = Pipeline.create(
        {
            "run_id": run_id,
            "source": {
                "type": "elasticsearch",
                "config": {
                    "host": f"localhost:{port}",
                    "index_pattern": {"allow": ["my_.*"]},
                    "ingest_index_templates": True,
                    "index_template_pattern": {"allow": ["my_.*"]},
                    "env": "PROD",
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": output_path},
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()


@pytest.mark.integration
def test_elasticsearch_ingest(elasticsearch_runner, pytestconfig, tmp_path):
    output_path = f"{tmp_path}/elasticsearch_mces.json"
    _run_pipeline(ELASTICSEARCH_PORT, "elasticsearch-test", output_path)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=_resources_dir / _GOLDEN,
    )


@pytest.mark.integration
def test_opensearch_ingest(opensearch_runner, pytestconfig, tmp_path):
    output_path = f"{tmp_path}/opensearch_mces.json"
    _run_pipeline(OPENSEARCH_PORT, "opensearch-test", output_path)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=_resources_dir / _GOLDEN,
    )
