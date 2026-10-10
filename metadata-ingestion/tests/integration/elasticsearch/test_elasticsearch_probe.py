"""`datahub recipe probe` against a real Elasticsearch.

The unit tests drive the probe over a fake client. This covers what only a
real cluster shows: the indices `GET _alias` really returns (no hidden ones,
so no data stream's backing index), an index with no mappings, the server's built-in templates, and the probe's verdicts against
what Elasticsearch ingestion emits.

The probe's seed is named `probe*`, apart from the goldens' `my_*` objects.
"""

import json
import socket
import time
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Mapping, Optional, Set, Tuple

import pytest
import requests
import yaml
from click.testing import CliRunner, Result

from datahub.cli.recipe_cli import recipe as recipe_cli
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.metadata.schema_classes import SubTypesClass
from datahub.metadata.urns import DatasetUrn
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    JudgedRecord,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

# The parity harness masks its reports against the process-global registry,
# as the CLI does; an earlier test's registered secret would otherwise redact
# this fixture's identifiers.
pytestmark = [
    pytest.mark.integration_batch_2,
    pytest.mark.usefixtures("_isolate_secret_registry"),
]

_SOURCE_TYPE = "elasticsearch"
_RESOURCES = Path(__file__).parent
_PORT = 29200
_BASE_URL = f"http://localhost:{_PORT}"
_EXIT_SOURCE = 3
# Above the cluster's built-in templates, so no listing is cut.
_LIMIT = 1000

_FIELDS = {"properties": {"id": {"type": "long"}, "note": {"type": "text"}}}
_SETTINGS = {"number_of_shards": 1, "number_of_replicas": 0}

_SEED_PUTS: List[Tuple[str, Optional[Dict[str, object]]]] = [
    ("probe_orders", {"settings": _SETTINGS, "mappings": _FIELDS}),
    ("probe_tmp_load", {"settings": _SETTINGS, "mappings": _FIELDS}),
    ("probe_empty", {"settings": _SETTINGS}),
    (
        "_template/probe_legacy",
        {"index_patterns": ["probe-legacy-*"], "mappings": _FIELDS},
    ),
    (
        "_index_template/probe_settings_only",
        {"index_patterns": ["probe-settings-*"], "template": {"settings": _SETTINGS}},
    ),
    (
        "_index_template/probe_logs",
        {
            "index_patterns": ["probe-logs*"],
            "data_stream": {},
            # Above the built-in logs-*-* template's priority.
            "priority": 500,
            "template": {"settings": _SETTINGS, "mappings": _FIELDS},
        },
    ),
    ("_data_stream/probe-logs-app", None),
]


def _seed() -> None:
    deadline = time.monotonic() + 240
    while True:
        try:
            health = requests.get(
                f"{_BASE_URL}/_cluster/health",
                params={"wait_for_status": "yellow", "timeout": "5s"},
                timeout=10,
            )
            if health.status_code == 200:
                break
        except requests.RequestException:
            pass
        if time.monotonic() > deadline:
            raise TimeoutError("the cluster did not become healthy")
        time.sleep(2)
    for path, body in _SEED_PUTS:
        requests.put(f"{_BASE_URL}/{path}", json=body, timeout=30).raise_for_status()


@pytest.fixture(scope="module")
def elasticsearch(docker_compose_runner: Callable) -> Iterator[None]:
    with docker_compose_runner(
        _RESOURCES / "docker-compose.elasticsearch.yml", "elasticsearch-probe"
    ):
        _seed()
        yield


def _recipe(**extra: object) -> Dict[str, object]:
    return {"host": f"localhost:{_PORT}", **extra}


def _probe_cli(tmp_path: Path, recipe: Mapping[str, object], command: str) -> Result:
    recipe_file = tmp_path / "recipe.yml"
    recipe_file.write_text(
        yaml.safe_dump({"source": {"type": _SOURCE_TYPE, "config": dict(recipe)}})
    )
    return CliRunner().invoke(
        recipe_cli, ["probe", "run", command, "--recipe", str(recipe_file)]
    )


def _records(command: str) -> Dict[str, Dict[str, object]]:
    result = run_probe_method(_SOURCE_TYPE, _recipe(), command, {"limit": _LIMIT})
    assert isinstance(result.result, list)
    return {record["name"]: record for record in result.result}


def _backing_index() -> str:
    response = requests.get(f"{_BASE_URL}/_data_stream/probe-logs-app", timeout=30)
    response.raise_for_status()
    (stream,) = response.json()["data_streams"]
    (backing,) = stream["indices"]
    return str(backing["index_name"])


def test_indices_carry_mappings(elasticsearch: None) -> None:
    indices = _records("indices")
    assert indices["probe_orders"]["mapped_fields"] == 2
    assert indices["probe_empty"]["mapped_fields"] == 0


def test_hidden_backing_indices_are_not_listed(elasticsearch: None) -> None:
    """Elasticsearch 8's `GET _alias` leaves out hidden indices, a data
    stream's backing indices among them, so ingestion never sees the stream;
    the probe, listing the same way, does not list it either."""
    assert _backing_index() not in _records("indices")


def test_templates_list_legacy_and_composable(elasticsearch: None) -> None:
    templates = _records("index_templates")
    assert templates["probe_legacy"]["template_type"] == "legacy"
    assert templates["probe_legacy"]["mapped_fields"] == 2
    assert templates["probe_settings_only"]["template_type"] == "composable"
    assert templates["probe_settings_only"]["mapped_fields"] == 0


def _closed_local_port() -> int:
    """A port nothing listens on: one the OS just handed out and took back."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe_socket:
        probe_socket.bind(("127.0.0.1", 0))
        return probe_socket.getsockname()[1]


def test_an_unreachable_cluster_exits_as_the_source(tmp_path: Path) -> None:
    recipe = {"host": f"127.0.0.1:{_closed_local_port()}"}
    result = _probe_cli(tmp_path, recipe, "indices")
    assert result.exit_code == _EXIT_SOURCE, result.output
    assert "ConnectionError" in json.loads(result.stderr)["error"]


def _emitted(*sub_types: str) -> Callable[[EmittedIndex], Set[str]]:
    def emitted(index: EmittedIndex) -> Set[str]:
        return {
            DatasetUrn.from_string(urn).name
            for urn in index.urns("dataset", with_aspect=SubTypesClass)
            if any(
                isinstance(aspect, SubTypesClass)
                and set(sub_types) & set(aspect.typeNames)
                for aspect in index.aspects[urn]
            )
        }

    return emitted


def _index_identity(record: JudgedRecord) -> str:
    # A backing index is emitted as its data stream.
    return record.attributes.get("data_stream", record.name)


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param({"ingest_index_templates": True}, id="defaults"),
        pytest.param(
            {
                "index_pattern": {
                    "allow": ["^probe", "^\\.ds-probe"],
                    "deny": [".*_tmp_.*"],
                },
                "ingest_index_templates": True,
                "index_template_pattern": {"allow": ["^probe"]},
            },
            id="patterns",
        ),
    ],
)
def test_probe_verdicts_match_ingestion(
    elasticsearch: None, tmp_path: Path, extra: Dict[str, object]
) -> None:
    report = assert_probe_parity(
        _SOURCE_TYPE,
        _recipe(**extra),
        pipeline_ingestion(_SOURCE_TYPE, tmp_path),
        [
            ParityListing(
                "indices",
                "indices",
                emitted=_emitted(
                    DatasetSubTypes.ELASTIC_INDEX, DatasetSubTypes.ELASTIC_DATASTREAM
                ),
                kwargs={"limit": _LIMIT},
                identity=_index_identity,
            ),
            ParityListing(
                "templates",
                "index_templates",
                emitted=_emitted(DatasetSubTypes.ELASTIC_INDEX_TEMPLATE),
                kwargs={"limit": _LIMIT},
            ),
        ],
    )
    assert "probe_orders" in report.kinds["indices"].included
    assert "probe-logs-app" not in report.kinds["indices"].emitted
    assert report.excluded_by("indices")["probe_empty"] == "no_mapped_fields"
    assert report.excluded_by("templates")["probe_settings_only"] == (
        "no_mapped_fields"
    )
    if "index_pattern" in extra:
        assert report.excluded_by("indices")["probe_tmp_load"] == "index_pattern"
