"""Elasticsearch's probe: index and template names with the facts that decide
whether ingestion emits them, judged by the predicate ingestion filters with."""

import json
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Mapping, Optional, Sequence, Set

import pytest
import yaml
from click.testing import CliRunner, Result
from opensearchpy.exceptions import (
    AuthenticationException,
    ConnectionError,
    NotFoundError,
)

from datahub.cli.recipe_cli import recipe as recipe_cli
from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.elastic_search import ElasticsearchSourceConfig
from datahub.ingestion.source.elastic_search_probe import ElasticsearchMetadataProbe
from datahub.metadata.schema_classes import SubTypesClass
from datahub.metadata.urns import DatasetUrn
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    JudgedRecord,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

pytestmark = pytest.mark.usefixtures("_isolate_secret_registry")

# A credential the fixture registers, so its masking can be checked.
_SECRET_VALUE = "probe-secret-value"
_SOURCE_TYPE = "elasticsearch"
_EXIT_SOURCE = 3

_FIELDS = {"properties": {"id": {"type": "long"}, "note": {"type": "text"}}}
_BACKING = ".ds-logs-app-2026.01.01-000001"

_INDICES: Dict[str, Dict[str, object]] = {
    "orders": {"mappings": _FIELDS},
    "tmp_orders": {"mappings": _FIELDS},
    "empty_index": {"mappings": {}},
    "_internal": {"mappings": _FIELDS},
    _BACKING: {"mappings": _FIELDS, "data_stream": "logs-app"},
}
_LEGACY: Dict[str, Dict[str, object]] = {
    "legacy_template": {"index_patterns": ["legacy-*"], "mappings": _FIELDS},
    "settings_only": {"index_patterns": ["s-*"], "settings": {}},
}
_COMPOSABLE: List[Dict[str, object]] = [
    {
        "name": "composable_template",
        "index_template": {
            "index_patterns": ["c-*"],
            "template": {"mappings": _FIELDS},
        },
    },
    {
        "name": "tmp_template",
        "index_template": {
            "index_patterns": ["t-*"],
            "template": {"mappings": _FIELDS},
        },
    },
]


class _FakeIndices:
    def __init__(self, owner: "_FakeOpenSearch") -> None:
        self._owner = owner

    def get_alias(self) -> Dict[str, object]:
        if _FakeOpenSearch.fail_with is not None:
            raise _FakeOpenSearch.fail_with
        return {name: {"aliases": {}} for name in _INDICES}

    def get(self, *, index: str) -> Dict[str, object]:
        self._owner.index_requests.append(index)
        return {name: _INDICES[name] for name in index.split(",")}

    def get_template(self, *, name: Optional[str] = None) -> Dict[str, object]:
        if name is None:
            return dict(_LEGACY)
        return {name: _LEGACY[name]}

    def get_index_template(self, *, name: Optional[str] = None) -> Dict[str, object]:
        if _FakeOpenSearch.composable_error is not None:
            raise _FakeOpenSearch.composable_error
        listed = [t for t in _COMPOSABLE if name is None or t["name"] == name]
        return {"index_templates": listed}


class _FakeOpenSearch:
    instances: List["_FakeOpenSearch"] = []
    fail_with: Optional[Exception] = None
    composable_error: Optional[Exception] = None

    def __init__(self, host: str, **kwargs: object) -> None:
        self.host = host
        self.kwargs = kwargs
        self.closed = False
        self.index_requests: List[str] = []
        self.indices = _FakeIndices(self)
        _FakeOpenSearch.instances.append(self)

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def fake_opensearch(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    _FakeOpenSearch.instances = []
    _FakeOpenSearch.fail_with = None
    _FakeOpenSearch.composable_error = None
    monkeypatch.setattr(
        "datahub.ingestion.source.elastic_search.OpenSearch", _FakeOpenSearch
    )
    yield


def _recipe(**extra: object) -> Dict[str, object]:
    return {
        "host": "localhost:9200",
        "username": "probe_user",
        "password": _SECRET_VALUE,
        **extra,
    }


def _records(command: str, **recipe: object) -> List[Dict[str, object]]:
    result = run_probe_method(_SOURCE_TYPE, _recipe(**recipe), command, {})
    assert isinstance(result.result, list)
    return result.result


def _judge(
    kind: str,
    names: List[str],
    attributes: Optional[Sequence[Mapping[str, str]]] = None,
    **recipe: object,
) -> FilterCheckResult:
    return check_filters(
        _SOURCE_TYPE, _recipe(**recipe), kind, [], names, attributes=attributes
    )


def _verdicts(result: FilterCheckResult) -> Dict[str, Optional[str]]:
    return {v.name: (None if v.included else v.excluded_by) for v in result.results}


def _probe_cli(tmp_path: Path, recipe: Mapping[str, object], command: str) -> Result:
    recipe_file = tmp_path / "recipe.yml"
    recipe_file.write_text(
        yaml.safe_dump({"source": {"type": _SOURCE_TYPE, "config": dict(recipe)}})
    )
    return CliRunner().invoke(
        recipe_cli, ["probe", "run", command, "--recipe", str(recipe_file)]
    )


def test_indices_carry_what_decides_their_emission() -> None:
    by_name = {r["name"]: r for r in _records("indices")}
    assert set(by_name) == set(_INDICES)
    assert by_name["orders"] == {"name": "orders", "mapped_fields": 2}
    assert by_name["empty_index"]["mapped_fields"] == 0
    assert by_name[_BACKING]["data_stream"] == "logs-app"


def test_index_bodies_are_fetched_in_batches_by_name() -> None:
    _records("indices")
    (client,) = _FakeOpenSearch.instances
    assert len(client.index_requests) == 1
    assert set(client.index_requests[0].split(",")) == set(_INDICES)


def test_templates_list_legacy_and_composable() -> None:
    by_name = {r["name"]: r for r in _records("index_templates")}
    assert by_name["legacy_template"]["template_type"] == "legacy"
    assert by_name["settings_only"]["mapped_fields"] == 0
    assert by_name["composable_template"] == {
        "name": "composable_template",
        "template_type": "composable",
        "mapped_fields": 2,
    }


def test_the_probe_builds_ingestions_client_and_closes_it() -> None:
    _records("indices", api_key="encoded-key-value", url_prefix="cluster-a")
    (client,) = _FakeOpenSearch.instances
    assert client.host == "localhost:9200"
    assert client.kwargs["http_auth"] == ("probe_user", _SECRET_VALUE)
    assert client.kwargs["url_prefix"] == "cluster-a"
    assert client.kwargs["headers"] == {"Authorization": "ApiKey encoded-key-value"}
    assert client.closed


def test_composable_templates_degrade_like_ingestion() -> None:
    _FakeOpenSearch.composable_error = NotFoundError(404, "not_found", {})
    result = run_probe_method(_SOURCE_TYPE, _recipe(), "index_templates", {})
    assert isinstance(result.result, list)
    assert {r["name"] for r in result.result} == set(_LEGACY)
    assert result.warnings and "HTTP 404" in result.warnings[0]


def test_a_composable_listing_that_cannot_connect_fails(tmp_path: Path) -> None:
    _FakeOpenSearch.composable_error = ConnectionError("N/A", "refused", None)
    result = _probe_cli(tmp_path, _recipe(), "index_templates")
    assert result.exit_code == _EXIT_SOURCE, result.output


def test_an_unreachable_cluster_exits_as_the_source(tmp_path: Path) -> None:
    _FakeOpenSearch.fail_with = ConnectionError("N/A", "refused", None)
    result = _probe_cli(tmp_path, _recipe(), "indices")
    assert result.exit_code == _EXIT_SOURCE, result.output
    assert "ConnectionError" in json.loads(result.stderr)["error"]


def test_a_refused_login_exits_as_the_source_with_its_status(tmp_path: Path) -> None:
    _FakeOpenSearch.fail_with = AuthenticationException(
        401, "security_exception", {"error": _SECRET_VALUE}
    )
    result = _probe_cli(tmp_path, _recipe(), "indices")
    assert result.exit_code == _EXIT_SOURCE, result.output
    assert "401" in json.loads(result.stderr)["error"]
    assert _SECRET_VALUE not in result.output


def test_index_verdicts_apply_the_pattern_then_the_mappings() -> None:
    names = ["orders", "_internal", "empty_index"]
    attributes = [
        {"mapped_fields": "2"},
        {"mapped_fields": "2"},
        {"mapped_fields": "0"},
    ]
    assert _verdicts(_judge(DatasetSubTypes.ELASTIC_INDEX, names, attributes)) == {
        "orders": None,
        "_internal": "index_pattern",
        "empty_index": "no_mapped_fields",
    }


def test_a_bare_index_name_is_judged_on_the_pattern_with_a_warning() -> None:
    result = _judge(DatasetSubTypes.ELASTIC_INDEX, ["empty_index"])
    assert result.results[0].included
    assert any("--from-run" in w for w in result.warnings)


def test_templates_are_excluded_unless_ingest_index_templates() -> None:
    names = ["legacy_template"]
    attributes = [{"mapped_fields": "2"}]
    off = _judge(DatasetSubTypes.ELASTIC_INDEX_TEMPLATE, names, attributes)
    assert _verdicts(off) == {"legacy_template": "ingest_index_templates"}
    on = _judge(
        DatasetSubTypes.ELASTIC_INDEX_TEMPLATE,
        names,
        attributes,
        ingest_index_templates=True,
        index_template_pattern={"deny": ["legacy_.*"]},
    )
    assert _verdicts(on) == {"legacy_template": "index_template_pattern"}


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
    "extra, templates_expected, empty_index_by",
    [
        pytest.param({}, False, "no_mapped_fields", id="defaults"),
        pytest.param(
            {
                "index_pattern": {"deny": ["^_.*", "^tmp_.*"]},
                "ingest_index_templates": True,
                "index_template_pattern": {"deny": ["^tmp_.*"]},
            },
            True,
            "no_mapped_fields",
            id="patterns_and_templates",
        ),
        pytest.param(
            {"index_pattern": {"allow": ["^\\.ds-.*"]}, "ingest_index_templates": True},
            True,
            "index_pattern",
            id="data_streams_only",
        ),
    ],
)
def test_probe_verdicts_match_ingestion(
    tmp_path: Path,
    extra: Dict[str, object],
    templates_expected: bool,
    empty_index_by: str,
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
                identity=_index_identity,
            ),
            ParityListing(
                "templates",
                "index_templates",
                emitted=_emitted(DatasetSubTypes.ELASTIC_INDEX_TEMPLATE),
                expect_empty=not templates_expected,
            ),
        ],
    )
    assert report.excluded_by("indices")["empty_index"] == empty_index_by
    if templates_expected:
        assert report.excluded_by("templates")["settings_only"] == "no_mapped_fields"


def test_the_provider_is_what_the_config_names() -> None:
    assert (
        ElasticsearchSourceConfig.probe_provider_class() is ElasticsearchMetadataProbe
    )
