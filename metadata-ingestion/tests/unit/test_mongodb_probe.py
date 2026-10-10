"""MongoDB's probe: names only, through ingestion's own client, judged by the
same predicate ingestion filters with."""

import json
from pathlib import Path
from typing import Dict, Iterator, List, Mapping, Optional, Set

import pytest
import yaml
from click.testing import CliRunner, Result
from pymongo.errors import OperationFailure, ServerSelectionTimeoutError

from datahub.cli.recipe_cli import recipe as recipe_cli
from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.mongodb import MONGODB_COLLECTION_KIND, MongoDBConfig
from datahub.ingestion.source.mongodb_probe import MongoDBMetadataProbe
from datahub.metadata.urns import DatasetUrn
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    FanOut,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

pytestmark = pytest.mark.usefixtures("_isolate_secret_registry")

# A credential the fixture registers, so its masking can be checked.
_SECRET_VALUE = "probe-secret-value"
_SOURCE_TYPE = "mongodb"
_EXIT_USER = 2
_EXIT_SOURCE = 3

_CLUSTER: Dict[str, List[str]] = {
    "admin": ["system.users", "system.version"],
    "config": ["system.sessions"],
    "local": ["startup_log"],
    "shop": ["orders", "customers", "tmp_load", "system.views", "order_summary"],
    "scratch": ["junk"],
    "Shop2": ["items"],
}


class _FakeDatabase:
    def __init__(self, collections: List[str]) -> None:
        self._collections = collections

    def list_collection_names(self) -> List[str]:
        return list(self._collections)


class _FakeAdmin:
    def command(self, name: str) -> Dict[str, int]:
        return {"ok": 1}


class _FakeMongoClient:
    """Answers what ingestion (schema inference off) and the probe call."""

    instances: List["_FakeMongoClient"] = []
    fail_with: Optional[Exception] = None

    def __init__(self, uri: str, **options: object) -> None:
        self.uri = uri
        self.options = options
        self.closed = False
        self.admin = _FakeAdmin()
        _FakeMongoClient.instances.append(self)

    def list_database_names(self) -> List[str]:
        if _FakeMongoClient.fail_with is not None:
            raise _FakeMongoClient.fail_with
        return list(_CLUSTER)

    def __getitem__(self, name: str) -> _FakeDatabase:
        return _FakeDatabase(_CLUSTER.get(name, []))

    def server_info(self) -> Dict[str, object]:
        return {"versionArray": [7, 0, 0]}

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def fake_mongo(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    _FakeMongoClient.instances = []
    _FakeMongoClient.fail_with = None
    monkeypatch.setattr(
        "datahub.ingestion.source.mongodb.MongoClient", _FakeMongoClient
    )
    yield


def _recipe(**extra: object) -> Dict[str, object]:
    return {
        "connect_uri": "mongodb://localhost:27017",
        "username": "probe_user",
        "password": _SECRET_VALUE,
        "enableSchemaInference": False,
        **extra,
    }


def _listed(command: str, **kwargs: object) -> List[str]:
    result = run_probe_method(_SOURCE_TYPE, _recipe(), command, dict(kwargs))
    assert isinstance(result.result, list)
    return result.result


def _judge(
    kind: str,
    names: List[str],
    parent: Optional[List[str]] = None,
    **recipe: object,
) -> FilterCheckResult:
    return check_filters(_SOURCE_TYPE, _recipe(**recipe), kind, parent or [], names)


def _verdicts(result: FilterCheckResult) -> Dict[str, Optional[str]]:
    return {v.name: (None if v.included else v.excluded_by) for v in result.results}


def _probe_cli(tmp_path: Path, recipe: Mapping[str, object], *args: str) -> Result:
    recipe_file = tmp_path / "recipe.yml"
    recipe_file.write_text(
        yaml.safe_dump({"source": {"type": _SOURCE_TYPE, "config": dict(recipe)}})
    )
    return CliRunner().invoke(
        recipe_cli, ["probe", "run", *args[:1], "--recipe", str(recipe_file), *args[1:]]
    )


def test_databases_include_the_ones_ingestion_skips() -> None:
    assert _listed("databases") == sorted(_CLUSTER)


def test_collections_include_system_collections() -> None:
    assert _listed("collections", database="shop") == sorted(_CLUSTER["shop"])


def test_the_probe_builds_ingestions_client_and_closes_it() -> None:
    _listed("databases", limit=5)
    (client,) = _FakeMongoClient.instances
    assert client.uri == "mongodb://localhost:27017"
    assert client.options["username"] == "probe_user"
    assert client.options["password"] == _SECRET_VALUE
    assert client.options["datetime_conversion"] == "DATETIME_AUTO"
    assert client.options["serverSelectionTimeoutMS"] == 10_000
    assert client.closed


def test_the_recipes_own_options_beat_the_probes_defaults() -> None:
    run_probe_method(
        _SOURCE_TYPE,
        _recipe(options={"serverSelectionTimeoutMS": 1234}),
        "databases",
        {},
    )
    assert _FakeMongoClient.instances[0].options["serverSelectionTimeoutMS"] == 1234


def test_an_unknown_database_is_the_callers_mistake(tmp_path: Path) -> None:
    result = _probe_cli(tmp_path, _recipe(), "collections", "--database", "nope")
    assert result.exit_code == _EXIT_USER, result.output


def test_a_case_only_miss_points_at_the_listed_spelling(tmp_path: Path) -> None:
    result = _probe_cli(tmp_path, _recipe(), "collections", "--database", "shop2")
    assert result.exit_code == _EXIT_USER, result.output
    assert "Shop2" in json.loads(result.stderr)["error"]


def test_an_unreachable_server_exits_as_the_source(tmp_path: Path) -> None:
    _FakeMongoClient.fail_with = ServerSelectionTimeoutError("no servers")
    result = _probe_cli(tmp_path, _recipe(), "databases")
    assert result.exit_code == _EXIT_SOURCE, result.output
    assert "ServerSelectionTimeoutError" in json.loads(result.stderr)["error"]


def test_a_refused_login_is_named_by_its_code(tmp_path: Path) -> None:
    _FakeMongoClient.fail_with = OperationFailure(
        f"Authentication failed for {_SECRET_VALUE}",
        code=18,
        details={"codeName": "AuthenticationFailed", "code": 18},
    )
    result = _probe_cli(tmp_path, _recipe(), "databases")
    assert result.exit_code == _EXIT_SOURCE, result.output
    assert "AuthenticationFailed" in json.loads(result.stderr)["error"]
    assert _SECRET_VALUE not in result.output


def test_a_listing_carries_kind_and_parent() -> None:
    result = run_probe_method(
        _SOURCE_TYPE, _recipe(), "collections", {"database": "shop"}
    )
    assert result.kind == MONGODB_COLLECTION_KIND
    assert result.parent_path == ["shop"]


def test_system_databases_are_excluded_whatever_the_pattern() -> None:
    verdicts = _verdicts(
        _judge(
            DatasetContainerSubTypes.DATABASE,
            ["admin", "config", "local", "shop", "scratch"],
            database_pattern={"deny": ["scratch"]},
        )
    )
    assert verdicts == {
        "admin": "system_database",
        "config": "system_database",
        "local": "system_database",
        "shop": None,
        "scratch": "database_pattern",
    }


def test_collection_pattern_is_matched_against_database_dot_collection() -> None:
    result = _judge(
        MONGODB_COLLECTION_KIND,
        ["orders", "tmp_load"],
        parent=["shop"],
        collection_pattern={"allow": ["^shop\\."], "deny": [".*\\.tmp_.*"]},
    )
    assert _verdicts(result) == {"orders": None, "tmp_load": "collection_pattern"}
    assert [v.target for v in result.results] == ["shop.orders", "shop.tmp_load"]


def test_a_collection_in_an_excluded_database_is_excluded() -> None:
    result = _judge(MONGODB_COLLECTION_KIND, ["startup_log"], parent=["local"])
    assert not result.results[0].included


def test_system_collections_follow_exclude_system_collections() -> None:
    names = ["system.views", "orders"]
    assert _verdicts(_judge(MONGODB_COLLECTION_KIND, names, parent=["shop"])) == {
        "system.views": "excludeSystemCollections",
        "orders": None,
    }
    kept = _judge(
        MONGODB_COLLECTION_KIND, names, parent=["shop"], excludeSystemCollections=False
    )
    assert all(v.included for v in kept.results)


def test_a_collection_without_its_database_is_judged_with_a_warning() -> None:
    result = _judge(MONGODB_COLLECTION_KIND, ["orders"])
    assert result.warnings
    assert result.results[0].target == "orders"


def _collections(index: EmittedIndex) -> Set[str]:
    return {DatasetUrn.from_string(urn).name for urn in index.urns("dataset")}


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param({}, id="defaults"),
        pytest.param(
            {
                "database_pattern": {"deny": ["scratch"]},
                "collection_pattern": {"deny": [".*\\.tmp_.*"]},
            },
            id="patterns",
        ),
        pytest.param(
            {"collection_pattern": {"allow": ["shop\\.order"]}},
            id="qualified_allow",
        ),
        pytest.param({"excludeSystemCollections": False}, id="system_collections"),
    ],
)
def test_probe_verdicts_match_ingestion(
    tmp_path: Path, extra: Dict[str, object]
) -> None:
    fan_out = FanOut("databases", "database")
    report = assert_probe_parity(
        _SOURCE_TYPE,
        _recipe(**extra),
        pipeline_ingestion(_SOURCE_TYPE, tmp_path),
        [
            ParityListing(
                "databases",
                "databases",
                emitted=lambda index: index.container_names(
                    DatasetContainerSubTypes.DATABASE
                ),
            ),
            ParityListing(
                "collections", "collections", emitted=_collections, fan_out=fan_out
            ),
        ],
    )
    assert report.excluded_by("databases")["admin"] == "system_database"


def test_the_provider_is_what_the_config_names() -> None:
    assert MongoDBConfig.probe_provider_class() is MongoDBMetadataProbe
