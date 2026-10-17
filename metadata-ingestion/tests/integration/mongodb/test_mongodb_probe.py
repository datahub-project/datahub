"""`datahub recipe probe` against a real MongoDB.

The unit tests drive the probe over a fake client. This covers what only a
real server shows: the system databases it really lists, the system.views
collection a view creates, a refused login's code, and the probe's verdicts
against what MongoDB ingestion emits.

The probe's seed lives in databases of its own, so the goldens built from
setup/mongo_init.js never see it.
"""

import json
import socket
import subprocess
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Mapping, Set

import pytest
import yaml
from click.testing import CliRunner, Result

from datahub.cli.recipe_cli import recipe as recipe_cli
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.metadata.urns import DatasetUrn
from tests.test_helpers.docker_helpers import wait_for_port
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    FanOut,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

# The parity harness masks its reports against the process-global registry,
# as the CLI does; an earlier test's registered secret would otherwise redact
# this fixture's identifiers.
pytestmark = [
    pytest.mark.integration_batch_4,
    pytest.mark.usefixtures("_isolate_secret_registry"),
]

_SOURCE_TYPE = "mongodb"
_CONTAINER = "testmongodb"
_MONGO_PORT = 27017
_COMPOSE = Path(__file__).parent / "docker-compose.yml"
# The compose file's, read rather than repeated.
_ENVIRONMENT = yaml.safe_load(_COMPOSE.read_text())["services"][_CONTAINER][
    "environment"
]
_USERNAME = str(_ENVIRONMENT["MONGO_INITDB_ROOT_USERNAME"])
_PASSWORD = str(_ENVIRONMENT["MONGO_INITDB_ROOT_PASSWORD"])
_DOCKER_TIMEOUT_SECONDS = 120
_WRONG_SECRET = "not-the-right-one"
_EXIT_USER = 2
_EXIT_SOURCE = 3

_SEED = """
const shop = db.getSiblingDB("probe_shop");
shop.orders.insertOne({ id: 1, amount: 10 });
shop.customers.insertOne({ id: 1, region: "north" });
shop.tmp_load.insertOne({ id: 1 });
shop.createView("order_summary", "orders", [{ $project: { amount: 1 } }]);
db.getSiblingDB("probe_scratch").junk.insertOne({ id: 1 });
"""


@pytest.fixture(scope="module")
def mongo_port(docker_compose_runner: Callable) -> Iterator[int]:
    with docker_compose_runner(_COMPOSE, "mongo") as docker_services:
        wait_for_port(docker_services, _CONTAINER, _MONGO_PORT)
        subprocess.run(
            [
                "docker",
                "exec",
                _CONTAINER,
                "mongosh",
                "--quiet",
                "-u",
                _USERNAME,
                "-p",
                _PASSWORD,
                "--authenticationDatabase",
                "admin",
                "--eval",
                _SEED,
            ],
            check=True,
            timeout=_DOCKER_TIMEOUT_SECONDS,
        )
        yield docker_services.port_for(_CONTAINER, _MONGO_PORT)


def _recipe(port: int, **extra: object) -> Dict[str, object]:
    return {
        "connect_uri": f"mongodb://localhost:{port}",
        "username": _USERNAME,
        "password": _PASSWORD,
        **extra,
    }


def _probe_cli(
    tmp_path: Path, recipe: Mapping[str, object], command: str, *params: str
) -> Result:
    recipe_file = tmp_path / "recipe.yml"
    recipe_file.write_text(
        yaml.safe_dump({"source": {"type": _SOURCE_TYPE, "config": dict(recipe)}})
    )
    return CliRunner().invoke(
        recipe_cli, ["probe", "run", command, "--recipe", str(recipe_file), *params]
    )


def _listed(port: int, command: str, **kwargs: object) -> List[str]:
    result = run_probe_method(_SOURCE_TYPE, _recipe(port), command, dict(kwargs))
    assert isinstance(result.result, list)
    return result.result


def test_listings_return_the_seeded_objects(mongo_port: int) -> None:
    databases = _listed(mongo_port, "databases")
    assert {"admin", "config", "local", "mngdb", "probe_shop", "probe_scratch"} <= set(
        databases
    )
    # The view, and the system.views collection creating it made.
    assert set(_listed(mongo_port, "collections", database="probe_shop")) == {
        "orders",
        "customers",
        "tmp_load",
        "order_summary",
        "system.views",
    }


def test_an_unlisted_database_is_refused(mongo_port: int, tmp_path: Path) -> None:
    result = _probe_cli(
        tmp_path, _recipe(mongo_port), "collections", "--database", "PROBE_SHOP"
    )
    assert result.exit_code == _EXIT_USER, result.output
    assert "probe_shop" in json.loads(result.stderr)["error"]


def test_a_refused_login_exits_as_the_source(mongo_port: int, tmp_path: Path) -> None:
    recipe = {**_recipe(mongo_port), "password": _WRONG_SECRET}
    result = _probe_cli(tmp_path, recipe, "databases")
    assert result.exit_code == _EXIT_SOURCE, result.output
    assert "AuthenticationFailed" in json.loads(result.stderr)["error"]
    assert _WRONG_SECRET not in result.output


def _closed_local_port() -> int:
    """A port nothing listens on: one the OS just handed out and took back."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe_socket:
        probe_socket.bind(("127.0.0.1", 0))
        return probe_socket.getsockname()[1]


def test_an_unreachable_server_exits_as_the_source(tmp_path: Path) -> None:
    recipe = {
        **_recipe(_closed_local_port()),
        "options": {"serverSelectionTimeoutMS": 1000},
    }
    result = _probe_cli(tmp_path, recipe, "databases")
    assert result.exit_code == _EXIT_SOURCE, result.output
    assert "ServerSelectionTimeoutError" in json.loads(result.stderr)["error"]


def _collections(index: EmittedIndex) -> Set[str]:
    return {DatasetUrn.from_string(urn).name for urn in index.urns("dataset")}


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param({}, id="defaults"),
        pytest.param(
            {
                "enableSchemaInference": False,
                "database_pattern": {"allow": ["probe_.*"], "deny": ["probe_scratch"]},
                "collection_pattern": {"deny": [".*\\.tmp_.*"]},
            },
            id="patterns",
        ),
        pytest.param(
            {
                "enableSchemaInference": False,
                "database_pattern": {"allow": ["probe_shop"]},
                "excludeSystemCollections": False,
            },
            id="system_collections",
        ),
    ],
)
def test_probe_verdicts_match_ingestion(
    mongo_port: int, tmp_path: Path, extra: Dict[str, object]
) -> None:
    report = assert_probe_parity(
        _SOURCE_TYPE,
        _recipe(mongo_port, **extra),
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
                "collections",
                "collections",
                emitted=_collections,
                fan_out=FanOut("databases", "database"),
            ),
        ],
    )
    assert report.excluded_by("databases")["admin"] == "system_database"
    if extra.get("excludeSystemCollections") is False:
        assert "probe_shop.system.views" in report.kinds["collections"].included
    else:
        assert (
            report.excluded_by("collections")["probe_shop.system.views"]
            == "excludeSystemCollections"
        )
    if "collection_pattern" in extra:
        assert (
            report.excluded_by("collections")["probe_shop.tmp_load"]
            == "collection_pattern"
        )
        assert report.excluded_by("databases")["probe_scratch"] == "database_pattern"
