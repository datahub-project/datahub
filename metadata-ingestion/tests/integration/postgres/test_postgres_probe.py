"""`datahub recipe probe` against a real Postgres.

The unit tests drive the SQL probe over sqlite and fakes. This covers what
only a real server shows: quoted mixed-case identifiers, the `probe sql`
gate over a real catalog, libpq's statement_timeout reaching the server, and
the probe's pattern verdicts against what Postgres ingestion emits.

The seed (setup/probe_setup.sql) lives in a database of its own, so the
goldens built from setup.sql never see it.
"""

import json
import subprocess
import time
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Set, Type

import pytest
import yaml
from click.testing import CliRunner, Result

from datahub.cli.recipe_cli import recipe as recipe_cli
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.agent.sql_passthrough import QueryBudget
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sql.postgres.source import PostgresConfig
from datahub.ingestion.source.sql.sqlalchemy_probe import SqlAlchemyMetadataProbe
from datahub.metadata.schema_classes import SubTypesClass
from datahub.metadata.urns import DatasetUrn
from tests.test_helpers.docker_helpers import wait_for_port
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    FanOut,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

# The parity harness masks its reports against the process-global registry, as
# the CLI does; a secret an earlier test in the batch registered would otherwise
# redact any of this fixture's identifiers that contain it.
pytestmark = [
    pytest.mark.integration,
    pytest.mark.usefixtures("_isolate_secret_registry"),
]

_SOURCE_TYPE = "postgres"
_CONTAINER = "testpostgres"
_POSTGRES_PORT = 5432
_DATABASE = "probetest"
# The compose file's, read rather than repeated.
_PASSWORD = str(
    yaml.safe_load((Path(__file__).parent / "docker-compose.yml").read_text())[
        "services"
    ][_CONTAINER]["environment"]["POSTGRES_PASSWORD"]
)
# Each docker call's ceiling: a wedged daemon fails the fixture, not the
# whole batch at the process backstop.
_DOCKER_TIMEOUT_SECONDS = 120
# CLI exit codes (recipe_cli): 2 is the caller's input, 3 the source.
_EXIT_USER = 2
_EXIT_CONNECTION = 3


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig: pytest.Config) -> Path:
    return pytestconfig.rootpath / "tests/integration/postgres"


def _is_postgres_up() -> bool:
    logs = subprocess.run(
        ["docker", "logs", _CONTAINER],
        capture_output=True,
        text=True,
        timeout=_DOCKER_TIMEOUT_SECONDS,
    )
    return "PostgreSQL init process complete; ready for start up." in (
        logs.stdout + logs.stderr
    )


@pytest.fixture(scope="module")
def postgres_port(
    docker_compose_runner: Callable, test_resources_dir: Path
) -> Iterator[int]:
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "postgres"
    ) as docker_services:
        wait_for_port(
            docker_services,
            _CONTAINER,
            _POSTGRES_PORT,
            timeout=120,
            checker=_is_postgres_up,
        )
        subprocess.run(
            [
                "docker",
                "exec",
                _CONTAINER,
                "psql",
                "-v",
                "ON_ERROR_STOP=1",
                "-U",
                "postgres",
                "-f",
                "/setup/probe_setup.sql",
            ],
            check=True,
            timeout=_DOCKER_TIMEOUT_SECONDS,
        )
        yield docker_services.port_for(_CONTAINER, _POSTGRES_PORT)


def _recipe(port: int, **extra: object) -> Dict[str, object]:
    return {
        "host_port": f"localhost:{port}",
        "database": _DATABASE,
        "username": "postgres",
        "password": _PASSWORD,
        **extra,
    }


def _provider_class() -> Type[SqlAlchemyMetadataProbe]:
    provider_cls = PostgresConfig.probe_provider_class()
    assert issubclass(provider_cls, SqlAlchemyMetadataProbe)
    return provider_cls


def _probe_cli(
    tmp_path: Path, recipe: Dict[str, object], command: str, *params: str
) -> Result:
    """`datahub recipe probe run`, as an agent calls it."""
    recipe_file = tmp_path / "recipe.yml"
    recipe_file.write_text(
        yaml.safe_dump({"source": {"type": _SOURCE_TYPE, "config": recipe}})
    )
    return CliRunner().invoke(
        recipe_cli,
        ["probe", "run", command, "--recipe", str(recipe_file), *params],
    )


def _listed(port: int, command: str, **kwargs: object) -> List:
    result = run_probe_method(_SOURCE_TYPE, _recipe(port), command, dict(kwargs))
    assert isinstance(result.result, list)
    return result.result


def test_listings_return_the_seeded_objects(postgres_port: int) -> None:
    assert {"sales", "MixedCase", "scratch", "public"} <= set(
        _listed(postgres_port, "containers")
    )
    assert set(_listed(postgres_port, "tables", schema="sales")) == {
        "orders",
        "customers",
        "tmp_load",
        "secrets",
    }
    # Postgres ingestion lists materialized views among the views.
    assert set(_listed(postgres_port, "views", schema="sales")) == {
        "big_orders",
        "v_internal",
        "order_totals",
    }
    columns = _listed(postgres_port, "columns", schema="sales", table="orders")
    assert [c["name"] for c in columns] == ["id", "customer_id", "amount"]


def test_a_quoted_mixed_case_name_resolves_only_as_listed(
    postgres_port: int, tmp_path: Path
) -> None:
    assert _listed(postgres_port, "tables", schema="MixedCase") == ["CamelOrders"]
    columns = _listed(postgres_port, "columns", schema="MixedCase", table="CamelOrders")
    assert [c["name"] for c in columns] == ["OrderId", "Amount"]

    # Postgres folds an unquoted identifier to lower case, so a caller who
    # wrote the name as SQL would mean a different object: refused, with the
    # listed spelling as the only accepted one.
    recipe = _recipe(postgres_port)
    as_listed = _probe_cli(
        tmp_path, recipe, "columns", "--schema", "MixedCase", "--table", "CamelOrders"
    )
    assert as_listed.exit_code == 0, as_listed.output
    for schema, table in (("mixedcase", "CamelOrders"), ("MixedCase", "camelorders")):
        result = _probe_cli(
            tmp_path, recipe, "columns", "--schema", schema, "--table", table
        )
        assert result.exit_code == _EXIT_USER, result.output


def test_sql_gate_admits_the_catalog_and_refuses_user_tables(
    postgres_port: int, tmp_path: Path
) -> None:
    run = run_probe_method(
        _SOURCE_TYPE,
        _recipe(postgres_port),
        "sql",
        {
            "query": "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'sales' ORDER BY table_name"
        },
    )
    assert isinstance(run.result, dict)
    rows = run.result["rows"]
    assert ["orders"] in rows and ["secrets"] in rows

    recipe = _recipe(postgres_port)
    for query in (
        "SELECT secret_value FROM sales.secrets",
        # Statement text of every session; not in Postgres's catalog scope.
        "SELECT query FROM pg_catalog.pg_stat_activity",
    ):
        result = _probe_cli(tmp_path, recipe, "sql", "--query", query)
        assert result.exit_code == _EXIT_USER, result.output
        assert "seeded-secret-value" not in result.output


def _budget(monkeypatch: pytest.MonkeyPatch, seconds: int) -> None:
    monkeypatch.setattr(
        _provider_class(), "query_budget", QueryBudget(timeout_seconds=seconds)
    )


def test_statement_timeout_reaches_the_server(
    postgres_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Not the default, so the value read back can only be the budget's.
    _budget(monkeypatch, 7)
    config = PostgresConfig.model_validate(_recipe(postgres_port))
    with _provider_class().for_config(config) as provider:
        shown = provider.execute_catalog_query("SHOW statement_timeout", 2)
    assert shown.rows == [["7s"]]


def test_statement_timeout_cuts_a_slow_catalog_query(
    postgres_port: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _budget(monkeypatch, 1)
    # A catalog query the gate admits, but whose cross join runs far past a
    # second: only the server-side ceiling stops it.
    slow = (
        "SELECT count(*) FROM information_schema.columns AS a "
        "CROSS JOIN information_schema.columns AS b "
        "CROSS JOIN information_schema.columns AS c"
    )
    result = _probe_cli(tmp_path, _recipe(postgres_port), "sql", "--query", slow)
    assert result.exit_code == _EXIT_CONNECTION, result.output
    # query_canceled: statement_timeout, not a dropped connection.
    assert "57014" in json.loads(result.stderr)["error"]


# A non-routable address: a SYN sent there is never answered, the way a
# firewall drops one.
_BLACKHOLE_HOST_PORT = "10.255.255.1:5432"


def test_an_unanswered_connect_gives_up_within_the_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """libpq has no connect timeout of its own, so without the probe's an
    unanswered SYN held the probe for the OS's TCP timeout (75s on macOS)."""
    _budget(monkeypatch, 2)
    # Set, it is the connect timeout the probe defers to, not the budget's.
    monkeypatch.delenv("PGCONNECT_TIMEOUT", raising=False)
    recipe = {**_recipe(_POSTGRES_PORT), "host_port": _BLACKHOLE_HOST_PORT}
    started = time.monotonic()
    result = _probe_cli(tmp_path, recipe, "containers")
    elapsed = time.monotonic() - started
    assert result.exit_code == _EXIT_CONNECTION, result.output
    assert elapsed < 20, f"the connect took {elapsed:.0f}s"


def _datasets(sub_type: str) -> Callable[[EmittedIndex], Set[str]]:
    """Emitted datasets of one subtype, as `schema.name`: the database is the
    recipe's, and the probe's fan-out qualifies by schema alone."""

    def emitted(index: EmittedIndex) -> Set[str]:
        names: Set[str] = set()
        for urn in index.urns("dataset", with_aspect=SubTypesClass):
            if any(
                isinstance(aspect, SubTypesClass) and sub_type in aspect.typeNames
                for aspect in index.aspects[urn]
            ):
                database, _, rest = DatasetUrn.from_string(urn).name.partition(".")
                assert database == _DATABASE
                names.add(rest)
        return names

    return emitted


def test_probe_verdicts_match_ingestion(postgres_port: int, tmp_path: Path) -> None:
    recipe = _recipe(
        postgres_port,
        schema_pattern={"allow": [".*"], "deny": ["information_schema", "scratch"]},
        table_pattern={
            "allow": [f"{_DATABASE}\\.(sales|MixedCase|scratch)\\..*"],
            "deny": [".*\\.tmp_.*"],
        },
        view_pattern={"deny": [".*\\.v_.*"]},
    )
    fan_out = FanOut("containers", "schema")
    listings: List[ParityListing] = [
        ParityListing(
            "schemas",
            "containers",
            emitted=lambda index: index.container_names(
                DatasetContainerSubTypes.SCHEMA
            ),
        ),
        ParityListing(
            "tables",
            "tables",
            emitted=_datasets(DatasetSubTypes.TABLE),
            fan_out=fan_out,
        ),
        ParityListing(
            "views",
            "views",
            emitted=_datasets(DatasetSubTypes.VIEW),
            fan_out=fan_out,
        ),
    ]

    report = assert_probe_parity(
        _SOURCE_TYPE, recipe, pipeline_ingestion(_SOURCE_TYPE, tmp_path), listings
    )

    assert report.kinds["schemas"].included == {"public", "sales", "MixedCase"}
    assert set(report.excluded_by("schemas")) == {"information_schema", "scratch"}
    assert report.kinds["tables"].included == {
        "sales.orders",
        "sales.customers",
        "sales.secrets",
        "MixedCase.CamelOrders",
    }
    # Each exclusion is pinned to the rule ingestion applied.
    assert "sales.tmp_load" in report.excluded_by("tables")
    assert "scratch.junk" in report.excluded_by("tables")
    assert report.kinds["views"].included == {"sales.big_orders", "sales.order_totals"}
    assert "sales.v_internal" in report.excluded_by("views")
