"""`datahub recipe probe` against a real MySQL.

The unit tests drive the SQL probe over sqlite and fakes. This covers what
only a real server shows: identifier case, the `probe sql` gate over a real
catalog (executable comments included), the session's max_execution_time,
and the probe's pattern verdicts against what MySQL ingestion emits.

The seed (setup/probe_setup.sql) lives in databases of its own, so the
goldens built from setup.sql never see it.
"""

import json
import socket
import subprocess
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Set, Type

import pytest
import yaml
from click.testing import CliRunner, Result
from sqlalchemy import create_engine

from datahub.cli.recipe_cli import recipe as recipe_cli
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.agent.sql_passthrough import CatalogRows, QueryBudget
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sql.mysql import MySQLConfig
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
# the CLI does; an earlier test's registered secret would otherwise redact this
# fixture's identifiers (e.g. "test" inside "test_cases").
pytestmark = [
    pytest.mark.integration,
    pytest.mark.usefixtures("_isolate_secret_registry"),
]

_SOURCE_TYPE = "mysql"
_CONTAINER = "testmysql"
_MYSQL_PORT = 3306
# The compose file's, read rather than repeated.
_PASSWORD = str(
    yaml.safe_load((Path(__file__).parent / "docker-compose.yml").read_text())[
        "services"
    ][_CONTAINER]["environment"]["MYSQL_ROOT_PASSWORD"]
)
# Each docker call's ceiling: a wedged daemon fails the fixture, not the
# whole batch at the process backstop.
_DOCKER_TIMEOUT_SECONDS = 120
_SECRET = "seeded-secret-value"
# CLI exit codes (recipe_cli): 2 is the caller's input, 3 the source.
_EXIT_USER = 2
_EXIT_CONNECTION = 3
# Above every system database's relation count, so no fan-out listing is cut.
_LIMIT = 1000


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig: pytest.Config) -> Path:
    return pytestconfig.rootpath / "tests/integration/mysql"


def _is_mysql_up() -> bool:
    logs = subprocess.run(
        ["docker", "logs", _CONTAINER],
        capture_output=True,
        text=True,
        timeout=_DOCKER_TIMEOUT_SECONDS,
    )
    return any(
        "/usr/sbin/mysqld: ready for connections." in line and str(_MYSQL_PORT) in line
        for line in (logs.stdout + logs.stderr).splitlines()
    )


@pytest.fixture(scope="module")
def mysql_port(
    docker_compose_runner: Callable, test_resources_dir: Path
) -> Iterator[int]:
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "mysql"
    ) as docker_services:
        wait_for_port(
            docker_services,
            _CONTAINER,
            _MYSQL_PORT,
            timeout=120,
            checker=_is_mysql_up,
        )
        subprocess.run(
            [
                "docker",
                "exec",
                _CONTAINER,
                "mysql",
                "-uroot",
                f"-p{_PASSWORD}",
                "-e",
                "source /setup/probe_setup.sql",
            ],
            check=True,
            timeout=_DOCKER_TIMEOUT_SECONDS,
        )
        yield docker_services.port_for(_CONTAINER, _MYSQL_PORT)


def _recipe(port: int, **extra: object) -> Dict[str, object]:
    return {
        "host_port": f"localhost:{port}",
        "username": "root",
        "password": _PASSWORD,
        **extra,
    }


def _provider_class() -> Type[SqlAlchemyMetadataProbe]:
    provider_cls = MySQLConfig.probe_provider_class()
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


def test_listings_return_the_seeded_objects(mysql_port: int) -> None:
    assert {"probe_sales", "probe_scratch"} <= set(_listed(mysql_port, "containers"))
    assert {
        name.lower() for name in _listed(mysql_port, "tables", schema="probe_sales")
    } == {"orders", "customers", "tmp_load", "secrets", "camelorders"}
    assert set(_listed(mysql_port, "views", schema="probe_sales")) == {
        "big_orders",
        "v_internal",
    }
    columns = _listed(mysql_port, "columns", schema="probe_sales", table="orders")
    assert [c["name"] for c in columns] == ["id", "customer_id", "amount"]


def test_a_table_name_resolves_only_as_listed(mysql_port: int, tmp_path: Path) -> None:
    # Whether the server keeps `CamelOrders` or folds it to lower case is
    # lower_case_table_names, which varies by platform. Stable either way:
    # the listed spelling is accepted, and any other spelling is refused.
    (listed,) = [
        name
        for name in _listed(mysql_port, "tables", schema="probe_sales")
        if name.lower() == "camelorders"
    ]
    recipe = _recipe(mysql_port)
    as_listed = _probe_cli(
        tmp_path, recipe, "columns", "--schema", "probe_sales", "--table", listed
    )
    assert as_listed.exit_code == 0, as_listed.output
    assert [c["name"] for c in json.loads(as_listed.stdout)["result"]] == [
        "OrderId",
        "Amount",
    ]

    for schema, table in (
        ("PROBE_SALES", listed),
        ("probe_sales", listed.swapcase()),
    ):
        result = _probe_cli(
            tmp_path, recipe, "columns", "--schema", schema, "--table", table
        )
        assert result.exit_code == _EXIT_USER, result.output


@pytest.fixture
def executed_queries(monkeypatch: pytest.MonkeyPatch) -> List[str]:
    """Every query the provider sends to the server, after the gate."""
    provider_cls = _provider_class()
    original = provider_cls.execute_catalog_query
    sent: List[str] = []

    def recording(self: SqlAlchemyMetadataProbe, query: str, limit: int) -> CatalogRows:
        sent.append(query)
        return original(self, query, limit)

    monkeypatch.setattr(provider_cls, "execute_catalog_query", recording)
    return sent


def test_sql_gate_admits_the_catalog(
    mysql_port: int, tmp_path: Path, executed_queries: List[str]
) -> None:
    result = _probe_cli(
        tmp_path,
        _recipe(mysql_port),
        "sql",
        "--query",
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'probe_sales'",
    )
    assert result.exit_code == 0, result.output
    rows = json.loads(result.stdout)["result"]["rows"]
    assert ["orders"] in rows and ["secrets"] in rows
    # The recorder sees what reaches the server, so its silence below means
    # something.
    assert len(executed_queries) == 1


@pytest.mark.parametrize(
    "query",
    [
        pytest.param("SELECT secret_value FROM probe_sales.secrets", id="user_table"),
        # Other sessions' SQL text, withheld inside information_schema.
        pytest.param(
            "SELECT info FROM information_schema.processlist", id="processlist"
        ),
        pytest.param(
            "SELECT trx_query FROM information_schema.innodb_trx", id="innodb_trx"
        ),
    ],
)
def test_sql_gate_refuses_outside_the_catalog(
    mysql_port: int, tmp_path: Path, executed_queries: List[str], query: str
) -> None:
    result = _probe_cli(tmp_path, _recipe(mysql_port), "sql", "--query", query)
    assert result.exit_code == _EXIT_USER, result.output
    assert executed_queries == []


def test_sql_gate_refuses_an_executable_comment(
    mysql_port: int, tmp_path: Path, executed_queries: List[str]
) -> None:
    # sqlglot reads `/*! ... */` as a comment; MySQL runs what is inside it.
    query = (
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'probe_sales' "
        "/*! UNION SELECT secret_value FROM probe_sales.secrets */"
    )
    engine = create_engine(f"mysql+pymysql://root:{_PASSWORD}@localhost:{mysql_port}/")
    try:
        with engine.connect() as conn:
            direct = [row[0] for row in conn.exec_driver_sql(query)]
    finally:
        engine.dispose()
    # The danger is real: run as written, the query reads a user table.
    assert _SECRET in direct

    result = _probe_cli(tmp_path, _recipe(mysql_port), "sql", "--query", query)
    assert result.exit_code == _EXIT_USER, result.output
    assert _SECRET not in result.output
    # Refused before the server saw it.
    assert executed_queries == []


def test_a_misspelled_column_in_the_callers_query_is_the_callers(
    mysql_port: int, tmp_path: Path, executed_queries: List[str]
) -> None:
    """The server answers errno 1054 and the driver no SQLSTATE; the query
    reached the server, and the mistake was the caller's."""
    query = "SELECT no_such_column FROM information_schema.tables"
    result = _probe_cli(tmp_path, _recipe(mysql_port), "sql", "--query", query)
    assert result.exit_code == _EXIT_USER, result.output
    assert len(executed_queries) == 1


def _budget(monkeypatch: pytest.MonkeyPatch, seconds: int) -> None:
    monkeypatch.setattr(
        _provider_class(), "query_budget", QueryBudget(timeout_seconds=seconds)
    )


def test_max_execution_time_reaches_the_session(
    mysql_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Not the default, so the value read back can only be the budget's.
    _budget(monkeypatch, 7)
    config = MySQLConfig.model_validate(_recipe(mysql_port))
    with _provider_class().for_config(config) as provider:
        shown = provider.execute_catalog_query("SELECT @@SESSION.max_execution_time", 2)
    assert shown.rows == [[7000]]


def test_max_execution_time_cuts_a_slow_catalog_query(
    mysql_port: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _budget(monkeypatch, 1)
    # A catalog query the gate admits, but whose cross join runs far past a
    # second: only the server-side ceiling stops it.
    slow = (
        "SELECT count(*) FROM information_schema.columns AS a "
        "CROSS JOIN information_schema.columns AS b "
        "CROSS JOIN information_schema.columns AS c"
    )
    result = _probe_cli(tmp_path, _recipe(mysql_port), "sql", "--query", slow)
    assert result.exit_code == _EXIT_CONNECTION, result.output
    # ER_QUERY_TIMEOUT: max_execution_time, not a dropped connection.
    assert "3024" in json.loads(result.stderr)["error"]


def _closed_local_port() -> int:
    """A port nothing listens on: one the OS just handed out and took back."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe_socket:
        probe_socket.bind(("127.0.0.1", 0))
        return probe_socket.getsockname()[1]


def test_a_refused_connection_is_named_through_the_real_driver(
    tmp_path: Path,
) -> None:
    """PyMySQL keeps the socket's ConnectionRefusedError only as the context
    of its errno-2003 OperationalError, which covers DNS, timeouts and TLS
    as well; the chain is what tells them apart."""
    recipe = {**_recipe(0), "host_port": f"127.0.0.1:{_closed_local_port()}"}
    result = _probe_cli(tmp_path, recipe, "containers")
    assert result.exit_code == _EXIT_CONNECTION, result.output
    error = json.loads(result.stderr)["error"]
    assert "errno 2003" in error
    assert "ConnectionRefused" in error
    assert _PASSWORD not in result.output


def _datasets(sub_type: str) -> Callable[[EmittedIndex], Set[str]]:
    """Emitted datasets of one subtype, as `database.name`, which is both
    the URN's name and the probe fan-out's qualified name."""

    def emitted(index: EmittedIndex) -> Set[str]:
        return {
            DatasetUrn.from_string(urn).name
            for urn in index.urns("dataset", with_aspect=SubTypesClass)
            if any(
                isinstance(aspect, SubTypesClass) and sub_type in aspect.typeNames
                for aspect in index.aspects[urn]
            )
        }

    return emitted


def test_probe_verdicts_match_ingestion(mysql_port: int, tmp_path: Path) -> None:
    # database_pattern: MySQL's container pattern (schema_pattern is its
    # deprecated alias).
    recipe = _recipe(
        mysql_port,
        database_pattern={"allow": ["probe_.*"], "deny": ["probe_scratch"]},
        table_pattern={"allow": ["probe_.*"], "deny": [".*\\.tmp_.*"]},
        view_pattern={"deny": [".*\\.v_.*"]},
    )
    fan_out = FanOut("containers", "schema", {"limit": _LIMIT})
    listings: List[ParityListing] = [
        ParityListing(
            "databases",
            "containers",
            emitted=lambda index: index.container_names(
                DatasetContainerSubTypes.DATABASE
            ),
            kwargs={"limit": _LIMIT},
        ),
        ParityListing(
            "tables",
            "tables",
            emitted=_datasets(DatasetSubTypes.TABLE),
            kwargs={"limit": _LIMIT},
            fan_out=fan_out,
        ),
        ParityListing(
            "views",
            "views",
            emitted=_datasets(DatasetSubTypes.VIEW),
            kwargs={"limit": _LIMIT},
            fan_out=fan_out,
        ),
    ]

    report = assert_probe_parity(
        _SOURCE_TYPE, recipe, pipeline_ingestion(_SOURCE_TYPE, tmp_path), listings
    )

    assert report.kinds["databases"].included == {"probe_sales"}
    assert "probe_scratch" in report.excluded_by("databases")
    assert {name.lower() for name in report.kinds["tables"].included} == {
        "probe_sales.orders",
        "probe_sales.customers",
        "probe_sales.secrets",
        "probe_sales.camelorders",
    }
    assert "probe_sales.tmp_load" in report.excluded_by("tables")
    assert "probe_scratch.junk" in report.excluded_by("tables")
    assert report.kinds["views"].included == {"probe_sales.big_orders"}
    assert "probe_sales.v_internal" in report.excluded_by("views")
