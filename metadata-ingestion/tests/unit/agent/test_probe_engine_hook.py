"""The hook that sets up the probe's engine: probe_engine_settings.

The probe builds its own engine instead of constructing the connector's Source,
which fires ingestion telemetry and wants a PipelineContext. That keeps it cheap
and side-effect free, and it also means the probe skips whatever the Source does
to its engine afterwards. For most dialects that is nothing. Athena replaces the
dialect outright, so on the stock one the probe would answer differently from the
ingestion it exists to predict.
"""

import subprocess
import sys
from typing import Any, Callable, Dict, List, Optional

import pytest
import sqlalchemy

from datahub.ingestion.agent.sql_passthrough import QueryBudget
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.sql import sqlalchemy_probe
from datahub.ingestion.source.sql.athena import (
    AthenaConfig,
    AthenaProbeReadFailed,
    CustomAthenaRestDialect,
)
from datahub.ingestion.source.sql.cockroachdb import CockroachDBConfig
from datahub.ingestion.source.sql.doris.doris_source import DorisConfig
from datahub.ingestion.source.sql.hive.hive_metastore_config import HiveMetastore
from datahub.ingestion.source.sql.mssql.source import SQLServerConfig
from datahub.ingestion.source.sql.mysql import MySQLConfig
from datahub.ingestion.source.sql.postgres import PostgresConfig
from datahub.ingestion.source.sql.sql_config import (
    ProbeEngineSettings,
    SQLCommonConfig,
)
from datahub.ingestion.source.sql.sql_generic import SQLAlchemyGenericConfig
from datahub.ingestion.source.sql.sqlalchemy_probe import (
    SqlAlchemyMetadataProbe,
    build_probe_engine,
)
from datahub.ingestion.source.sql.starrocks import StarRocksConfig
from datahub.ingestion.source.sql.tidb import TiDBConfig
from datahub.ingestion.source.sql.timescaledb import TimescaleDBConfig


class _PlainConfig(SQLCommonConfig):
    def get_sql_alchemy_url(self) -> str:
        return "sqlite://"

    @property
    def db(self) -> str:
        return "db"


_BUDGET = QueryBudget(timeout_seconds=30)


def test_for_config_runs_the_declared_setup_on_the_engine_it_built(monkeypatch):
    """The wiring itself: a connector's setup step must see the provider's
    own engine, or it protects nothing."""

    prepared: List[object] = []

    class _Engine:
        dialect = type("D", (), {"name": "sqlite"})()

        def dispose(self) -> None:
            pass

    class _Config(_PlainConfig):
        def probe_engine_settings(self, budget: QueryBudget) -> ProbeEngineSettings:
            return super().probe_engine_settings(budget).followed_by(prepared.append)

    engine = _Engine()
    monkeypatch.setattr(sqlalchemy_probe, "create_engine", lambda url, **kw: engine)
    monkeypatch.setattr(sqlalchemy_probe, "inspect", lambda target: object())

    SqlAlchemyMetadataProbe.for_config(_Config())

    assert prepared == [engine], "the step did not receive the provider's engine"


def test_a_connectors_step_runs_after_its_protocols_own():
    order: List[str] = []
    settings = (
        ProbeEngineSettings(prepare=lambda engine: order.append("protocol"))
        .followed_by(lambda engine: order.append("connector"))
        .followed_by(lambda engine: order.append("last"))
    )
    assert settings.prepare is not None
    settings.prepare(sqlalchemy.create_engine("sqlite://"))
    assert order == ["protocol", "connector", "last"]


def _athena_setup() -> Callable[[Any], None]:
    prepare = (
        AthenaConfig.parse_obj(
            {
                "aws_region": "us-east-1",
                "query_result_location": "s3://bucket/prefix/",
                "work_group": "primary",
            }
        )
        .probe_engine_settings(_BUDGET)
        .prepare
    )
    assert prepare is not None
    return prepare


def test_athena_substitutes_the_dialect_its_source_uses():
    """PyAthena's own dialect omits ICEBERG from get_table_names and mis-parses
    complex column types, which is why AthenaSource.get_inspectors replaces it.
    The probe must make the same substitution or report a different catalog."""

    class _Engine:
        dialect: Any = "stock"

    engine = _Engine()
    _athena_setup()(engine)
    assert isinstance(engine.dialect, CustomAthenaRestDialect)


def test_an_unreadable_athena_schema_fails_instead_of_looking_empty():
    """The dialect's S3 Tables fallback logs, warns and returns an empty list.

    That is right for ingestion, which emits what it can and reports the gap.
    A probe has nowhere to record the gap, and its own comment says what the
    silence costs: missing IAM permissions and expired credentials look
    identical to an empty schema. Reporting "no tables" when the truth is
    "could not read" is the confusion this interface exists to prevent, so on
    the probe path the warning has to become the failure.
    """

    class _Engine:
        dialect: Any = "stock"

    engine = _Engine()
    _athena_setup()(engine)

    # Exactly the call the fallback's except-branch makes.
    with pytest.raises(AthenaProbeReadFailed, match="catalog=c"):
        engine.dialect._report.warning(
            message="Failed to list S3 Tables via boto3 fallback.",
            context="catalog=c, schema=s",
            exc=RuntimeError("AccessDenied"),
        )
    # Not a ValueError: nothing is wrong with the caller's arguments, the source
    # could not be read, so it must exit 3 rather than 2.
    assert not issubclass(AthenaProbeReadFailed, ValueError)


# --- the engine settings a dialect declares ---------------------------------


def _capture_engine(monkeypatch: pytest.MonkeyPatch) -> Dict[str, Any]:
    """Patch engine construction; returns where for_config's kwargs land."""
    captured: Dict[str, Any] = {}

    class _Engine:
        dialect = type("D", (), {"name": "postgresql"})()

        def dispose(self) -> None:
            pass

    def _create_engine(url: str, **kwargs: Any) -> _Engine:
        captured["kwargs"] = kwargs
        return _Engine()

    monkeypatch.setattr(sqlalchemy_probe, "create_engine", _create_engine)
    monkeypatch.setattr(sqlalchemy_probe, "inspect", lambda target: object())
    return captured


def test_a_url_with_no_known_protocol_gets_the_recipes_options_alone(monkeypatch):
    """No protocol here is known to take a ceiling or a label, and a wrong
    connect_arg stops the connection opening. With no ceiling applied, none
    is reported."""
    captured = _capture_engine(monkeypatch)
    config = _PlainConfig(options={"pool_size": 3})

    assert config.probe_engine_settings(QueryBudget()) == ProbeEngineSettings()
    probe = SqlAlchemyMetadataProbe.for_config(config)

    assert captured["kwargs"] == {"pool_size": 3}
    assert probe.query_budget.timeout_seconds is None


def test_for_config_applies_the_declared_settings(monkeypatch):
    """Connect args go over the recipe's own, the protocol's step runs before
    the connector's own engine setup, and the reported budget keeps the
    timeout only because the settings say it applies."""
    captured = _capture_engine(monkeypatch)
    order: List[str] = []
    budgets: List[QueryBudget] = []

    class _Config(_PlainConfig):
        def probe_engine_settings(self, budget: QueryBudget) -> ProbeEngineSettings:
            budgets.append(budget)
            return ProbeEngineSettings(
                connect_args={"shared": "dialect", "added": 1},
                prepare=lambda engine: order.append("dialect"),
                timeout_applies=True,
            ).followed_by(lambda engine: order.append("connector"))

    config = _Config(
        options={"connect_args": {"sslmode": "require", "shared": "recipe"}, "x": 2}
    )
    probe = SqlAlchemyMetadataProbe.for_config(config)

    assert captured["kwargs"] == {
        "connect_args": {"sslmode": "require", "shared": "dialect", "added": 1},
        "x": 2,
    }
    assert config.options["connect_args"]["shared"] == "recipe", "recipe mutated"
    assert order == ["dialect", "connector"]
    assert budgets == [SqlAlchemyMetadataProbe.query_budget]
    assert probe.query_budget == SqlAlchemyMetadataProbe.query_budget


def _settings(config: SQLCommonConfig) -> ProbeEngineSettings:
    return config.probe_engine_settings(QueryBudget(timeout_seconds=30))


_CREDS = {"username": "u", "password": "p"}


@pytest.mark.parametrize(
    "make_config, connect_arg_keys, prepares, applies",
    [
        (
            lambda: PostgresConfig(host_port="h:5432", **_CREDS),
            ["application_name", "connect_timeout", "options"],
            False,
            True,
        ),
        (
            lambda: CockroachDBConfig(host_port="h:26257", **_CREDS),
            ["application_name", "connect_timeout", "options"],
            False,
            True,
        ),
        (
            lambda: TimescaleDBConfig(host_port="h:5432", **_CREDS),
            ["application_name", "connect_timeout", "options"],
            False,
            True,
        ),
        (
            lambda: RedshiftConfig(host_port="h:5439", **_CREDS),
            ["application_name"],
            True,
            True,
        ),
        (
            lambda: MySQLConfig(host_port="h:3306", **_CREDS),
            ["program_name"],
            True,
            False,
        ),
        (
            lambda: MySQLConfig(host_port="h:3306", scheme="mariadb+pymysql", **_CREDS),
            ["program_name"],
            True,
            False,
        ),
        (
            lambda: TiDBConfig(host_port="h:4000", **_CREDS),
            ["program_name"],
            True,
            False,
        ),
        (
            lambda: DorisConfig(host_port="h:9030", **_CREDS),
            ["program_name"],
            False,
            False,
        ),
        # Its own step: the sql_variant converter, which acts only on pyodbc.
        (lambda: SQLServerConfig(host_port="h:1433", **_CREDS), [], True, False),
        (
            lambda: HiveMetastore(host_port="h:3306", **_CREDS),
            ["program_name"],
            True,
            False,
        ),
        (
            lambda: HiveMetastore(
                host_port="h:5432", scheme="postgresql+psycopg2", **_CREDS
            ),
            ["application_name", "connect_timeout", "options"],
            False,
            True,
        ),
    ],
    ids=[
        "postgres",
        "cockroachdb",
        "timescaledb",
        "redshift",
        "mysql",
        "mariadb",
        "tidb",
        "doris",
        "mssql",
        "hive-metastore-on-mysql",
        "hive-metastore-on-postgres",
    ],
)
def test_each_dialect_declares_its_own_engine_settings(
    make_config: Callable[[], SQLCommonConfig],
    connect_arg_keys: List[str],
    prepares: bool,
    applies: bool,
) -> None:
    """Who declares what. The values themselves are pinned in
    test_query_budget.py and test_query_attribution.py."""
    settings = _settings(make_config())
    assert sorted(settings.connect_args) == connect_arg_keys
    assert (settings.prepare is not None) == prepares
    assert settings.timeout_applies == applies


@pytest.mark.parametrize(
    "make_config",
    [
        lambda: PostgresConfig(host_port="h:5432", sqlalchemy_uri="sqlite://"),
        lambda: MySQLConfig(host_port="h:3306", sqlalchemy_uri="sqlite://"),
        lambda: RedshiftConfig(host_port="h:5439", sqlalchemy_uri="sqlite://"),
        lambda: HiveMetastore(host_port="h:3306", sqlalchemy_uri="sqlite://"),
    ],
    ids=["postgres", "mysql", "redshift", "hive-metastore"],
)
def test_a_config_pointed_at_a_url_with_no_known_protocol_declares_nothing(
    make_config: Callable[[], SQLCommonConfig],
) -> None:
    """A recipe's sqlalchemy_uri may name a dialect other than the config's
    own, and that driver may reject the config's own protocol's settings,
    which stops the connection opening at all."""
    assert _settings(make_config()) == ProbeEngineSettings()


@pytest.mark.parametrize(
    "make_config, connect_arg_keys, prepares, applies",
    [
        (
            lambda: RedshiftConfig(
                host_port="h:5439", sqlalchemy_uri="postgresql://h:5439/dev"
            ),
            ["application_name", "connect_timeout", "options"],
            False,
            True,
        ),
        (
            lambda: StarRocksConfig(
                host_port="h:9030", sqlalchemy_uri="mysql+pymysql://h:9030/db"
            ),
            ["program_name"],
            True,
            False,
        ),
        (
            lambda: PostgresConfig(
                host_port="h:5432", sqlalchemy_uri="mysql+pymysql://h/db"
            ),
            ["program_name"],
            True,
            False,
        ),
    ],
    ids=["redshift-on-libpq", "starrocks-on-pymysql", "postgres-on-pymysql"],
)
def test_a_config_pointed_at_another_protocol_gets_that_protocols_settings(
    make_config: Callable[[], SQLCommonConfig],
    connect_arg_keys: List[str],
    prepares: bool,
    applies: bool,
) -> None:
    """The settings follow the wire protocol the URL names, not the config:
    a Redshift recipe dialling a libpq URL is bounded as libpq is."""
    settings = _settings(make_config())
    assert sorted(settings.connect_args) == connect_arg_keys
    assert (settings.prepare is not None) == prepares
    assert settings.timeout_applies == applies


def test_no_timeout_means_no_ceiling_and_none_claimed():
    """The label still applies; nothing bounds a budget that has no timeout."""
    budget = QueryBudget(timeout_seconds=None)
    for config in (
        PostgresConfig(host_port="h:5432", **_CREDS),
        RedshiftConfig(host_port="h:5439", **_CREDS),
        MySQLConfig(host_port="h:3306", **_CREDS),
    ):
        settings = config.probe_engine_settings(budget)
        assert settings.prepare is None, type(config).__name__
        assert not settings.timeout_applies, type(config).__name__
        assert "options" not in settings.connect_args, type(config).__name__
        assert settings.connect_args, type(config).__name__


@pytest.mark.parametrize(
    "url, connect_arg_keys, prepares, applies",
    [
        (
            "postgresql://h/db",
            ["application_name", "connect_timeout", "options"],
            False,
            True,
        ),
        (
            "cockroachdb+psycopg2://h/db",
            ["application_name", "connect_timeout", "options"],
            False,
            True,
        ),
        ("redshift+redshift_connector://h/db", ["application_name"], True, True),
        ("mysql+pymysql://h/db", ["program_name"], True, False),
        ("mysql+mysqlconnector://h/db", [], True, False),
        ("mariadb+pymysql://h/db", ["program_name"], True, False),
        ("doris+pymysql://h/db", ["program_name"], False, False),
        ("sqlite://", [], False, False),
    ],
)
def test_the_generic_source_takes_the_settings_of_the_dialect_its_url_names(
    url: str, connect_arg_keys: List[str], prepares: bool, applies: bool
) -> None:
    """The generic source connects to whatever dialect its recipe names, so
    it has that dialect's ceiling and label, not none."""
    settings = _settings(SQLAlchemyGenericConfig(platform="p", connect_uri=url))
    assert sorted(settings.connect_args) == connect_arg_keys
    assert (settings.prepare is not None) == prepares
    assert settings.timeout_applies == applies


def test_the_generic_source_needs_no_connector_extra_for_its_settings() -> None:
    """The generic source's extra carries none of Redshift's config stack
    (path specs need `parse` and `wcmatch`), so reaching Redshift's ceiling
    must not import it. Checked in a fresh interpreter: this one has already
    imported everything."""
    code = (
        "import sys\n"
        "from datahub.ingestion.agent.sql_passthrough import QueryBudget\n"
        "from datahub.ingestion.source.sql.sql_generic import SQLAlchemyGenericConfig\n"
        "config = SQLAlchemyGenericConfig(\n"
        "    platform='redshift', connect_uri='redshift+redshift_connector://h/db'\n"
        ")\n"
        "assert config.probe_engine_settings(QueryBudget()).prepare is not None\n"
        "print(sorted(m for m in ('parse', 'wcmatch', "
        "'datahub.ingestion.source.redshift.config') if m in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=120
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert result.stdout.strip().splitlines()[-1] == "[]"


def test_a_config_may_declare_the_sqlglot_dialect_its_queries_parse_as(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Undeclared, the engine's dialect name is mapped through the family's
    table (SQLAlchemy's `postgresql` is sqlglot's `postgres`); declared, the
    config's name is used as it stands."""
    _capture_engine(monkeypatch)

    class _Declaring(_PlainConfig):
        @classmethod
        def probe_sqlglot_dialect(cls) -> Optional[str]:
            return "duckdb"

    assert SqlAlchemyMetadataProbe.for_config(_PlainConfig()).sql_dialect == "postgres"
    assert SqlAlchemyMetadataProbe.for_config(_Declaring()).sql_dialect == "duckdb"


def test_build_probe_engine_runs_the_settings_on_every_engine_it_builds(tmp_path):
    """A provider opening several engines (one per database) builds each
    through build_probe_engine, so none skips the connector's setup."""
    prepared: List[object] = []
    url = f"sqlite:///{tmp_path}/x.db"
    settings = ProbeEngineSettings(prepare=prepared.append)

    engines = [build_probe_engine(_PlainConfig(), url, settings) for _ in range(2)]
    try:
        assert prepared == engines
        assert all(str(engine.url) == url for engine in engines)
    finally:
        for engine in engines:
            engine.dispose()
