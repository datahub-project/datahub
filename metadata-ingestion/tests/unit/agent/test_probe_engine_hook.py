"""The hook that lets a connector finish the probe's engine.

The probe builds its own engine instead of constructing the connector's Source,
which fires ingestion telemetry and wants a PipelineContext. That keeps it cheap
and side-effect free, and it also means the probe skips whatever the Source does
to its engine afterwards. For most dialects that is nothing. Athena replaces the
dialect outright, so on the stock one the probe would answer differently from the
ingestion it exists to predict.
"""

from typing import Any, Callable, Dict, List

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
from datahub.ingestion.source.sql.sqlalchemy_probe import SqlAlchemyMetadataProbe
from datahub.ingestion.source.sql.tidb import TiDBConfig
from datahub.ingestion.source.sql.timescaledb import TimescaleDBConfig


class _PlainConfig(SQLCommonConfig):
    def get_sql_alchemy_url(self) -> str:
        return "postgresql://u:p@h/db"

    @property
    def db(self) -> str:
        return "db"


def test_the_default_hook_leaves_the_engine_alone():
    """Most dialects need nothing, so the base must not require an override."""

    class _Engine:
        dialect = "untouched"

    engine = _Engine()
    _PlainConfig().probe_prepare_engine(engine)
    assert engine.dialect == "untouched"


def test_for_config_calls_the_hook_on_the_engine_it_built(monkeypatch):
    """The wiring itself: a connector that overrides the hook must see the
    provider's own engine, or the override protects nothing."""

    prepared: List[object] = []

    class _Engine:
        dialect = type("D", (), {"name": "postgresql"})()

        def dispose(self) -> None:
            pass

    class _Config(_PlainConfig):
        def probe_prepare_engine(self, engine: Any) -> None:
            prepared.append(engine)

    engine = _Engine()
    # create_engine is imported lazily inside for_config, so it is patched on
    # sqlalchemy; inspect is bound at module import, so it is patched there.
    monkeypatch.setattr(sqlalchemy, "create_engine", lambda url, **kw: engine)
    monkeypatch.setattr(sqlalchemy_probe, "inspect", lambda target: object())

    SqlAlchemyMetadataProbe.for_config(_Config())

    assert prepared == [engine], "the hook did not receive the provider's engine"


def test_athena_substitutes_the_dialect_its_source_uses():
    """PyAthena's own dialect omits ICEBERG from get_table_names and mis-parses
    complex column types, which is why AthenaSource.get_inspectors replaces it.
    The probe must make the same substitution or report a different catalog."""

    class _Engine:
        dialect: Any = "stock"

    engine = _Engine()
    config = AthenaConfig.parse_obj(
        {
            "aws_region": "us-east-1",
            "query_result_location": "s3://bucket/prefix/",
            "work_group": "primary",
        }
    )
    config.probe_prepare_engine(engine)
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
    AthenaConfig.parse_obj(
        {
            "aws_region": "us-east-1",
            "query_result_location": "s3://bucket/prefix/",
            "work_group": "primary",
        }
    ).probe_prepare_engine(engine)

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

    monkeypatch.setattr(sqlalchemy, "create_engine", _create_engine)
    monkeypatch.setattr(sqlalchemy_probe, "inspect", lambda target: object())
    return captured


def test_a_config_that_declares_nothing_gets_the_recipes_options_alone(monkeypatch):
    """The default declares no settings, whatever dialect the URL names: what a
    dialect's driver accepts is that dialect's config's to say, and a wrong
    connect_arg stops the connection opening. With no ceiling applied, none
    is reported."""
    captured = _capture_engine(monkeypatch)
    config = _PlainConfig(options={"pool_size": 3})

    assert config.probe_engine_settings(QueryBudget()) == ProbeEngineSettings()
    probe = SqlAlchemyMetadataProbe.for_config(config)

    assert captured["kwargs"] == {"pool_size": 3}
    assert probe.query_budget.timeout_seconds is None


def test_for_config_applies_the_declared_settings(monkeypatch):
    """Connect args go over the recipe's own, the dialect's step runs before
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
            )

        def probe_prepare_engine(self, engine: Any) -> None:
            order.append("connector")

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
            ["application_name", "options"],
            False,
            True,
        ),
        (
            lambda: CockroachDBConfig(host_port="h:26257", **_CREDS),
            ["application_name", "options"],
            False,
            True,
        ),
        (
            lambda: TimescaleDBConfig(host_port="h:5432", **_CREDS),
            ["application_name", "options"],
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
        (lambda: SQLServerConfig(host_port="h:1433", **_CREDS), [], False, False),
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
            ["application_name", "options"],
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
def test_a_config_pointed_at_another_dialect_declares_nothing(
    make_config: Callable[[], SQLCommonConfig],
) -> None:
    """A recipe's sqlalchemy_uri may name a dialect other than the config's
    own, and that driver may reject the config's settings, which stops the
    connection opening at all."""
    assert _settings(make_config()) == ProbeEngineSettings()


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
