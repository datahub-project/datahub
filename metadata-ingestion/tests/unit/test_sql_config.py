from typing import Iterator, Optional

import pytest

from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sql.sql_config import (
    BasicSQLAlchemyConfig,
    SQLAlchemyConnectionConfig,
    SQLCommonConfig,
)
from datahub.ingestion.source.sql.sqlalchemy_probe import SqlAlchemyMetadataProbe
from datahub.masking.secret_registry import SecretRegistry


class _Config(SQLCommonConfig):
    def get_sql_alchemy_url(self) -> str:
        return "sqlite://"


def test_view_pattern_follows_table_pattern_unless_given() -> None:
    config = _Config.model_validate({"table_pattern": {"deny": ["db\\.sch\\.secret"]}})
    assert not config.view_pattern.allowed("db.sch.secret")

    config = _Config.model_validate(
        {"table_pattern": {"deny": [".*"]}, "view_pattern": {"allow": [".*"]}}
    )
    assert config.view_pattern.allowed("db.sch.anything")


def test_profile_pattern_reaches_the_profiler_only_when_profiling_is_on() -> None:
    on = _Config.model_validate(
        {"profiling": {"enabled": True}, "profile_pattern": {"deny": [".*big"]}}
    )
    assert on.is_profiling_enabled()
    assert on.profiling._allow_deny_patterns is on.profile_pattern

    off = _Config.model_validate({"profile_pattern": {"deny": [".*big"]}})
    assert not off.is_profiling_enabled()
    assert off.profiling._allow_deny_patterns is not off.profile_pattern


def test_connection_url_prefers_the_explicit_uri() -> None:
    config = SQLAlchemyConnectionConfig.model_validate(
        {"host_port": "h:1", "scheme": "mysql", "sqlalchemy_uri": "mysql://u@other/db"}
    )
    assert config.get_sql_alchemy_url() == "mysql://u@other/db"

    built = SQLAlchemyConnectionConfig.model_validate(
        {"host_port": "h:1", "scheme": "mysql", "username": "u", "password": "p"}
    )
    assert built.get_sql_alchemy_url(database="db2") == "mysql://u:p@h:1/db2"


def test_connection_url_needs_a_host_or_a_uri() -> None:
    config = SQLAlchemyConnectionConfig.model_validate(
        {"host_port": "", "scheme": "mysql"}
    )
    with pytest.raises(ValueError):
        config.get_sql_alchemy_url()


@pytest.fixture
def fresh_secret_registry() -> Iterator[SecretRegistry]:
    SecretRegistry.reset_instance()
    try:
        yield SecretRegistry.get_instance()
    finally:
        SecretRegistry.reset_instance()


@pytest.mark.parametrize(
    "sqlalchemy_uri, password",
    [
        pytest.param(
            "postgresql://my_user:my-uri-secret@my-host/my_db",
            "my-uri-secret",
            id="userinfo",
        ),
        pytest.param(
            "postgresql://my_user:Str0ng%21Uri%23Secret@my-host/my_db",
            "Str0ng!Uri#Secret",
            id="userinfo_decoded",
        ),
        pytest.param(
            "postgresql://my_user@my-host/my_db?sslmode=require&password=my-query-secret",
            "my-query-secret",
            id="password_query_param",
        ),
        pytest.param(
            "mssql+pyodbc://my_user@my-host/my_db?PWD=my-odbc-secret",
            "my-odbc-secret",
            id="query_param_case_insensitive",
        ),
    ],
)
def test_sqlalchemy_uri_password_is_registered_for_masking(
    fresh_secret_registry: SecretRegistry, sqlalchemy_uri: str, password: str
) -> None:
    SQLAlchemyConnectionConfig.model_validate(
        {"host_port": "h:1", "scheme": "postgresql", "sqlalchemy_uri": sqlalchemy_uri}
    )

    assert password in fresh_secret_registry.get_all_secrets()


@pytest.mark.parametrize(
    "sqlalchemy_uri",
    [
        pytest.param("postgresql://my_user@my-host/my_db?sslmode=require", id="none"),
        pytest.param("not a url", id="unparseable"),
        pytest.param("postgresql://my_user:my-secret@my-host:bad/my_db", id="bad_port"),
    ],
)
def test_sqlalchemy_uri_without_a_readable_password_registers_nothing(
    fresh_secret_registry: SecretRegistry, sqlalchemy_uri: str
) -> None:
    config = SQLAlchemyConnectionConfig.model_validate(
        {"host_port": "h:1", "scheme": "postgresql", "sqlalchemy_uri": sqlalchemy_uri}
    )

    assert config.sqlalchemy_uri == sqlalchemy_uri
    assert fresh_secret_registry.get_all_secrets() == {}


def test_probe_hook_defaults_describe_a_three_tier_source() -> None:
    config = BasicSQLAlchemyConfig.model_validate(
        {"host_port": "h:1", "scheme": "mysql"}
    )

    assert config.probe_container_kind() == DatasetContainerSubTypes.SCHEMA
    assert config.probe_kind_overrides() == {"containers": "Schema"}
    assert config.probe_ancestor_kinds(DatasetSubTypes.TABLE) == (
        DatasetContainerSubTypes.DATABASE,
        DatasetContainerSubTypes.SCHEMA,
    )
    assert config.probe_sqlglot_dialect() is None
    assert config.probe_normalize_container("Sales") == "Sales"
    assert config.probe_sql_alchemy_url() == config.get_sql_alchemy_url()
    assert config.probe_provider_class() is SqlAlchemyMetadataProbe
    warnings: list = []
    target: Optional[str] = config.probe_filter_target(
        schema="sch", entity="t", warn=warnings.append
    )
    assert target is None and warnings == []
    # The default catalog scope withholds other sessions' SQL text.
    assert "processlist" in {
        r.rsplit(".", 1)[-1] for r in config.probe_catalog_scope().excluded_relations
    }
