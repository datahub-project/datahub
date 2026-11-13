import logging
from typing import Dict, Iterator, List, Optional

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


@pytest.fixture(autouse=True)
def fresh_secret_registry() -> Iterator[SecretRegistry]:
    # Validating a config with a sqlalchemy_uri registers secrets in the
    # process-global registry; clear it in place so no test leaks into another.
    registry = SecretRegistry.get_instance()
    registry.clear()
    try:
        yield registry
    finally:
        registry.clear()


def _registered_uri_secrets(sqlalchemy_uri: str) -> Dict[str, str]:
    SQLAlchemyConnectionConfig.model_validate(
        {"host_port": "h:1", "scheme": "postgresql", "sqlalchemy_uri": sqlalchemy_uri}
    )
    return SecretRegistry.get_instance().get_all_secrets()


@pytest.mark.parametrize(
    "sqlalchemy_uri, passwords",
    [
        pytest.param(
            "postgresql://my_user:my-uri-secret@my-host/my_db",
            ["my-uri-secret"],
            id="userinfo",
        ),
        pytest.param(
            "postgresql://my_user:Str0ng%21Uri%23Secret@my-host/my_db",
            ["Str0ng!Uri#Secret", "Str0ng%21Uri%23Secret"],
            id="userinfo_decoded_and_as_written",
        ),
        pytest.param(
            "postgresql://my_user:Str0ng%21Uri#Secret@my-host/my_db",
            ["Str0ng%21Uri#Secret"],
            id="userinfo_mixed_encoding_as_written",
        ),
        pytest.param(
            "postgresql://my_user:my-uri-secret@my-host:bad/my_db",
            ["my-uri-secret"],
            id="userinfo_of_an_invalid_uri",
        ),
        pytest.param(
            "postgresql://my_user@my-host/my_db?sslmode=require&password=my-query-secret",
            ["my-query-secret"],
            id="password_query_param",
        ),
        pytest.param(
            "postgresql://my_user:my-uri-secret@my-host/my_db?password=my-query-secret",
            ["my-uri-secret", "my-query-secret"],
            id="userinfo_and_query_param",
        ),
        pytest.param(
            "postgresql://my_user@my-host/my_db?password=my-first-secret&password=my-second-secret",
            ["my-first-secret", "my-second-secret"],
            id="repeated_query_param",
        ),
        pytest.param(
            "mssql+pyodbc://my_user@my-host/my_db?PWD=my-odbc-secret",
            ["my-odbc-secret"],
            id="query_param_case_insensitive",
        ),
        pytest.param(
            "mssql+pyodbc:///?odbc_connect=DRIVER%3D%7BMy+Driver%7D%3BSERVER%3Dmy-host%3BPWD%3D%7Bmy%3Bodbc-secret%7D",
            ["my;odbc-secret"],
            id="odbc_connect_pwd",
        ),
    ],
)
def test_sqlalchemy_uri_passwords_are_registered_for_masking(
    sqlalchemy_uri: str, passwords: List[str]
) -> None:
    registered = _registered_uri_secrets(sqlalchemy_uri)

    for password in passwords:
        assert password in registered


def test_sqlalchemy_uri_with_unencoded_at_does_not_register_the_misparsed_prefix() -> (
    None
):
    # SQLAlchemy splits "my@secret" into password "my" and host "secret@my-host".
    registered = _registered_uri_secrets("postgresql://my_user:abc@secret@my-host/db")

    assert "abc@secret" in registered
    assert "abc" not in registered


@pytest.mark.parametrize(
    "sqlalchemy_uri",
    [
        pytest.param("postgresql://my_user@my-host/my_db?sslmode=require", id="none"),
        pytest.param("not a url", id="unparseable"),
    ],
)
def test_sqlalchemy_uri_without_a_password_registers_nothing(
    sqlalchemy_uri: str,
) -> None:
    assert _registered_uri_secrets(sqlalchemy_uri) == {}


def test_unparseable_sqlalchemy_uri_warns_that_it_may_not_be_masked(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING):
        _registered_uri_secrets("not a url")

    assert "may not be masked" in caplog.text


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
