from typing import Optional

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
