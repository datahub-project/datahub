"""Two-tier sources (MySQL, MariaDB, ClickHouse, ...) have no schema layer:
the database is the container, and the Inspector's "schemas" are databases."""

import pathlib
import sqlite3
from typing import Any, List

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.metadata.schema_classes import ContainerClass


def _config(**overrides: Any) -> TwoTierSQLAlchemyConfig:
    return TwoTierSQLAlchemyConfig.model_validate(
        {"host_port": "localhost:3306", "scheme": "mysql+pymysql", **overrides}
    )


def _source(**overrides: Any) -> TwoTierSQLAlchemySource:
    return TwoTierSQLAlchemySource(
        _config(**overrides), PipelineContext(run_id="test"), "mysql"
    )


def test_url_is_built_from_parts_with_the_current_database() -> None:
    config = _config(username="u", password="p", database="db1")

    assert config.get_sql_alchemy_url() == "mysql+pymysql://u:p@localhost:3306/db1"
    assert config.get_sql_alchemy_url(current_db="db2") == (
        "mysql+pymysql://u:p@localhost:3306/db2"
    )


def test_explicit_uri_keeps_its_password_and_query_but_swaps_the_database() -> None:
    config = _config(
        sqlalchemy_uri="mysql+pymysql://u:secret@host:3307/db1?charset=utf8"
    )

    url = config.get_sql_alchemy_url(current_db="db2", uri_opts={"ssl": "true"})

    assert url.startswith("mysql+pymysql://u:secret@host:3307/db2?")
    assert "charset=utf8" in url and "ssl=true" in url


def test_schema_pattern_is_an_alias_for_database_pattern() -> None:
    config = _config(schema_pattern={"allow": ["prod_.*"]})
    assert config.database_pattern.allowed("prod_db")
    assert not config.database_pattern.allowed("dev_db")


def test_containers_are_databases() -> None:
    source = _source()
    assert TwoTierSQLAlchemyConfig.probe_container_kind() == (
        DatasetContainerSubTypes.DATABASE
    )
    assert source.get_db_schema("db1.table") == (None, "db1")
    # The "schema" the loops pass around is the database itself.
    assert list(source.get_allowed_schemas(None, "db1")) == ["db1"]  # type: ignore[arg-type]
    assert list(source.gen_schema_containers(schema="db1", database="db1")) == []
    with pytest.raises(NotImplementedError):
        source.gen_schema_key("db1", "db1")


def test_tables_are_parented_by_the_database_container() -> None:
    source = _source()
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,db1.t,PROD)"

    workunits = list(
        source.add_table_to_schema_container(dataset_urn, db_name="db1", schema="db1")
    )

    assert len(workunits) == 1
    mcp = workunits[0].metadata
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    assert isinstance(mcp.aspect, ContainerClass)
    assert (
        mcp.aspect.container == source.get_database_container_key("db1", "db1").as_urn()
    )


@pytest.fixture
def sqlite_dir(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> pathlib.Path:
    # Relative sqlite URLs make `current_db` a file name under tmp_path, so the
    # per-database reconnect in get_inspectors runs against real engines.
    monkeypatch.chdir(tmp_path)
    sqlite3.connect("main").close()
    return tmp_path


def _two_tier_sqlite(**overrides: Any) -> TwoTierSQLAlchemySource:
    config = TwoTierSQLAlchemyConfig.model_validate(
        {
            "host_port": "",
            "scheme": "sqlite",
            "sqlalchemy_uri": "sqlite:///main",
            **overrides,
        }
    )
    return TwoTierSQLAlchemySource(config, PipelineContext(run_id="test"), "sqlite")


def test_get_inspectors_opens_one_connection_per_allowed_database(
    sqlite_dir: pathlib.Path,
) -> None:
    source = _two_tier_sqlite()

    inspectors = list(source.get_inspectors())

    assert [source.get_db_name(i) for i in inspectors] == ["main"]
    assert source.get_db_name(inspectors[0]) == "main"


def test_database_pattern_filters_the_databases_connected_to(
    sqlite_dir: pathlib.Path,
) -> None:
    denied = _two_tier_sqlite(database_pattern={"deny": ["main"]})
    assert list(denied.get_inspectors()) == []

    # A recipe naming its database connects to that one alone, without listing.
    pinned = _two_tier_sqlite(database="other")
    names: List[str] = [pinned.get_db_name(i) for i in pinned.get_inspectors()]
    assert names == ["other"]
