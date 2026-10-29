"""Two-tier sources (MySQL, MariaDB, ClickHouse, ...) have no schema layer:
the database is the container, and the Inspector's "schemas" are databases."""

from typing import Any, Callable, List
from unittest.mock import create_autospec

import pytest
from sqlalchemy.engine import Inspector

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
    """Per-database reconnects rebuild the recipe's own URI: the password must
    not be masked and its query string must survive alongside uri_opts."""
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
    inspector = create_autospec(Inspector, instance=True)
    assert list(source.get_allowed_schemas(inspector, "db1")) == ["db1"]
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
def main_db(sqlite_file_db: Callable[..., str]) -> str:
    """A relative ``sqlite:///main`` URL: swapping its database part for
    ``current_db`` names another file in the same temp dir, so the per-database
    reconnect in get_inspectors runs against real engines."""
    return sqlite_file_db("main")


def _two_tier_sqlite(url: str, **overrides: Any) -> TwoTierSQLAlchemySource:
    config = TwoTierSQLAlchemyConfig.model_validate(
        {"host_port": "", "scheme": "sqlite", "sqlalchemy_uri": url, **overrides}
    )
    return TwoTierSQLAlchemySource(config, PipelineContext(run_id="test"), "sqlite")


def test_get_inspectors_opens_one_connection_per_allowed_database(
    main_db: str,
) -> None:
    source = _two_tier_sqlite(main_db)

    inspectors = list(source.get_inspectors())

    assert [source.get_db_name(i) for i in inspectors] == ["main"]


def test_database_pattern_filters_the_databases_connected_to(main_db: str) -> None:
    denied = _two_tier_sqlite(main_db, database_pattern={"deny": ["main"]})
    assert list(denied.get_inspectors()) == []

    # A recipe naming its database connects to that one alone, without listing.
    pinned = _two_tier_sqlite(main_db, database="other")
    names: List[str] = [pinned.get_db_name(i) for i in pinned.get_inspectors()]
    assert names == ["other"]
