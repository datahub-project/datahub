from unittest.mock import MagicMock, Mock, patch

import pytest

from datahub.ingestion.source.sql.trino import (
    KNOWN_CONNECTOR_PLATFORM_MAPPING,
    TWO_TIER_CONNECTORS,
    ConnectorDetail,
    TrinoConfig,
    TrinoSource,
)


def test_trino_config_defaults():
    config = TrinoConfig(
        host_port="localhost:8080",
        database="test_catalog",
        username="test_user",
    )

    assert config.include_column_lineage is True
    assert config.ingest_lineage_to_connectors is True
    assert config.trino_as_primary is True
    assert config.catalog_to_connector_details == {}


def test_platform_names_are_lowercase():
    """DataHub convention requires lowercase platform names"""
    for platform_name in KNOWN_CONNECTOR_PLATFORM_MAPPING.values():
        assert platform_name == platform_name.lower()


def test_two_tier_vs_three_tier_classification():
    """Verify 2-tier connectors are properly classified"""
    assert "hive" in TWO_TIER_CONNECTORS
    assert "mysql" in TWO_TIER_CONNECTORS

    assert "postgresql" not in TWO_TIER_CONNECTORS
    assert "snowflake_distributed" not in TWO_TIER_CONNECTORS


def test_get_source_dataset_urn_two_tier():
    """Two-tier connectors use platform_instance.schema.table format"""
    config = TrinoConfig(
        host_port="localhost:8080",
        database="hive_catalog",
        username="test",
        catalog_to_connector_details={
            "hive_catalog": ConnectorDetail(
                connector_platform="hive",
                platform_instance="prod",
                env="PROD",
            )
        },
    )

    mock_ctx = MagicMock()
    mock_ctx.graph = None
    source = TrinoSource(config, mock_ctx)

    mock_inspector = Mock()
    mock_inspector.engine = Mock()

    with patch(
        "datahub.ingestion.source.sql.trino.get_catalog_connector_name",
        return_value="hive",
    ):
        urn = source._get_source_dataset_urn(
            dataset_name="hive_catalog.schema1.table1",
            inspector=mock_inspector,
            schema="schema1",
            table="table1",
        )

    assert urn == "urn:li:dataset:(urn:li:dataPlatform:hive,prod.schema1.table1,PROD)"


def test_get_source_dataset_urn_three_tier():
    """Three-tier connectors use platform_instance.database.schema.table format"""
    config = TrinoConfig(
        host_port="localhost:8080",
        database="pg_catalog",
        username="test",
        catalog_to_connector_details={
            "pg_catalog": ConnectorDetail(
                connector_platform="postgresql",
                connector_database="my_db",
                platform_instance="prod_pg",
                env="PROD",
            )
        },
    )

    mock_ctx = MagicMock()
    mock_ctx.graph = None
    source = TrinoSource(config, mock_ctx)

    mock_inspector = Mock()
    mock_inspector.engine = Mock()

    with patch(
        "datahub.ingestion.source.sql.trino.get_catalog_connector_name",
        return_value="postgresql",
    ):
        urn = source._get_source_dataset_urn(
            dataset_name="pg_catalog.public.users",
            inspector=mock_inspector,
            schema="public",
            table="users",
        )

    assert (
        urn
        == "urn:li:dataset:(urn:li:dataPlatform:postgres,prod_pg.my_db.public.users,PROD)"
    )


def test_get_source_dataset_urn_unsupported_connector_returns_none():
    config = TrinoConfig(
        host_port="localhost:8080",
        database="unsupported_catalog",
        username="test",
    )

    mock_ctx = MagicMock()
    mock_ctx.graph = None
    source = TrinoSource(config, mock_ctx)

    mock_inspector = Mock()
    mock_inspector.engine = Mock()

    with patch(
        "datahub.ingestion.source.sql.trino.get_catalog_connector_name",
        return_value="unsupported_connector",
    ):
        urn = source._get_source_dataset_urn(
            dataset_name="unsupported_catalog.schema1.table1",
            inspector=mock_inspector,
            schema="schema1",
            table="table1",
        )

    assert urn is None


def test_three_tier_connector_without_database_returns_none():
    """Three-tier connectors require connector_database to be specified"""
    config = TrinoConfig(
        host_port="localhost:8080",
        database="pg_catalog",
        username="test",
        catalog_to_connector_details={
            "pg_catalog": ConnectorDetail(connector_platform="postgresql")
        },
    )

    mock_ctx = MagicMock()
    mock_ctx.graph = None
    source = TrinoSource(config, mock_ctx)

    mock_inspector = Mock()
    mock_inspector.engine = Mock()

    with patch(
        "datahub.ingestion.source.sql.trino.get_catalog_connector_name",
        return_value="postgresql",
    ):
        urn = source._get_source_dataset_urn(
            dataset_name="pg_catalog.public.users",
            inspector=mock_inspector,
            schema="public",
            table="users",
        )

    assert urn is None


def test_trino_identifier_format():
    config = TrinoConfig(
        host_port="localhost:8080",
        database="my_catalog",
        username="test",
    )

    identifier = config.get_identifier(schema="my_schema", table="my_table")

    assert identifier == "my_catalog.my_schema.my_table"


@pytest.mark.parametrize(
    "connector_name,expected_platform",
    [
        ("postgresql", "postgres"),
        ("snowflake_distributed", "snowflake"),
        ("delta_lake", "delta-lake"),
    ],
)
def test_connector_to_platform_name_mapping(connector_name, expected_platform):
    """Test connector names that differ from their platform names"""
    assert KNOWN_CONNECTOR_PLATFORM_MAPPING[connector_name] == expected_platform
