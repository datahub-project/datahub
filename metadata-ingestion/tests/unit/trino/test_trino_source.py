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
    # incremental_lineage is inherited from SQLCommonConfig via BasicSQLAlchemyConfig
    assert config.incremental_lineage is False


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


def test_config_validation_column_lineage_requires_connector_lineage():
    """Test that include_column_lineage requires ingest_lineage_to_connectors"""
    with pytest.raises(
        ValueError, match="include_column_lineage requires ingest_lineage_to_connectors"
    ):
        TrinoConfig(
            host_port="localhost:8080",
            database="test_catalog",
            username="test_user",
            ingest_lineage_to_connectors=False,
            include_column_lineage=True,
        )


def test_config_validation_allows_column_lineage_when_connector_lineage_enabled():
    """Test that include_column_lineage works when ingest_lineage_to_connectors is enabled"""
    config = TrinoConfig(
        host_port="localhost:8080",
        database="test_catalog",
        username="test_user",
        ingest_lineage_to_connectors=True,
        include_column_lineage=True,
    )
    assert config.include_column_lineage is True
    assert config.ingest_lineage_to_connectors is True


def test_config_validation_allows_both_disabled():
    """Test that both lineage flags can be disabled"""
    config = TrinoConfig(
        host_port="localhost:8080",
        database="test_catalog",
        username="test_user",
        ingest_lineage_to_connectors=False,
        include_column_lineage=False,
    )
    assert config.include_column_lineage is False
    assert config.ingest_lineage_to_connectors is False


@pytest.mark.parametrize(
    "connector_name,is_two_tier,expected_platform",
    [
        # Two-tier connectors
        ("cassandra", True, "cassandra"),
        ("clickhouse", True, "clickhouse"),
        ("delta_lake", True, "delta-lake"),
        ("delta-lake", True, "delta-lake"),
        ("druid", True, "druid"),
        ("elasticsearch", True, "elasticsearch"),
        ("glue", True, "glue"),
        ("hive", True, "hive"),
        ("hudi", True, "hudi"),
        ("iceberg", True, "iceberg"),
        ("mariadb", True, "mariadb"),
        ("mongodb", True, "mongodb"),
        ("mysql", True, "mysql"),
        ("pinot", True, "pinot"),
        # Three-tier connectors
        ("bigquery", False, "bigquery"),
        ("databricks", False, "databricks"),
        ("db2", False, "db2"),
        ("oracle", False, "oracle"),
        ("postgresql", False, "postgres"),
        ("redshift", False, "redshift"),
        ("snowflake_distributed", False, "snowflake"),
        ("snowflake_parallel", False, "snowflake"),
        ("snowflake_jdbc", False, "snowflake"),
        ("sqlserver", False, "mssql"),
        ("teradata", False, "teradata"),
        ("vertica", False, "vertica"),
    ],
)
def test_all_supported_connectors(connector_name, is_two_tier, expected_platform):
    """Test that all supported connectors are properly configured"""
    # Verify connector is in platform mapping
    assert connector_name in KNOWN_CONNECTOR_PLATFORM_MAPPING
    assert KNOWN_CONNECTOR_PLATFORM_MAPPING[connector_name] == expected_platform

    # Verify two-tier classification is correct
    if is_two_tier:
        assert connector_name in TWO_TIER_CONNECTORS
    else:
        assert connector_name not in TWO_TIER_CONNECTORS


def test_get_source_dataset_urn_cassandra_two_tier():
    """Test Cassandra as a two-tier connector"""
    config = TrinoConfig(
        host_port="localhost:8080",
        database="cassandra_catalog",
        username="test",
        catalog_to_connector_details={
            "cassandra_catalog": ConnectorDetail(
                connector_platform="cassandra",
                platform_instance="prod_cassandra",
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
        return_value="cassandra",
    ):
        urn = source._get_source_dataset_urn(
            dataset_name="cassandra_catalog.keyspace1.users",
            inspector=mock_inspector,
            schema="keyspace1",
            table="users",
        )

    assert (
        urn
        == "urn:li:dataset:(urn:li:dataPlatform:cassandra,prod_cassandra.keyspace1.users,PROD)"
    )


def test_get_source_dataset_urn_oracle_three_tier():
    """Test Oracle as a three-tier connector"""
    config = TrinoConfig(
        host_port="localhost:8080",
        database="oracle_catalog",
        username="test",
        catalog_to_connector_details={
            "oracle_catalog": ConnectorDetail(
                connector_platform="oracle",
                connector_database="ORCL",
                platform_instance="prod_oracle",
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
        return_value="oracle",
    ):
        urn = source._get_source_dataset_urn(
            dataset_name="oracle_catalog.hr.employees",
            inspector=mock_inspector,
            schema="hr",
            table="employees",
        )

    assert (
        urn
        == "urn:li:dataset:(urn:li:dataPlatform:oracle,prod_oracle.ORCL.hr.employees,PROD)"
    )


def test_get_source_dataset_urn_mongodb_two_tier():
    """Test MongoDB as a two-tier connector"""
    config = TrinoConfig(
        host_port="localhost:8080",
        database="mongo_catalog",
        username="test",
        catalog_to_connector_details={
            "mongo_catalog": ConnectorDetail(
                connector_platform="mongodb",
                platform_instance="prod_mongo",
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
        return_value="mongodb",
    ):
        urn = source._get_source_dataset_urn(
            dataset_name="mongo_catalog.mydb.mycollection",
            inspector=mock_inspector,
            schema="mydb",
            table="mycollection",
        )

    assert (
        urn
        == "urn:li:dataset:(urn:li:dataPlatform:mongodb,prod_mongo.mydb.mycollection,PROD)"
    )


def test_get_source_dataset_urn_sqlserver_three_tier():
    """Test SQL Server as a three-tier connector (maps to mssql platform)"""
    config = TrinoConfig(
        host_port="localhost:8080",
        database="sqlserver_catalog",
        username="test",
        catalog_to_connector_details={
            "sqlserver_catalog": ConnectorDetail(
                connector_platform="sqlserver",
                connector_database="master",
                platform_instance="prod_sqlserver",
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
        return_value="sqlserver",
    ):
        urn = source._get_source_dataset_urn(
            dataset_name="sqlserver_catalog.dbo.customers",
            inspector=mock_inspector,
            schema="dbo",
            table="customers",
        )

    assert (
        urn
        == "urn:li:dataset:(urn:li:dataPlatform:mssql,prod_sqlserver.master.dbo.customers,PROD)"
    )


def test_incremental_lineage_config():
    """Test that incremental_lineage config is supported (inherited from SQLCommonConfig)"""
    config = TrinoConfig(
        host_port="localhost:8080",
        database="test_catalog",
        username="test_user",
        incremental_lineage=True,
    )

    assert config.incremental_lineage is True

    config_disabled = TrinoConfig(
        host_port="localhost:8080",
        database="test_catalog",
        username="test_user",
        incremental_lineage=False,
    )

    assert config_disabled.incremental_lineage is False
