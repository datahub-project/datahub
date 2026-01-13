import pytest
from lark import Token, Tree

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    AthenaPlatformOverride,
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    ResolvePlatformInstanceFromDatasetTypeMapping,
)
from datahub.ingestion.source.powerbi.m_query.data_classes import (
    DataAccessFunctionDetail,
    IdentifierAccessor,
)
from datahub.ingestion.source.powerbi.m_query.pattern_handler import (
    AmazonAthenaLineage,
    MSSqlLineage,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Table


@pytest.fixture
def creator():
    config = PowerBiDashboardSourceConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
    )

    table = Table(
        name="test_table",
        full_name="db.schema.test_table",
    )

    return MSSqlLineage(
        ctx=PipelineContext(run_id="test-run-id"),
        table=table,
        reporter=PowerBiDashboardSourceReport(),
        config=config,
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
    )


def test_parse_three_part_table_reference(creator):
    v = creator.create_urn_using_old_parser(
        "SELECT * FROM [dwhdbt].[dbo2].[my_table] where oper_day_date > getdate() - 5",
        db_name="default_db",
        server="server",
    )
    assert len(v) == 1
    assert (
        v[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:mssql,dwhdbt.dbo2.my_table,PROD)"
    )


def test_parse_two_part_table_reference(creator):
    v = creator.create_urn_using_old_parser(
        "SELECT * FROM my_schema.my_table",
        db_name="default_db",
        server="server",
    )
    assert len(v) == 1
    assert (
        v[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:mssql,default_db.my_schema.my_table,PROD)"
    )


def test_parse_one_part_table_reference(creator):
    v = creator.create_urn_using_old_parser(
        "SELECT * FROM my_table",
        db_name="default_db",
        server="server",
    )
    assert len(v) == 1
    assert (
        v[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:mssql,default_db.dbo.my_table,PROD)"
    )


# Amazon Athena Tests


@pytest.fixture
def athena_config():
    """Config for Athena tests."""
    return PowerBiDashboardSourceConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
    )


@pytest.fixture
def athena_table():
    """Table for Athena tests."""
    return Table(
        name="sales_data",
        full_name="awsdatacatalog.analytics.sales_data",
    )


@pytest.fixture
def athena_lineage(athena_config, athena_table):
    """AmazonAthenaLineage instance for testing."""
    return AmazonAthenaLineage(
        ctx=PipelineContext(run_id="test-run-id"),
        table=athena_table,
        reporter=PowerBiDashboardSourceReport(),
        config=athena_config,
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            athena_config
        ),
    )


def test_athena_lineage_valid_three_level_hierarchy(athena_lineage):
    """Test Athena lineage extraction with valid catalog.database.table hierarchy."""
    # Create mock data access function detail with three-level hierarchy
    table_accessor = IdentifierAccessor(
        identifier="table", items={"Name": "sales_data"}, next=None
    )
    db_accessor = IdentifierAccessor(
        identifier="database", items={"Name": "analytics"}, next=table_accessor
    )
    catalog_accessor = IdentifierAccessor(
        identifier="catalog", items={"Name": "awsdatacatalog"}, next=db_accessor
    )

    # Mock argument list (Tree) with region
    arg_list: Tree = Tree(
        "arg_list", [Tree("string", [Token("STRING", '"us-east-1"')])]
    )

    data_access_func_detail = DataAccessFunctionDetail(
        arg_list=arg_list,
        data_access_function_name="AmazonAthena.Databases",
        identifier_accessor=catalog_accessor,
    )

    lineage = athena_lineage.create_lineage(data_access_func_detail)

    assert len(lineage.upstreams) == 1
    assert (
        lineage.upstreams[0].data_platform_pair.datahub_data_platform_name == "athena"
    )
    assert "analytics.sales_data" in lineage.upstreams[0].urn
    assert "athena" in lineage.upstreams[0].urn
    # Catalog name should not be in the URN
    assert "awsdatacatalog.analytics" not in lineage.upstreams[0].urn


def test_athena_lineage_missing_server(athena_lineage):
    """Test Athena lineage returns empty when server/region is missing."""
    catalog_accessor = IdentifierAccessor(
        identifier="catalog", items={"Name": "awsdatacatalog"}, next=None
    )

    # Empty argument list (no region)
    arg_list: Tree = Tree("arg_list", [])

    data_access_func_detail = DataAccessFunctionDetail(
        arg_list=arg_list,
        data_access_function_name="AmazonAthena.Databases",
        identifier_accessor=catalog_accessor,
    )

    lineage = athena_lineage.create_lineage(data_access_func_detail)

    assert len(lineage.upstreams) == 0
    assert len(lineage.column_lineage) == 0


def test_athena_lineage_missing_identifier_accessor(athena_lineage):
    """Test Athena lineage returns empty when identifier accessor is None."""
    arg_list: Tree = Tree(
        "arg_list", [Tree("string", [Token("STRING", '"us-east-1"')])]
    )

    data_access_func_detail = DataAccessFunctionDetail(
        arg_list=arg_list,
        data_access_function_name="AmazonAthena.Databases",
        identifier_accessor=None,
    )

    lineage = athena_lineage.create_lineage(data_access_func_detail)

    assert len(lineage.upstreams) == 0
    assert len(lineage.column_lineage) == 0


def test_athena_lineage_incomplete_hierarchy_missing_database(athena_lineage):
    """Test Athena lineage returns empty when database level is missing."""
    catalog_accessor = IdentifierAccessor(
        identifier="catalog", items={"Name": "awsdatacatalog"}, next=None
    )

    arg_list: Tree = Tree(
        "arg_list", [Tree("string", [Token("STRING", '"us-west-2"')])]
    )

    data_access_func_detail = DataAccessFunctionDetail(
        arg_list=arg_list,
        data_access_function_name="AmazonAthena.Databases",
        identifier_accessor=catalog_accessor,
    )

    lineage = athena_lineage.create_lineage(data_access_func_detail)

    assert len(lineage.upstreams) == 0


def test_athena_lineage_incomplete_hierarchy_missing_table(athena_lineage):
    """Test Athena lineage returns empty when table level is missing."""
    db_accessor = IdentifierAccessor(
        identifier="database", items={"Name": "analytics"}, next=None
    )
    catalog_accessor = IdentifierAccessor(
        identifier="catalog", items={"Name": "awsdatacatalog"}, next=db_accessor
    )

    arg_list: Tree = Tree(
        "arg_list", [Tree("string", [Token("STRING", '"eu-west-1"')])]
    )

    data_access_func_detail = DataAccessFunctionDetail(
        arg_list=arg_list,
        data_access_function_name="AmazonAthena.Databases",
        identifier_accessor=catalog_accessor,
    )

    lineage = athena_lineage.create_lineage(data_access_func_detail)

    assert len(lineage.upstreams) == 0


def test_athena_lineage_malformed_items_missing_name_key(athena_lineage):
    """Test Athena lineage handles missing 'Name' key gracefully."""
    table_accessor = IdentifierAccessor(
        identifier="table", items={"InvalidKey": "sales_data"}, next=None
    )
    db_accessor = IdentifierAccessor(
        identifier="database", items={"Name": "analytics"}, next=table_accessor
    )
    catalog_accessor = IdentifierAccessor(
        identifier="catalog", items={"Name": "awsdatacatalog"}, next=db_accessor
    )

    arg_list: Tree = Tree(
        "arg_list", [Tree("string", [Token("STRING", '"us-east-1"')])]
    )

    data_access_func_detail = DataAccessFunctionDetail(
        arg_list=arg_list,
        data_access_function_name="AmazonAthena.Databases",
        identifier_accessor=catalog_accessor,
    )

    lineage = athena_lineage.create_lineage(data_access_func_detail)

    # Should handle KeyError gracefully and return empty lineage
    assert len(lineage.upstreams) == 0


def test_athena_lineage_different_regions(athena_lineage):
    """Test Athena lineage with different AWS regions."""
    regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]

    for region in regions:
        table_accessor = IdentifierAccessor(
            identifier="table", items={"Name": "test_table"}, next=None
        )
        db_accessor = IdentifierAccessor(
            identifier="database", items={"Name": "test_db"}, next=table_accessor
        )
        catalog_accessor = IdentifierAccessor(
            identifier="catalog", items={"Name": "awsdatacatalog"}, next=db_accessor
        )

        arg_list: Tree = Tree(
            "arg_list", [Tree("string", [Token("STRING", f'"{region}"')])]
        )

        data_access_func_detail = DataAccessFunctionDetail(
            arg_list=arg_list,
            data_access_function_name="AmazonAthena.Databases",
            identifier_accessor=catalog_accessor,
        )

        lineage = athena_lineage.create_lineage(data_access_func_detail)

        assert len(lineage.upstreams) == 1
        # URN should contain the qualified table name (without catalog)
        assert "test_db.test_table" in lineage.upstreams[0].urn
        assert "awsdatacatalog.test_db" not in lineage.upstreams[0].urn


def test_athena_platform_pair(athena_lineage):
    """Test that Athena returns correct platform pair."""
    platform_pair = athena_lineage.get_platform_pair()

    assert platform_pair.datahub_data_platform_name == "athena"
    assert platform_pair.powerbi_data_platform_name == "Amazon Athena"


def test_athena_custom_catalog_name(athena_lineage):
    """Test Athena lineage with custom Glue catalog name."""
    table_accessor = IdentifierAccessor(
        identifier="table", items={"Name": "sales_data"}, next=None
    )
    db_accessor = IdentifierAccessor(
        identifier="database", items={"Name": "analytics"}, next=table_accessor
    )
    # Custom Glue catalog instead of AwsDataCatalog
    catalog_accessor = IdentifierAccessor(
        identifier="catalog", items={"Name": "my_glue_catalog"}, next=db_accessor
    )

    arg_list: Tree = Tree(
        "arg_list", [Tree("string", [Token("STRING", '"us-west-2"')])]
    )

    data_access_func_detail = DataAccessFunctionDetail(
        arg_list=arg_list,
        data_access_function_name="AmazonAthena.Databases",
        identifier_accessor=catalog_accessor,
    )

    lineage = athena_lineage.create_lineage(data_access_func_detail)

    # Should still work with custom catalog
    assert len(lineage.upstreams) == 1
    # URN should NOT include catalog name (even custom ones)
    assert "analytics.sales_data" in lineage.upstreams[0].urn
    assert "my_glue_catalog" not in lineage.upstreams[0].urn


def test_athena_empty_database_name(athena_lineage):
    """Test Athena lineage with empty database name."""
    table_accessor = IdentifierAccessor(
        identifier="table", items={"Name": "sales_data"}, next=None
    )
    db_accessor = IdentifierAccessor(
        identifier="database", items={"Name": ""}, next=table_accessor
    )
    catalog_accessor = IdentifierAccessor(
        identifier="catalog", items={"Name": "awsdatacatalog"}, next=db_accessor
    )

    arg_list: Tree = Tree(
        "arg_list", [Tree("string", [Token("STRING", '"us-east-1"')])]
    )

    data_access_func_detail = DataAccessFunctionDetail(
        arg_list=arg_list,
        data_access_function_name="AmazonAthena.Databases",
        identifier_accessor=catalog_accessor,
    )

    lineage = athena_lineage.create_lineage(data_access_func_detail)

    # Should return empty lineage for empty database name
    assert len(lineage.upstreams) == 0


def test_athena_empty_table_name(athena_lineage):
    """Test Athena lineage with empty table name."""
    table_accessor = IdentifierAccessor(
        identifier="table", items={"Name": ""}, next=None
    )
    db_accessor = IdentifierAccessor(
        identifier="database", items={"Name": "analytics"}, next=table_accessor
    )
    catalog_accessor = IdentifierAccessor(
        identifier="catalog", items={"Name": "awsdatacatalog"}, next=db_accessor
    )

    arg_list: Tree = Tree(
        "arg_list", [Tree("string", [Token("STRING", '"us-east-1"')])]
    )

    data_access_func_detail = DataAccessFunctionDetail(
        arg_list=arg_list,
        data_access_function_name="AmazonAthena.Databases",
        identifier_accessor=catalog_accessor,
    )

    lineage = athena_lineage.create_lineage(data_access_func_detail)

    # Should return empty lineage for empty table name
    assert len(lineage.upstreams) == 0


def test_athena_whitespace_only_names(athena_lineage):
    """Test Athena lineage with whitespace-only database/table names."""
    table_accessor = IdentifierAccessor(
        identifier="table", items={"Name": "   "}, next=None
    )
    db_accessor = IdentifierAccessor(
        identifier="database", items={"Name": "  "}, next=table_accessor
    )
    catalog_accessor = IdentifierAccessor(
        identifier="catalog", items={"Name": "awsdatacatalog"}, next=db_accessor
    )

    arg_list: Tree = Tree(
        "arg_list", [Tree("string", [Token("STRING", '"us-east-1"')])]
    )

    data_access_func_detail = DataAccessFunctionDetail(
        arg_list=arg_list,
        data_access_function_name="AmazonAthena.Databases",
        identifier_accessor=catalog_accessor,
    )

    lineage = athena_lineage.create_lineage(data_access_func_detail)

    # Should return empty lineage for whitespace-only names
    assert len(lineage.upstreams) == 0


# ODBC Athena Catalog Stripping Tests


@pytest.fixture
def odbc_lineage(athena_config, athena_table):
    """OdbcLineage instance for testing catalog stripping."""
    from datahub.ingestion.source.powerbi.m_query.pattern_handler import OdbcLineage

    return OdbcLineage(
        ctx=PipelineContext(run_id="test-run-id"),
        table=athena_table,
        reporter=PowerBiDashboardSourceReport(),
        config=athena_config,
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            athena_config
        ),
    )


def test_odbc_strip_athena_catalog_from_upstreams(odbc_lineage):
    """Test that ODBC strips catalog prefix from upstream table URNs."""
    from datahub.ingestion.source.powerbi.config import DataPlatformPair
    from datahub.ingestion.source.powerbi.m_query.data_classes import (
        DataPlatformTable,
        Lineage,
    )

    platform_pair = DataPlatformPair(
        datahub_data_platform_name="athena",
        powerbi_data_platform_name="Amazon Athena",
    )

    # Lineage with catalog prefix in upstream URN
    original_lineage = Lineage(
        upstreams=[
            DataPlatformTable(
                data_platform_pair=platform_pair,
                urn="urn:li:dataset:(urn:li:dataPlatform:athena,awsdatacatalog.thread-prod-normalized-parquet.accounts,PROD)",
            )
        ],
        column_lineage=[],
    )

    stripped_lineage = odbc_lineage._strip_athena_catalog_from_lineage(original_lineage)

    assert len(stripped_lineage.upstreams) == 1
    # Catalog should be stripped
    assert (
        stripped_lineage.upstreams[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:athena,thread-prod-normalized-parquet.accounts,PROD)"
    )
    # Original prefix should not be present
    assert "awsdatacatalog" not in stripped_lineage.upstreams[0].urn


def test_odbc_strip_athena_catalog_from_column_lineage(odbc_lineage):
    """Test that ODBC strips catalog prefix from column lineage URNs."""
    from datahub.ingestion.source.powerbi.config import DataPlatformPair
    from datahub.ingestion.source.powerbi.m_query.data_classes import (
        DataPlatformTable,
        Lineage,
    )
    from datahub.sql_parsing.sqlglot_lineage import (
        ColumnLineageInfo,
        ColumnRef,
        DownstreamColumnRef,
    )

    platform_pair = DataPlatformPair(
        datahub_data_platform_name="athena",
        powerbi_data_platform_name="Amazon Athena",
    )

    # Lineage with catalog prefix in both upstream and column lineage URNs
    original_lineage = Lineage(
        upstreams=[
            DataPlatformTable(
                data_platform_pair=platform_pair,
                urn="urn:li:dataset:(urn:li:dataPlatform:athena,awsdatacatalog.mydb.mytable,PROD)",
            )
        ],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=None, column="id"),
                upstreams=[
                    ColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:athena,awsdatacatalog.mydb.mytable,PROD)",
                        column="id",
                    )
                ],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=None, column="name"),
                upstreams=[
                    ColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:athena,awsdatacatalog.mydb.mytable,PROD)",
                        column="name",
                    )
                ],
            ),
        ],
    )

    stripped_lineage = odbc_lineage._strip_athena_catalog_from_lineage(original_lineage)

    # Check upstream URN is stripped
    assert (
        stripped_lineage.upstreams[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:athena,mydb.mytable,PROD)"
    )

    # Check column lineage URNs are stripped
    assert len(stripped_lineage.column_lineage) == 2
    for col_info in stripped_lineage.column_lineage:
        for col_ref in col_info.upstreams:
            assert "awsdatacatalog" not in col_ref.table
            assert (
                col_ref.table
                == "urn:li:dataset:(urn:li:dataPlatform:athena,mydb.mytable,PROD)"
            )


def test_odbc_strip_athena_catalog_preserves_non_catalog_urns(odbc_lineage):
    """Test that URNs without catalog prefix are preserved unchanged."""
    from datahub.ingestion.source.powerbi.config import DataPlatformPair
    from datahub.ingestion.source.powerbi.m_query.data_classes import (
        DataPlatformTable,
        Lineage,
    )

    platform_pair = DataPlatformPair(
        datahub_data_platform_name="athena",
        powerbi_data_platform_name="Amazon Athena",
    )

    # Lineage without catalog prefix (already in database.table format)
    original_lineage = Lineage(
        upstreams=[
            DataPlatformTable(
                data_platform_pair=platform_pair,
                urn="urn:li:dataset:(urn:li:dataPlatform:athena,mydb.mytable,PROD)",
            )
        ],
        column_lineage=[],
    )

    stripped_lineage = odbc_lineage._strip_athena_catalog_from_lineage(original_lineage)

    # URN should remain unchanged
    assert (
        stripped_lineage.upstreams[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:athena,mydb.mytable,PROD)"
    )


def test_odbc_strip_athena_3part_catalog_from_upstreams(odbc_lineage):
    """Test that ODBC strips any 3-part table names to 2-part format (not just awsdatacatalog)."""
    from datahub.ingestion.source.powerbi.config import DataPlatformPair
    from datahub.ingestion.source.powerbi.m_query.data_classes import (
        DataPlatformTable,
        Lineage,
    )

    platform_pair = DataPlatformPair(
        datahub_data_platform_name="athena",
        powerbi_data_platform_name="Amazon Athena",
    )

    # Lineage with 3-part table name (catalog.database.table)
    original_lineage = Lineage(
        upstreams=[
            DataPlatformTable(
                data_platform_pair=platform_pair,
                urn="urn:li:dataset:(urn:li:dataPlatform:athena,my_catalog.my_schema.my_table,PROD)",
            )
        ],
        column_lineage=[],
    )

    stripped_lineage = odbc_lineage._strip_athena_catalog_from_lineage(original_lineage)

    assert len(stripped_lineage.upstreams) == 1
    # First part should be stripped, leaving database.table format
    assert (
        stripped_lineage.upstreams[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:athena,my_schema.my_table,PROD)"
    )


def test_athena_table_platform_override():
    """Test that Athena applies table-specific platform overrides for federated queries."""
    from datahub.ingestion.source.powerbi.config import (
        DataPlatformPair,
        PowerBiDashboardSourceConfig,
    )
    from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
        ResolvePlatformInstanceFromDatasetTypeMapping,
    )
    from datahub.ingestion.source.powerbi.m_query.data_classes import (
        DataPlatformTable,
        Lineage,
    )
    from datahub.ingestion.source.powerbi.m_query.pattern_handler import OdbcLineage
    from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Table

    # Config with platform override for specific table
    config = PowerBiDashboardSourceConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
        athena_table_platform_override=[
            AthenaPlatformOverride(
                database="my_schema", table="my_table", platform="mysql"
            ),
        ],
    )

    table = Table(name="test_table", full_name="test_table")

    odbc = OdbcLineage(
        ctx=PipelineContext(run_id="test-run-id"),
        table=table,
        reporter=PowerBiDashboardSourceReport(),
        config=config,
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
    )

    platform_pair = DataPlatformPair(
        datahub_data_platform_name="athena",
        powerbi_data_platform_name="Amazon Athena",
    )

    # Lineage with athena platform that should be overridden to mysql
    original_lineage = Lineage(
        upstreams=[
            DataPlatformTable(
                data_platform_pair=platform_pair,
                urn="urn:li:dataset:(urn:li:dataPlatform:athena,my_schema.my_table,PROD)",
            )
        ],
        column_lineage=[],
    )

    overridden_lineage = odbc._apply_table_platform_override(
        original_lineage, dsn="TestDSN"
    )

    assert len(overridden_lineage.upstreams) == 1
    # Platform should be changed from athena to mysql
    assert (
        overridden_lineage.upstreams[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:mysql,my_schema.my_table,PROD)"
    )


def test_athena_table_platform_override_no_match():
    """Test that tables not in override config are unchanged."""
    from datahub.ingestion.source.powerbi.config import (
        DataPlatformPair,
        PowerBiDashboardSourceConfig,
    )
    from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
        ResolvePlatformInstanceFromDatasetTypeMapping,
    )
    from datahub.ingestion.source.powerbi.m_query.data_classes import (
        DataPlatformTable,
        Lineage,
    )
    from datahub.ingestion.source.powerbi.m_query.pattern_handler import OdbcLineage
    from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Table

    # Config with platform override for a different table
    config = PowerBiDashboardSourceConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
        athena_table_platform_override=[
            AthenaPlatformOverride(
                database="other_schema", table="other_table", platform="mysql"
            ),
        ],
    )

    table = Table(name="test_table", full_name="test_table")

    odbc = OdbcLineage(
        ctx=PipelineContext(run_id="test-run-id"),
        table=table,
        reporter=PowerBiDashboardSourceReport(),
        config=config,
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
    )

    platform_pair = DataPlatformPair(
        datahub_data_platform_name="athena",
        powerbi_data_platform_name="Amazon Athena",
    )

    # Lineage with table not in override config
    original_lineage = Lineage(
        upstreams=[
            DataPlatformTable(
                data_platform_pair=platform_pair,
                urn="urn:li:dataset:(urn:li:dataPlatform:athena,my_schema.my_table,PROD)",
            )
        ],
        column_lineage=[],
    )

    overridden_lineage = odbc._apply_table_platform_override(
        original_lineage, dsn="TestDSN"
    )

    assert len(overridden_lineage.upstreams) == 1
    # Platform should remain unchanged (athena)
    assert (
        overridden_lineage.upstreams[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:athena,my_schema.my_table,PROD)"
    )


def test_athena_table_platform_override_dsn_scoped():
    """Test that DSN-scoped override keys take precedence over global keys."""
    from datahub.ingestion.source.powerbi.config import (
        DataPlatformPair,
        PowerBiDashboardSourceConfig,
    )
    from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
        ResolvePlatformInstanceFromDatasetTypeMapping,
    )
    from datahub.ingestion.source.powerbi.m_query.data_classes import (
        DataPlatformTable,
        Lineage,
    )
    from datahub.ingestion.source.powerbi.m_query.pattern_handler import OdbcLineage
    from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Table

    # Config with both DSN-scoped and global overrides for the same table
    config = PowerBiDashboardSourceConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
        athena_table_platform_override=[
            AthenaPlatformOverride(
                database="my_schema", table="my_table", platform="mysql", dsn="ProdDSN"
            ),
            AthenaPlatformOverride(
                database="my_schema",
                table="my_table",
                platform="postgres",
                dsn="DevDSN",
            ),
            AthenaPlatformOverride(
                database="my_schema", table="my_table", platform="oracle"
            ),  # Global fallback
        ],
    )

    table = Table(name="test_table", full_name="test_table")

    odbc = OdbcLineage(
        ctx=PipelineContext(run_id="test-run-id"),
        table=table,
        reporter=PowerBiDashboardSourceReport(),
        config=config,
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
    )

    platform_pair = DataPlatformPair(
        datahub_data_platform_name="athena",
        powerbi_data_platform_name="Amazon Athena",
    )

    original_lineage = Lineage(
        upstreams=[
            DataPlatformTable(
                data_platform_pair=platform_pair,
                urn="urn:li:dataset:(urn:li:dataPlatform:athena,my_schema.my_table,PROD)",
            )
        ],
        column_lineage=[],
    )

    # Test DSN-scoped key for ProdDSN -> mysql
    overridden_lineage = odbc._apply_table_platform_override(
        original_lineage, dsn="ProdDSN"
    )
    assert (
        overridden_lineage.upstreams[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:mysql,my_schema.my_table,PROD)"
    )

    # Test DSN-scoped key for DevDSN -> postgres
    overridden_lineage = odbc._apply_table_platform_override(
        original_lineage, dsn="DevDSN"
    )
    assert (
        overridden_lineage.upstreams[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:postgres,my_schema.my_table,PROD)"
    )

    # Test fallback to global key for unknown DSN -> oracle
    overridden_lineage = odbc._apply_table_platform_override(
        original_lineage, dsn="UnknownDSN"
    )
    assert (
        overridden_lineage.upstreams[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:oracle,my_schema.my_table,PROD)"
    )


def test_athena_table_platform_override_column_lineage():
    """Test that platform override is applied to column lineage URNs."""
    from datahub.ingestion.source.powerbi.config import (
        DataPlatformPair,
        PowerBiDashboardSourceConfig,
    )
    from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
        ResolvePlatformInstanceFromDatasetTypeMapping,
    )
    from datahub.ingestion.source.powerbi.m_query.data_classes import (
        DataPlatformTable,
        Lineage,
    )
    from datahub.ingestion.source.powerbi.m_query.pattern_handler import OdbcLineage
    from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Table
    from datahub.sql_parsing.sqlglot_lineage import (
        ColumnLineageInfo,
        ColumnRef,
        DownstreamColumnRef,
    )

    config = PowerBiDashboardSourceConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
        athena_table_platform_override=[
            AthenaPlatformOverride(
                database="my_schema", table="my_table", platform="mysql"
            ),
        ],
    )

    table = Table(name="test_table", full_name="test_table")

    odbc = OdbcLineage(
        ctx=PipelineContext(run_id="test-run-id"),
        table=table,
        reporter=PowerBiDashboardSourceReport(),
        config=config,
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
    )

    platform_pair = DataPlatformPair(
        datahub_data_platform_name="athena",
        powerbi_data_platform_name="Amazon Athena",
    )

    original_lineage = Lineage(
        upstreams=[
            DataPlatformTable(
                data_platform_pair=platform_pair,
                urn="urn:li:dataset:(urn:li:dataPlatform:athena,my_schema.my_table,PROD)",
            )
        ],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=None, column="id"),
                upstreams=[
                    ColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:athena,my_schema.my_table,PROD)",
                        column="id",
                    )
                ],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=None, column="name"),
                upstreams=[
                    ColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:athena,my_schema.my_table,PROD)",
                        column="name",
                    )
                ],
            ),
        ],
    )

    overridden_lineage = odbc._apply_table_platform_override(
        original_lineage, dsn="TestDSN"
    )

    # Upstream URN should be overridden
    assert (
        overridden_lineage.upstreams[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:mysql,my_schema.my_table,PROD)"
    )

    # Column lineage URNs should also be overridden
    assert len(overridden_lineage.column_lineage) == 2
    for col_info in overridden_lineage.column_lineage:
        for col_ref in col_info.upstreams:
            assert "mysql" in col_ref.table
            assert "athena" not in col_ref.table


def test_athena_table_platform_override_dsn_with_special_chars():
    """Test that DSN-scoped keys work with special characters (e.g., spaces)."""
    from datahub.ingestion.source.powerbi.config import (
        DataPlatformPair,
        PowerBiDashboardSourceConfig,
    )
    from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
        ResolvePlatformInstanceFromDatasetTypeMapping,
    )
    from datahub.ingestion.source.powerbi.m_query.data_classes import (
        DataPlatformTable,
        Lineage,
    )
    from datahub.ingestion.source.powerbi.m_query.pattern_handler import OdbcLineage
    from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Table

    config = PowerBiDashboardSourceConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
        athena_table_platform_override=[
            AthenaPlatformOverride(
                database="normalized-data",
                table="users",
                platform="mysql",
                dsn="RDS MYSQL",
            ),
        ],
    )

    table = Table(name="test_table", full_name="test_table")

    odbc = OdbcLineage(
        ctx=PipelineContext(run_id="test-run-id"),
        table=table,
        reporter=PowerBiDashboardSourceReport(),
        config=config,
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
    )

    platform_pair = DataPlatformPair(
        datahub_data_platform_name="athena",
        powerbi_data_platform_name="Amazon Athena",
    )

    original_lineage = Lineage(
        upstreams=[
            DataPlatformTable(
                data_platform_pair=platform_pair,
                urn="urn:li:dataset:(urn:li:dataPlatform:athena,normalized-data.users,PROD)",
            )
        ],
        column_lineage=[],
    )

    # DSN with space should match
    overridden_lineage = odbc._apply_table_platform_override(
        original_lineage, dsn="RDS MYSQL"
    )
    assert (
        overridden_lineage.upstreams[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:mysql,normalized-data.users,PROD)"
    )


def test_odbc_query_lineage_integration_catalog_stripping_and_platform_override():
    """
    Integration test: Verify catalog stripping and platform override work together
    through the full query_lineage() path.

    This tests the real-world scenario where:
    1. Athena query has 3-part table name (catalog.database.table)
    2. Catalog prefix is stripped to 2-part (database.table)
    3. Platform is overridden from athena to mysql for federated tables
    """
    from datahub.ingestion.source.powerbi.config import (
        DataPlatformPair,
        PowerBiDashboardSourceConfig,
    )
    from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
        ResolvePlatformInstanceFromDatasetTypeMapping,
    )
    from datahub.ingestion.source.powerbi.m_query.pattern_handler import OdbcLineage
    from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Table

    # Config with platform override for the 2-part table name (after catalog stripping)
    config = PowerBiDashboardSourceConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
        dsn_to_platform_name={
            "ThreadProdDataLake": "athena",
        },
        athena_table_platform_override=[
            # Override uses 2-part name AFTER catalog stripping
            AthenaPlatformOverride(
                database="normalized-data",
                table="normalized_accounts",
                platform="mysql",
            ),
        ],
    )

    table = Table(name="test_table", full_name="test_table")

    odbc = OdbcLineage(
        ctx=PipelineContext(run_id="test-run-id"),
        table=table,
        reporter=PowerBiDashboardSourceReport(),
        config=config,
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
    )

    platform_pair = DataPlatformPair(
        datahub_data_platform_name="athena",
        powerbi_data_platform_name="Amazon Athena",
    )

    # SQL query with 3-part table name (catalog.database.table)
    sql_query = (
        'SELECT * FROM "thread-prod-data"."normalized-data"."normalized_accounts"'
    )

    # Call the full query_lineage() method
    result = odbc.query_lineage(
        query=sql_query,
        platform_pair=platform_pair,
        server_name="athena-server",
        dsn="ThreadProdDataLake",
    )

    # Verify the result
    assert len(result.upstreams) == 1

    # The URN should have:
    # 1. Catalog stripped: thread-prod-data.normalized-data.normalized_accounts -> normalized-data.normalized_accounts
    # 2. Platform overridden: athena -> mysql
    assert (
        result.upstreams[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:mysql,normalized-data.normalized_accounts,PROD)"
    )


# Tests for athena_table_platform_override config validation
def test_athena_table_platform_override_platform_with_spaces():
    """Test that platform names with spaces raise validation error."""
    with pytest.raises(ValueError) as exc_info:
        PowerBiDashboardSourceConfig(
            tenant_id="test-tenant-id",
            client_id="test-client-id",
            client_secret="test-client-secret",
            athena_table_platform_override=[
                AthenaPlatformOverride(
                    database="database", table="table", platform="my sql"
                ),
            ],
        )
    assert "contains spaces" in str(exc_info.value)


def test_athena_table_platform_override_unknown_platform_warns(caplog):
    """Test that unknown platform names trigger a warning but don't fail."""
    import logging

    with caplog.at_level(logging.WARNING):
        config = PowerBiDashboardSourceConfig(
            tenant_id="test-tenant-id",
            client_id="test-client-id",
            client_secret="test-client-secret",
            athena_table_platform_override=[
                AthenaPlatformOverride(
                    database="database",
                    table="table",
                    platform="mysq1",  # typo
                ),
            ],
        )
    # Config should be created successfully
    assert len(config.athena_table_platform_override) == 1
    assert config.athena_table_platform_override[0].platform == "mysq1"
    # But a warning should be logged
    assert "mysq1" in caplog.text
    assert "not a recognized DataHub platform" in caplog.text


def test_athena_table_platform_override_known_platform_no_warning(caplog):
    """Test that known platform names don't trigger warnings."""
    import logging

    with caplog.at_level(logging.WARNING):
        config = PowerBiDashboardSourceConfig(
            tenant_id="test-tenant-id",
            client_id="test-client-id",
            client_secret="test-client-secret",
            athena_table_platform_override=[
                AthenaPlatformOverride(
                    database="database", table="table", platform="mysql"
                ),
            ],
        )
    # Config should be created successfully
    assert len(config.athena_table_platform_override) == 1
    assert config.athena_table_platform_override[0].platform == "mysql"
    # No warning should be logged for known platform
    assert "not a recognized DataHub platform" not in caplog.text
