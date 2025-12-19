import pytest
from lark import Token, Tree

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
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
