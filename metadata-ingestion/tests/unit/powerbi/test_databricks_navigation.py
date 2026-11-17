import pytest

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
from datahub.ingestion.source.powerbi.m_query.pattern_handler import DatabricksLineage
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Table


@pytest.fixture
def databricks_config():
    """Config for Databricks tests."""
    return PowerBiDashboardSourceConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
    )


@pytest.fixture
def databricks_lineage(databricks_config):
    """DatabricksLineage instance for testing."""
    return DatabricksLineage(
        ctx=PipelineContext(run_id="test-run-id"),
        table=Table(name="my_table", full_name="my_catalog.my_schema.my_table"),
        reporter=PowerBiDashboardSourceReport(),
        config=databricks_config,
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            databricks_config
        ),
    )


def _databricks_arg_list() -> dict:
    """Databricks.Catalogs(<host>, <http path>, null) argument list."""
    return {
        "kind": "InvokeExpression",
        "content": {
            "kind": "ArrayWrapper",
            "elements": [
                {
                    "kind": "LiteralExpression",
                    "literalKind": "Text",
                    "literal": '"adb-123.azuredatabricks.net"',
                },
                {
                    "kind": "LiteralExpression",
                    "literalKind": "Text",
                    "literal": '"/sql/1.0/endpoints/12345dc91aa25844"',
                },
            ],
        },
    }


_DATABRICKS_EXPECTED_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:databricks,my_catalog.my_schema.my_table,PROD)"
)


def test_databricks_lineage_leaf_step_without_kind(databricks_lineage):
    """A leaf navigation step written without Kind="Table" still resolves, using
    its position in the catalog/schema/table chain."""
    table_accessor = IdentifierAccessor(
        identifier="my_table", items={"Name": "my_table"}, next=None
    )
    schema_accessor = IdentifierAccessor(
        identifier="my_schema",
        items={"Name": "my_schema", "Kind": "Schema"},
        next=table_accessor,
    )
    database_accessor = IdentifierAccessor(
        identifier="my_catalog",
        items={"Name": "my_catalog", "Kind": "Database"},
        next=schema_accessor,
    )

    lineage = databricks_lineage.create_lineage(
        DataAccessFunctionDetail(
            arg_list=_databricks_arg_list(),
            data_access_function_name="Databricks.Catalogs",
            identifier_accessor=database_accessor,
            node_map={},
        )
    )

    assert len(lineage.upstreams) == 1
    assert lineage.upstreams[0].urn == _DATABRICKS_EXPECTED_URN


def test_databricks_lineage_middle_step_without_kind(databricks_lineage):
    """Kind may be absent on a step in the middle of the chain, not just the leaf.
    The schema step here carries only Name, so it takes Schema -- the first level
    the catalog step has not already filled."""
    table_accessor = IdentifierAccessor(
        identifier="my_table", items={"Name": "my_table", "Kind": "Table"}, next=None
    )
    schema_accessor = IdentifierAccessor(
        identifier="my_schema",
        items={"Name": "my_schema"},
        next=table_accessor,
    )
    database_accessor = IdentifierAccessor(
        identifier="my_catalog",
        items={"Name": "my_catalog", "Kind": "Database"},
        next=schema_accessor,
    )

    lineage = databricks_lineage.create_lineage(
        DataAccessFunctionDetail(
            arg_list=_databricks_arg_list(),
            data_access_function_name="Databricks.Catalogs",
            identifier_accessor=database_accessor,
            node_map={},
        )
    )

    assert len(lineage.upstreams) == 1
    assert lineage.upstreams[0].urn == _DATABRICKS_EXPECTED_URN


def test_databricks_lineage_step_without_kind_or_name_warns(databricks_lineage):
    """A navigation step carrying neither Kind nor Name is reported as a warning
    rather than aborting the whole table with a bare KeyError."""
    table_accessor = IdentifierAccessor(identifier="", items={}, next=None)
    schema_accessor = IdentifierAccessor(
        identifier="my_schema",
        items={"Name": "my_schema", "Kind": "Schema"},
        next=table_accessor,
    )
    database_accessor = IdentifierAccessor(
        identifier="my_catalog",
        items={"Name": "my_catalog", "Kind": "Database"},
        next=schema_accessor,
    )

    lineage = databricks_lineage.create_lineage(
        DataAccessFunctionDetail(
            arg_list=_databricks_arg_list(),
            data_access_function_name="Databricks.Catalogs",
            identifier_accessor=database_accessor,
            node_map={},
        )
    )

    assert lineage.upstreams == []
    warning_titles = [entry.title for entry in databricks_lineage.reporter.warnings]
    assert any("navigation step" in (t or "").lower() for t in warning_titles), (
        f"Expected a warning about the unusable navigation step; got: {warning_titles}"
    )
