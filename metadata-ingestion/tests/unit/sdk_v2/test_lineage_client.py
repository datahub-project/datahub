import pathlib
from typing import Dict, List, Set, cast
from unittest.mock import MagicMock, Mock, patch

import pytest

from datahub.errors import SdkUsageError
from datahub.metadata.schema_classes import (
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)
from datahub.sdk.lineage_client import LineageClient
from datahub.sdk.main_client import DataHubClient
from datahub.sql_parsing.sql_parsing_common import QueryType
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
    SqlParsingResult,
)
from datahub.testing import mce_helpers
from datahub.utilities.urns.error import InvalidUrnError

_GOLDEN_DIR = pathlib.Path(__file__).parent / "lineage_client_golden"
_GOLDEN_DIR.mkdir(exist_ok=True)


@pytest.fixture
def mock_graph() -> Mock:
    graph = Mock()

    return graph


@pytest.fixture
def client(mock_graph: Mock) -> DataHubClient:
    return DataHubClient(graph=mock_graph)


def assert_client_golden(client: DataHubClient, golden_path: pathlib.Path) -> None:
    mcps = client._graph.emit_mcps.call_args[0][0]  # type: ignore

    mce_helpers.check_goldens_stream(
        outputs=mcps,
        golden_path=golden_path,
        ignore_order=False,
    )


def test_get_fuzzy_column_lineage(client: DataHubClient) -> None:
    """Test the fuzzy column lineage matching algorithm."""
    # Create a minimal client just for testing the method
    test_cases = [
        # Exact matches
        {
            "upstream_fields": {"id", "name", "email"},
            "downstream_fields": {"id", "name", "phone"},
            "expected": {"id": ["id"], "name": ["name"]},
        },
        # Case insensitive matches
        {
            "upstream_fields": {"ID", "Name", "Email"},
            "downstream_fields": {"id", "name", "phone"},
            "expected": {"id": ["ID"], "name": ["Name"]},
        },
        # Camel case to snake case
        {
            "upstream_fields": {"id", "user_id", "full_name"},
            "downstream_fields": {"id", "userId", "fullName"},
            "expected": {
                "id": ["id"],
                "userId": ["user_id"],
                "fullName": ["full_name"],
            },
        },
        # Snake case to camel case
        {
            "upstream_fields": {"id", "userId", "fullName"},
            "downstream_fields": {"id", "user_id", "full_name"},
            "expected": {
                "id": ["id"],
                "user_id": ["userId"],
                "full_name": ["fullName"],
            },
        },
        # Mixed matches
        {
            "upstream_fields": {"id", "customer_id", "user_name"},
            "downstream_fields": {
                "id",
                "customerId",
                "address",
            },
            "expected": {"id": ["id"], "customerId": ["customer_id"]},
        },
        # Mixed matches with different casing
        {
            "upstream_fields": {"id", "customer_id", "userName", "address_id"},
            "downstream_fields": {"id", "customerId", "user_name", "user_address"},
            "expected": {
                "id": ["id"],
                "customerId": ["customer_id"],
                "user_name": ["userName"],
            },  # user_address <> address_id shouldn't match
        },
    ]

    # Run test cases
    for test_case in test_cases:
        result = client.lineage._get_fuzzy_column_lineage(
            cast(Set[str], test_case["upstream_fields"]),
            cast(Set[str], test_case["downstream_fields"]),
        )
        assert result == test_case["expected"], (
            f"Test failed: {result} != {test_case['expected']}"
        )


def test_get_strict_column_lineage(client: DataHubClient) -> None:
    """Test the strict column lineage matching algorithm."""
    test_cases = [
        # Exact matches
        {
            "upstream_fields": {"id", "name", "email"},
            "downstream_fields": {"id", "name", "phone"},
            "expected": {"id": ["id"], "name": ["name"]},
        },
        # No matches
        {
            "upstream_fields": {"col1", "col2", "col3"},
            "downstream_fields": {"col4", "col5", "col6"},
            "expected": {},
        },
        # Case mismatch (should match)
        {
            "upstream_fields": {"ID", "Name", "Email"},
            "downstream_fields": {"id", "name", "email"},
            "expected": {"id": ["ID"], "name": ["Name"], "email": ["Email"]},
        },
    ]

    # Run test cases
    for test_case in test_cases:
        result = client.lineage._get_strict_column_lineage(
            cast(Set[str], test_case["upstream_fields"]),
            cast(Set[str], test_case["downstream_fields"]),
        )
        assert result == test_case["expected"], (
            f"Test failed: {result} != {test_case['expected']}"
        )


def test_add_dataset_copy_lineage_auto_fuzzy(client: DataHubClient) -> None:
    """Test auto fuzzy column lineage mapping."""

    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"

    # Create upstream and downstream schema
    upstream_schema = SchemaMetadataClass(
        schemaName="upstream_table",
        platform="urn:li:dataPlatform:snowflake",
        version=1,
        hash="1234567890",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
            SchemaFieldClass(
                fieldPath="user_id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
            SchemaFieldClass(
                fieldPath="address",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
            SchemaFieldClass(
                fieldPath="age",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
        ],
    )

    downstream_schema = SchemaMetadataClass(
        schemaName="downstream_table",
        platform="urn:li:dataPlatform:snowflake",
        version=1,
        hash="1234567890",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
            SchemaFieldClass(
                fieldPath="userId",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
            SchemaFieldClass(
                fieldPath="score",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
        ],
    )

    with patch.object(LineageClient, "_get_fields_from_dataset_urn") as mock_method:
        mock_method.side_effect = lambda urn: sorted(
            {
                field.fieldPath
                for field in (
                    upstream_schema if "upstream" in str(urn) else downstream_schema
                ).fields
            }
        )

        client.lineage.add_dataset_copy_lineage(
            upstream=upstream,
            downstream=downstream,
            column_lineage="auto_fuzzy",
        )

    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_copy_fuzzy_golden.json")


def test_add_dataset_copy_lineage_auto_strict(client: DataHubClient) -> None:
    """Test strict column lineage with field matches."""
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"

    upstream_schema = SchemaMetadataClass(
        schemaName="upstream_table",
        platform="urn:li:dataPlatform:snowflake",
        version=1,
        hash="1234567890",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
            SchemaFieldClass(
                fieldPath="name",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
            SchemaFieldClass(
                fieldPath="user_id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
            SchemaFieldClass(
                fieldPath="address",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
        ],
    )

    downstream_schema = SchemaMetadataClass(
        schemaName="downstream_table",
        platform="urn:li:dataPlatform:snowflake",
        version=1,
        hash="1234567890",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
            SchemaFieldClass(
                fieldPath="name",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
            SchemaFieldClass(
                fieldPath="address",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
            SchemaFieldClass(
                fieldPath="score",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
        ],
    )

    with patch.object(LineageClient, "_get_fields_from_dataset_urn") as mock_method:
        mock_method.side_effect = lambda urn: sorted(
            {
                field.fieldPath
                for field in (
                    upstream_schema if "upstream" in str(urn) else downstream_schema
                ).fields
            }
        )

        client.lineage.add_dataset_copy_lineage(
            upstream=upstream,
            downstream=downstream,
            column_lineage="auto_strict",
        )

    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_copy_strict_golden.json")


def test_add_dataset_transform_lineage_basic(client: DataHubClient) -> None:
    """Test basic lineage without column mapping or query."""

    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"

    client.lineage.add_dataset_transform_lineage(
        upstream=upstream,
        downstream=downstream,
    )
    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_basic_golden.json")


def test_add_dataset_transform_lineage_complete(client: DataHubClient) -> None:
    """Test complete lineage with column mapping and query."""

    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"
    transformation_text = (
        "SELECT us_col1 as ds_col1, us_col2 + us_col3 as ds_col2 FROM upstream_table"
    )
    column_lineage: Dict[str, List[str]] = {
        "ds_col1": ["us_col1"],  # Simple 1:1 mapping
        "ds_col2": ["us_col2", "us_col3"],  # 2:1 mapping
    }

    client.lineage.add_dataset_transform_lineage(
        upstream=upstream,
        downstream=downstream,
        transformation_text=transformation_text,
        column_lineage=column_lineage,
    )
    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_complete_golden.json")


def test_infer_lineage_from_sql(client: DataHubClient) -> None:
    """Test adding lineage from SQL parsing with a golden file."""

    mock_result = SqlParsingResult(
        in_tables=["urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"],
        out_tables=[
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_summary,PROD)"
        ],
        query_type=QueryType.SELECT,
        debug_info=MagicMock(error=None, table_error=None),
    )

    query_text = (
        "create table sales_summary as SELECT price, qty, unit_cost FROM orders"
    )

    with patch(
        "datahub.sql_parsing.sqlglot_lineage.create_lineage_sql_parsed_result",
        return_value=mock_result,
    ):
        client.lineage.infer_lineage_from_sql(
            query_text=query_text, platform="snowflake", env="PROD"
        )

    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_from_sql_golden.json")


def test_infer_lineage_from_sql_with_multiple_upstreams(
    client: DataHubClient,
) -> None:
    """Test adding lineage for a dataset with multiple upstreams."""

    mock_result = SqlParsingResult(
        in_tables=[
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,products,PROD)",
        ],
        out_tables=[
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_summary,PROD)"
        ],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(
                    column="product_name",
                ),
                upstreams=[
                    ColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:snowflake,sales,PROD)",
                        column="product_name",
                    )
                ],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(
                    column="total_quantity",
                ),
                upstreams=[
                    ColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:snowflake,sales,PROD)",
                        column="quantity",
                    )
                ],
            ),
        ],
        query_type=QueryType.SELECT,
        debug_info=MagicMock(error=None, table_error=None),
    )

    query_text = """
    CREATE TABLE sales_summary AS
    SELECT 
        p.product_name,
        SUM(s.quantity) as total_quantity,
    FROM sales s
    JOIN products p ON s.product_id = p.id
    GROUP BY p.product_name
    """

    with patch(
        "datahub.sql_parsing.sqlglot_lineage.create_lineage_sql_parsed_result",
        return_value=mock_result,
    ):
        client.lineage.infer_lineage_from_sql(
            query_text=query_text, platform="snowflake", env="PROD"
        )

    # Validate against golden file
    assert_client_golden(
        client, _GOLDEN_DIR / "test_lineage_from_sql_multiple_upstreams_golden.json"
    )


def test_add_datajob_lineage(client: DataHubClient) -> None:
    """Test adding lineage for datajobs using DataJobPatchBuilder."""

    datajob_urn = (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),transform_job)"
    )
    input_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"
    )
    input_datajob_urn = (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),upstream_job)"
    )
    output_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,target_table,PROD)"
    )

    client.lineage.add_datajob_lineage(
        datajob=datajob_urn,
        upstreams=[input_dataset_urn, input_datajob_urn],
        downstreams=[output_dataset_urn],
    )

    assert_client_golden(client, _GOLDEN_DIR / "test_datajob_lineage_golden.json")


def test_add_datajob_inputs_only(client: DataHubClient) -> None:
    """Test adding only inputs to a datajob."""

    datajob_urn = (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),process_job)"
    )
    input_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"
    )

    client.lineage.add_datajob_lineage(
        datajob=datajob_urn,
        upstreams=[input_dataset_urn],
    )

    assert_client_golden(client, _GOLDEN_DIR / "test_datajob_inputs_only_golden.json")


def test_add_datajob_outputs_only(client: DataHubClient) -> None:
    """Test adding only outputs to a datajob."""

    datajob_urn = (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),transform_job)"
    )
    output_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,target_table,PROD)"
    )

    client.lineage.add_datajob_lineage(
        datajob=datajob_urn, downstreams=[output_dataset_urn]
    )

    # Validate lineage MCPs
    assert_client_golden(client, _GOLDEN_DIR / "test_datajob_outputs_only_golden.json")


def test_add_datajob_lineage_validation(client: DataHubClient) -> None:
    """Test validation checks in add_datajob_lineage."""

    datajob_urn = (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),transform_job)"
    )
    invalid_urn = "urn:li:glossaryNode:something"

    with pytest.raises(
        InvalidUrnError,
        match="Passed an urn of type glossaryNode to the from_string method of DataJobUrn",
    ):
        client.lineage.add_datajob_lineage(
            datajob=invalid_urn,
            upstreams=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"
            ],
        )

    with pytest.raises(InvalidUrnError):
        client.lineage.add_datajob_lineage(datajob=datajob_urn, upstreams=[invalid_urn])

    with pytest.raises(InvalidUrnError):
        client.lineage.add_datajob_lineage(
            datajob=datajob_urn, downstreams=[invalid_urn]
        )


def test_add_lineage_dataset_to_dataset_copy_basic(client: DataHubClient) -> None:
    """Test add_lineage method with dataset to dataset and various column lineage strategies."""
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"

    client.lineage.add_lineage(upstream=upstream, downstream=downstream)
    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_copy_basic_golden.json")


def test_add_lineage_dataset_to_dataset_copy_auto_fuzzy(client: DataHubClient) -> None:
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"

    upstream_schema = SchemaMetadataClass(
        schemaName="upstream_table",
        platform="urn:li:dataPlatform:snowflake",
        version=1,
        hash="1234567890",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
        ],
    )

    downstream_schema = SchemaMetadataClass(
        schemaName="downstream_table",
        platform="urn:li:dataPlatform:snowflake",
        version=1,
        hash="1234567890",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
        ],
    )

    # Auto fuzzy column lineage
    with patch.object(LineageClient, "_get_fields_from_dataset_urn") as mock_method:
        mock_method.side_effect = lambda urn: sorted(
            {
                field.fieldPath
                for field in (
                    upstream_schema if "upstream" in str(urn) else downstream_schema
                ).fields
            }
        )

        client.lineage.add_dataset_copy_lineage(
            upstream=upstream,
            downstream=downstream,
            column_lineage="auto_fuzzy",
        )

    assert_client_golden(
        client, _GOLDEN_DIR / "test_lineage_copy_auto_fuzzy_golden.json"
    )


def test_add_lineage_dataset_to_dataset_copy_auto_strict(client: DataHubClient) -> None:
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"

    upstream_schema = SchemaMetadataClass(
        schemaName="upstream_table",
        platform="urn:li:dataPlatform:snowflake",
        version=1,
        hash="1234567890",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
        ],
    )

    downstream_schema = SchemaMetadataClass(
        schemaName="downstream_table",
        platform="urn:li:dataPlatform:snowflake",
        version=1,
        hash="1234567890",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
        ],
    )
    # Auto strict column lineage
    with patch.object(LineageClient, "_get_fields_from_dataset_urn") as mock_method:
        mock_method.side_effect = lambda urn: sorted(
            {
                field.fieldPath
                for field in (
                    upstream_schema if "upstream" in str(urn) else downstream_schema
                ).fields
            }
        )

        client.lineage.add_dataset_copy_lineage(
            upstream=upstream,
            downstream=downstream,
            column_lineage="auto_strict",
        )

    assert_client_golden(
        client, _GOLDEN_DIR / "test_lineage_copy_auto_strict_golden.json"
    )


def test_add_lineage_dataset_to_dataset_copy_custom_mapping(
    client: DataHubClient,
) -> None:
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"

    column_mapping = {"name": ["name", "full_name"]}
    client.lineage.add_lineage(
        upstream=upstream, downstream=downstream, column_lineage_mapping=column_mapping
    )
    assert_client_golden(
        client, _GOLDEN_DIR / "test_lineage_custom_mapping_golden.json"
    )


def test_add_lineage_dataset_to_dataset_transform(client: DataHubClient) -> None:
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"
    transformation_text = (
        "SELECT user_id as userId, full_name as fullName FROM upstream_table"
    )
    column_mapping = {"userId": ["user_id"], "fullName": ["full_name"]}
    client.lineage.add_lineage(
        upstream=upstream,
        downstream=downstream,
        transformation_text=transformation_text,
        column_lineage_mapping=column_mapping,
    )
    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_transform_golden.json")


def test_add_lineage_datajob_as_downstream(client: DataHubClient) -> None:
    """Test add_lineage method with datajob as downstream."""
    upstream_dataset = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"
    )
    upstream_datajob = (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),process_job)"
    )
    downstream_datajob = (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),process_job)"
    )

    client.lineage.add_lineage(upstream=upstream_dataset, downstream=downstream_datajob)
    client.lineage.add_lineage(upstream=upstream_datajob, downstream=downstream_datajob)

    assert_client_golden(
        client, _GOLDEN_DIR / "test_lineage_datajob_as_downstream_golden.json"
    )


def test_add_lineage_dataset_as_downstream(client: DataHubClient) -> None:
    """Test add_lineage method with dataset as downstream."""
    upstream_dataset = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"
    )
    upstream_datajob = (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),process_job)"
    )
    downstream_dataset = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,target_table,PROD)"
    )

    client.lineage.add_lineage(upstream=upstream_dataset, downstream=downstream_dataset)
    client.lineage.add_lineage(upstream=upstream_datajob, downstream=downstream_dataset)

    assert_client_golden(
        client, _GOLDEN_DIR / "test_lineage_dataset_as_downstream_golden.json"
    )


def test_add_lineage_dashboard_as_downstream(client: DataHubClient) -> None:
    """Test add_lineage method with dashboard as downstream."""
    upstream_dataset = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"
    )
    upstream_chart = "urn:li:chart:(urn:li:dataPlatform:snowflake,chart_id)"
    upstream_dashboard = "urn:li:dashboard:(urn:li:dataPlatform:snowflake,dashboard_id)"
    downstream_dashboard = (
        "urn:li:dashboard:(urn:li:dataPlatform:snowflake,dashboard_id)"
    )

    client.lineage.add_lineage(
        upstream=upstream_dataset, downstream=downstream_dashboard
    )
    client.lineage.add_lineage(upstream=upstream_chart, downstream=downstream_dashboard)
    client.lineage.add_lineage(
        upstream=upstream_dashboard, downstream=downstream_dashboard
    )

    assert_client_golden(
        client, _GOLDEN_DIR / "test_lineage_dashboard_as_downstream_golden.json"
    )


def test_add_lineage_chart_as_downstream(client: DataHubClient) -> None:
    """Test add_lineage method with chart as downstream."""
    upstream_dataset = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"
    )
    downstream_chart = "urn:li:chart:(urn:li:dataPlatform:snowflake,chart_id)"

    client.lineage.add_lineage(upstream=upstream_dataset, downstream=downstream_chart)

    assert_client_golden(
        client, _GOLDEN_DIR / "test_lineage_chart_as_downstream_golden.json"
    )


def test_add_lineage_invalid_lineage_combination(client: DataHubClient) -> None:
    """Test add_lineage method with invalid upstream URN."""
    upstream_glossary_node = "urn:li:glossaryNode:something"
    downstream_dataset = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,target_table,PROD)"
    )
    upstream_dashboard = "urn:li:dashboard:(urn:li:dataPlatform:snowflake,dashboard_id)"
    downstream_chart = "urn:li:chart:(urn:li:dataPlatform:snowflake,chart_id)"
    downstream_datajob = (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),process_job)"
    )

    with pytest.raises(
        SdkUsageError,
        match="Unsupported entity type combination: glossaryNode -> dataset",
    ):
        client.lineage.add_lineage(
            upstream=upstream_glossary_node, downstream=downstream_dataset
        )

    with pytest.raises(
        SdkUsageError,
        match="Unsupported entity type combination: dashboard -> chart",
    ):
        client.lineage.add_lineage(
            upstream=upstream_dashboard, downstream=downstream_chart
        )

    with pytest.raises(
        SdkUsageError,
        match="Unsupported entity type combination: dashboard -> dataJob",
    ):
        client.lineage.add_lineage(
            upstream=upstream_dashboard, downstream=downstream_datajob
        )


def test_add_lineage_invalid_parameter_combinations(client: DataHubClient) -> None:
    """Test add_lineage method with invalid parameter combinations."""
    # Dataset to DataJob with column_lineage (not supported)
    with pytest.raises(
        SdkUsageError,
        match="Column lineage and query text are only applicable for dataset-to-dataset lineage",
    ):
        client.lineage.add_lineage(
            upstream="urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)",
            downstream="urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),process_job)",
            column_lineage_mapping={"target_col": ["source_col"]},
        )

    # Dataset to DataJob with transformation_text (not supported)
    with pytest.raises(
        SdkUsageError,
        match="Column lineage and query text are only applicable for dataset-to-dataset lineage",
    ):
        client.lineage.add_lineage(
            upstream="urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)",
            downstream="urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),process_job)",
            transformation_text="SELECT * FROM source_table",
        )

    # DataJob to Dataset with column_lineage (not supported)
    with pytest.raises(
        SdkUsageError,
        match="Column lineage and query text are only applicable for dataset-to-dataset lineage",
    ):
        client.lineage.add_lineage(
            upstream="urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),process_job)",
            downstream="urn:li:dataset:(urn:li:dataPlatform:snowflake,target_table,PROD)",
            column_lineage_mapping={"target_col": ["source_col"]},
        )
