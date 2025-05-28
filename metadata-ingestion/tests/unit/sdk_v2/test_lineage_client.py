import pathlib
from typing import Dict, List, Optional, Set, cast
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
    # Test cases
    test_cases = [
        # Case 1: Exact matches
        {
            "upstream_fields": {"id", "name", "email"},
            "downstream_fields": {"id", "name", "phone"},
            "expected": {"id": ["id"], "name": ["name"]},
        },
        # Case 2: Case insensitive matches
        {
            "upstream_fields": {"ID", "Name", "Email"},
            "downstream_fields": {"id", "name", "phone"},
            "expected": {"id": ["ID"], "name": ["Name"]},
        },
        # Case 3: Camel case to snake case
        {
            "upstream_fields": {"id", "user_id", "full_name"},
            "downstream_fields": {"id", "userId", "fullName"},
            "expected": {
                "id": ["id"],
                "userId": ["user_id"],
                "fullName": ["full_name"],
            },
        },
        # Case 4: Snake case to camel case
        {
            "upstream_fields": {"id", "userId", "fullName"},
            "downstream_fields": {"id", "user_id", "full_name"},
            "expected": {
                "id": ["id"],
                "user_id": ["userId"],
                "full_name": ["fullName"],
            },
        },
        # Case 5: Mixed matches
        {
            "upstream_fields": {"id", "customer_id", "user_name"},
            "downstream_fields": {
                "id",
                "customerId",
                "address",
            },
            "expected": {"id": ["id"], "customerId": ["customer_id"]},
        },
        # Case 6: Mixed matches with different casing
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
    for i, test_case in enumerate(test_cases):
        result = client.lineage._get_fuzzy_column_lineage(
            cast(Set[str], test_case["upstream_fields"]),
            cast(Set[str], test_case["downstream_fields"]),
        )
        assert result == test_case["expected"], (
            f"Test case {i + 1} failed: {result} != {test_case['expected']}"
        )


def test_get_strict_column_lineage(client: DataHubClient) -> None:
    """Test the strict column lineage matching algorithm."""
    # Create a minimal client just for testing the method

    # Define test cases
    test_cases = [
        # Case 1: Exact matches
        {
            "upstream_fields": {"id", "name", "email"},
            "downstream_fields": {"id", "name", "phone"},
            "expected": {"id": ["id"], "name": ["name"]},
        },
        # Case 2: No matches
        {
            "upstream_fields": {"col1", "col2", "col3"},
            "downstream_fields": {"col4", "col5", "col6"},
            "expected": {},
        },
        # Case 3: Case mismatch (should match)
        {
            "upstream_fields": {"ID", "Name", "Email"},
            "downstream_fields": {"id", "name", "email"},
            "expected": {"id": ["ID"], "name": ["Name"], "email": ["Email"]},
        },
    ]

    # Run test cases
    for i, test_case in enumerate(test_cases):
        result = client.lineage._get_strict_column_lineage(
            cast(Set[str], test_case["upstream_fields"]),
            cast(Set[str], test_case["downstream_fields"]),
        )
        assert result == test_case["expected"], f"Test case {i + 1} failed"


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

    # Use patch.object with a context manager
    with patch.object(LineageClient, "_get_fields_from_dataset_urn") as mock_method:
        # Configure the mock with a simpler side effect function
        mock_method.side_effect = lambda urn: sorted(
            {
                field.fieldPath
                for field in (
                    upstream_schema if "upstream" in str(urn) else downstream_schema
                ).fields
            }
        )

        # Now use client.lineage with the patched method
        client.lineage.add_dataset_copy_lineage(
            upstream=upstream,
            downstream=downstream,
            column_lineage="auto_fuzzy",
        )

    # Use golden file for assertion
    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_copy_fuzzy_golden.json")


def test_add_dataset_copy_lineage_auto_strict(client: DataHubClient) -> None:
    """Test strict column lineage with field matches."""
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

        # Run the lineage function
        client.lineage.add_dataset_copy_lineage(
            upstream=upstream,
            downstream=downstream,
            column_lineage="auto_strict",
        )

    # Use golden file for assertion
    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_copy_strict_golden.json")


def test_add_dataset_transform_lineage_basic(client: DataHubClient) -> None:
    """Test basic lineage without column mapping or query."""

    # Basic lineage test
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
    query_text = (
        "SELECT us_col1 as ds_col1, us_col2 + us_col3 as ds_col2 FROM upstream_table"
    )
    column_lineage: Dict[str, List[str]] = {
        "ds_col1": ["us_col1"],  # Simple 1:1 mapping
        "ds_col2": ["us_col2", "us_col3"],  # 2:1 mapping
    }

    client.lineage.add_dataset_transform_lineage(
        upstream=upstream,
        downstream=downstream,
        query_text=query_text,
        column_lineage=column_lineage,
    )
    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_complete_golden.json")


def test_add_dataset_lineage_from_sql(client: DataHubClient) -> None:
    """Test adding lineage from SQL parsing with a golden file."""

    # Create minimal mock result with necessary info
    mock_result = SqlParsingResult(
        in_tables=["urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"],
        out_tables=[
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_summary,PROD)"
        ],
        column_lineage=[],  # Simplified - we only care about table-level lineage for this test
        query_type=QueryType.SELECT,
        debug_info=MagicMock(error=None, table_error=None),
    )

    # Simple SQL that would produce the expected lineage
    query_text = (
        "create table sales_summary as SELECT price, qty, unit_cost FROM orders"
    )

    # Patch SQL parser and execute lineage creation
    with patch(
        "datahub.sql_parsing.sqlglot_lineage.create_lineage_sql_parsed_result",
        return_value=mock_result,
    ):
        client.lineage.add_dataset_lineage_from_sql(
            query_text=query_text, platform="snowflake", env="PROD"
        )

    # Validate against golden file
    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_from_sql_golden.json")


def test_add_dataset_lineage_from_sql_with_multiple_upstreams(
    client: DataHubClient,
) -> None:
    """Test adding lineage for a dataset with multiple upstreams."""

    # Create minimal mock result with necessary info
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

    # Simple SQL that would produce the expected lineage
    query_text = """
    CREATE TABLE sales_summary AS
    SELECT 
        p.product_name,
        SUM(s.quantity) as total_quantity,
    FROM sales s
    JOIN products p ON s.product_id = p.id
    GROUP BY p.product_name
    """

    # Patch SQL parser and execute lineage creation
    with patch(
        "datahub.sql_parsing.sqlglot_lineage.create_lineage_sql_parsed_result",
        return_value=mock_result,
    ):
        client.lineage.add_dataset_lineage_from_sql(
            query_text=query_text, platform="snowflake", env="PROD"
        )

    # Validate against golden file
    assert_client_golden(
        client, _GOLDEN_DIR / "test_lineage_from_sql_multiple_upstreams_golden.json"
    )


def test_add_datajob_lineage(client: DataHubClient) -> None:
    """Test adding lineage for datajobs using DataJobPatchBuilder."""

    # Define URNs for test with correct format
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

    # Test adding both upstream and downstream connections
    client.lineage.add_datajob_lineage(
        datajob=datajob_urn,
        upstreams=[input_dataset_urn, input_datajob_urn],
        downstreams=[output_dataset_urn],
    )

    # Validate lineage MCPs against golden file
    assert_client_golden(client, _GOLDEN_DIR / "test_datajob_lineage_golden.json")


def test_add_datajob_inputs_only(client: DataHubClient) -> None:
    """Test adding only inputs to a datajob."""

    # Define URNs for test
    datajob_urn = (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),process_job)"
    )
    input_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"
    )

    # Test adding just upstream connections
    client.lineage.add_datajob_lineage(
        datajob=datajob_urn,
        upstreams=[input_dataset_urn],
    )

    # Validate lineage MCPs
    assert_client_golden(client, _GOLDEN_DIR / "test_datajob_inputs_only_golden.json")


def test_add_datajob_outputs_only(client: DataHubClient) -> None:
    """Test adding only outputs to a datajob."""

    # Define URNs for test
    datajob_urn = (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),transform_job)"
    )
    output_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,target_table,PROD)"
    )

    # Test adding just downstream connections
    client.lineage.add_datajob_lineage(
        datajob=datajob_urn, downstreams=[output_dataset_urn]
    )

    # Validate lineage MCPs
    assert_client_golden(client, _GOLDEN_DIR / "test_datajob_outputs_only_golden.json")


def test_add_datajob_lineage_validation(client: DataHubClient) -> None:
    """Test validation checks in add_datajob_lineage."""

    # Define URNs for test
    datajob_urn = (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),transform_job)"
    )
    invalid_urn = "urn:li:glossaryNode:something"

    # Test with invalid datajob URN
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

    # Test with invalid upstream URN
    with pytest.raises(InvalidUrnError):
        client.lineage.add_datajob_lineage(datajob=datajob_urn, upstreams=[invalid_urn])

    # Test with invalid downstream URN
    with pytest.raises(InvalidUrnError):
        client.lineage.add_datajob_lineage(
            datajob=datajob_urn, downstreams=[invalid_urn]
        )


def test_add_lineage_dataset_to_dataset_copy_basic(client: DataHubClient) -> None:
    """Test add_lineage method with dataset to dataset and various column lineage strategies."""
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"

    # Test case 1: Basic dataset to dataset copy lineage (no column mapping)
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

    # Test case 2: Auto fuzzy column lineage
    with patch.object(LineageClient, "_get_fields_from_dataset_urn") as mock_method:
        # Configure the mock with a simpler side effect function
        mock_method.side_effect = lambda urn: sorted(
            {
                field.fieldPath
                for field in (
                    upstream_schema if "upstream" in str(urn) else downstream_schema
                ).fields
            }
        )

        # Now use client.lineage with the patched method
        client.lineage.add_dataset_copy_lineage(
            upstream=upstream,
            downstream=downstream,
            column_lineage="auto_fuzzy",
        )

    # Use golden file for assertion
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
    # Test case 3: Auto strict column lineage
    with patch.object(LineageClient, "_get_fields_from_dataset_urn") as mock_method:
        # Configure the mock with a simpler side effect function
        mock_method.side_effect = lambda urn: sorted(
            {
                field.fieldPath
                for field in (
                    upstream_schema if "upstream" in str(urn) else downstream_schema
                ).fields
            }
        )

        # Run the lineage function
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

    # Test case 4: Custom column mapping
    column_mapping = {"Age": ["name", "customer_id"]}
    client.lineage.add_lineage(
        upstream=upstream, downstream=downstream, column_lineage=column_mapping
    )
    assert_client_golden(
        client, _GOLDEN_DIR / "test_lineage_custom_mapping_golden.json"
    )


def test_add_lineage_dataset_to_dataset_transform(client: DataHubClient) -> None:
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"
    query_text = "SELECT user_id as userId, full_name as fullName FROM upstream_table"
    column_mapping = {"userId": ["user_id"], "fullName": ["full_name"]}
    # Test case 5: Dataset to dataset with transformation and column mapping
    client.lineage.add_lineage(
        upstream=upstream,
        downstream=downstream,
        query_text=query_text,
        column_lineage=column_mapping,
    )
    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_transform_golden.json")


def test_add_lineage_dataset_to_datajob(client: DataHubClient) -> None:
    """Test add_lineage method with dataset to datajob."""
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"
    downstream = (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),process_job)"
    )

    # Call the method
    client.lineage.add_lineage(upstream=upstream, downstream=downstream)

    # Validate lineage MCPs against golden file
    assert_client_golden(
        client, _GOLDEN_DIR / "test_lineage_dataset_to_datajob_golden.json"
    )


def test_add_lineage_datajob_to_dataset(client: DataHubClient) -> None:
    """Test add_lineage method with datajob to dataset."""
    upstream = "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),process_job)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,target_table,PROD)"

    # Call the method
    client.lineage.add_lineage(upstream=upstream, downstream=downstream)

    # Validate lineage MCPs against golden file
    assert_client_golden(
        client, _GOLDEN_DIR / "test_lineage_datajob_to_dataset_golden.json"
    )


def test_add_lineage_datajob_to_datajob(client: DataHubClient) -> None:
    """Test add_lineage method with datajob to datajob."""
    upstream = (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),upstream_job)"
    )
    downstream = (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),downstream_job)"
    )

    # Call the method
    client.lineage.add_lineage(upstream=upstream, downstream=downstream)

    # Validate lineage MCPs against golden file
    assert_client_golden(
        client, _GOLDEN_DIR / "test_lineage_datajob_to_datajob_golden.json"
    )


def test_add_lineage_invalid_upstream(client: DataHubClient) -> None:
    """Test add_lineage method with invalid upstream URN."""
    upstream = "urn:li:glossaryNode:something"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,target_table,PROD)"

    # Should raise an InvalidUrnError
    with pytest.raises(
        SdkUsageError,
        match="Unsupported entity type combination: glossaryNode -> dataset",
    ):
        client.lineage.add_lineage(upstream=upstream, downstream=downstream)


def test_add_lineage_invalid_downstream(client: DataHubClient) -> None:
    """Test add_lineage method with invalid downstream URN."""
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"
    downstream = "urn:li:glossaryNode:something"

    # Should raise an InvalidUrnError
    with pytest.raises(
        SdkUsageError,
        match="Unsupported entity type combination: dataset -> glossaryNode",
    ):
        client.lineage.add_lineage(upstream=upstream, downstream=downstream)


def test_add_lineage_invalid_parameter_combinations(client: DataHubClient) -> None:
    """Test add_lineage method with invalid parameter combinations."""
    # Dataset to DataJob with column_lineage (not supported)
    with pytest.raises(SdkUsageError):
        client.lineage.add_lineage(
            upstream="urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)",
            downstream="urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),process_job)",
            column_lineage={"target_col": ["source_col"]},
        )

    # Dataset to DataJob with query_text (not supported)
    with pytest.raises(SdkUsageError):
        client.lineage.add_lineage(
            upstream="urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)",
            downstream="urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),process_job)",
            query_text="SELECT * FROM source_table",
        )

    # DataJob to Dataset with column_lineage (not supported)
    with pytest.raises(SdkUsageError):
        client.lineage.add_lineage(
            upstream="urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),process_job)",
            downstream="urn:li:dataset:(urn:li:dataPlatform:snowflake,target_table,PROD)",
            column_lineage={"target_col": ["source_col"]},
        )


def test_get_lineage_basic(client: DataHubClient) -> None:
    """Test basic lineage retrieval with default parameters."""
    source_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"

    # Mock GraphQL response
    mock_response = {
        "scrollAcrossLineage": {
            "nextScrollId": None,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)",
                        "type": "DATASET",
                        "platform": {"name": "snowflake"},
                        "properties": {
                            "name": "upstream_table",
                            "description": "Upstream source table",
                        },
                    },
                    "degree": 1,
                }
            ],
        }
    }

    # Patch the GraphQL execution method
    with patch.object(client._graph, "execute_graphql", return_value=mock_response):
        results = client.lineage.get_lineage(source_urn=source_urn)

    # Validate results
    assert len(results) == 1
    assert (
        results[0]["urn"]
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    )
    assert results[0]["type"] == "DATASET"
    assert results[0]["name"] == "upstream_table"
    assert results[0]["platform"] == "snowflake"
    assert results[0]["direction"] == "upstream"
    assert results[0]["hops"] == 1


def test_get_lineage_with_entity_type_filters(client: DataHubClient) -> None:
    """Test lineage retrieval with entity type and platform filters."""
    source_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"

    # Mock GraphQL response with multiple entity types
    mock_response = {
        "scrollAcrossLineage": {
            "nextScrollId": None,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)",
                        "type": "DATASET",
                        "platform": {"name": "snowflake"},
                        "properties": {
                            "name": "upstream_table",
                            "description": "Upstream source table",
                        },
                    },
                    "degree":1,
                },
            ],
        }
    }

    # Patch the GraphQL execution method to return results for multiple calls
    with patch.object(client._graph, "execute_graphql", return_value=mock_response):
        results = client.lineage.get_lineage(
            source_urn=source_urn,
            filters={
                "entity_type": ["DATASET"],
            },
        )

    # Validate results
    assert len(results) == 1
    assert {r["type"] for r in results} == {"DATASET"}
    assert {r["platform"] for r in results} == {"snowflake"}


def test_get_lineage_downstream(client: DataHubClient) -> None:
    """Test downstream lineage retrieval."""
    source_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"

    # Mock GraphQL response for downstream lineage
    mock_response = {
        "scrollAcrossLineage": {
            "nextScrollId": None,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)",
                        "type": "DATASET",
                        "properties": {
                            "name": "downstream_table",
                            "description": "Downstream target table",
                            "platform": {"name": "snowflake"},
                        },
                    },
                    "degree": 1,
                }
            ],
        }
    }

    # Patch the GraphQL execution method
    with patch.object(client._graph, "execute_graphql", return_value=mock_response):
        results = client.lineage.get_lineage(
            source_urn=source_urn, direction="downstream"
        )

    # Validate results
    assert len(results) == 1
    assert (
        results[0]["urn"]
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"
    )
    assert results[0]["direction"] == "downstream"


def test_get_lineage_multiple_hops(client: DataHubClient) -> None:
    """Test lineage retrieval with multiple hops."""
    source_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"

    # Mock GraphQL response with multiple hops
    mock_response = {
        "scrollAcrossLineage": {
            "nextScrollId": None,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table1,PROD)",
                        "type": "DATASET",
                        "properties": {
                            "name": "upstream_table1",
                            "description": "First upstream table",
                            "platform": {"name": "snowflake"},
                        },
                    },
                    "degree": 1,
                },
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table2,PROD)",
                        "type": "DATASET",
                        "properties": {
                            "name": "upstream_table2",
                            "description": "Second upstream table",
                            "platform": {"name": "snowflake"},
                        },
                    },
                    "degree": 2,
                },
            ],
        }
    }

    # Patch the GraphQL execution method
    with patch.object(client._graph, "execute_graphql", return_value=mock_response):
        results = client.lineage.get_lineage(source_urn=source_urn, max_hops=2)

    # Validate results
    assert len(results) == 2
    assert results[0]["hops"] == 1
    assert results[0]["type"] == "DATASET"
    assert results[1]["hops"] == 2
    assert results[1]["type"] == "DATASET"


def test_get_lineage_no_results(client: DataHubClient) -> None:
    """Test lineage retrieval with no results."""
    source_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"

    # Mock GraphQL response with no results
    mock_response: Dict[str, Dict[str, Optional[List]]] = {
        "scrollAcrossLineage": {"nextScrollId": None, "searchResults": []}
    }

    # Patch the GraphQL execution method
    with patch.object(client._graph, "execute_graphql", return_value=mock_response):
        results = client.lineage.get_lineage(source_urn=source_urn)

    # Validate results
    assert len(results) == 0


def test_get_lineage_pagination(client: DataHubClient) -> None:
    """Test lineage retrieval with pagination."""
    source_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"

    # Mock GraphQL responses with pagination
    mock_responses = [
        {
            "scrollAcrossLineage": {
                "nextScrollId": "scroll_token_1",
                "searchResults": [
                    {
                        "entity": {
                            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table1,PROD)",
                            "type": "DATASET",
                            "properties": {
                                "name": "upstream_table1",
                                "platform": {"name": "snowflake"},
                            },
                        },
                        "degree": 1,
                    }
                ],
            }
        },
        {
            "scrollAcrossLineage": {
                "nextScrollId": None,
                "searchResults": [
                    {
                        "entity": {
                            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table2,PROD)",
                            "type": "DATASET",
                            "properties": {
                                "name": "upstream_table2",
                                "platform": {"name": "snowflake"},
                            },
                        },
                        "degree": 1,
                    }
                ],
            }
        },
    ]

    # Patch the GraphQL execution method to simulate pagination
    with patch.object(client._graph, "execute_graphql", side_effect=mock_responses):
        results = client.lineage.get_lineage(source_urn=source_urn)

    # Validate results
    assert len(results) == 2
    assert {result["name"] for result in results} == {
        "upstream_table1",
        "upstream_table2",
    }
