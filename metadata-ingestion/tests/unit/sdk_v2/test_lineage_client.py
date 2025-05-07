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
from tests.test_helpers import mce_helpers

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


def test_get_fuzzy_column_lineage():
    """Test the fuzzy column lineage matching algorithm."""
    # Create a minimal client just for testing the method
    client = MagicMock(spec=DataHubClient)
    lineage_client = LineageClient(client=client)

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
        result = lineage_client._get_fuzzy_column_lineage(
            cast(Set[str], test_case["upstream_fields"]),
            cast(Set[str], test_case["downstream_fields"]),
        )
        assert result == test_case["expected"], (
            f"Test case {i + 1} failed: {result} != {test_case['expected']}"
        )


def test_get_strict_column_lineage():
    """Test the strict column lineage matching algorithm."""
    # Create a minimal client just for testing the method
    client = MagicMock(spec=DataHubClient)
    lineage_client = LineageClient(client=client)

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
        result = lineage_client._get_strict_column_lineage(
            cast(Set[str], test_case["upstream_fields"]),
            cast(Set[str], test_case["downstream_fields"]),
        )
        assert result == test_case["expected"], f"Test case {i + 1} failed"


def test_add_dataset_copy_lineage_auto_fuzzy(client: DataHubClient) -> None:
    """Test auto fuzzy column lineage mapping."""
    lineage_client = LineageClient(client=client)

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

    # Mock the _get_fields_from_dataset_urn method to return our test fields
    lineage_client._get_fields_from_dataset_urn = MagicMock()  # type: ignore
    lineage_client._get_fields_from_dataset_urn.side_effect = lambda urn: sorted(
        {  # type: ignore
            field.fieldPath
            for field in (
                upstream_schema if "upstream" in str(urn) else downstream_schema
            ).fields
        }
    )

    # Run the lineage function
    lineage_client.add_dataset_copy_lineage(
        upstream=upstream,
        downstream=downstream,
        column_lineage="auto_fuzzy",
    )

    # Use golden file for assertion
    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_copy_fuzzy_golden.json")


def test_add_dataset_copy_lineage_auto_strict(client: DataHubClient) -> None:
    """Test strict column lineage with field matches."""
    lineage_client = LineageClient(client=client)

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

    # Mock the _get_fields_from_dataset_urn method to return our test fields
    lineage_client._get_fields_from_dataset_urn = MagicMock()  # type: ignore
    lineage_client._get_fields_from_dataset_urn.side_effect = lambda urn: sorted(
        {  # type: ignore
            field.fieldPath
            for field in (
                upstream_schema if "upstream" in str(urn) else downstream_schema
            ).fields
        }
    )

    # Run the lineage function
    lineage_client.add_dataset_copy_lineage(
        upstream=upstream,
        downstream=downstream,
        column_lineage="auto_strict",
    )

    # Mock the _get_fields_from_dataset_urn method to return our test fields
    lineage_client._get_fields_from_dataset_urn = MagicMock()  # type: ignore
    lineage_client._get_fields_from_dataset_urn.side_effect = lambda urn: sorted(
        {  # type: ignore
            field.fieldPath
            for field in (
                upstream_schema if "upstream" in str(urn) else downstream_schema
            ).fields
        }
    )

    # Run the lineage function
    lineage_client.add_dataset_copy_lineage(
        upstream=upstream,
        downstream=downstream,
        column_lineage="auto_strict",
    )

    # Use golden file for assertion
    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_copy_strict_golden.json")


def test_add_dataset_transform_lineage_basic(client: DataHubClient) -> None:
    """Test basic lineage without column mapping or query."""
    lineage_client = LineageClient(client=client)

    # Basic lineage test
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"

    lineage_client.add_dataset_transform_lineage(
        upstream=upstream,
        downstream=downstream,
    )
    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_basic_golden.json")


def test_add_dataset_transform_lineage_complete(client: DataHubClient) -> None:
    """Test complete lineage with column mapping and query."""
    lineage_client = LineageClient(client=client)

    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"
    query_text = (
        "SELECT us_col1 as ds_col1, us_col2 + us_col3 as ds_col2 FROM upstream_table"
    )
    column_lineage: Dict[str, List[str]] = {
        "ds_col1": ["us_col1"],  # Simple 1:1 mapping
        "ds_col2": ["us_col2", "us_col3"],  # 2:1 mapping
    }

    lineage_client.add_dataset_transform_lineage(
        upstream=upstream,
        downstream=downstream,
        query_text=query_text,
        column_lineage=column_lineage,
    )
    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_complete_golden.json")


def test_add_dataset_lineage_from_sql(client: DataHubClient) -> None:
    """Test adding lineage from SQL parsing with a golden file."""

    lineage_client = LineageClient(client=client)

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
        lineage_client.add_dataset_lineage_from_sql(
            query_text=query_text, platform="snowflake", env="PROD"
        )

    # Validate against golden file
    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_from_sql_golden.json")


def test_add_dataset_lineage_from_sql_with_multiple_upstreams(
    client: DataHubClient,
) -> None:
    """Test adding lineage for a dataset with multiple upstreams."""
    lineage_client = LineageClient(client=client)

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
        lineage_client.add_dataset_lineage_from_sql(
            query_text=query_text, platform="snowflake", env="PROD"
        )

    # Validate against golden file
    assert_client_golden(
        client, _GOLDEN_DIR / "test_lineage_from_sql_multiple_upstreams_golden.json"
    )


def test_add_datajob_lineage(client: DataHubClient) -> None:
    """Test adding lineage for datajobs using DataJobPatchBuilder."""
    lineage_client = LineageClient(client=client)

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
    lineage_client.add_datajob_lineage(
        datajob=datajob_urn,
        upstreams=[input_dataset_urn, input_datajob_urn],
        downstreams=[output_dataset_urn],
    )

    # Validate lineage MCPs against golden file
    assert_client_golden(client, _GOLDEN_DIR / "test_datajob_lineage_golden.json")


def test_add_datajob_inputs_only(client: DataHubClient) -> None:
    """Test adding only inputs to a datajob."""
    lineage_client = LineageClient(client=client)

    # Define URNs for test
    datajob_urn = (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),process_job)"
    )
    input_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"
    )

    # Test adding just upstream connections
    lineage_client.add_datajob_lineage(
        datajob=datajob_urn,
        upstreams=[input_dataset_urn],
    )

    # Validate lineage MCPs
    assert_client_golden(client, _GOLDEN_DIR / "test_datajob_inputs_only_golden.json")


def test_add_datajob_outputs_only(client: DataHubClient) -> None:
    """Test adding only outputs to a datajob."""
    lineage_client = LineageClient(client=client)

    # Define URNs for test
    datajob_urn = (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),transform_job)"
    )
    output_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,target_table,PROD)"
    )

    # Test adding just downstream connections
    lineage_client.add_datajob_lineage(
        datajob=datajob_urn, downstreams=[output_dataset_urn]
    )

    # Validate lineage MCPs
    assert_client_golden(client, _GOLDEN_DIR / "test_datajob_outputs_only_golden.json")


def test_add_datajob_lineage_validation(client: DataHubClient) -> None:
    """Test validation checks in add_datajob_lineage."""
    lineage_client = LineageClient(client=client)

    # Define URNs for test
    datajob_urn = (
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),transform_job)"
    )
    invalid_urn = "urn:li:glossaryNode:something"

    # Test with invalid datajob URN
    with pytest.raises(
        SdkUsageError, match=r"The datajob parameter must be a DataJob URN"
    ):
        lineage_client.add_datajob_lineage(
            datajob=invalid_urn,
            upstreams=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"
            ],
        )

    # Test with invalid upstream URN
    with pytest.raises(
        SdkUsageError, match=r"Upstream URN .* must be either a dataset or datajob URN"
    ):
        lineage_client.add_datajob_lineage(datajob=datajob_urn, upstreams=[invalid_urn])

    # Test with invalid downstream URN
    with pytest.raises(SdkUsageError, match=r"Downstream URN .* must be a dataset URN"):
        lineage_client.add_datajob_lineage(
            datajob=datajob_urn, downstreams=[invalid_urn]
        )
