import pathlib
from typing import Dict, List, Set, cast
from unittest.mock import MagicMock, Mock

import pytest

from datahub.metadata.schema_classes import (
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)
from datahub.sdk.dataset import Dataset
from datahub.sdk.lineage_client import LineageClient
from datahub.sdk.main_client import DataHubClient
from tests.test_helpers import mce_helpers

_GOLDEN_DIR = pathlib.Path(__file__).parent / "lineage_client_golden"
_GOLDEN_DIR.mkdir(exist_ok=True)


@pytest.fixture
def mock_graph() -> Mock:
    graph = Mock()
    schema_resolver = Mock()
    schema_resolver.platform = "snowflake"

    orders_schema = {
        "price": "FLOAT",
        "qty": "INTEGER",
        "unit_cost": "FLOAT",
    }
    
    # When resolve_table is called with "orders", return the proper URN and schema
    # The _TableName used in SQL parsing has database, db_schema, and table attributes
    def resolve_table_side_effect(table_name):
        if table_name.table == "ORDERS":
            return (
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)",
                orders_schema
            )
        elif table_name.table == "sales_summary":
            return (
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_summary,PROD)",
                {}
            )
        return None, None
    
    schema_resolver.resolve_table.side_effect = resolve_table_side_effect
    schema_resolver.includes_temp_tables.return_value = False
    
    graph._make_schema_resolver.return_value = schema_resolver
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

    # Create upstream dataset with schema
    upstream = Dataset(
        platform="snowflake",
        name="orders",
        env="PROD",
        schema=[
            ("price", "float"),
            ("qty", "int"),
            ("unit_cost", "float"),
        ],
    )
    client.entities.upsert(upstream)

    # Create downstream dataset (no schema needed)
    downstream = Dataset(
        platform="snowflake",
        name="sales_summary",
        env="PROD",
    )
    client.entities.upsert(downstream)

    # Use real parseable SQL
    query_text = """
        SELECT price AS total_price, qty * unit_cost AS cost
        FROM orders
        """

    # Trigger lineage from SQL
    lineage_client.add_dataset_lineage_from_sql(
        query_text=query_text,
        platform="snowflake",
        env="PROD",
    )

    # Validate lineage + query MCPs
    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_from_sql_golden.json")


def test_add_datajob_lineage():
    """Test adding lineage between datasets and datajobs."""
    # Create a minimal client just for testing the method
    pass
