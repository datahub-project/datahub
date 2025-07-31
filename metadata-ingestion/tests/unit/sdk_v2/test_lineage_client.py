import pathlib
from typing import Dict, List, Optional, Sequence, Set, cast
from unittest.mock import Mock, patch

import pytest

from datahub.errors import SdkUsageError
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_filters import FilterDsl as F
from datahub.sql_parsing.sql_parsing_common import QueryType
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
    SqlParsingResult,
)
from datahub.testing import mce_helpers

_GOLDEN_DIR = pathlib.Path(__file__).parent / "lineage_client_golden"
_GOLDEN_DIR.mkdir(exist_ok=True)


@pytest.fixture
def mock_graph() -> Mock:
    graph = Mock()

    return graph


@pytest.fixture
def client(mock_graph: Mock) -> DataHubClient:
    return DataHubClient(graph=mock_graph)


def assert_client_golden(
    client: DataHubClient, golden_path: pathlib.Path, ignore_paths: Sequence[str] = ()
) -> None:
    mcps = client._graph.emit_mcps.call_args[0][0]  # type: ignore

    mce_helpers.check_goldens_stream(
        outputs=mcps,
        golden_path=golden_path,
        ignore_order=False,
        ignore_paths=ignore_paths,
    )


@pytest.mark.parametrize(
    "upstream_fields, downstream_fields, expected",
    [
        # Exact matches
        (
            {"id", "name", "email"},
            {"id", "name", "phone"},
            {"id": ["id"], "name": ["name"]},
        ),
        # Case insensitive matches
        (
            {"ID", "Name", "Email"},
            {"id", "name", "phone"},
            {"id": ["ID"], "name": ["Name"]},
        ),
        # Camel case to snake case
        (
            {"id", "user_id", "full_name"},
            {"id", "userId", "fullName"},
            {"id": ["id"], "userId": ["user_id"], "fullName": ["full_name"]},
        ),
        # Snake case to camel case
        (
            {"id", "userId", "fullName"},
            {"id", "user_id", "full_name"},
            {"id": ["id"], "user_id": ["userId"], "full_name": ["fullName"]},
        ),
        # Mixed matches
        (
            {"id", "customer_id", "user_name"},
            {"id", "customerId", "address"},
            {"id": ["id"], "customerId": ["customer_id"]},
        ),
        # Mixed matches with different casing
        (
            {"id", "customer_id", "userName", "address_id"},
            {"id", "customerId", "user_name", "user_address"},
            {"id": ["id"], "customerId": ["customer_id"], "user_name": ["userName"]},
        ),
    ],
)
def test_get_fuzzy_column_lineage(
    client: DataHubClient,
    upstream_fields: Set[str],
    downstream_fields: Set[str],
    expected: Dict[str, List[str]],
) -> None:
    result = client.lineage._get_fuzzy_column_lineage(
        cast(Set[str], upstream_fields),
        cast(Set[str], downstream_fields),
    )
    assert result == expected, f"Test failed: {result} != {expected}"


@pytest.mark.parametrize(
    "upstream_fields, downstream_fields, expected",
    [
        # Exact matches
        (
            {"id", "name", "email"},
            {"id", "name", "phone"},
            {"id": ["id"], "name": ["name"]},
        ),
        # No matches
        ({"col1", "col2", "col3"}, {"col4", "col5", "col6"}, {}),
        # Case mismatch (should match)
        (
            {"ID", "Name", "Email"},
            {"id", "name", "email"},
            {"id": ["ID"], "name": ["Name"], "email": ["Email"]},
        ),
    ],
)
def test_get_strict_column_lineage(
    client: DataHubClient,
    upstream_fields: Set[str],
    downstream_fields: Set[str],
    expected: Dict[str, List[str]],
) -> None:
    result = client.lineage._get_strict_column_lineage(
        cast(Set[str], upstream_fields),
        cast(Set[str], downstream_fields),
    )
    """Test the strict column lineage matching algorithm."""
    assert result == expected, f"Test failed: {result} != {expected}"


def test_infer_lineage_from_sql(client: DataHubClient) -> None:
    """Test adding lineage from SQL parsing with a golden file."""

    mock_result = SqlParsingResult(
        in_tables=["urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"],
        out_tables=[
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_summary,PROD)"
        ],
        query_type=QueryType.SELECT,
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


def test_add_lineage_dataset_to_dataset_copy_basic(client: DataHubClient) -> None:
    """Test add_lineage method with dataset to dataset and various column lineage strategies."""
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"

    client.lineage.add_lineage(upstream=upstream, downstream=downstream)
    assert_client_golden(client, _GOLDEN_DIR / "test_lineage_copy_basic_golden.json")


def test_add_lineage_dataset_to_dataset_copy_custom_mapping(
    client: DataHubClient,
) -> None:
    upstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"

    column_mapping = {"name": ["name", "full_name"]}
    client.lineage.add_lineage(
        upstream=upstream, downstream=downstream, column_lineage=column_mapping
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
        column_lineage=column_mapping,
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
        client,
        _GOLDEN_DIR / "test_lineage_dashboard_as_downstream_golden.json",
        ["time"],
    )


def test_add_lineage_chart_as_downstream(client: DataHubClient) -> None:
    """Test add_lineage method with chart as downstream."""
    upstream_dataset = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"
    )
    downstream_chart = "urn:li:chart:(urn:li:dataPlatform:snowflake,chart_id)"

    client.lineage.add_lineage(upstream=upstream_dataset, downstream=downstream_chart)

    assert_client_golden(
        client, _GOLDEN_DIR / "test_lineage_chart_as_downstream_golden.json", ["time"]
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
            column_lineage={"target_col": ["source_col"]},
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
        results[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    )
    assert results[0].type == "DATASET"
    assert results[0].name == "upstream_table"
    assert results[0].platform == "snowflake"
    assert results[0].direction == "upstream"
    assert results[0].hops == 1


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
                    "degree": 1,
                },
            ],
        }
    }

    # Patch the GraphQL execution method to return results for multiple calls
    with patch.object(client._graph, "execute_graphql", return_value=mock_response):
        results = client.lineage.get_lineage(
            source_urn=source_urn,
            filter=F.entity_type("dataset"),
        )

    # Validate results
    assert len(results) == 1
    assert {r.type for r in results} == {"DATASET"}
    assert {r.platform for r in results} == {"snowflake"}


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
            source_urn=source_urn,
            direction="downstream",
        )

    # Validate results
    assert len(results) == 1
    assert (
        results[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"
    )
    assert results[0].direction == "downstream"


def test_get_lineage_multiple_hops(
    client: DataHubClient, caplog: pytest.LogCaptureFixture
) -> None:
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

    # check warning if logged when max_hops > 2
    with (
        patch.object(client._graph, "execute_graphql", return_value=mock_response),
        caplog.at_level("WARNING"),
    ):
        client.lineage.get_lineage(source_urn=source_urn, max_hops=3)
        assert any(
            "the search will try to find the full lineage graph" in msg
            for msg in caplog.messages
        )

    # Validate results
    assert len(results) == 2
    assert results[0].hops == 1
    assert results[0].type == "DATASET"
    assert results[1].hops == 2
    assert results[1].type == "DATASET"


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


def test_get_lineage_column_lineage_with_source_column(client: DataHubClient) -> None:
    """Test lineage retrieval with column lineage."""
    source_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD)"
    source_column = "source_column"

    # Mock GraphQL response with column lineage
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
                },
            ],
        }
    }

    # Patch the GraphQL execution method
    with patch.object(client._graph, "execute_graphql", return_value=mock_response):
        results = client.lineage.get_lineage(
            source_urn=source_urn,
            source_column=source_column,
        )

    # Validate results
    assert len(results) == 1
    assert (
        results[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    )


def test_get_lineage_column_lineage_with_schema_field_urn(
    client: DataHubClient,
) -> None:
    """Test lineage retrieval with column lineage."""
    source_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,source_table,PROD),source_column)"

    # Mock GraphQL response with column lineage
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
                },
            ],
        }
    }

    # Patch the GraphQL execution method
    with patch.object(client._graph, "execute_graphql", return_value=mock_response):
        results = client.lineage.get_lineage(
            source_urn=source_urn,
        )

    # Validate results
    assert len(results) == 1
    assert (
        results[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
    )
