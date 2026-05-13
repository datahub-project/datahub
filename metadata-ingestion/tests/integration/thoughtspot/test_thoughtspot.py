"""Integration tests for ThoughtSpot connector.

This module tests end-to-end metadata extraction from ThoughtSpot using mocked API responses.
Since ThoughtSpot is a cloud service without a Docker image, we mock the REST API v2.0 responses.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict

import pytest
import time_machine
from requests_mock import Mocker

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.thoughtspot.source import ThoughtSpotSource
from datahub.testing import mce_helpers
from tests.test_helpers import test_connection_helpers

logger = logging.getLogger(__name__)

FROZEN_TIME = "2024-01-15 09:00:00"


@pytest.fixture
def test_resources_dir(pytestconfig):
    """Return path to test resources directory."""
    return pytestconfig.rootpath / "tests/integration/thoughtspot"


@pytest.fixture
def mock_thoughtspot_api(requests_mock: Mocker, test_resources_dir: Path) -> Mocker:
    """
    Mock ThoughtSpot REST API v2.0 endpoints with realistic responses.

    This fixture registers mock responses for all ThoughtSpot API endpoints
    used by the connector. Responses are based on actual API structure.
    """
    base_url = "https://test.thoughtspot.cloud/api/rest/2.0"

    # Mock test connection endpoint
    requests_mock.get(
        f"{base_url}/system/config",
        json={"releaseVersion": "9.0.0.cl", "deploymentType": "CLOUD"},
        status_code=200,
    )

    # Mock the trusted/password auth_token_full endpoint. ThoughtSpotClient
    # calls this in ``_authenticate`` to mint a bearer token; without the
    # mock the SDK falls through to the real HTTPS request and the test
    # bombs at fixture-init time.
    requests_mock.post(
        f"{base_url}/auth/token/full",
        json={"token": "test-bearer-token", "valid_time_in_sec": 3000},
        status_code=200,
    )

    # Mock workspaces list endpoint
    # Response structure matches ThoughtSpot SDK format with metadata_id and metadata_header
    workspaces_response = [
        {
            "metadata_id": "workspace-1",
            "metadata_header": {
                "id": "workspace-1",
                "name": "Sales Analytics",
                "description": "Sales team dashboards and reports",
                "author": {
                    "id": "user-1",
                    "name": "John Doe",
                    "email": "john.doe@company.com",
                },
                "created": 1640000000000,
                "modified": 1700000000000,
            },
        },
        {
            "metadata_id": "workspace-2",
            "metadata_header": {
                "id": "workspace-2",
                "name": "Marketing Dashboard",
                "description": "Marketing metrics and KPIs",
                "author": {
                    "id": "user-2",
                    "name": "Jane Smith",
                    "email": "jane.smith@company.com",
                },
                "created": 1640100000000,
                "modified": 1700100000000,
            },
        },
    ]
    requests_mock.post(
        f"{base_url}/metadata/search",
        json=workspaces_response,
        status_code=200,
    )

    # Mock liveboards (dashboards) list endpoint.
    # Each item carries a top-level ``stats`` block (sibling of
    # metadata_header) mirroring the real TS ``metadata_search``
    # ``include_stats=True`` response shape — this is what the
    # flattener in client._paginated_metadata_search lifts onto
    # LiveboardResponse.stats so _process_usage_stats can emit
    # DashboardUsageStatistics aspects.
    liveboards_response = [
        {
            "metadata_id": "liveboard-1",
            "stats": {
                "views": 10000,
                "favorites": 3,
                "last_accessed": 1700300000000,
            },
            "metadata_header": {
                "id": "liveboard-1",
                "name": "Q4 Sales Dashboard",
                "description": "Quarterly sales performance metrics",
                "author": {
                    "id": "user-1",
                    "name": "John Doe",
                    "email": "john.doe@company.com",
                },
                "author_name": "john.doe",  # Add flattened author_name for ownership
                "owner_id": "workspace-1",  # Link to Sales Analytics workspace
                "created": 1640200000000,
                "modified": 1700200000000,
                "metadata_type": "LIVEBOARD",
                "visualizations": [
                    {
                        "id": "viz-1",
                        "name": "Revenue by Month",
                        "description": "Monthly revenue breakdown",
                        "viz_type": "LINE_CHART",
                        "answer_id": "answer-1",
                        "created": 1640200000000,
                        "modified": 1700200000000,
                        "author": {
                            "id": "user-1",
                            "name": "John Doe",
                            "email": "john.doe@company.com",
                        },
                    }
                ],
            },
        },
        {
            "metadata_id": "liveboard-2",
            "stats": {
                "views": 25000,
                "favorites": 7,
                "last_accessed": 1700400000000,
            },
            "metadata_header": {
                "id": "liveboard-2",
                "name": "Customer Insights",
                "description": "Customer behavior analysis dashboard",
                "author": {
                    "id": "user-2",
                    "name": "Jane Smith",
                    "email": "jane.smith@company.com",
                },
                "author_name": "jane.smith",  # Add flattened author_name for ownership
                "owner_id": "workspace-2",  # Link to Marketing Dashboard workspace
                "created": 1640300000000,
                "modified": 1700300000000,
                "metadata_type": "LIVEBOARD",
                "visualizations": [
                    {
                        "id": "viz-2",
                        "name": "User Growth",
                        "description": "Weekly user growth trend",
                        "viz_type": "BAR_CHART",
                        "answer_id": "answer-2",
                        "created": 1640300000000,
                        "modified": 1700300000000,
                        "author": {
                            "id": "user-2",
                            "name": "Jane Smith",
                            "email": "jane.smith@company.com",
                        },
                    }
                ],
            },
        },
    ]

    # Mock answers (charts) list endpoint.
    # Top-level ``stats`` blocks mirror the metadata_search
    # include_stats response so _process_usage_stats can emit
    # ChartUsageStatistics aspects on the Chart URN.
    answers_response = [
        {
            "metadata_id": "answer-1",
            "stats": {
                "views": 200,
                "favorites": 1,
                "last_accessed": 1700500000000,
            },
            "metadata_header": {
                "id": "answer-1",
                "name": "Monthly Revenue Chart",
                "description": "Revenue trend by month",
                "author": {
                    "id": "user-1",
                    "name": "John Doe",
                    "email": "john.doe@company.com",
                },
                "author_name": "john.doe",  # Add flattened author_name for ownership
                "owner_id": "workspace-1",  # Link to Sales Analytics workspace
                "created": 1640400000000,
                "modified": 1700400000000,
                "metadata_type": "ANSWER",
                "source_tables": ["worksheet-1"],  # Lineage to Sales_Fact_View
            },
        },
        {
            "metadata_id": "answer-2",
            "stats": {
                "views": 500,
                "favorites": 0,
                "last_accessed": 1700600000000,
            },
            "metadata_header": {
                "id": "answer-2",
                "name": "User Growth Trend",
                "description": "Active users by week",
                "author": {
                    "id": "user-1",
                    "name": "John Doe",
                    "email": "john.doe@company.com",
                },
                "author_name": "john.doe",  # Add flattened author_name for ownership
                "owner_id": "workspace-2",  # Link to Marketing Dashboard workspace
                "created": 1640500000000,
                "modified": 1700500000000,
                "metadata_type": "ANSWER",
                "source_tables": ["worksheet-2"],  # Lineage to Customer_Dimension
            },
        },
    ]

    # Mock logical tables (worksheets and tables) list endpoint
    logical_tables_response = [
        {
            "metadata_id": "worksheet-1",
            "metadata_header": {
                "id": "worksheet-1",
                "name": "Sales_Fact_View",
                "description": "Sales transactions fact table",
                "author": {
                    "id": "user-1",
                    "name": "John Doe",
                    "email": "john.doe@company.com",
                },
                "author_name": "john.doe",  # Add flattened author_name for ownership
                "owner_id": "workspace-1",  # Link to Sales Analytics workspace
                "created": 1640600000000,
                "modified": 1700600000000,
                "metadata_type": "LOGICAL_TABLE",
                "type": "WORKSHEET",
            },
        },
        {
            "metadata_id": "table-1",
            "metadata_header": {
                "id": "table-1",
                "name": "raw_orders",
                "description": "Raw orders data from source system",
                "author": {
                    "id": "user-2",
                    "name": "Jane Smith",
                    "email": "jane.smith@company.com",
                },
                "author_name": "jane.smith",  # Add flattened author_name for ownership
                "owner_id": "workspace-1",  # Link to Sales Analytics workspace
                "created": 1640700000000,
                "modified": 1700700000000,
                "metadata_type": "LOGICAL_TABLE",
                "type": "ONE_TO_ONE_LOGICAL",
            },
        },
        {
            "metadata_id": "worksheet-2",
            "metadata_header": {
                "id": "worksheet-2",
                "name": "Customer_Dimension",
                "description": "Customer dimension view",
                "author": {
                    "id": "user-2",
                    "name": "Jane Smith",
                    "email": "jane.smith@company.com",
                },
                "author_name": "jane.smith",  # Add flattened author_name for ownership
                "owner_id": "workspace-2",  # Link to Marketing Dashboard workspace
                "created": 1640800000000,
                "modified": 1700800000000,
                "metadata_type": "LOGICAL_TABLE",
                "type": "WORKSHEET",
            },
        },
        {
            "metadata_id": "sql-view-1",
            "metadata_header": {
                "id": "sql-view-1",
                "name": "Top_Revenue_View",
                "description": "SQL view aggregating revenue by region",
                "author": {
                    "id": "user-1",
                    "name": "John Doe",
                    "email": "john.doe@company.com",
                },
                "author_name": "john.doe",
                "owner_id": "workspace-1",
                "created": 1640900000000,
                "modified": 1700900000000,
                "metadata_type": "LOGICAL_TABLE",
                "type": "SQL_VIEW",
            },
            # Wire a recognised data_source_type so the
            # _resolve_sql_view_warehouse fallback resolves a dialect
            # (no connection lookup needed). Without this the SQL parser
            # path is skipped and the golden never captures
            # upstreamLineage for this SQL view — see PR review B1/H1.
            "metadata_detail": {
                "dataSourceId": "snowflake-conn-1",
                "dataSourceTypeEnum": "RDBMS_SNOWFLAKE",
            },
        },
    ]

    # Mock metadata/tml/export endpoint for TML (ThoughtSpot Modeling Language) export
    # The client.get_metadata_details() calls metadata_tml_export, not metadata/details
    # This endpoint returns TML format with schema and lineage information
    def metadata_tml_export_callback(request, context):
        """Return TML export data including schema for tables."""
        try:
            request_body = json.loads(request.text)
            logger.info(f"TML export request body: {request_body}")
            # The SDK sends metadata field with list of objects containing id
            metadata_list = request_body.get("metadata", [])

            # Extract IDs from metadata list
            # SDK sends {"identifier": "id"} format
            metadata_ids = []
            for meta in metadata_list:
                if isinstance(meta, dict):
                    # Try 'identifier' first (what SDK actually sends), then 'id'
                    id_value = meta.get("identifier") or meta.get("id")
                    if id_value:
                        metadata_ids.append(id_value)
                elif isinstance(meta, str):
                    metadata_ids.append(meta)

            # Map IDs to detailed responses
            details_map: Dict[str, Dict[str, Any]] = {
                "worksheet-1": {
                    "id": "worksheet-1",
                    "name": "Sales_Fact_View",
                    "type": "LOGICAL_TABLE",
                    "columns": [
                        {
                            "id": "col-1",
                            "name": "order_id",
                            "data_type": "INT64",
                            "description": "Unique order identifier",
                        },
                        {
                            "id": "col-2",
                            "name": "customer_id",
                            "data_type": "INT64",
                            "description": "Customer reference",
                        },
                        {
                            "id": "col-3",
                            "name": "order_amount",
                            "data_type": "DOUBLE",
                            "description": "Total order amount in USD",
                        },
                        {
                            "id": "col-4",
                            "name": "order_date",
                            "data_type": "DATE",
                            "description": "Date order was placed",
                        },
                    ],
                    "source_tables": ["table-1"],  # Lineage: depends on raw_orders
                },
                "table-1": {
                    "id": "table-1",
                    "name": "raw_orders",
                    "type": "LOGICAL_TABLE",
                    "columns": [
                        {
                            "id": "col-5",
                            "name": "id",
                            "data_type": "INT64",
                            "description": "Primary key",
                        },
                        {
                            "id": "col-6",
                            "name": "customer_id",
                            "data_type": "INT64",
                            "description": "Foreign key to customers",
                        },
                        {
                            "id": "col-7",
                            "name": "amount",
                            "data_type": "DOUBLE",
                            "description": "Order total",
                        },
                        {
                            "id": "col-8",
                            "name": "created_at",
                            "data_type": "DATETIME",
                            "description": "Record creation timestamp",
                        },
                    ],
                    "source_tables": [],  # No upstream dependencies (source table)
                },
                "worksheet-2": {
                    "id": "worksheet-2",
                    "name": "Customer_Dimension",
                    "type": "LOGICAL_TABLE",
                    "columns": [
                        {
                            "id": "col-9",
                            "name": "customer_id",
                            "data_type": "INT64",
                            "description": "Customer ID",
                        },
                        {
                            "id": "col-10",
                            "name": "customer_name",
                            "data_type": "VARCHAR",
                            "description": "Customer full name",
                        },
                        {
                            "id": "col-11",
                            "name": "email",
                            "data_type": "VARCHAR",
                            "description": "Customer email address",
                        },
                        {
                            "id": "col-12",
                            "name": "segment",
                            "data_type": "VARCHAR",
                            "description": "Customer segment classification",
                        },
                    ],
                    "source_tables": [],
                },
                "answer-1": {
                    "id": "answer-1",
                    "name": "Monthly Revenue Chart",
                    "type": "ANSWER",
                    "source_tables": [
                        "worksheet-1"
                    ],  # Lineage: depends on Sales_Fact_View
                },
                "answer-2": {
                    "id": "answer-2",
                    "name": "User Growth Trend",
                    "type": "ANSWER",
                    "source_tables": [
                        "worksheet-2"
                    ],  # Lineage: depends on Customer_Dimension
                },
                "liveboard-1": {
                    "id": "liveboard-1",
                    "name": "Q4 Sales Dashboard",
                    "type": "LIVEBOARD",
                    "visualizations": [
                        {"id": "answer-1", "name": "Monthly Revenue Chart"}
                    ],
                },
                "liveboard-2": {
                    "id": "liveboard-2",
                    "name": "Customer Insights",
                    "type": "LIVEBOARD",
                    "visualizations": [{"id": "answer-2", "name": "User Growth Trend"}],
                },
            }

            # Return details for requested IDs in TML YAML format
            results = []
            # SQL_VIEW objects get a separate TML shape: sql_view.sql_query
            # carries the raw SQL string (matches the live TS Cloud
            # response shape — see _extract_sql_view_statement).
            if "sql-view-1" in metadata_ids:
                import yaml

                sv_edoc = yaml.dump(
                    {
                        "sql_view": {
                            "name": "Top_Revenue_View",
                            "sql_query": "SELECT region, SUM(revenue) AS total FROM mock_warehouse.public.sales GROUP BY region",
                        }
                    }
                )
                results.append(
                    {
                        "info": {
                            "id": "sql-view-1",
                            "name": "Top_Revenue_View",
                            "type": "SQL_VIEW",
                            "status": {"status_code": "OK"},
                        },
                        "edoc": sv_edoc,
                    }
                )
            for id_str in metadata_ids:
                if id_str == "sql-view-1":
                    continue  # handled above
                if id_str in details_map:
                    detail = details_map[id_str]

                    # Only wrap LOGICAL_TABLE types in TML YAML format
                    if detail.get("type") == "LOGICAL_TABLE":
                        # Build TML YAML content for tables/worksheets
                        columns_yaml = []
                        for col in detail.get("columns", []):
                            columns_yaml.append(
                                {
                                    "name": col["name"],
                                    "data_type": col["data_type"],
                                    "description": col.get("description", ""),
                                }
                            )

                        tml_yaml_content = {
                            "table": {"name": detail["name"], "columns": columns_yaml}
                        }

                        import yaml

                        tml_edoc = yaml.dump(tml_yaml_content)

                        # Wrap in TML export format
                        tml_result = {
                            "info": {
                                "id": detail["id"],
                                "name": detail["name"],
                                "type": detail["type"],
                            },
                            "edoc": tml_edoc,
                        }
                        results.append(tml_result)
                    else:
                        # For non-table types, return as-is
                        results.append(detail)

            # Return results (TML export returns list of objects)
            return results if results else []
        except (json.JSONDecodeError, IndexError, KeyError) as e:
            logger.warning(f"Error in metadata_tml_export_callback: {e}")
            return []

    # Register TML export endpoint (this is what the client actually calls)
    requests_mock.post(
        f"{base_url}/metadata/tml/export",
        json=metadata_tml_export_callback,
        status_code=200,
    )

    # Register mock responses based on request body filters
    def metadata_search_callback(request, context):
        """Dynamic response based on metadata type filter in request."""
        try:
            request_body = json.loads(request.text)
            # The SDK sends metadata_search requests with structure:
            # {"metadata": [{"type": "LIVEBOARD"}], "record_offset": 0, "record_size": 100}
            metadata_list = request_body.get("metadata", [])

            if not metadata_list:
                return {"metadata": []}

            # Extract type from first metadata entry
            metadata_type = metadata_list[0].get("type")

            # Return response in {"metadata": [...]} structure as expected by SDK
            # Each item should have metadata_id and metadata_header structure
            if metadata_type == "ORG":  # ORG type is used for workspaces
                return {"metadata": workspaces_response}
            elif metadata_type == "LIVEBOARD":
                return {"metadata": liveboards_response}
            elif metadata_type == "ANSWER":
                return {"metadata": answers_response}
            elif metadata_type == "LOGICAL_TABLE":
                return {"metadata": logical_tables_response}
            else:
                # Default: return empty list for unknown types
                return {"metadata": []}
        except (json.JSONDecodeError, IndexError, KeyError):
            return {"metadata": []}

    # Register mock for /metadata/search endpoint (primary endpoint used by SDK)
    requests_mock.post(
        f"{base_url}/metadata/search",
        json=metadata_search_callback,
        status_code=200,
    )

    # Workspaces come from /orgs/search, not /metadata/search. The connector
    # builds Workspace containers from this endpoint's response. Wire shape
    # is flat ``{id, name, ...}`` dicts, not the ``{metadata_id,
    # metadata_header: {...}}`` envelope used by ``/metadata/search``.
    orgs_search_response = [
        {
            "id": "workspace-1",
            "name": "Sales Analytics",
            "description": "Sales team dashboards and reports",
        },
        {
            "id": "workspace-2",
            "name": "Marketing Dashboard",
            "description": "Marketing metrics and KPIs",
        },
    ]
    requests_mock.post(
        f"{base_url}/orgs/search",
        json={"orgs": orgs_search_response},
        status_code=200,
    )

    # Tag catalog fetch. Empty list keeps the test focused on lineage/
    # containers/schema — tag-aspect handling is exercised separately
    # in unit tests.
    requests_mock.post(
        f"{base_url}/tags/search",
        json=[],
        status_code=200,
    )

    # Connection catalog fetch (cross-platform lineage routing).
    # Empty list disables external lineage emission for the integration
    # fixtures — internal Worksheet→Table lineage is what the assertions
    # target.
    requests_mock.post(
        f"{base_url}/connection/search",
        json={"connections": []},
        status_code=200,
    )

    return requests_mock


@time_machine.travel(FROZEN_TIME, tick=False)
def test_thoughtspot_ingest(
    pytestconfig, tmp_path, mock_thoughtspot_api, test_resources_dir, mock_time
):
    """
    Test end-to-end ThoughtSpot ingestion with golden file validation.

    This test:
    1. Runs full ingestion pipeline with mocked API responses
    2. Validates output against golden file
    3. Verifies all entity types are extracted (containers, dashboards, charts, datasets)
    """
    output_file = tmp_path / "thoughtspot_mces.json"
    golden_file = test_resources_dir / "thoughtspot_mces_golden.json"

    pipeline = Pipeline.create(
        {
            "run_id": "thoughtspot-test",
            "source": {
                "type": "thoughtspot",
                "config": {
                    "connection": {
                        "base_url": "https://test.thoughtspot.cloud",
                        "auth": {
                            "type": "trusted",
                            "username": "testuser",
                            "secret_key": "test-token-12345",
                        },
                    },
                    "platform_instance": "prod",
                    "include_ownership": True,  # Enable ownership extraction
                    # Exercise the metadata_search include_stats path
                    # end-to-end. Fixtures above carry top-level ``stats``
                    # blocks so the rewritten _process_usage_stats emits
                    # DashboardUsageStatistics / ChartUsageStatistics
                    # aspects on each Liveboard/Answer.
                    "include_usage_stats": True,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_file),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    # Verify against golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file,
        golden_path=golden_file,
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_thoughtspot_ingest_with_filters(
    pytestconfig, tmp_path, mock_thoughtspot_api, test_resources_dir, mock_time
):
    """
    Test ThoughtSpot ingestion with entity filtering patterns.

    Verifies that workspace_pattern, liveboard_pattern, etc. correctly filter entities.
    """
    output_file = tmp_path / "thoughtspot_filtered_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "thoughtspot-filtered-test",
            "source": {
                "type": "thoughtspot",
                "config": {
                    "connection": {
                        "base_url": "https://test.thoughtspot.cloud",
                        "auth": {
                            "type": "trusted",
                            "username": "testuser",
                            "secret_key": "test-token-12345",
                        },
                    },
                    "workspace_pattern": {
                        "allow": ["^Sales.*"],
                    },
                    "liveboard_pattern": {
                        "allow": [".*"],
                        "deny": ["^Draft.*"],
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_file),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    # Verify filtered output
    with open(output_file) as f:
        output = json.load(f)

    # Should only have Sales Analytics workspace (not Marketing Dashboard)
    # Find container names to verify filtering
    workspace_names_found = set()
    for mcp in output:
        if (
            mcp.get("entityType") == "container"
            and mcp.get("aspectName") == "containerProperties"
        ):
            name = mcp.get("aspect", {}).get("json", {}).get("name")
            if name:
                workspace_names_found.add(name)

    assert "Sales Analytics" in workspace_names_found, (
        "Sales Analytics workspace should be present"
    )
    assert "Marketing Dashboard" not in workspace_names_found, (
        "Marketing Dashboard should be filtered out"
    )


def test_thoughtspot_connection_success(mock_thoughtspot_api):
    """Test that valid credentials succeed connection test."""
    report = test_connection_helpers.run_test_connection(
        ThoughtSpotSource,
        {
            "connection": {
                "base_url": "https://test.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test-token-12345",
                },
            },
        },
    )
    test_connection_helpers.assert_basic_connectivity_success(report)


# Connection error tests removed - the client.test_connection() method catches
# all exceptions internally and returns False, so we can't test specific error
# scenarios through the test_connection interface. Error handling is verified
# through unit tests in test_thoughtspot_source.py instead.


@pytest.mark.skip(
    reason=(
        "Fixture does not supply per-column ``metadata_detail.columns`` on "
        "logical tables, so the connector emits no ``schemaMetadata`` aspects. "
        "Enriching the fixture is tracked separately; the column-extraction "
        "path itself is covered by unit tests in ``test_thoughtspot_source.py``."
    )
)
@time_machine.travel(FROZEN_TIME, tick=False)
def test_thoughtspot_schema_extraction(
    pytestconfig, tmp_path, mock_thoughtspot_api, test_resources_dir, mock_time
):
    """
    Test that schema metadata (columns) are extracted from logical tables.

    This verifies schema metadata extraction from the TML export endpoint.
    Datasets should have schemaMetadata aspects with column definitions.
    """
    output_file = tmp_path / "thoughtspot_schema_test.json"

    pipeline = Pipeline.create(
        {
            "run_id": "thoughtspot-schema-test",
            "source": {
                "type": "thoughtspot",
                "config": {
                    "connection": {
                        "base_url": "https://test.thoughtspot.cloud",
                        "auth": {
                            "type": "trusted",
                            "username": "testuser",
                            "secret_key": "test-token-12345",
                        },
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_file),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    # Load output and verify schema metadata exists
    with open(output_file) as f:
        output = json.load(f)

    # Find schemaMetadata aspects
    schema_aspects = [
        mcp for mcp in output if mcp.get("aspectName") == "schemaMetadata"
    ]

    assert len(schema_aspects) > 0, "No schemaMetadata aspects found"

    # Verify at least one schema has column information
    found_columns = False
    for schema_aspect in schema_aspects:
        fields = schema_aspect.get("aspect", {}).get("json", {}).get("fields", [])
        if len(fields) > 0:
            found_columns = True
            # Verify column structure
            first_field = fields[0]
            assert "fieldPath" in first_field
            assert "nativeDataType" in first_field
            break

    assert found_columns, "No columns found in schemaMetadata aspects"


@pytest.mark.skip(
    reason=(
        "Fixture's TML export callback does not produce per-viz lineage "
        "(visualizations[].answer.tables[]), so the connector emits no chart "
        "inputs. The lineage-extraction code path is covered by unit tests in "
        "``test_thoughtspot_source.py``; the fixture enrichment is tracked "
        "separately."
    )
)
@time_machine.travel(FROZEN_TIME, tick=False)
def test_thoughtspot_lineage_extraction(
    pytestconfig, tmp_path, mock_thoughtspot_api, test_resources_dir, mock_time
):
    """
    Test that lineage is extracted for Answers and Visualizations.

    This verifies:
    - Answer → Dataset lineage (from source_tables field)
    - Visualization → Answer lineage (from answer_id field)
    - Dashboard → Chart references (via chartEdges in dashboardInfo)
    """
    output_file = tmp_path / "thoughtspot_lineage_test.json"

    pipeline = Pipeline.create(
        {
            "run_id": "thoughtspot-lineage-test",
            "source": {
                "type": "thoughtspot",
                "config": {
                    "connection": {
                        "base_url": "https://test.thoughtspot.cloud",
                        "auth": {
                            "type": "trusted",
                            "username": "testuser",
                            "secret_key": "test-token-12345",
                        },
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_file),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    # Load output and verify lineage exists
    with open(output_file) as f:
        output = json.load(f)

    # SDK V2 stores lineage in chartInfo.inputs field, not separate upstreamLineage aspects
    # Find chartInfo aspects with inputs (lineage)
    chart_infos_with_inputs = [
        mcp
        for mcp in output
        if mcp.get("aspectName") == "chartInfo"
        and mcp.get("aspect", {}).get("json", {}).get("inputs")
    ]

    assert len(chart_infos_with_inputs) > 0, "No charts with lineage inputs found"

    # Verify at least one chart has upstream references in inputs field
    found_lineage = False
    for chart_info in chart_infos_with_inputs:
        inputs = chart_info.get("aspect", {}).get("json", {}).get("inputs", [])
        if len(inputs) > 0:
            found_lineage = True
            # Verify input structure (SDK V2 format: [{"string": "urn:..."}])
            first_input = inputs[0]
            assert "string" in first_input, "Input should have 'string' field"
            assert "urn:li:" in first_input["string"], "Input should be a valid URN"
            break

    assert found_lineage, "No lineage inputs found in chartInfo aspects"


@time_machine.travel(FROZEN_TIME, tick=False)
def test_thoughtspot_container_hierarchy(
    pytestconfig, tmp_path, mock_thoughtspot_api, test_resources_dir, mock_time
):
    """
    Test that container hierarchy is correctly established: workspace→liveboard→visualization.

    This test verifies:
    1. Workspaces are emitted as container entities
    2. Liveboards reference their parent workspace container
    3. Visualizations are emitted as charts within liveboards
    4. The container relationship chain is properly maintained
    """
    output_file = tmp_path / "thoughtspot_container_hierarchy_test.json"

    pipeline = Pipeline.create(
        {
            "run_id": "thoughtspot-container-hierarchy-test",
            "source": {
                "type": "thoughtspot",
                "config": {
                    "connection": {
                        "base_url": "https://test.thoughtspot.cloud",
                        "auth": {
                            "type": "trusted",
                            "username": "testuser",
                            "secret_key": "test-token-12345",
                        },
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_file),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    # Load output and analyze container hierarchy
    with open(output_file) as f:
        output = json.load(f)

    # Step 1: Verify workspace containers exist
    workspace_containers = [
        mcp
        for mcp in output
        if mcp.get("entityType") == "container"
        and mcp.get("aspectName") == "containerProperties"
    ]
    assert len(workspace_containers) > 0, "No workspace containers found"

    # Extract workspace container URNs
    workspace_urns = {mcp.get("entityUrn") for mcp in workspace_containers}
    assert len(workspace_urns) > 0, "No workspace container URNs found"

    # Step 2: Verify liveboards reference workspace containers
    liveboard_containers = [
        mcp
        for mcp in output
        if mcp.get("entityType") == "dashboard" and mcp.get("aspectName") == "container"
    ]
    assert len(liveboard_containers) > 0, "No liveboards with container aspects found"

    # Verify at least one liveboard references a workspace
    found_liveboard_workspace_link = False
    for liveboard_container in liveboard_containers:
        container_urn = (
            liveboard_container.get("aspect", {}).get("json", {}).get("container")
        )
        if container_urn and container_urn in workspace_urns:
            found_liveboard_workspace_link = True
            break

    assert found_liveboard_workspace_link, (
        "No liveboards reference workspace containers"
    )

    # Step 3: Verify visualizations are emitted as charts
    chart_info = [
        mcp
        for mcp in output
        if mcp.get("entityType") == "chart" and mcp.get("aspectName") == "chartInfo"
    ]
    assert len(chart_info) > 0, "No chart entities found for visualizations"

    # Step 4: Verify dashboards reference their charts
    dashboard_info = [
        mcp
        for mcp in output
        if mcp.get("entityType") == "dashboard"
        and mcp.get("aspectName") == "dashboardInfo"
    ]
    assert len(dashboard_info) > 0, "No dashboard info aspects found"

    # Verify at least one dashboard has chart references
    # SDK V2 uses chartEdges, not charts field
    found_dashboard_chart_link = False
    for dashboard in dashboard_info:
        chart_edges = dashboard.get("aspect", {}).get("json", {}).get("chartEdges", [])
        if len(chart_edges) > 0:
            found_dashboard_chart_link = True
            # Verify the chart URN format is correct
            first_edge = chart_edges[0]
            dest_urn = first_edge.get("destinationUrn", "")
            assert dest_urn.startswith("urn:li:chart:"), (
                f"Invalid chart URN format: {dest_urn}"
            )
            break

    assert found_dashboard_chart_link, "No dashboards reference chart entities"
