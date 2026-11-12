"""Shared pytest fixtures for ThoughtSpot integration tests.

Defines the ``mock_thoughtspot_api`` fixture (and its supporting
``test_resources_dir``) that mock the full ThoughtSpot REST API v2.0
surface area exercised by the connector. Living in ``conftest.py``
makes these fixtures auto-discoverable by every test module in this
directory.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict

import pytest
from requests_mock import Mocker

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
                # SourceTableRef coercer accepts {id, name} dicts; bare
                # GUID strings are dropped with a warning.
                "source_tables": [
                    {"id": "worksheet-1", "name": "Sales_Fact_View"},
                ],
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
                "source_tables": [
                    {"id": "worksheet-2", "name": "Customer_Dimension"},
                ],
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
            # metadata_search returns columns under metadata_detail when
            # include_details=True. _parse_columns_from_metadata_detail
            # reads header.{id,name,description}, dataType, type,
            # physicalColumnName and sources from this wire shape.
            "metadata_detail": {
                "columns": [
                    {
                        "header": {
                            "id": "col-1",
                            "name": "order_id",
                            "description": "Unique order identifier",
                        },
                        "dataType": "INT64",
                        "type": "ATTRIBUTE",
                    },
                    {
                        "header": {
                            "id": "col-2",
                            "name": "customer_id",
                            "description": "Customer reference",
                        },
                        "dataType": "INT64",
                        "type": "ATTRIBUTE",
                    },
                    {
                        "header": {
                            "id": "col-3",
                            "name": "order_amount",
                            "description": "Total order amount in USD",
                        },
                        "dataType": "DOUBLE",
                        "type": "MEASURE",
                    },
                    {
                        "header": {
                            "id": "col-4",
                            "name": "order_date",
                            "description": "Date order was placed",
                        },
                        "dataType": "DATE",
                        "type": "ATTRIBUTE",
                    },
                ],
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
            "metadata_detail": {
                "columns": [
                    {
                        "header": {
                            "id": "col-5",
                            "name": "id",
                            "description": "Primary key",
                        },
                        "dataType": "INT64",
                        "type": "ATTRIBUTE",
                    },
                    {
                        "header": {
                            "id": "col-6",
                            "name": "customer_id",
                            "description": "Foreign key to customers",
                        },
                        "dataType": "INT64",
                        "type": "ATTRIBUTE",
                    },
                    {
                        "header": {
                            "id": "col-7",
                            "name": "amount",
                            "description": "Order total",
                        },
                        "dataType": "DOUBLE",
                        "type": "MEASURE",
                    },
                    {
                        "header": {
                            "id": "col-8",
                            "name": "created_at",
                            "description": "Record creation timestamp",
                        },
                        "dataType": "DATE_TIME",
                        "type": "ATTRIBUTE",
                    },
                ],
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
            "metadata_detail": {
                "columns": [
                    {
                        "header": {
                            "id": "col-9",
                            "name": "customer_id",
                            "description": "Customer ID",
                        },
                        "dataType": "INT64",
                        "type": "ATTRIBUTE",
                    },
                    {
                        "header": {
                            "id": "col-10",
                            "name": "customer_name",
                            "description": "Customer full name",
                        },
                        "dataType": "VARCHAR",
                        "type": "ATTRIBUTE",
                    },
                    {
                        "header": {
                            "id": "col-11",
                            "name": "email",
                            "description": "Customer email address",
                        },
                        "dataType": "VARCHAR",
                        "type": "ATTRIBUTE",
                    },
                    {
                        "header": {
                            "id": "col-12",
                            "name": "segment",
                            "description": "Customer segment classification",
                        },
                        "dataType": "VARCHAR",
                        "type": "ATTRIBUTE",
                    },
                ],
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
