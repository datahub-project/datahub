#!/usr/bin/env python3
"""
Script to regenerate the golden file for ThoughtSpot integration tests.

This script runs the ThoughtSpot ingestion with mocked API responses
and outputs the result to the golden file location.

Workaround for urllib3 dependency issues that block pytest execution.
"""

import json
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent.parent.parent / "src"
sys.path.insert(0, str(src_path))

import time_machine  # noqa: E402
from requests_mock import Mocker  # noqa: E402

from datahub.ingestion.run.pipeline import Pipeline  # noqa: E402

FROZEN_TIME = "2024-01-15 09:00:00"


def setup_mock_api(requests_mock: Mocker) -> None:
    """Setup mock ThoughtSpot API responses."""
    base_url = "https://test.thoughtspot.cloud/api/rest/2.0"

    # Mock test connection endpoint
    requests_mock.get(
        f"{base_url}/system/config",
        json={"releaseVersion": "9.0.0.cl", "deploymentType": "CLOUD"},
        status_code=200,
    )

    # Mock workspaces list endpoint
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

    # Mock liveboards (dashboards) list endpoint
    liveboards_response = [
        {
            "metadata_id": "liveboard-1",
            "metadata_header": {
                "id": "liveboard-1",
                "name": "Q4 Sales Dashboard",
                "description": "Quarterly sales performance metrics",
                "author": {
                    "id": "user-1",
                    "name": "John Doe",
                    "email": "john.doe@company.com",
                },
                "author_name": "john.doe",
                "owner_id": "workspace-1",
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
            "metadata_header": {
                "id": "liveboard-2",
                "name": "Customer Insights",
                "description": "Customer behavior analysis dashboard",
                "author": {
                    "id": "user-2",
                    "name": "Jane Smith",
                    "email": "jane.smith@company.com",
                },
                "author_name": "jane.smith",
                "owner_id": "workspace-2",
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

    # Mock answers (charts) list endpoint
    answers_response = [
        {
            "metadata_id": "answer-1",
            "metadata_header": {
                "id": "answer-1",
                "name": "Monthly Revenue Chart",
                "description": "Revenue trend by month",
                "author": {
                    "id": "user-1",
                    "name": "John Doe",
                    "email": "john.doe@company.com",
                },
                "author_name": "john.doe",
                "owner_id": "workspace-1",
                "created": 1640400000000,
                "modified": 1700400000000,
                "metadata_type": "ANSWER",
                "source_tables": ["worksheet-1"],  # Lineage to Sales_Fact_View
            },
        },
        {
            "metadata_id": "answer-2",
            "metadata_header": {
                "id": "answer-2",
                "name": "User Growth Trend",
                "description": "Active users by week",
                "author": {
                    "id": "user-1",
                    "name": "John Doe",
                    "email": "john.doe@company.com",
                },
                "author_name": "john.doe",
                "owner_id": "workspace-2",
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
                "author_name": "john.doe",
                "owner_id": "workspace-1",
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
                "author_name": "jane.smith",
                "owner_id": "workspace-1",
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
                "author_name": "jane.smith",
                "owner_id": "workspace-2",
                "created": 1640800000000,
                "modified": 1700800000000,
                "metadata_type": "LOGICAL_TABLE",
                "type": "WORKSHEET",
            },
        },
    ]

    # Mock metadata/tml/export endpoint for TML export with schema details
    def metadata_tml_export_callback(request, context):
        """Return TML export data including schema for tables."""
        try:
            request_body = json.loads(request.text)
            metadata_list = request_body.get("metadata", [])

            # Extract IDs
            metadata_ids = []
            for meta in metadata_list:
                if isinstance(meta, dict) and "id" in meta:
                    metadata_ids.append(meta["id"])
                elif isinstance(meta, str):
                    metadata_ids.append(meta)

            # Map IDs to detailed responses
            details_map = {
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
                    "source_tables": ["table-1"],
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
                    "source_tables": [],
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
                    "source_tables": ["worksheet-1"],
                },
                "answer-2": {
                    "id": "answer-2",
                    "name": "User Growth Trend",
                    "type": "ANSWER",
                    "source_tables": ["worksheet-2"],
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

            # Return details for requested IDs
            results = []
            for id_str in metadata_ids:
                if id_str in details_map:
                    results.append(details_map[id_str])

            return results if results else []
        except (json.JSONDecodeError, IndexError, KeyError) as e:
            print(f"Error in metadata_tml_export_callback: {e}")
            return []

    # Register TML export endpoint
    requests_mock.post(
        f"{base_url}/metadata/tml/export",
        json=metadata_tml_export_callback,
        status_code=200,
    )

    # Register dynamic metadata search callback
    def metadata_search_callback(request, context):
        """Dynamic response based on metadata type filter in request."""
        try:
            request_body = json.loads(request.text)
            metadata_list = request_body.get("metadata", [])

            if not metadata_list:
                return {"metadata": []}

            # Extract type from first metadata entry
            metadata_type = metadata_list[0].get("type")

            # Return response in {"metadata": [...]} structure
            if metadata_type == "ORG":  # ORG type is used for workspaces
                return {"metadata": workspaces_response}
            elif metadata_type == "LIVEBOARD":
                return {"metadata": liveboards_response}
            elif metadata_type == "ANSWER":
                return {"metadata": answers_response}
            elif metadata_type == "LOGICAL_TABLE":
                return {"metadata": logical_tables_response}
            else:
                return {"metadata": []}
        except (json.JSONDecodeError, IndexError, KeyError):
            return {"metadata": []}

    # Register mock for /metadata/search endpoint
    requests_mock.post(
        f"{base_url}/metadata/search",
        json=metadata_search_callback,
        status_code=200,
    )


def main():
    """Run ingestion and generate golden file."""
    output_file = Path(__file__).parent / "thoughtspot_mces_golden.json"

    print(f"Regenerating golden file: {output_file}")

    with Mocker() as m:
        setup_mock_api(m)

        with time_machine.travel(FROZEN_TIME, tick=False):
            pipeline = Pipeline.create(
                {
                    "run_id": "thoughtspot-test",
                    "source": {
                        "type": "thoughtspot",
                        "config": {
                            "connection": {
                                "base_url": "https://test.thoughtspot.cloud",
                                "username": "testuser",
                                "secret_key": "test-token-12345",
                            },
                            "platform_instance": "prod",
                            "include_ownership": True,
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

    # Analyze the golden file
    with open(output_file) as f:
        data = json.load(f)

    aspects: dict[str, int] = {}
    entity_types: dict[str, int] = {}
    for item in data:
        entity_type = item.get("entityType", "unknown")
        aspect_name = item.get("aspectName", "unknown")
        entity_types[entity_type] = entity_types.get(entity_type, 0) + 1
        aspects[aspect_name] = aspects.get(aspect_name, 0) + 1

    print("\n✓ Golden file regenerated successfully!")
    print(f"  Total events: {len(data)}")
    print(f"  Entity types: {json.dumps(entity_types, indent=2)}")
    print(f"  Aspects: {json.dumps(aspects, indent=2)}")

    # Check for critical aspects
    has_schema = aspects.get("schemaMetadata", 0) > 0
    has_lineage = aspects.get("upstreamLineage", 0) > 0

    print(f"\n  Schema metadata: {'✓ PRESENT' if has_schema else '✗ MISSING'}")
    print(f"  Upstream lineage: {'✓ PRESENT' if has_lineage else '✗ MISSING'}")

    if has_schema and has_lineage:
        print("\n🎉 SUCCESS: Golden file includes schema and lineage!")
    else:
        print("\n⚠ WARNING: Golden file is missing critical aspects")
        sys.exit(1)


if __name__ == "__main__":
    main()
