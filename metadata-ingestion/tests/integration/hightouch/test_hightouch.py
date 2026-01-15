"""Integration tests for Hightouch source."""

from unittest import mock

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.hightouch.models import (
    HightouchDestination,
    HightouchFieldMapping,
    HightouchModel,
    HightouchSourceConnection,
    HightouchSync,
    HightouchSyncRun,
)
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-01-01 12:00:00"


def get_mock_api_responses():
    """Return mock API responses as dictionaries (like JSON from API)."""
    return {
        "sources": [
            {
                "id": "source_1",
                "name": "Production Snowflake",
                "slug": "prod-snowflake",
                "type": "snowflake",
                "workspaceId": "workspace_1",
                "createdAt": "2023-01-01T00:00:00Z",
                "updatedAt": "2023-01-02T00:00:00Z",
                "configuration": {"account": "test.snowflakecomputing.com"},
            }
        ],
        "models": [
            {
                "id": "model_1",
                "name": "Customer 360 Model",
                "slug": "customer-360-model",
                "workspaceId": "workspace_1",
                "sourceId": "source_1",
                "queryType": "raw_sql",
                "createdAt": "2023-01-01T00:00:00Z",
                "updatedAt": "2023-01-02T00:00:00Z",
                "primaryKey": "customer_id",
                "description": "Unified customer view from multiple sources",
                "isSchema": False,
                "tags": {"team": "growth", "priority": "high"},
            },
            {
                "id": "model_2",
                "name": "Product Events",
                "slug": "product-events",
                "workspaceId": "workspace_1",
                "sourceId": "source_1",
                "queryType": "table",
                "createdAt": "2023-01-01T00:00:00Z",
                "updatedAt": "2023-01-02T00:00:00Z",
                "primaryKey": "event_id",
                "description": "Product usage events",
                "isSchema": False,
            },
        ],
        "destinations": [
            {
                "id": "dest_1",
                "name": "Salesforce Production",
                "slug": "salesforce-prod",
                "type": "salesforce",
                "workspaceId": "workspace_1",
                "createdAt": "2023-01-01T00:00:00Z",
                "updatedAt": "2023-01-02T00:00:00Z",
                "configuration": {"instance_url": "https://test.salesforce.com"},
            },
            {
                "id": "dest_2",
                "name": "HubSpot Marketing",
                "slug": "hubspot-marketing",
                "type": "hubspot",
                "workspaceId": "workspace_1",
                "createdAt": "2023-01-01T00:00:00Z",
                "updatedAt": "2023-01-02T00:00:00Z",
                "configuration": {},
            },
        ],
        "syncs": [
            {
                "id": "sync_1",
                "slug": "customers-to-salesforce",
                "workspaceId": "workspace_1",
                "modelId": "model_1",
                "destinationId": "dest_1",
                "createdAt": "2023-01-01T00:00:00Z",
                "updatedAt": "2023-01-02T00:00:00Z",
                "configuration": {
                    "destinationTable": "Contact",
                    "fieldMappings": [
                        {
                            "sourceField": "customer_id",
                            "destinationField": "External_ID__c",
                            "isPrimaryKey": True,
                        },
                        {
                            "sourceField": "email",
                            "destinationField": "Email",
                            "isPrimaryKey": False,
                        },
                        {
                            "sourceField": "first_name",
                            "destinationField": "FirstName",
                            "isPrimaryKey": False,
                        },
                    ],
                },
                "disabled": False,
            },
            {
                "id": "sync_2",
                "slug": "events-to-hubspot",
                "workspaceId": "workspace_1",
                "modelId": "model_2",
                "destinationId": "dest_2",
                "createdAt": "2023-01-01T00:00:00Z",
                "updatedAt": "2023-01-02T00:00:00Z",
                "configuration": {
                    "object": "contacts",
                    "columnMappings": {
                        "email": "user_email",
                        "event_name": "event_type",
                    },
                },
                "disabled": False,
            },
        ],
        "sync_runs": {
            "sync_1": [
                {
                    "id": "run_1",
                    "syncId": "sync_1",
                    "status": "success",
                    "startedAt": "2023-12-01T10:00:00Z",
                    "finishedAt": "2023-12-01T10:05:00Z",
                    "createdAt": "2023-12-01T10:00:00Z",
                    "querySize": 1000,
                    "completionRatio": 1.0,
                    "plannedRows": {
                        "addedCount": 50,
                        "changedCount": 30,
                        "removedCount": 5,
                    },
                    "successfulRows": {
                        "addedCount": 50,
                        "changedCount": 30,
                        "removedCount": 5,
                    },
                    "failedRows": {
                        "addedCount": 0,
                        "changedCount": 0,
                        "removedCount": 0,
                    },
                }
            ],
            "sync_2": [],
        },
    }


@time_machine.travel(FROZEN_TIME)
@pytest.mark.integration
def test_hightouch_source_basic(pytestconfig, tmp_path):
    """Test basic Hightouch ingestion with mocked API."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/hightouch"

    mock_responses = get_mock_api_responses()

    with mock.patch(
        "datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient"
    ) as mock_api_class:
        mock_api_instance = mock_api_class.return_value

        mock_api_instance._parse_model = lambda d: HightouchModel(**d) if d else None
        mock_api_instance._parse_source = (
            lambda d: HightouchSourceConnection(**d) if d else None
        )
        mock_api_instance._parse_destination = (
            lambda d: HightouchDestination(**d) if d else None
        )
        mock_api_instance._parse_sync = lambda d: HightouchSync(**d) if d else None
        mock_api_instance._parse_sync_run = (
            lambda d: HightouchSyncRun(**d) if d else None
        )

        # NOW use the parsers to set return values
        mock_api_instance.get_syncs.return_value = [
            mock_api_instance._parse_sync(s) for s in mock_responses["syncs"]
        ]
        mock_api_instance.get_models.return_value = [
            mock_api_instance._parse_model(m) for m in mock_responses["models"]
        ]
        mock_api_instance.get_workspaces.return_value = []
        mock_api_instance.get_contracts.return_value = []

        def mock_get_model(model_id):
            model_dict = next(
                (m for m in mock_responses["models"] if m["id"] == model_id), None
            )
            return mock_api_instance._parse_model(model_dict) if model_dict else None

        def mock_get_source(source_id):
            source_dict = next(
                (s for s in mock_responses["sources"] if s["id"] == source_id), None
            )
            return mock_api_instance._parse_source(source_dict) if source_dict else None

        def mock_get_destination(dest_id):
            dest_dict = next(
                (d for d in mock_responses["destinations"] if d["id"] == dest_id), None
            )
            return (
                mock_api_instance._parse_destination(dest_dict) if dest_dict else None
            )

        def mock_get_sync_runs(sync_id, limit=10):
            runs = mock_responses["sync_runs"].get(sync_id, [])[:limit]
            return [mock_api_instance._parse_sync_run(r) for r in runs]

        def mock_extract_field_mappings(sync):
            config = sync.configuration
            field_mappings = []

            if "fieldMappings" in config:
                for mapping in config["fieldMappings"]:
                    field_mappings.append(
                        HightouchFieldMapping(
                            source_field=mapping["sourceField"],
                            destination_field=mapping["destinationField"],
                            is_primary_key=mapping.get("isPrimaryKey", False),
                        )
                    )
            elif "columnMappings" in config:
                for dest_field, source_field in config["columnMappings"].items():
                    field_mappings.append(
                        HightouchFieldMapping(
                            source_field=str(source_field),
                            destination_field=str(dest_field),
                            is_primary_key=False,
                        )
                    )

            return field_mappings

        # Attach side effects
        mock_api_instance.get_model_by_id.side_effect = mock_get_model
        mock_api_instance.get_source_by_id.side_effect = mock_get_source
        mock_api_instance.get_destination_by_id.side_effect = mock_get_destination
        mock_api_instance.get_sync_runs.side_effect = mock_get_sync_runs
        mock_api_instance.extract_field_mappings.side_effect = (
            mock_extract_field_mappings
        )

        pipeline = Pipeline.create(
            {
                "run_id": "hightouch-test-run",
                "source": {
                    "type": "hightouch",
                    "config": {
                        "api_config": {
                            "api_key": "test_api_key",
                            "base_url": "https://api.hightouch.com/api/v1",
                        },
                        "env": "PROD",
                        "emit_models_as_datasets": True,
                        "include_sync_runs": True,
                        "max_sync_runs_per_sync": 5,
                        "parse_model_sql": True,
                        "include_contracts": False,
                        "include_table_lineage_to_sibling": False,
                        "extract_workspaces_to_containers": False,
                        "sources_to_platform_instance": {
                            "source_1": {
                                "platform": "snowflake",
                                "platform_instance": "prod-snowflake",
                                "env": "PROD",
                                "database": "analytics",
                            }
                        },
                        "destinations_to_platform_instance": {
                            "dest_1": {
                                "platform": "salesforce",
                                "platform_instance": "prod-salesforce",
                                "env": "PROD",
                            }
                        },
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/hightouch_mces.json",
                    },
                },
            }
        )

        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"{tmp_path}/hightouch_mces.json",
            golden_path=test_resources_dir / "hightouch_mces_golden.json",
            ignore_paths=[
                # Timestamps have millisecond variations that can't be frozen
                r"root\[\d+\]\['aspect'\]\['json'\]\['upstreams'\]\[\d+\]\['auditStamp'\]\['time'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['upstreams'\]\[\d+\]\['created'\]\['time'\]",
            ],
        )


@time_machine.travel(FROZEN_TIME)
@pytest.mark.integration
def test_hightouch_source_with_patterns(pytestconfig, tmp_path):
    """Test Hightouch ingestion with filtering patterns."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/hightouch"

    mock_responses = get_mock_api_responses()

    with mock.patch(
        "datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient"
    ) as mock_api_class:
        mock_api_instance = mock_api_class.return_value

        mock_api_instance._parse_model = lambda d: HightouchModel(**d) if d else None
        mock_api_instance._parse_source = (
            lambda d: HightouchSourceConnection(**d) if d else None
        )
        mock_api_instance._parse_destination = (
            lambda d: HightouchDestination(**d) if d else None
        )
        mock_api_instance._parse_sync = lambda d: HightouchSync(**d) if d else None

        mock_api_instance.get_syncs.return_value = [
            mock_api_instance._parse_sync(s) for s in mock_responses["syncs"]
        ]
        mock_api_instance.get_models.return_value = [
            mock_api_instance._parse_model(m) for m in mock_responses["models"]
        ]
        mock_api_instance.get_workspaces.return_value = []
        mock_api_instance.get_contracts.return_value = []

        def mock_get_model(model_id):
            model_dict = next(
                (m for m in mock_responses["models"] if m["id"] == model_id), None
            )
            return mock_api_instance._parse_model(model_dict) if model_dict else None

        def mock_get_source(source_id):
            source_dict = next(
                (s for s in mock_responses["sources"] if s["id"] == source_id), None
            )
            return mock_api_instance._parse_source(source_dict) if source_dict else None

        def mock_get_destination(dest_id):
            dest_dict = next(
                (d for d in mock_responses["destinations"] if d["id"] == dest_id), None
            )
            return (
                mock_api_instance._parse_destination(dest_dict) if dest_dict else None
            )

        mock_api_instance.get_model_by_id.side_effect = mock_get_model
        mock_api_instance.get_source_by_id.side_effect = mock_get_source
        mock_api_instance.get_destination_by_id.side_effect = mock_get_destination
        mock_api_instance.get_sync_runs.return_value = []
        mock_api_instance.extract_field_mappings.return_value = []

        pipeline = Pipeline.create(
            {
                "run_id": "hightouch-test-patterns",
                "source": {
                    "type": "hightouch",
                    "config": {
                        "api_config": {
                            "api_key": "test_api_key",
                        },
                        "env": "PROD",
                        "emit_models_as_datasets": True,
                        "include_sync_runs": False,
                        "sync_patterns": {
                            "allow": ["customers-.*"],
                            "deny": [".*test.*"],
                        },
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/hightouch_filtered_mces.json",
                    },
                },
            }
        )

        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"{tmp_path}/hightouch_filtered_mces.json",
            golden_path=test_resources_dir / "hightouch_filtered_mces_golden.json",
            ignore_paths=[
                # Timestamps have millisecond variations that can't be frozen
                r"root\[\d+\]\['aspect'\]\['json'\]\['upstreams'\]\[\d+\]\['auditStamp'\]\['time'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['upstreams'\]\[\d+\]\['created'\]\['time'\]",
            ],
        )
