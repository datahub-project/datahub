"""Integration tests for Hightouch source."""

from datetime import datetime
from unittest import mock

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.hightouch.data_classes import (
    HightouchDestination,
    HightouchModel,
    HightouchSource,
    HightouchSync,
    HightouchSyncRun,
)
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-01-01 12:00:00"


def get_mock_api_responses():
    """Return mock API responses for testing."""
    return {
        "sources": [
            HightouchSource(
                id="source_1",
                name="Production Snowflake",
                slug="prod-snowflake",
                type="snowflake",
                workspace_id="workspace_1",
                created_at=datetime(2023, 1, 1, 0, 0, 0),
                updated_at=datetime(2023, 1, 2, 0, 0, 0),
                configuration={"account": "test.snowflakecomputing.com"},
            )
        ],
        "models": [
            HightouchModel(
                id="model_1",
                name="Customer 360 Model",
                slug="customer-360-model",
                workspace_id="workspace_1",
                source_id="source_1",
                query_type="raw_sql",
                created_at=datetime(2023, 1, 1, 0, 0, 0),
                updated_at=datetime(2023, 1, 2, 0, 0, 0),
                primary_key="customer_id",
                description="Unified customer view from multiple sources",
                is_schema=False,
                tags={"team": "growth", "priority": "high"},
            ),
            HightouchModel(
                id="model_2",
                name="Product Events",
                slug="product-events",
                workspace_id="workspace_1",
                source_id="source_1",
                query_type="table",
                created_at=datetime(2023, 1, 1, 0, 0, 0),
                updated_at=datetime(2023, 1, 2, 0, 0, 0),
                primary_key="event_id",
                description="Product usage events",
                is_schema=False,
            ),
        ],
        "destinations": [
            HightouchDestination(
                id="dest_1",
                name="Salesforce Production",
                slug="salesforce-prod",
                type="salesforce",
                workspace_id="workspace_1",
                created_at=datetime(2023, 1, 1, 0, 0, 0),
                updated_at=datetime(2023, 1, 2, 0, 0, 0),
                configuration={"instance_url": "https://test.salesforce.com"},
            ),
            HightouchDestination(
                id="dest_2",
                name="HubSpot Marketing",
                slug="hubspot-marketing",
                type="hubspot",
                workspace_id="workspace_1",
                created_at=datetime(2023, 1, 1, 0, 0, 0),
                updated_at=datetime(2023, 1, 2, 0, 0, 0),
                configuration={},
            ),
        ],
        "syncs": [
            HightouchSync(
                id="sync_1",
                slug="customers-to-salesforce",
                workspace_id="workspace_1",
                model_id="model_1",
                destination_id="dest_1",
                created_at=datetime(2023, 1, 1, 0, 0, 0),
                updated_at=datetime(2023, 1, 2, 0, 0, 0),
                configuration={
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
                        {
                            "sourceField": "last_name",
                            "destinationField": "LastName",
                            "isPrimaryKey": False,
                        },
                    ],
                },
                schedule={"type": "interval", "interval": 3600},
                disabled=False,
                primary_key="customer_id",
            ),
            HightouchSync(
                id="sync_2",
                slug="events-to-hubspot",
                workspace_id="workspace_1",
                model_id="model_2",
                destination_id="dest_2",
                created_at=datetime(2023, 1, 1, 0, 0, 0),
                updated_at=datetime(2023, 1, 2, 0, 0, 0),
                configuration={
                    "destinationTable": "Events",
                    "columnMappings": {
                        "EventId": "event_id",
                        "UserId": "user_id",
                        "EventType": "event_type",
                        "Timestamp": "event_timestamp",
                    },
                },
                schedule={"type": "cron", "cron": "0 * * * *"},
                disabled=False,
            ),
        ],
        "sync_runs": {
            "sync_1": [
                HightouchSyncRun(
                    id="run_1",
                    sync_id="sync_1",
                    status="success",
                    started_at=datetime(2023, 12, 1, 10, 0, 0),
                    finished_at=datetime(2023, 12, 1, 10, 5, 0),
                    created_at=datetime(2023, 12, 1, 10, 0, 0),
                    completion_ratio=1.0,
                    planned_rows={"added": 1000, "changed": 250, "removed": 50},
                    successful_rows={"added": 995, "changed": 248, "removed": 50},
                    failed_rows={"added": 5, "changed": 2, "removed": 0},
                    query_size=2048000,
                ),
                HightouchSyncRun(
                    id="run_2",
                    sync_id="sync_1",
                    status="success",
                    started_at=datetime(2023, 12, 1, 11, 0, 0),
                    finished_at=datetime(2023, 12, 1, 11, 4, 30),
                    created_at=datetime(2023, 12, 1, 11, 0, 0),
                    completion_ratio=1.0,
                    planned_rows={"added": 50, "changed": 100, "removed": 10},
                    successful_rows={"added": 50, "changed": 100, "removed": 10},
                    failed_rows={"added": 0, "changed": 0, "removed": 0},
                    query_size=512000,
                ),
            ],
            "sync_2": [
                HightouchSyncRun(
                    id="run_3",
                    sync_id="sync_2",
                    status="failed",
                    started_at=datetime(2023, 12, 1, 10, 0, 0),
                    finished_at=datetime(2023, 12, 1, 10, 1, 0),
                    created_at=datetime(2023, 12, 1, 10, 0, 0),
                    completion_ratio=0.5,
                    error={
                        "message": "API rate limit exceeded",
                        "code": "RATE_LIMIT",
                    },
                    planned_rows={"added": 5000, "changed": 0, "removed": 0},
                    successful_rows={"added": 2500, "changed": 0, "removed": 0},
                    failed_rows={"added": 2500, "changed": 0, "removed": 0},
                    query_size=10240000,
                ),
            ],
        },
    }


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_hightouch_source_basic(pytestconfig, tmp_path):
    """Test basic Hightouch ingestion with mocked API."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/hightouch"

    # Create mock API responses
    mock_responses = get_mock_api_responses()

    with mock.patch(
        "datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient"
    ) as mock_api_class:
        # Configure the mock
        mock_api_instance = mock_api_class.return_value
        mock_api_instance.get_syncs.return_value = mock_responses["syncs"]
        mock_api_instance.get_models.return_value = mock_responses["models"]

        # Mock individual entity lookups
        def mock_get_model(model_id):
            return next((m for m in mock_responses["models"] if m.id == model_id), None)

        def mock_get_source(source_id):
            return next(
                (s for s in mock_responses["sources"] if s.id == source_id), None
            )

        def mock_get_destination(dest_id):
            return next(
                (d for d in mock_responses["destinations"] if d.id == dest_id), None
            )

        def mock_get_sync_runs(sync_id, limit=10):
            return mock_responses["sync_runs"].get(sync_id, [])[:limit]

        def mock_extract_field_mappings(sync):
            from datahub.ingestion.source.hightouch.data_classes import FieldMapping

            config = sync.configuration
            field_mappings = []

            if "fieldMappings" in config:
                for mapping in config["fieldMappings"]:
                    field_mappings.append(
                        FieldMapping(
                            source_field=mapping["sourceField"],
                            destination_field=mapping["destinationField"],
                            is_primary_key=mapping.get("isPrimaryKey", False),
                        )
                    )
            elif "columnMappings" in config:
                for dest_field, source_field in config["columnMappings"].items():
                    field_mappings.append(
                        FieldMapping(
                            source_field=str(source_field),
                            destination_field=str(dest_field),
                            is_primary_key=False,
                        )
                    )

            return field_mappings

        mock_api_instance.get_model_by_id.side_effect = mock_get_model
        mock_api_instance.get_source_by_id.side_effect = mock_get_source
        mock_api_instance.get_destination_by_id.side_effect = mock_get_destination
        mock_api_instance.get_sync_runs.side_effect = mock_get_sync_runs
        mock_api_instance.extract_field_mappings.side_effect = (
            mock_extract_field_mappings
        )

        # Create pipeline config
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
                        "include_model_lineage": True,
                        "include_sync_runs": True,
                        "max_sync_runs_per_sync": 5,
                        "include_column_lineage": True,
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

        # Run the pipeline
        pipeline.run()
        pipeline.raise_from_status()

        # Verify output
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"{tmp_path}/hightouch_mces.json",
            golden_path=test_resources_dir / "hightouch_mces_golden.json",
        )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_hightouch_source_with_patterns(pytestconfig, tmp_path):
    """Test Hightouch ingestion with filtering patterns."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/hightouch"

    mock_responses = get_mock_api_responses()

    with mock.patch(
        "datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient"
    ) as mock_api_class:
        mock_api_instance = mock_api_class.return_value
        mock_api_instance.get_syncs.return_value = mock_responses["syncs"]
        mock_api_instance.get_models.return_value = mock_responses["models"]

        def mock_get_model(model_id):
            return next((m for m in mock_responses["models"] if m.id == model_id), None)

        def mock_get_source(source_id):
            return next(
                (s for s in mock_responses["sources"] if s.id == source_id), None
            )

        def mock_get_destination(dest_id):
            return next(
                (d for d in mock_responses["destinations"] if d.id == dest_id), None
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
        )
