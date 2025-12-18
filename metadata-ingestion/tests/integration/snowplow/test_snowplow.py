"""Integration tests for Snowplow source with golden files."""

import json
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-01-01 00:00:00"


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_snowplow_ingest(pytestconfig, tmp_path, mock_time):
    """
    Test Snowplow ingestion with mocked API responses.

    This test:
    1. Mocks the Snowplow BDP API responses
    2. Runs ingestion to a file sink
    3. Compares output with golden file
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowplow"
    golden_file = test_resources_dir / "golden_files/snowplow_mces_golden.json"

    # Load mock data with ownership
    with open(test_resources_dir / "fixtures/data_structures_with_ownership.json") as f:
        mock_data_structures = json.load(f)

    # Mock the Snowplow BDP client
    with patch(
        "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
    ) as mock_client_class:
        # Create mock client instance
        mock_client = mock_client_class.return_value

        # Mock authentication (prevent real HTTP calls in __init__)
        mock_client._authenticate = lambda: None
        mock_client._jwt_token = "mock_jwt_token"
        mock_client.session = None  # Mock session

        # Mock get_data_structures API call
        # Parse directly as list of DataStructure objects (API returns direct array)
        from datahub.ingestion.source.snowplow.snowplow_models import DataStructure

        data_structures = [
            DataStructure.model_validate(ds) for ds in mock_data_structures
        ]
        mock_client.get_data_structures.return_value = data_structures

        # Mock test_connection
        mock_client.test_connection.return_value = True

        # Mock get_users for ownership resolution
        from datahub.ingestion.source.snowplow.snowplow_models import User

        mock_users = [
            User(id="user1", email="ryan@company.com", name="Ryan Smith"),
            User(id="user2", email="jane@company.com", name="Jane Doe"),
            User(id="user3", email="alice@company.com", name="Alice Johnson"),
            User(id="user4", email="bob@company.com", name="Bob Wilson"),
        ]
        mock_client.get_users.return_value = mock_users

        # Run ingestion
        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "snowplow",
                    "config": {
                        "bdp_connection": {
                            "organization_id": "test-org-uuid",
                            "api_key_id": "test-key-id",
                            "api_key": "test-secret",
                        },
                        "extract_event_specifications": False,
                        "extract_tracking_scenarios": False,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(tmp_path / "snowplow_mces.json")},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    # Verify golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "snowplow_mces.json",
        golden_path=golden_file,
        ignore_paths=[
            # Ignore timestamps and run IDs
            r"root\[\d+\]\['proposedSnapshot'\]\['com\.linkedin\.pegasus2avro\.metadata\.snapshot\.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com\.linkedin\.pegasus2avro\.schema\.SchemaMetadata'\]\['created'\]",
            r"root\[\d+\]\['systemMetadata'\]\['lastObserved'\]",
            r"root\[\d+\]\['systemMetadata'\]\['runId'\]",
        ],
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_snowplow_event_specs_and_tracking_scenarios(pytestconfig, tmp_path, mock_time):
    """
    Test Snowplow ingestion with event specifications and tracking scenarios.

    This test verifies:
    1. Event specifications are extracted
    2. Tracking scenarios are extracted
    3. Lineage from event specs to schemas is created
    4. Container relationships from tracking scenarios to event specs are created
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowplow"
    golden_file = test_resources_dir / "golden_files/snowplow_event_specs_golden.json"

    # Load mock data
    with open(test_resources_dir / "fixtures/data_structures_with_ownership.json") as f:
        mock_data_structures = json.load(f)

    with open(test_resources_dir / "fixtures/event_specifications_response.json") as f:
        mock_event_specs_response = json.load(f)

    with open(test_resources_dir / "fixtures/tracking_scenarios_response.json") as f:
        mock_tracking_scenarios_response = json.load(f)

    # Mock the Snowplow BDP client
    with patch(
        "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
    ) as mock_client_class:
        # Create mock client instance
        mock_client = mock_client_class.return_value

        # Mock authentication
        mock_client._authenticate = lambda: None
        mock_client._jwt_token = "mock_jwt_token"
        mock_client.session = None

        # Mock get_data_structures
        from datahub.ingestion.source.snowplow.snowplow_models import (
            DataStructure,
            EventSpecification,
            TrackingScenario,
            User,
        )

        data_structures = [
            DataStructure.model_validate(ds) for ds in mock_data_structures
        ]
        mock_client.get_data_structures.return_value = data_structures

        # Mock get_event_specifications
        event_specs = [
            EventSpecification.model_validate(es)
            for es in mock_event_specs_response["data"]
        ]
        mock_client.get_event_specifications.return_value = event_specs

        # Mock get_tracking_scenarios
        tracking_scenarios = [
            TrackingScenario.model_validate(ts)
            for ts in mock_tracking_scenarios_response["data"]
        ]
        mock_client.get_tracking_scenarios.return_value = tracking_scenarios

        # Mock get_pipelines (needed for event-specific DataFlows)
        from datahub.ingestion.source.snowplow.snowplow_models import (
            Pipeline as SnowplowPipelineModel,
            PipelineConfig,
        )

        mock_pipeline = SnowplowPipelineModel(
            id="pipeline-test-id",
            name="Default",
            status="active",
            label="Test Pipeline",
            workspace_id="workspace-123",
            config=PipelineConfig(
                collector_endpoints=["collector.example.com"],
                incomplete_stream_deployed=False,
                enrich_accept_invalid=False,
            ),
        )
        mock_client.get_pipelines.return_value = [mock_pipeline]

        # Mock get_enrichments (return empty for this test)
        mock_client.get_enrichments.return_value = []

        # Mock test_connection
        mock_client.test_connection.return_value = True

        # Mock get_users for ownership resolution
        mock_users = [
            User(id="user1", email="ryan@company.com", name="Ryan Smith"),
            User(id="user2", email="jane@company.com", name="Jane Doe"),
            User(id="user3", email="alice@company.com", name="Alice Johnson"),
            User(id="user4", email="bob@company.com", name="Bob Wilson"),
        ]
        mock_client.get_users.return_value = mock_users

        # Run ingestion with event specs and tracking scenarios enabled
        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "snowplow",
                    "config": {
                        "bdp_connection": {
                            "organization_id": "test-org-uuid",
                            "api_key_id": "test-key-id",
                            "api_key": "test-secret",
                        },
                        "extract_event_specifications": True,
                        "extract_tracking_scenarios": True,
                        "extract_pipelines": True,  # Enable event-specific DataFlows
                        "extract_enrichments": False,  # Disable enrichments for this test
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": str(tmp_path / "snowplow_event_specs_mces.json")
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    # Verify golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "snowplow_event_specs_mces.json",
        golden_path=golden_file,
        ignore_paths=[
            # Ignore timestamps and run IDs
            r"root\[\d+\]\['proposedSnapshot'\]\['com\.linkedin\.pegasus2avro\.metadata\.snapshot\.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com\.linkedin\.pegasus2avro\.schema\.SchemaMetadata'\]\['created'\]",
            r"root\[\d+\]\['systemMetadata'\]\['lastObserved'\]",
            r"root\[\d+\]\['systemMetadata'\]\['runId'\]",
        ],
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_snowplow_data_products(pytestconfig, tmp_path, mock_time):
    """
    Test Snowplow ingestion with data products.

    This test verifies:
    1. Data products are extracted
    2. Ownership from data products is extracted
    3. Container relationships from data products to event specs are created
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowplow"
    golden_file = test_resources_dir / "golden_files/snowplow_data_products_golden.json"

    # Load mock data
    with open(test_resources_dir / "fixtures/data_structures_with_ownership.json") as f:
        mock_data_structures = json.load(f)

    with open(test_resources_dir / "fixtures/event_specifications_response.json") as f:
        mock_event_specs_response = json.load(f)

    with open(test_resources_dir / "fixtures/tracking_scenarios_response.json") as f:
        mock_tracking_scenarios_response = json.load(f)

    with open(test_resources_dir / "fixtures/data_products_response.json") as f:
        mock_data_products_response = json.load(f)

    # Mock the Snowplow BDP client
    with patch(
        "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
    ) as mock_client_class:
        # Create mock client instance
        mock_client = mock_client_class.return_value

        # Mock authentication
        mock_client._authenticate = lambda: None
        mock_client._jwt_token = "mock_jwt_token"
        mock_client.session = None

        # Mock get_data_structures
        from datahub.ingestion.source.snowplow.snowplow_models import (
            DataProduct,
            DataStructure,
            EventSpecification,
            TrackingScenario,
            User,
        )

        data_structures = [
            DataStructure.model_validate(ds) for ds in mock_data_structures
        ]
        mock_client.get_data_structures.return_value = data_structures

        # Mock get_event_specifications
        event_specs = [
            EventSpecification.model_validate(es)
            for es in mock_event_specs_response["data"]
        ]
        mock_client.get_event_specifications.return_value = event_specs

        # Mock get_tracking_scenarios
        tracking_scenarios = [
            TrackingScenario.model_validate(ts)
            for ts in mock_tracking_scenarios_response["data"]
        ]
        mock_client.get_tracking_scenarios.return_value = tracking_scenarios

        # Mock get_data_products
        data_products = [
            DataProduct.model_validate(dp) for dp in mock_data_products_response["data"]
        ]
        mock_client.get_data_products.return_value = data_products

        # Mock get_pipelines (needed for event-specific DataFlows)
        from datahub.ingestion.source.snowplow.snowplow_models import (
            Pipeline as SnowplowPipelineModel,
            PipelineConfig,
        )

        mock_pipeline = SnowplowPipelineModel(
            id="pipeline-test-id",
            name="Default",
            status="active",
            label="Test Pipeline",
            workspace_id="workspace-123",
            config=PipelineConfig(
                collector_endpoints=["collector.example.com"],
                incomplete_stream_deployed=False,
                enrich_accept_invalid=False,
            ),
        )
        mock_client.get_pipelines.return_value = [mock_pipeline]

        # Mock get_enrichments (return empty for this test)
        mock_client.get_enrichments.return_value = []

        # Mock test_connection
        mock_client.test_connection.return_value = True

        # Mock get_users for ownership resolution
        mock_users = [
            User(id="user1", email="ryan@company.com", name="Ryan Smith"),
            User(id="user2", email="jane@company.com", name="Jane Doe"),
            User(id="user3", email="alice@company.com", name="Alice Johnson"),
            User(id="user4", email="bob@company.com", name="Bob Wilson"),
        ]
        mock_client.get_users.return_value = mock_users

        # Run ingestion with data products enabled
        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "snowplow",
                    "config": {
                        "bdp_connection": {
                            "organization_id": "test-org-uuid",
                            "api_key_id": "test-key-id",
                            "api_key": "test-secret",
                        },
                        "extract_event_specifications": True,
                        "extract_tracking_scenarios": True,
                        "extract_data_products": True,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": str(tmp_path / "snowplow_data_products_mces.json")
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    # Verify golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "snowplow_data_products_mces.json",
        golden_path=golden_file,
        ignore_paths=[
            # Ignore timestamps and run IDs
            r"root\[\d+\]\['proposedSnapshot'\]\['com\.linkedin\.pegasus2avro\.metadata\.snapshot\.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com\.linkedin\.pegasus2avro\.schema\.SchemaMetadata'\]\['created'\]",
            r"root\[\d+\]\['systemMetadata'\]\['lastObserved'\]",
            r"root\[\d+\]\['systemMetadata'\]\['runId'\]",
        ],
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_snowplow_pipelines(pytestconfig, tmp_path, mock_time):
    """
    Test Snowplow ingestion with pipelines as DataFlow entities.

    This test verifies:
    1. Pipelines are extracted as DataFlow entities
    2. Pipeline configuration is captured in custom properties
    3. Pipeline status is included
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowplow"
    golden_file = test_resources_dir / "golden_files/snowplow_pipelines_golden.json"

    # Load mock data
    with open(test_resources_dir / "fixtures/data_structures_with_ownership.json") as f:
        mock_data_structures = json.load(f)

    with open(test_resources_dir / "fixtures/pipelines_response.json") as f:
        mock_pipelines_response = json.load(f)

    # Mock the Snowplow BDP client
    with patch(
        "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
    ) as mock_client_class:
        # Create mock client instance
        mock_client = mock_client_class.return_value

        # Mock authentication
        mock_client._authenticate = lambda: None
        mock_client._jwt_token = "mock_jwt_token"
        mock_client.session = None

        # Mock get_data_structures
        from datahub.ingestion.source.snowplow.snowplow_models import (
            DataStructure,
            Pipeline as SnowplowPipeline,
        )

        data_structures = [
            DataStructure.model_validate(ds) for ds in mock_data_structures
        ]
        mock_client.get_data_structures.return_value = data_structures

        # Mock get_pipelines
        pipelines = [
            SnowplowPipeline.model_validate(p)
            for p in mock_pipelines_response["pipelines"]
        ]
        mock_client.get_pipelines.return_value = pipelines

        # Mock other methods to return empty lists
        mock_client.get_event_specifications.return_value = []
        mock_client.get_tracking_scenarios.return_value = []
        mock_client.get_data_products.return_value = []

        # Mock user lookups
        from datahub.ingestion.source.snowplow.snowplow_models import User

        mock_users = [
            User(id="user1", email="ryan@company.com", name="Ryan Smith"),
            User(id="user2", email="jane@company.com", name="Jane Doe"),
        ]
        mock_client.get_users.return_value = mock_users

        # Run ingestion with pipelines enabled
        pipeline_run = Pipeline.create(
            {
                "source": {
                    "type": "snowplow",
                    "config": {
                        "bdp_connection": {
                            "organization_id": "test-org-uuid",
                            "api_key_id": "test-key-id",
                            "api_key": "test-secret",
                        },
                        "extract_event_specifications": False,
                        "extract_tracking_scenarios": False,
                        "extract_data_products": False,
                        "extract_pipelines": True,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": str(tmp_path / "snowplow_pipelines_mces.json")
                    },
                },
            }
        )
        pipeline_run.run()
        pipeline_run.raise_from_status()

    # Verify golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "snowplow_pipelines_mces.json",
        golden_path=golden_file,
        ignore_paths=[
            # Ignore timestamps and run IDs
            r"root\[\d+\]\['proposedSnapshot'\]\['com\.linkedin\.pegasus2avro\.metadata\.snapshot\.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com\.linkedin\.pegasus2avro\.schema\.SchemaMetadata'\]\['created'\]",
            r"root\[\d+\]\['systemMetadata'\]\['lastObserved'\]",
            r"root\[\d+\]\['systemMetadata'\]\['runId'\]",
        ],
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_snowplow_enrichments(pytestconfig, tmp_path, mock_time):
    """
    Test Snowplow ingestion with enrichments as DataJob entities.

    This test verifies:
    1. Enrichments are extracted as DataJob entities
    2. Enrichments are linked to their parent pipeline DataFlow
    3. Enrichment configuration and status are captured
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowplow"
    golden_file = test_resources_dir / "golden_files/snowplow_enrichments_golden.json"

    # Load mock data
    with open(test_resources_dir / "fixtures/data_structures_with_ownership.json") as f:
        mock_data_structures = json.load(f)

    with open(test_resources_dir / "fixtures/pipelines_response.json") as f:
        mock_pipelines_response = json.load(f)

    with open(test_resources_dir / "fixtures/enrichments_response.json") as f:
        mock_enrichments_response = json.load(f)

    # Mock the Snowplow BDP client
    with patch(
        "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
    ) as mock_client_class:
        # Create mock client instance
        mock_client = mock_client_class.return_value

        # Mock authentication
        mock_client._authenticate = lambda: None
        mock_client._jwt_token = "mock_jwt_token"
        mock_client.session = None

        # Mock get_data_structures
        from datahub.ingestion.source.snowplow.snowplow_models import (
            DataStructure,
            Enrichment,
            Pipeline as SnowplowPipeline,
        )

        data_structures = [
            DataStructure.model_validate(ds) for ds in mock_data_structures
        ]
        mock_client.get_data_structures.return_value = data_structures

        # Mock get_pipelines
        pipelines = [
            SnowplowPipeline.model_validate(p)
            for p in mock_pipelines_response["pipelines"]
        ]
        mock_client.get_pipelines.return_value = pipelines

        # Mock get_enrichments
        enrichments = [Enrichment.model_validate(e) for e in mock_enrichments_response]
        mock_client.get_enrichments.return_value = enrichments

        # Mock get_event_specifications (needed for event-specific enrichments)
        with open(
            test_resources_dir / "fixtures/event_specifications_response.json"
        ) as f:
            mock_event_specs_response = json.load(f)

        from datahub.ingestion.source.snowplow.snowplow_models import EventSpecification

        event_specs = [
            EventSpecification.model_validate(es)
            for es in mock_event_specs_response["data"]
        ]
        mock_client.get_event_specifications.return_value = event_specs

        # Mock other methods to return empty lists
        mock_client.get_tracking_scenarios.return_value = []
        mock_client.get_data_products.return_value = []

        # Mock user lookups
        from datahub.ingestion.source.snowplow.snowplow_models import User

        mock_users = [
            User(id="user1", email="ryan@company.com", name="Ryan Smith"),
            User(id="user2", email="jane@company.com", name="Jane Doe"),
        ]
        mock_client.get_users.return_value = mock_users

        # Mock get_organization (for warehouse destination discovery)
        with open(test_resources_dir / "fixtures/organization_response.json") as f:
            mock_organization_response = json.load(f)

        from datahub.ingestion.source.snowplow.snowplow_models import Organization

        organization = Organization.model_validate(mock_organization_response)
        mock_client.get_organization.return_value = organization

        # Run ingestion with enrichments enabled
        pipeline_run = Pipeline.create(
            {
                "source": {
                    "type": "snowplow",
                    "config": {
                        "bdp_connection": {
                            "organization_id": "test-org-uuid",
                            "api_key_id": "test-key-id",
                            "api_key": "test-secret",
                        },
                        "extract_event_specifications": True,  # Needed for event-specific enrichments
                        "extract_tracking_scenarios": False,
                        "extract_data_products": False,
                        "extract_pipelines": True,
                        "extract_enrichments": True,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": str(tmp_path / "snowplow_enrichments_mces.json")
                    },
                },
            }
        )
        pipeline_run.run()
        pipeline_run.raise_from_status()

    # Verify golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "snowplow_enrichments_mces.json",
        golden_path=golden_file,
        ignore_paths=[
            # Ignore timestamps and run IDs
            r"root\[\d+\]\['proposedSnapshot'\]\['com\.linkedin\.pegasus2avro\.metadata\.snapshot\.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com\.linkedin\.pegasus2avro\.schema\.SchemaMetadata'\]\['created'\]",
            r"root\[\d+\]\['systemMetadata'\]\['lastObserved'\]",
            r"root\[\d+\]\['systemMetadata'\]\['runId'\]",
        ],
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
@pytest.mark.slow
def test_snowplow_iglu_autodiscovery(pytestconfig, tmp_path, mock_time):
    """
    Test Snowplow ingestion with Iglu-only mode (open-source Snowplow).

    This test:
    1. Requires Docker with Iglu Server running (docker-compose.iglu.yml)
    2. Uses automatic schema discovery via /api/schemas endpoint
    3. Verifies schemas are extracted from Iglu registry

    Setup:
        cd tests/integration/snowplow/setup
        docker compose -f docker-compose.iglu.yml up -d
        python setup_iglu.py

    Teardown:
        cd tests/integration/snowplow/setup
        docker compose -f docker-compose.iglu.yml down -v
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowplow"
    golden_file = (
        test_resources_dir / "golden_files/snowplow_iglu_autodiscovery_golden.json"
    )

    # Check if Iglu Server is running
    import requests

    try:
        response = requests.get("http://localhost:8081/api/meta/health", timeout=2)
        if response.status_code != 200:
            pytest.skip(
                "Iglu Server not running. Start with: cd tests/integration/snowplow/setup && docker compose -f docker-compose.iglu.yml up -d && python setup_iglu.py"
            )
    except requests.exceptions.RequestException:
        pytest.skip(
            "Iglu Server not running. Start with: cd tests/integration/snowplow/setup && docker compose -f docker-compose.iglu.yml up -d && python setup_iglu.py"
        )

    # Run ingestion with Iglu-only mode
    pipeline = Pipeline.create(
        {
            "source": {
                "type": "snowplow",
                "config": {
                    "iglu_connection": {
                        "iglu_server_url": "http://localhost:8081",
                    },
                    "schema_types_to_extract": ["event", "entity"],
                    "extract_event_specifications": False,
                    "extract_tracking_scenarios": False,
                    "env": "TEST",
                    "platform_instance": "iglu_autodiscovery_test",
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": str(tmp_path / "snowplow_iglu_mces.json")},
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    # Verify golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "snowplow_iglu_mces.json",
        golden_path=golden_file,
        ignore_paths=[
            # Ignore timestamps and run IDs
            r"root\[\d+\]\['proposedSnapshot'\]\['com\.linkedin\.pegasus2avro\.metadata\.snapshot\.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com\.linkedin\.pegasus2avro\.schema\.SchemaMetadata'\]\['created'\]",
            r"root\[\d+\]\['systemMetadata'\]\['lastObserved'\]",
            r"root\[\d+\]\['systemMetadata'\]\['runId'\]",
        ],
    )


@pytest.mark.integration
def test_snowplow_config_validation():
    """Test that invalid configurations are rejected."""
    # Missing required connection
    with pytest.raises(Exception, match="Either bdp_connection or iglu_connection"):
        Pipeline.create(
            {
                "source": {
                    "type": "snowplow",
                    "config": {},
                },
                "sink": {"type": "file", "config": {"filename": "/dev/null"}},
            }
        )
