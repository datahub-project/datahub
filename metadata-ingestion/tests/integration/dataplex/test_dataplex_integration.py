"""Integration tests for Dataplex source with mocked API and golden file validation."""

import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import Mock, patch

from freezegun import freeze_time
from google.cloud import dataplex_v1

from datahub.testing import mce_helpers
from tests.test_helpers.state_helpers import run_and_get_pipeline

FROZEN_TIME = "2024-01-15 12:00:00"


def dataplex_recipe(mcp_output_path: str) -> Dict[str, Any]:
    """Create a test recipe for Dataplex ingestion."""
    return {
        "source": {
            "type": "dataplex",
            "config": {
                "project_ids": ["test-project"],
                "location": "us-central1",
                "include_entities": True,
                "include_lineage": False,  # Disable lineage for simpler test
            },
        },
        "sink": {"type": "file", "config": {"filename": mcp_output_path}},
    }


def create_mock_lake(
    lake_id: str, display_name: Optional[str] = None, description: Optional[str] = None
) -> Mock:
    """Create a mock Dataplex Lake."""
    lake = Mock(spec=dataplex_v1.Lake)
    lake.name = f"projects/test-project/locations/us-central1/lakes/{lake_id}"
    lake.display_name = display_name or f"{lake_id} Display"
    lake.description = description or f"Description for {lake_id}"
    lake.create_time = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    lake.update_time = datetime.datetime(2024, 1, 10, tzinfo=datetime.timezone.utc)
    lake.labels = {"env": "test", "team": "data"}
    return lake


def create_mock_zone(
    lake_id: str,
    zone_id: str,
    zone_type: int = dataplex_v1.Zone.Type.RAW,
) -> Mock:
    """Create a mock Dataplex Zone."""
    zone = Mock(spec=dataplex_v1.Zone)
    zone.name = (
        f"projects/test-project/locations/us-central1/lakes/{lake_id}/zones/{zone_id}"
    )
    zone.display_name = f"{zone_id} Display"
    zone.description = f"Description for {zone_id}"
    zone.type_ = zone_type
    zone.create_time = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc)
    zone.update_time = datetime.datetime(2024, 1, 11, tzinfo=datetime.timezone.utc)
    zone.labels = {"zone_type": "raw", "owner": "data-team"}
    return zone


def create_mock_asset(
    lake_id: str,
    zone_id: str,
    asset_id: str,
    resource_type: int = dataplex_v1.Asset.ResourceSpec.Type.BIGQUERY_DATASET,
) -> Mock:
    """Create a mock Dataplex Asset."""
    asset = Mock(spec=dataplex_v1.Asset)
    asset.name = f"projects/test-project/locations/us-central1/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}"
    asset.display_name = f"{asset_id} Display"
    asset.description = f"Description for {asset_id}"

    # Create resource spec
    resource_spec = Mock()
    resource_spec.type_ = resource_type
    resource_spec.name = f"test_dataset_{asset_id}"
    asset.resource_spec = resource_spec

    asset.create_time = datetime.datetime(2024, 1, 3, tzinfo=datetime.timezone.utc)
    asset.update_time = datetime.datetime(2024, 1, 12, tzinfo=datetime.timezone.utc)
    asset.labels = {"asset_type": "dataset"}
    return asset


def create_mock_entity(
    lake_id: str, zone_id: str, asset_id: str, entity_id: str
) -> Mock:
    """Create a mock Dataplex Entity."""
    entity = Mock(spec=dataplex_v1.Entity)
    entity.name = f"projects/test-project/locations/us-central1/lakes/{lake_id}/zones/{zone_id}/entities/{entity_id}"
    entity.id = entity_id
    entity.asset = asset_id
    entity.data_path = f"gs://test-bucket/data/{entity_id}"
    entity.system = dataplex_v1.StorageSystem.BIGQUERY
    entity.type_ = dataplex_v1.Entity.Type.TABLE
    entity.format_ = Mock()
    entity.format_.format_ = dataplex_v1.StorageFormat.Format.PARQUET

    # Create schema
    schema = Mock()
    schema_field = Mock()
    schema_field.name = "test_column"
    schema_field.type_ = dataplex_v1.Schema.Type.STRING
    schema_field.mode = dataplex_v1.Schema.Mode.REQUIRED
    schema_field.description = "Test column description"
    schema.fields = [schema_field]
    entity.schema_ = schema

    entity.create_time = datetime.datetime(2024, 1, 4, tzinfo=datetime.timezone.utc)
    entity.update_time = datetime.datetime(2024, 1, 13, tzinfo=datetime.timezone.utc)
    return entity


@freeze_time(FROZEN_TIME)
@patch("google.auth.default")
@patch("google.cloud.dataplex_v1.DataplexServiceClient")
@patch("google.cloud.dataplex_v1.MetadataServiceClient")
@patch("google.cloud.dataplex_v1.CatalogServiceClient")
@patch("google.cloud.datacatalog.lineage_v1.LineageClient")
def test_dataplex_integration_with_golden_file(
    mock_lineage_client_class,
    mock_catalog_client_class,
    mock_metadata_client_class,
    mock_dataplex_client_class,
    mock_google_auth,
    pytestconfig,
    tmp_path,
):
    """Test Dataplex source with mocked API and golden file validation."""

    # Mock Google Application Default Credentials
    mock_credentials = Mock()
    mock_google_auth.return_value = (mock_credentials, "test-project")

    # Setup mock clients
    mock_dataplex_client = Mock()
    mock_dataplex_client_class.return_value = mock_dataplex_client

    mock_metadata_client = Mock()
    mock_metadata_client_class.return_value = mock_metadata_client

    mock_catalog_client = Mock()
    mock_catalog_client_class.return_value = mock_catalog_client
    # Mock empty entry groups to skip Entries API processing
    mock_catalog_client.list_entry_groups.return_value = []

    mock_lineage_client = Mock()
    mock_lineage_client_class.return_value = mock_lineage_client

    # Create mock data hierarchy
    mock_lake = create_mock_lake("test-lake-1", "Test Lake", "A test lake")
    mock_zone = create_mock_zone(
        "test-lake-1",
        "test-zone-1",
        dataplex_v1.Zone.Type.RAW,  # type: ignore[arg-type]
    )
    mock_asset = create_mock_asset(
        "test-lake-1",
        "test-zone-1",
        "test-asset-1",
        dataplex_v1.Asset.ResourceSpec.Type.BIGQUERY_DATASET,  # type: ignore[arg-type]
    )
    mock_entity = create_mock_entity(
        "test-lake-1", "test-zone-1", "test-asset-1", "test-entity-1"
    )

    # Configure mock responses
    mock_dataplex_client.list_lakes.return_value = [mock_lake]
    mock_dataplex_client.list_zones.return_value = [mock_zone]
    mock_dataplex_client.list_assets.return_value = [mock_asset]
    mock_dataplex_client.get_asset.return_value = mock_asset

    # MetadataServiceClient is used for listing entities
    mock_metadata_client.list_entities.return_value = [mock_entity]

    # Setup paths
    mcp_output_path = tmp_path / "dataplex_mces.json"
    mcp_golden_path = (
        Path(__file__).parent / "golden" / "dataplex_integration_golden.json"
    )

    # Run pipeline
    pipeline_config = dataplex_recipe(str(mcp_output_path))
    run_and_get_pipeline(pipeline_config)

    # Validate against golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(mcp_output_path),
        golden_path=str(mcp_golden_path),
    )


@freeze_time(FROZEN_TIME)
@patch("google.auth.default")
@patch("google.cloud.dataplex_v1.DataplexServiceClient")
@patch("google.cloud.dataplex_v1.MetadataServiceClient")
@patch("google.cloud.dataplex_v1.CatalogServiceClient")
@patch("google.cloud.datacatalog.lineage_v1.LineageClient")
def test_dataplex_integration_multiple_lakes(
    mock_lineage_client_class,
    mock_catalog_client_class,
    mock_metadata_client_class,
    mock_dataplex_client_class,
    mock_google_auth,
    pytestconfig,
    tmp_path,
):
    """Test Dataplex source with multiple lakes."""

    # Mock Google Application Default Credentials
    mock_credentials = Mock()
    mock_google_auth.return_value = (mock_credentials, "test-project")

    # Setup mock clients
    mock_dataplex_client = Mock()
    mock_dataplex_client_class.return_value = mock_dataplex_client

    mock_metadata_client = Mock()
    mock_metadata_client_class.return_value = mock_metadata_client

    mock_catalog_client = Mock()
    mock_catalog_client_class.return_value = mock_catalog_client
    # Mock empty entry groups to skip Entries API processing
    mock_catalog_client.list_entry_groups.return_value = []

    mock_lineage_client = Mock()
    mock_lineage_client_class.return_value = mock_lineage_client

    # Create multiple lakes
    mock_lake1 = create_mock_lake("test-lake-1", "Test Lake 1", "First test lake")
    mock_lake2 = create_mock_lake("test-lake-2", "Test Lake 2", "Second test lake")

    # Create zones for each lake
    mock_zone1 = create_mock_zone("test-lake-1", "raw-zone")
    mock_zone2 = create_mock_zone(
        "test-lake-2",
        "curated-zone",
        dataplex_v1.Zone.Type.CURATED,  # type: ignore[arg-type]
    )

    # Configure mock responses
    mock_dataplex_client.list_lakes.return_value = [mock_lake1, mock_lake2]

    def list_zones_side_effect(*args, **kwargs):
        parent = kwargs.get("parent", "")
        if "test-lake-1" in parent:
            return [mock_zone1]
        elif "test-lake-2" in parent:
            return [mock_zone2]
        return []

    mock_dataplex_client.list_zones.side_effect = list_zones_side_effect
    mock_dataplex_client.list_assets.return_value = []

    # MetadataServiceClient is used for listing entities
    mock_metadata_client.list_entities.return_value = []

    # Setup paths
    mcp_output_path = tmp_path / "dataplex_multi_lake_mces.json"
    mcp_golden_path = (
        Path(__file__).parent / "golden" / "dataplex_multi_lake_golden.json"
    )

    # Run pipeline
    pipeline_config = dataplex_recipe(str(mcp_output_path))
    run_and_get_pipeline(pipeline_config)

    # Validate against golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(mcp_output_path),
        golden_path=str(mcp_golden_path),
    )
