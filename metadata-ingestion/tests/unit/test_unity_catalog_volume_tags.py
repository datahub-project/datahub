from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from databricks.sdk.service.catalog import CatalogType, VolumeType

from datahub.api.entities.external.unity_catalog_external_entites import UnityCatalogTag
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.proxy_types import (
    Catalog,
    Metastore,
    Schema,
    Volume,
)
from datahub.ingestion.source.unity.source import UnityCatalogSource
from datahub.metadata.com.linkedin.pegasus2avro.common import GlobalTags
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeProposal


class TestUnityCatalogVolumeTags:
    """Test Unity Catalog volume tag extraction functionality."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock config with volume tag processing enabled."""
        return UnityCatalogSourceConfig.parse_obj(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
                "include_volumes": True,
                "include_external_lineage": True,
                "include_tags": True,  # Enable tags
                "env": "PROD",
            }
        )

    @pytest.fixture
    def mock_config_tags_disabled(self):
        """Create a mock config with tags disabled."""
        return UnityCatalogSourceConfig.parse_obj(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
                "include_volumes": True,
                "include_external_lineage": True,
                "include_tags": False,  # Disable tags
                "env": "PROD",
            }
        )

    @pytest.fixture
    def sample_volume_with_tags(self):
        """Create a sample volume for testing tag extraction."""
        metastore = Metastore(
            id="metastore_id",
            name="test_metastore",
            comment=None,
            global_metastore_id="global_id",
            metastore_id="metastore_id",
            owner="admin",
            cloud="aws",
            region="us-west-2",
        )

        catalog = Catalog(
            id="catalog_id",
            name="test_catalog",
            comment="Test catalog",
            metastore=metastore,
            owner="admin",
            type=CatalogType.MANAGED_CATALOG,
        )

        schema = Schema(
            id="schema_id",
            name="test_schema",
            comment="Test schema",
            catalog=catalog,
            owner="admin",
        )

        return Volume(
            id="volume_id",
            name="test_volume",
            comment="Test volume with tags",
            schema=schema,
            volume_type=VolumeType.EXTERNAL,
            storage_location="s3://my-bucket/volume-data/",
            owner="admin",
            created_at=datetime(2023, 8, 13, 10, 0, 0),
            created_by="creator@example.com",
            updated_at=datetime(2023, 8, 13, 10, 30, 0),
            updated_by="updater@example.com",
        )

    @pytest.fixture
    def sample_volume_tags(self):
        """Create sample Unity Catalog tags for testing."""
        return [
            UnityCatalogTag(key="environment", value="production"),
            UnityCatalogTag(key="team", value="data-engineering"),
            UnityCatalogTag(key="cost_center", value="analytics"),
        ]

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch.object(UnityCatalogSource, "gen_schema_key")
    @patch.object(UnityCatalogSource, "get_owner_urn", return_value=None)
    @patch.object(UnityCatalogSource, "_get_domain_aspect", return_value=None)
    def test_volume_tag_extraction_enabled(
        self,
        mock_domain,
        mock_owner,
        mock_schema_key,
        mock_proxy,
        mock_config,
        sample_volume_with_tags,
        sample_volume_tags,
    ):
        """Test that volume tags are extracted when include_tags=True."""
        # Setup mocks
        mock_proxy_instance = Mock()
        mock_proxy.return_value = mock_proxy_instance
        mock_proxy_instance.volumes.return_value = [sample_volume_with_tags]

        # Mock the volume tag extraction
        mock_proxy_instance.get_volume_tags.return_value = {
            "test_catalog.test_schema.test_volume": sample_volume_tags
        }

        # Mock schema key to avoid container creation
        mock_schema_key.return_value = Mock()

        # Create source with tags enabled
        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource(ctx=ctx, config=mock_config)
        source.unity_catalog_api_proxy = mock_proxy_instance

        # Process volumes
        work_units = list(source.process_volumes(sample_volume_with_tags.schema))

        # Verify that volume tags are extracted
        mock_proxy_instance.get_volume_tags.assert_called_once_with("test_catalog")

        # Find GlobalTags work units
        tag_work_units = [
            wu
            for wu in work_units
            if isinstance(wu, MetadataWorkUnit)
            and wu.metadata
            and isinstance(
                wu.metadata, (MetadataChangeProposal, MetadataChangeProposalWrapper)
            )
            and wu.metadata.aspect
            and isinstance(wu.metadata.aspect, GlobalTags)
        ]

        # Should have tags work unit
        assert len(tag_work_units) == 1, (
            f"Expected 1 tags work unit, got {len(tag_work_units)}"
        )

        tags_work_unit = tag_work_units[0]
        # Type narrowing - we know this is the correct type from the filter above
        assert isinstance(
            tags_work_unit.metadata,
            (MetadataChangeProposal, MetadataChangeProposalWrapper),
        )
        assert isinstance(tags_work_unit.metadata.aspect, GlobalTags)

        # Verify the tags are correctly applied
        global_tags = tags_work_unit.metadata.aspect
        assert len(global_tags.tags) == 3

        # Verify tag content (order may vary)
        tag_urns = [tag_assoc.tag for tag_assoc in global_tags.tags]
        expected_tag_urns = [
            "urn:li:tag:environment:production",
            "urn:li:tag:team:data-engineering",
            "urn:li:tag:cost_center:analytics",
        ]

        for expected_urn in expected_tag_urns:
            assert expected_urn in tag_urns, (
                f"Expected tag URN {expected_urn} not found in {tag_urns}"
            )

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch.object(UnityCatalogSource, "gen_schema_key")
    @patch.object(UnityCatalogSource, "get_owner_urn", return_value=None)
    @patch.object(UnityCatalogSource, "_get_domain_aspect", return_value=None)
    def test_volume_tag_extraction_disabled(
        self,
        mock_domain,
        mock_owner,
        mock_schema_key,
        mock_proxy,
        mock_config_tags_disabled,
        sample_volume_with_tags,
    ):
        """Test that volume tags are not extracted when include_tags=False."""
        # Setup mocks
        mock_proxy_instance = Mock()
        mock_proxy.return_value = mock_proxy_instance
        mock_proxy_instance.volumes.return_value = [sample_volume_with_tags]

        # Mock schema key to avoid container creation
        mock_schema_key.return_value = Mock()

        # Create source with tags disabled
        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource(ctx=ctx, config=mock_config_tags_disabled)
        source.unity_catalog_api_proxy = mock_proxy_instance

        # Process volumes
        work_units = list(source.process_volumes(sample_volume_with_tags.schema))

        # Verify that volume tags are NOT extracted
        mock_proxy_instance.get_volume_tags.assert_not_called()

        # Find GlobalTags work units
        tag_work_units = [
            wu
            for wu in work_units
            if isinstance(wu, MetadataWorkUnit)
            and wu.metadata
            and isinstance(
                wu.metadata, (MetadataChangeProposal, MetadataChangeProposalWrapper)
            )
            and wu.metadata.aspect
            and isinstance(wu.metadata.aspect, GlobalTags)
        ]

        # Should have NO tags work units when tags are disabled
        assert len(tag_work_units) == 0, (
            f"Expected 0 tags work units when tags disabled, got {len(tag_work_units)}"
        )

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch.object(UnityCatalogSource, "gen_schema_key")
    @patch.object(UnityCatalogSource, "get_owner_urn", return_value=None)
    @patch.object(UnityCatalogSource, "_get_domain_aspect", return_value=None)
    def test_volume_tag_extraction_no_tags_found(
        self,
        mock_domain,
        mock_owner,
        mock_schema_key,
        mock_proxy,
        mock_config,
        sample_volume_with_tags,
    ):
        """Test behavior when no tags are found for a volume."""
        # Setup mocks
        mock_proxy_instance = Mock()
        mock_proxy.return_value = mock_proxy_instance
        mock_proxy_instance.volumes.return_value = [sample_volume_with_tags]

        # Mock empty tags result
        mock_proxy_instance.get_volume_tags.return_value = {}

        # Mock schema key to avoid container creation
        mock_schema_key.return_value = Mock()

        # Create source with tags enabled
        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource(ctx=ctx, config=mock_config)
        source.unity_catalog_api_proxy = mock_proxy_instance

        # Process volumes
        work_units = list(source.process_volumes(sample_volume_with_tags.schema))

        # Verify that volume tags are extracted (called) but result is empty
        mock_proxy_instance.get_volume_tags.assert_called_once_with("test_catalog")

        # Find GlobalTags work units
        tag_work_units = [
            wu
            for wu in work_units
            if isinstance(wu, MetadataWorkUnit)
            and wu.metadata
            and isinstance(
                wu.metadata, (MetadataChangeProposal, MetadataChangeProposalWrapper)
            )
            and wu.metadata.aspect
            and isinstance(wu.metadata.aspect, GlobalTags)
        ]

        # Should have NO tags work units when no tags found
        assert len(tag_work_units) == 0, (
            f"Expected 0 tags work units when no tags found, got {len(tag_work_units)}"
        )

    def test_get_volume_tags_helper_method(
        self, mock_config, sample_volume_with_tags, sample_volume_tags
    ):
        """Test the _get_volume_tags helper method."""
        # Create source
        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource(ctx=ctx, config=mock_config)

        # Mock the proxy method
        source.unity_catalog_api_proxy = Mock()
        source.unity_catalog_api_proxy.get_volume_tags.return_value = {
            "test_catalog.test_schema.test_volume": sample_volume_tags,
            "other_catalog.other_schema.other_volume": [
                UnityCatalogTag(key="other", value="tag")
            ],
        }

        # Test the helper method
        result = source._get_volume_tags("test_catalog", "test_schema", "test_volume")

        # Should return the specific volume's tags
        assert len(result) == 3
        assert result == sample_volume_tags

        # Test with non-existent volume
        result_empty = source._get_volume_tags("nonexistent", "catalog", "volume")
        assert result_empty == []
