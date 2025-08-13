from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from databricks.sdk.service.catalog import CatalogType, VolumeType

from datahub.emitter.mce_builder import make_dataset_urn, make_storage_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.proxy_types import (
    Catalog,
    Metastore,
    Schema,
    Volume,
    VolumeExternalReference,
)
from datahub.ingestion.source.unity.source import UnityCatalogSource
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeProposal
from datahub.metadata.schema_classes import (
    GenericAspectClass,
    SubTypesClass,
    UpstreamLineageClass,
)


class TestUnityCatalogVolumeLineage:
    """Test Unity Catalog volume lineage processing functionality."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock config with volume processing enabled."""
        return UnityCatalogSourceConfig.parse_obj(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
                "include_volumes": True,
                "include_external_lineage": True,
                "env": "PROD",
            }
        )

    @pytest.fixture
    def sample_volume(self):
        """Create a sample volume with external storage location."""
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
            comment="Test volume for lineage",
            schema=schema,
            volume_type=VolumeType.EXTERNAL,
            storage_location="s3://my-bucket/volume-data/",
            owner="admin",
            created_at=datetime(2023, 8, 13, 10, 0, 0),
            created_by="creator@example.com",
            updated_at=datetime(2023, 8, 13, 10, 30, 0),
            updated_by="updater@example.com",
        )

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch.object(UnityCatalogSource, "gen_schema_key")
    @patch.object(UnityCatalogSource, "get_owner_urn", return_value=None)
    @patch.object(UnityCatalogSource, "_get_domain_aspect", return_value=None)
    def test_process_volumes_creates_correct_lineage_hierarchy(
        self,
        mock_domain,
        mock_owner,
        mock_schema_key,
        mock_proxy,
        mock_config,
        sample_volume,
    ):
        """Test that process_volumes creates the correct lineage hierarchy: Storage -> Volume Path -> Volume Dataset."""
        # Setup mocks
        mock_proxy_instance = Mock()
        mock_proxy.return_value = mock_proxy_instance
        mock_proxy_instance.volumes.return_value = [sample_volume]

        # Mock schema key to avoid container creation
        mock_schema_key.return_value = Mock()

        # Create source
        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource(ctx=ctx, config=mock_config)
        source.unity_catalog_api_proxy = mock_proxy_instance

        # Process volumes
        work_units = list(source.process_volumes(sample_volume.schema))

        # Extract relevant work units for testing
        lineage_work_units = [
            wu
            for wu in work_units
            if isinstance(wu, MetadataWorkUnit)
            and wu.metadata
            and isinstance(
                wu.metadata, (MetadataChangeProposal, MetadataChangeProposalWrapper)
            )
            and wu.metadata.aspect
            and isinstance(wu.metadata.aspect, UpstreamLineageClass)
        ]

        # Verify we have the expected lineage work units (2 lineage relationships)
        assert len(lineage_work_units) == 2, (
            f"Expected 2 lineage work units, got {len(lineage_work_units)}"
        )

        # Expected URNs - need to use the actual URN format from the source
        storage_urn = make_storage_urn("s3://my-bucket/volume-data", "PROD")
        volume_path_urn = make_dataset_urn(
            "dbfs", "/Volumes/test_catalog/test_schema/test_volume", "PROD"
        )
        # The actual volume dataset URN uses metastore_id in the name, not test_metastore
        volume_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:databricks,metastore_id.test_catalog.test_schema.test_volume,PROD)"

        # Verify storage URN doesn't have trailing slash
        assert (
            storage_urn
            == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/volume-data,PROD)"
        )

        # Extract lineage relationships
        lineage_relationships = {}
        for wu in lineage_work_units:
            # Type narrowing - we know these are the correct types from the filter above
            assert isinstance(
                wu.metadata, (MetadataChangeProposal, MetadataChangeProposalWrapper)
            )
            assert isinstance(wu.metadata.aspect, UpstreamLineageClass)

            downstream_urn = wu.metadata.entityUrn
            upstream_urns = [
                upstream.dataset for upstream in wu.metadata.aspect.upstreams
            ]
            lineage_relationships[downstream_urn] = upstream_urns

        # Verify correct lineage hierarchy
        # Volume Path should have Storage as upstream
        assert volume_path_urn in lineage_relationships
        assert lineage_relationships[volume_path_urn] == [storage_urn]

        # Volume Dataset should have Volume Path as upstream (and ONLY Volume Path)
        assert volume_dataset_urn in lineage_relationships
        assert lineage_relationships[volume_dataset_urn] == [volume_path_urn]

        # Verify no dataset has multiple upstreams (no combined lineage)
        for downstream, upstreams in lineage_relationships.items():
            assert len(upstreams) == 1, (
                f"Dataset {downstream} should have exactly 1 upstream, got {len(upstreams)}: {upstreams}"
            )

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch.object(UnityCatalogSource, "gen_schema_key")
    @patch.object(UnityCatalogSource, "get_owner_urn", return_value=None)
    @patch.object(UnityCatalogSource, "_get_domain_aspect", return_value=None)
    def test_process_volumes_creates_datasets_with_properties(
        self,
        mock_domain,
        mock_owner,
        mock_schema_key,
        mock_proxy,
        mock_config,
        sample_volume,
    ):
        """Test that volume datasets are created with properties."""
        # Setup mocks
        mock_proxy_instance = Mock()
        mock_proxy.return_value = mock_proxy_instance
        mock_proxy_instance.volumes.return_value = [sample_volume]

        # Mock schema key to avoid container creation
        mock_schema_key.return_value = Mock()

        # Create source
        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource(ctx=ctx, config=mock_config)
        source.unity_catalog_api_proxy = mock_proxy_instance

        # Process volumes
        work_units = list(source.process_volumes(sample_volume.schema))

        # Verify we have work units - at least lineage (2) + subtype (2) + some properties/aspects
        assert len(work_units) > 4, (
            f"Expected at least 5 work units, got {len(work_units)}"
        )

        # Verify we have both volume datasets (volume and volume path)
        volume_urns = set()
        for wu in work_units:
            if isinstance(wu, MetadataWorkUnit):
                if (
                    wu.metadata
                    and isinstance(
                        wu.metadata,
                        (MetadataChangeProposal, MetadataChangeProposalWrapper),
                    )
                    and wu.metadata.entityUrn
                ):
                    volume_urns.add(wu.metadata.entityUrn)

        # Expected URNs
        volume_path_urn = make_dataset_urn(
            "dbfs", "/Volumes/test_catalog/test_schema/test_volume", "PROD"
        )
        volume_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:databricks,metastore_id.test_catalog.test_schema.test_volume,PROD)"

        assert volume_path_urn in volume_urns, (
            f"Volume path URN not found in {volume_urns}"
        )
        assert volume_dataset_urn in volume_urns, (
            f"Volume dataset URN not found in {volume_urns}"
        )

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch.object(UnityCatalogSource, "gen_schema_key")
    @patch.object(UnityCatalogSource, "get_owner_urn", return_value=None)
    @patch.object(UnityCatalogSource, "_get_domain_aspect", return_value=None)
    def test_volume_dataset_includes_volume_path_property(
        self,
        mock_domain,
        mock_owner,
        mock_schema_key,
        mock_proxy,
        mock_config,
        sample_volume,
    ):
        """Test that volume dataset includes volume_path in custom properties."""
        # Setup mocks
        mock_proxy_instance = Mock()
        mock_proxy.return_value = mock_proxy_instance
        mock_proxy_instance.volumes.return_value = [sample_volume]

        # Mock schema key to avoid container creation
        mock_schema_key.return_value = Mock()

        # Create source
        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource(ctx=ctx, config=mock_config)
        source.unity_catalog_api_proxy = mock_proxy_instance

        # Process volumes
        work_units = list(source.process_volumes(sample_volume.schema))

        # Find volume dataset properties work unit
        volume_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:databricks,metastore_id.test_catalog.test_schema.test_volume,PROD)"

        # Find volume dataset properties in GenericAspectClass work units (patch MCPs)
        properties_work_unit = None
        for wu in work_units:
            if (
                isinstance(wu, MetadataWorkUnit)
                and wu.metadata
                and isinstance(
                    wu.metadata, (MetadataChangeProposal, MetadataChangeProposalWrapper)
                )
                and wu.metadata.entityUrn == volume_dataset_urn
                and isinstance(wu.metadata.aspect, GenericAspectClass)
            ):
                # Check if this GenericAspectClass contains the volume_path property
                aspect = wu.metadata.aspect
                if aspect.value and isinstance(aspect.value, bytes):
                    # Parse the JSON patch operations
                    import json

                    patch_ops = json.loads(aspect.value.decode("utf-8"))

                    # Look for volume_path in the patch operations
                    volume_path_found = False
                    for op in patch_ops:
                        if op.get("path") == "/customProperties/volume_path":
                            volume_path_value = op.get("value")
                            if (
                                volume_path_value
                                == "/Volumes/test_catalog/test_schema/test_volume"
                            ):
                                volume_path_found = True
                                properties_work_unit = wu
                                break

                    if volume_path_found:
                        break

        assert properties_work_unit is not None, (
            f"Volume dataset properties work unit not found. Looking for URN: {volume_dataset_urn}"
        )

        # Verify the properties aspect contains volume_path in the JSON patch
        import json

        # Type narrowing - we know these are the correct types from the filter above
        assert isinstance(
            properties_work_unit.metadata,
            (MetadataChangeProposal, MetadataChangeProposalWrapper),
        )
        assert isinstance(properties_work_unit.metadata.aspect, GenericAspectClass)

        patch_ops = json.loads(
            properties_work_unit.metadata.aspect.value.decode("utf-8")
        )

        # Find the volume_path property in the patch operations
        volume_path_op = None
        for op in patch_ops:
            if op.get("path") == "/customProperties/volume_path":
                volume_path_op = op
                break

        assert volume_path_op is not None, (
            "volume_path property not found in patch operations"
        )
        assert (
            volume_path_op.get("value")
            == "/Volumes/test_catalog/test_schema/test_volume"
        ), (
            f"Expected volume_path to be '/Volumes/test_catalog/test_schema/test_volume', "
            f"got '{volume_path_op.get('value')}'"
        )

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    def test_process_volumes_creates_correct_subtypes(
        self, mock_proxy, mock_config, sample_volume
    ):
        """Test that volume datasets are marked with correct subtypes."""
        # Setup mocks
        mock_proxy_instance = Mock()
        mock_proxy.return_value = mock_proxy_instance
        mock_proxy_instance.volumes.return_value = [sample_volume]

        # Create source
        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource(ctx=ctx, config=mock_config)
        source.unity_catalog_api_proxy = mock_proxy_instance

        # Process volumes
        work_units = list(source.process_volumes(sample_volume.schema))

        # Extract subtype work units
        subtype_work_units = [
            wu
            for wu in work_units
            if isinstance(wu, MetadataWorkUnit)
            and wu.metadata
            and isinstance(
                wu.metadata, (MetadataChangeProposal, MetadataChangeProposalWrapper)
            )
            and wu.metadata.aspect
            and isinstance(wu.metadata.aspect, SubTypesClass)
        ]

        # Verify both volume datasets have VOLUME subtype
        volume_subtypes = {}
        for wu in subtype_work_units:
            # Type narrowing - we know these are the correct types from the filter above
            assert isinstance(
                wu.metadata, (MetadataChangeProposal, MetadataChangeProposalWrapper)
            )
            assert isinstance(wu.metadata.aspect, SubTypesClass)

            urn = wu.metadata.entityUrn
            subtypes = wu.metadata.aspect.typeNames
            volume_subtypes[urn] = subtypes

        # Expected URNs for volume datasets (not storage) - fix the actual URN format used by the source
        volume_path_urn = make_dataset_urn(
            "dbfs", "/Volumes/test_catalog/test_schema/test_volume", "PROD"
        )
        volume_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:databricks,metastore_id.test_catalog.test_schema.test_volume,PROD)"

        # Both volume-related datasets should have VOLUME subtype
        assert volume_path_urn in volume_subtypes
        assert "Volume" in volume_subtypes[volume_path_urn]

        assert volume_dataset_urn in volume_subtypes
        assert "Volume" in volume_subtypes[volume_dataset_urn]

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    def test_process_volumes_handles_different_protocols(
        self, mock_proxy, mock_config, sample_volume
    ):
        """Test volume processing with different storage protocols."""
        test_cases = [
            ("s3://bucket/path/", "s3", "bucket/path"),
            ("s3a://bucket/data/", "s3", "bucket/data"),
            (
                "abfs://container@account.dfs.core.windows.net/path/",
                "abs",
                "container@account.dfs.core.windows.net/path",
            ),
            (
                "abfss://container@account.dfs.core.windows.net/data/",
                "abs",
                "container@account.dfs.core.windows.net/data",
            ),
            ("dbfs:///mnt/storage/", "dbfs", "/mnt/storage"),
            ("gs://bucket/path/", "gs", "bucket/path"),  # Unknown protocol fallback
        ]

        for storage_location, expected_platform, expected_path in test_cases:
            # Create volume with different storage location
            test_volume = Volume(
                id="volume_id",
                name="test_volume",
                comment="Test volume",
                schema=sample_volume.schema,
                volume_type=VolumeType.EXTERNAL,
                storage_location=storage_location,
                owner="admin",
                created_at=datetime.now(),
                created_by="test",
                updated_at=datetime.now(),
                updated_by="test",
            )

            # Setup mocks
            mock_proxy_instance = Mock()
            mock_proxy.return_value = mock_proxy_instance
            mock_proxy_instance.volumes.return_value = [test_volume]

            # Create source
            ctx = PipelineContext(run_id="test_run")
            source = UnityCatalogSource(ctx=ctx, config=mock_config)
            source.unity_catalog_api_proxy = mock_proxy_instance

            # Process volumes
            work_units = list(source.process_volumes(test_volume.schema))

            # Extract lineage work units
            lineage_work_units = [
                wu
                for wu in work_units
                if isinstance(wu, MetadataWorkUnit)
                and wu.metadata
                and isinstance(
                    wu.metadata, (MetadataChangeProposal, MetadataChangeProposalWrapper)
                )
                and wu.metadata.aspect
                and isinstance(wu.metadata.aspect, UpstreamLineageClass)
            ]

            # Verify correct storage URN is generated
            expected_storage_urn = f"urn:li:dataset:(urn:li:dataPlatform:{expected_platform},{expected_path},PROD)"

            # Find the lineage that should connect volume path to storage
            volume_path_lineage = None
            for wu in lineage_work_units:
                # Type narrowing - we know these are the correct types from the filter above
                assert isinstance(
                    wu.metadata, (MetadataChangeProposal, MetadataChangeProposalWrapper)
                )
                assert isinstance(wu.metadata.aspect, UpstreamLineageClass)

                if wu.metadata.entityUrn and wu.metadata.entityUrn.startswith(
                    "urn:li:dataset:(urn:li:dataPlatform:dbfs,"
                ):
                    volume_path_lineage = wu
                    break

            assert volume_path_lineage is not None, (
                f"No volume path lineage found for {storage_location}"
            )
            # Type narrowing for the found work unit
            assert isinstance(
                volume_path_lineage.metadata,
                (MetadataChangeProposal, MetadataChangeProposalWrapper),
            )
            assert isinstance(volume_path_lineage.metadata.aspect, UpstreamLineageClass)

            storage_upstream = volume_path_lineage.metadata.aspect.upstreams[0].dataset
            assert storage_upstream == expected_storage_urn, (
                f"Expected {expected_storage_urn}, got {storage_upstream}"
            )

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    def test_process_volumes_skips_volumes_without_storage_location(
        self, mock_proxy, mock_config, sample_volume
    ):
        """Test that volumes without storage location are skipped."""
        # Create managed volume without storage location
        managed_volume = Volume(
            id="managed_volume_id",
            name="managed_volume",
            comment="Managed volume without storage",
            schema=sample_volume.schema,
            volume_type=VolumeType.MANAGED,
            storage_location=None,  # No storage location
            owner="admin",
            created_at=datetime.now(),
            created_by="test",
            updated_at=datetime.now(),
            updated_by="test",
        )

        # Setup mocks
        mock_proxy_instance = Mock()
        mock_proxy.return_value = mock_proxy_instance
        mock_proxy_instance.volumes.return_value = [managed_volume]

        # Create source
        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource(ctx=ctx, config=mock_config)
        source.unity_catalog_api_proxy = mock_proxy_instance

        # Process volumes
        work_units = list(source.process_volumes(managed_volume.schema))

        # Should have no work units for volume without storage location
        assert len(work_units) == 0

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    def test_process_volumes_with_external_lineage_disabled(
        self, mock_proxy, sample_volume
    ):
        """Test that no lineage is created when external lineage is disabled."""
        # Config with external lineage disabled
        config_no_lineage = UnityCatalogSourceConfig.parse_obj(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
                "include_volumes": True,
                "include_external_lineage": False,  # Disabled
                "env": "PROD",
            }
        )

        # Setup mocks
        mock_proxy_instance = Mock()
        mock_proxy.return_value = mock_proxy_instance
        mock_proxy_instance.volumes.return_value = [sample_volume]

        # Create source
        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource(ctx=ctx, config=config_no_lineage)
        source.unity_catalog_api_proxy = mock_proxy_instance

        # Process volumes
        work_units = list(source.process_volumes(sample_volume.schema))

        # Should have no work units when external lineage is disabled
        assert len(work_units) == 0

    def test_volume_external_reference_creation(self, sample_volume):
        """Test VolumeExternalReference creation from volume."""
        external_ref = VolumeExternalReference.create_from_volume(sample_volume)

        assert external_ref is not None
        assert external_ref.storage_location == "s3://my-bucket/volume-data/"
        assert external_ref.name == "test_volume"
        assert external_ref.volume_type == VolumeType.EXTERNAL
        assert external_ref.last_updated == sample_volume.updated_at

    def test_volume_external_reference_no_storage_location(self, sample_volume):
        """Test VolumeExternalReference returns None when no storage location."""
        # Create volume without storage location
        volume_no_storage = Volume(
            id=sample_volume.id,
            name=sample_volume.name,
            comment=sample_volume.comment,
            schema=sample_volume.schema,
            volume_type=VolumeType.MANAGED,
            storage_location=None,  # No storage
            owner=sample_volume.owner,
            created_at=sample_volume.created_at,
            created_by=sample_volume.created_by,
            updated_at=sample_volume.updated_at,
            updated_by=sample_volume.updated_by,
        )

        external_ref = VolumeExternalReference.create_from_volume(volume_no_storage)
        assert external_ref is None
