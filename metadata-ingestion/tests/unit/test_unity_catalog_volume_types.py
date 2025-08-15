from datetime import datetime

import pytest
from databricks.sdk.service.catalog import CatalogType, VolumeType

from datahub.ingestion.source.unity.proxy_types import (
    Catalog,
    Metastore,
    Schema,
    Volume,
    VolumeExternalReference,
    VolumeReference,
)


class TestVolumeExternalReference:
    """Test the VolumeExternalReference dataclass."""

    def test_volume_external_reference_creation(self):
        """Test basic VolumeExternalReference creation."""
        ref = VolumeExternalReference(
            storage_location="s3://my-bucket/volume-data",
            name="test_volume",
            volume_type=VolumeType.EXTERNAL,
            last_updated=datetime(2023, 8, 13, 10, 30, 0),
        )

        assert ref.storage_location == "s3://my-bucket/volume-data"
        assert ref.name == "test_volume"
        assert ref.volume_type == VolumeType.EXTERNAL
        assert ref.last_updated == datetime(2023, 8, 13, 10, 30, 0)

    def test_volume_external_reference_no_has_permission(self):
        """Test that has_permission field is no longer present."""
        ref = VolumeExternalReference(
            storage_location="s3://bucket/data",
            name="test",
            volume_type=VolumeType.EXTERNAL,
        )

        assert not hasattr(ref, "has_permission")

    def test_volume_external_reference_optional_fields(self):
        """Test VolumeExternalReference with optional fields as None."""
        ref = VolumeExternalReference(
            storage_location="dbfs:///mnt/volume",
            name=None,
            volume_type=None,
            last_updated=None,
        )

        assert ref.storage_location == "dbfs:///mnt/volume"
        assert ref.name is None
        assert ref.volume_type is None
        assert ref.last_updated is None

    def test_create_from_volume_success(self):
        """Test successful creation from Volume object."""
        # Create test objects
        metastore = Metastore(
            id="metastore_id",
            name="test_metastore",
            comment=None,
            global_metastore_id="global_id",
            metastore_id="metastore_id",
            owner="owner",
            cloud="aws",
            region="us-west-2",
        )

        catalog = Catalog(
            id="catalog_id",
            name="test_catalog",
            comment=None,
            metastore=metastore,
            owner="owner",
            type=CatalogType.MANAGED_CATALOG,
        )

        schema = Schema(
            id="schema_id",
            name="test_schema",
            comment=None,
            catalog=catalog,
            owner="owner",
        )

        volume = Volume(
            id="volume_id",
            name="test_volume",
            comment="Test volume",
            schema=schema,
            volume_type=VolumeType.EXTERNAL,
            storage_location="s3://my-bucket/volume-data",
            owner="owner",
            created_at=datetime(2023, 8, 13, 10, 0, 0),
            created_by="creator",
            updated_at=datetime(2023, 8, 13, 10, 30, 0),
            updated_by="updater",
        )

        ref = VolumeExternalReference.create_from_volume(volume)

        assert ref is not None
        assert ref.storage_location == "s3://my-bucket/volume-data"
        assert ref.name == "test_volume"
        assert ref.volume_type == VolumeType.EXTERNAL
        assert ref.last_updated == datetime(2023, 8, 13, 10, 30, 0)

    def test_create_from_volume_no_storage_location(self):
        """Test creation from Volume with no storage location returns None."""
        # Create minimal test objects
        metastore = Metastore(
            id="metastore_id",
            name="test_metastore",
            comment=None,
            global_metastore_id=None,
            metastore_id=None,
            owner=None,
            cloud=None,
            region=None,
        )

        catalog = Catalog(
            id="catalog_id",
            name="test_catalog",
            comment=None,
            metastore=metastore,
            owner=None,
            type=CatalogType.MANAGED_CATALOG,
        )

        schema = Schema(
            id="schema_id",
            name="test_schema",
            comment=None,
            catalog=catalog,
            owner=None,
        )

        volume = Volume(
            id="volume_id",
            name="test_volume",
            comment=None,
            schema=schema,
            volume_type=VolumeType.MANAGED,
            storage_location=None,  # No storage location
            owner=None,
            created_at=None,
            created_by=None,
            updated_at=None,
            updated_by=None,
        )

        ref = VolumeExternalReference.create_from_volume(volume)
        assert ref is None

    def test_volume_external_reference_frozen_dataclass(self):
        """Test that VolumeExternalReference is frozen (immutable)."""
        ref = VolumeExternalReference(
            storage_location="s3://bucket/data",
            name="test",
            volume_type=VolumeType.EXTERNAL,
        )

        # Should raise AttributeError when trying to modify frozen dataclass
        with pytest.raises(AttributeError):
            ref.storage_location = "s3://other-bucket/data"  # type: ignore[misc]

    def test_volume_external_reference_ordering(self):
        """Test that VolumeExternalReference supports ordering."""
        ref1 = VolumeExternalReference(
            storage_location="s3://a-bucket/data",
            name="test1",
            volume_type=VolumeType.EXTERNAL,
        )

        ref2 = VolumeExternalReference(
            storage_location="s3://z-bucket/data",
            name="test2",
            volume_type=VolumeType.EXTERNAL,
        )

        # Should be orderable since it's marked with order=True
        assert ref1 < ref2
        assert sorted([ref2, ref1]) == [ref1, ref2]


class TestVolumeReference:
    """Test the VolumeReference functionality."""

    def test_volume_path_property(self):
        """Test the volume_path property returns correct DBFS path."""
        ref = VolumeReference(
            metastore="test_metastore",
            catalog="test_catalog",
            schema="test_schema",
            volume="test_volume",
        )

        expected_path = "/Volumes/test_catalog/test_schema/test_volume"
        assert ref.volume_path == expected_path

    def test_qualified_volume_name(self):
        """Test qualified volume name property."""
        ref = VolumeReference(
            metastore="test_metastore",
            catalog="test_catalog",
            schema="test_schema",
            volume="test_volume",
        )

        assert ref.qualified_volume_name == "test_catalog.test_schema.test_volume"

    def test_volume_reference_str_with_metastore(self):
        """Test string representation with metastore."""
        ref = VolumeReference(
            metastore="test_metastore",
            catalog="test_catalog",
            schema="test_schema",
            volume="test_volume",
        )

        assert str(ref) == "test_metastore.test_catalog.test_schema.test_volume"

    def test_volume_reference_str_without_metastore(self):
        """Test string representation without metastore."""
        ref = VolumeReference(
            metastore=None,
            catalog="test_catalog",
            schema="test_schema",
            volume="test_volume",
        )

        assert str(ref) == "test_catalog.test_schema.test_volume"


class TestVolumeIntegration:
    """Test Volume class integration with references."""

    def test_volume_creates_reference_automatically(self):
        """Test that Volume automatically creates VolumeReference in __post_init__."""
        metastore = Metastore(
            id="metastore_id",
            name="test_metastore",
            comment=None,
            global_metastore_id=None,
            metastore_id=None,
            owner=None,
            cloud=None,
            region=None,
        )

        catalog = Catalog(
            id="catalog_id",
            name="test_catalog",
            comment=None,
            metastore=metastore,
            owner=None,
            type=CatalogType.MANAGED_CATALOG,
        )

        schema = Schema(
            id="schema_id",
            name="test_schema",
            comment=None,
            catalog=catalog,
            owner=None,
        )

        volume = Volume(
            id="volume_id",
            name="test_volume",
            comment=None,
            schema=schema,
            volume_type=VolumeType.MANAGED,
            storage_location="s3://bucket/data",
            owner=None,
            created_at=None,
            created_by=None,
            updated_at=None,
            updated_by=None,
        )

        # Volume should automatically create ref in __post_init__
        assert volume.ref is not None
        assert isinstance(volume.ref, VolumeReference)
        assert volume.ref.volume == "test_volume"
        assert volume.ref.schema == "test_schema"
        assert volume.ref.catalog == "test_catalog"
        assert volume.ref.metastore == "metastore_id"
