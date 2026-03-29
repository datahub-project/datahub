"""Tests for data pack models."""

from datahub.cli.datapack.models import (
    DataPackInfo,
    LoadRecord,
    RegistryManifest,
    TrustTier,
)


class TestDataPackInfo:
    def test_minimal_pack(self) -> None:
        pack = DataPackInfo(
            name="test",
            description="A test pack",
            url="https://example.com/data.json",
        )
        assert pack.name == "test"
        assert pack.trust == TrustTier.CUSTOM
        assert pack.pack_format_version == "1"
        assert pack.sha256 is None
        assert pack.reference_timestamp is None

    def test_full_pack(self) -> None:
        pack = DataPackInfo(
            name="bootstrap",
            description="Bootstrap data",
            url="https://example.com/data.json",
            sha256="abc123",
            size_hint="~100 KB",
            tags=["demo"],
            trust=TrustTier.VERIFIED,
            reference_timestamp=1700000000000,
            min_server_version="0.14.0",
            min_cloud_version="0.3.5",
        )
        assert pack.trust == TrustTier.VERIFIED
        assert pack.tags == ["demo"]
        assert pack.reference_timestamp == 1700000000000


class TestRegistryManifest:
    def test_empty_manifest(self) -> None:
        manifest = RegistryManifest()
        assert manifest.schema_version == 1
        assert manifest.packs == {}

    def test_manifest_with_packs(self) -> None:
        manifest = RegistryManifest(
            schema_version=1,
            packs={
                "test": DataPackInfo(
                    name="test",
                    description="Test",
                    url="https://example.com/data.json",
                )
            },
        )
        assert "test" in manifest.packs

    def test_manifest_ignores_unknown_fields(self) -> None:
        """Old clients should still parse registries with new fields."""
        data = {
            "schema_version": 2,
            "packs": {},
            "some_future_field": "value",
        }
        manifest = RegistryManifest.model_validate(data)
        assert manifest.schema_version == 2


class TestLoadRecord:
    def test_load_record(self) -> None:
        record = LoadRecord(
            pack_name="bootstrap",
            run_id="datapack-bootstrap-123",
            loaded_at="2026-03-21T12:00:00Z",
            pack_url="https://example.com/data.json",
            pack_sha256="abc123",
        )
        assert record.run_id == "datapack-bootstrap-123"
