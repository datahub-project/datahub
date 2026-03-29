"""Tests for the data pack registry."""

from unittest.mock import MagicMock, patch

import pytest

from datahub.cli.datapack.models import DataPackInfo, RegistryManifest, TrustTier
from datahub.cli.datapack.registry import (
    _load_bundled_registry,
    get_pack,
    list_packs,
)


class TestBundledRegistry:
    def test_bundled_registry_loads(self) -> None:
        manifest = _load_bundled_registry()
        assert manifest.schema_version >= 1
        assert len(manifest.packs) > 0

    def test_bundled_packs_have_required_fields(self) -> None:
        manifest = _load_bundled_registry()
        for name, pack in manifest.packs.items():
            assert pack.name == name
            assert pack.description
            assert pack.url.startswith("https://")
            assert pack.trust == TrustTier.VERIFIED

    def test_bootstrap_pack_exists(self) -> None:
        manifest = _load_bundled_registry()
        assert "bootstrap" in manifest.packs

    def test_showcase_ecommerce_pack_exists(self) -> None:
        manifest = _load_bundled_registry()
        assert "showcase-ecommerce" in manifest.packs

    def test_all_packs_have_reference_timestamps(self) -> None:
        manifest = _load_bundled_registry()
        for name, pack in manifest.packs.items():
            assert pack.reference_timestamp is not None, (
                f"Pack '{name}' is missing reference_timestamp"
            )


class TestGetPack:
    def test_get_known_pack_from_bundled(self) -> None:
        """get_pack should find bundled packs even if remote is down."""
        with (
            patch(
                "datahub.cli.datapack.registry._fetch_remote_registry",
                return_value=None,
            ),
            patch(
                "datahub.cli.datapack.registry._read_cache",
                return_value=None,
            ),
        ):
            pack = get_pack("bootstrap")
        assert pack.name == "bootstrap"

    @patch("datahub.cli.datapack.registry.fetch_registry")
    def test_get_unknown_pack_raises(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = RegistryManifest(schema_version=1, packs={})
        with pytest.raises(Exception, match="Unknown data pack"):
            get_pack("nonexistent-pack")

    @patch("datahub.cli.datapack.registry.fetch_registry")
    def test_remote_pack_returned(self, mock_fetch: MagicMock) -> None:
        remote_pack = DataPackInfo(
            name="new-pack",
            description="A new remote pack",
            url="https://example.com/new.json",
            trust=TrustTier.COMMUNITY,
        )
        mock_fetch.return_value = RegistryManifest(
            schema_version=1, packs={"new-pack": remote_pack}
        )
        pack = get_pack("new-pack")
        assert pack.description == "A new remote pack"


class TestListPacks:
    def test_list_packs_returns_bundled_when_offline(self) -> None:
        with (
            patch(
                "datahub.cli.datapack.registry._fetch_remote_registry",
                return_value=None,
            ),
            patch(
                "datahub.cli.datapack.registry._read_cache",
                return_value=None,
            ),
        ):
            packs = list_packs()
        assert "bootstrap" in packs
        assert "showcase-ecommerce" in packs
