"""Unit tests for the DemoDataSource (demo-data ingestion source)."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from datahub.cli.datapack.loader import IndexFileEntry
from datahub.ingestion.source.demo_data import DemoDataConfig, DemoDataSource


class TestDemoDataConfig:
    def test_default_config(self):
        """Zero-config produces bootstrap with no time-shifting."""
        config = DemoDataConfig()
        assert config.pack_name == "bootstrap"
        assert config.no_time_shift is True
        assert config.pack_url is None
        assert config.trust_community is False
        assert config.trust_custom is False
        assert config.no_cache is False

    def test_custom_pack_name(self):
        config = DemoDataConfig(pack_name="showcase-ecommerce", no_time_shift=False)
        assert config.pack_name == "showcase-ecommerce"
        assert config.no_time_shift is False

    def test_pack_url_config(self):
        config = DemoDataConfig(pack_url="https://example.com/pack.json")
        assert config.pack_url == "https://example.com/pack.json"
        assert config.pack_name == "bootstrap"  # default still set


class TestDemoDataSource:
    def _make_pack_file(self, tmp_path: Path) -> Path:
        """Create a minimal MCP JSON file for testing."""
        pack_file = tmp_path / "test_pack.json"
        pack_file.write_text(json.dumps([]))
        return pack_file

    @patch("datahub.cli.datapack.registry.get_pack")
    @patch("datahub.cli.datapack.loader.download_pack")
    @patch("datahub.cli.datapack.loader.check_trust")
    def test_default_loads_bootstrap(self, mock_trust, mock_download, mock_get_pack):
        """Default config resolves 'bootstrap' pack from registry."""
        from datahub.cli.datapack.models import DataPackInfo, TrustTier

        mock_pack = DataPackInfo(
            name="bootstrap",
            description="test",
            url="https://example.com/bootstrap.json",
            trust=TrustTier.VERIFIED,
        )
        mock_get_pack.return_value = mock_pack

        with tempfile.NamedTemporaryFile(suffix=".json", mode="w") as f:
            json.dump([], f)
            f.flush()
            mock_download.return_value = [IndexFileEntry(path=Path(f.name))]

            ctx = MagicMock()
            config = DemoDataConfig()
            source = DemoDataSource(ctx, config)

            mock_get_pack.assert_called_once_with("bootstrap")
            mock_trust.assert_called_once()
            mock_download.assert_called_once()
            assert source.file_source is not None

    @patch("datahub.cli.datapack.loader.download_pack")
    @patch("datahub.cli.datapack.loader.check_trust")
    def test_pack_url_creates_custom_pack(self, mock_trust, mock_download):
        """pack_url creates an ad-hoc custom pack without hitting registry."""
        with tempfile.NamedTemporaryFile(suffix=".json", mode="w") as f:
            json.dump([], f)
            f.flush()
            mock_download.return_value = [IndexFileEntry(path=Path(f.name))]

            ctx = MagicMock()
            config = DemoDataConfig(pack_url="https://example.com/custom.json")
            _source = DemoDataSource(ctx, config)

            # Should NOT call get_pack when pack_url is provided
            mock_trust.assert_called_once()
            trust_call_pack = mock_trust.call_args[0][0]
            assert trust_call_pack.name == "custom"
            assert trust_call_pack.trust.value == "custom"

    @patch("datahub.cli.datapack.registry.get_pack")
    @patch("datahub.cli.datapack.loader.download_pack")
    @patch("datahub.cli.datapack.loader.check_trust")
    @patch("datahub.cli.datapack.time_shift.time_shift_file")
    def test_time_shift_when_enabled(
        self, mock_time_shift, mock_trust, mock_download, mock_get_pack
    ):
        """Time-shifting is invoked when no_time_shift=False and pack has reference_timestamp."""
        from datahub.cli.datapack.models import DataPackInfo, TrustTier

        mock_pack = DataPackInfo(
            name="test",
            description="test",
            url="https://example.com/test.json",
            trust=TrustTier.VERIFIED,
            reference_timestamp=1700000000000,
        )
        mock_get_pack.return_value = mock_pack

        with tempfile.NamedTemporaryFile(suffix=".json", mode="w") as f:
            json.dump([], f)
            f.flush()
            pack_path = Path(f.name)
            mock_download.return_value = [IndexFileEntry(path=pack_path)]

            shifted_path = Path(f.name)
            mock_time_shift.return_value = shifted_path

            ctx = MagicMock()
            config = DemoDataConfig(pack_name="test", no_time_shift=False)
            _source = DemoDataSource(ctx, config)

            mock_time_shift.assert_called_once()

    @patch("datahub.cli.datapack.registry.get_pack")
    @patch("datahub.cli.datapack.loader.download_pack")
    @patch("datahub.cli.datapack.loader.check_trust")
    @patch("datahub.cli.datapack.time_shift.time_shift_file")
    def test_no_time_shift_by_default(
        self, mock_time_shift, mock_trust, mock_download, mock_get_pack
    ):
        """Default config (no_time_shift=True) skips time-shifting."""
        from datahub.cli.datapack.models import DataPackInfo, TrustTier

        mock_pack = DataPackInfo(
            name="test",
            description="test",
            url="https://example.com/test.json",
            trust=TrustTier.VERIFIED,
            reference_timestamp=1700000000000,
        )
        mock_get_pack.return_value = mock_pack

        with tempfile.NamedTemporaryFile(suffix=".json", mode="w") as f:
            json.dump([], f)
            f.flush()
            mock_download.return_value = [IndexFileEntry(path=Path(f.name))]

            ctx = MagicMock()
            config = DemoDataConfig()  # default no_time_shift=True
            _source = DemoDataSource(ctx, config)

            mock_time_shift.assert_not_called()
