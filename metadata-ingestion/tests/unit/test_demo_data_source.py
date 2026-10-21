"""Unit tests for the DemoDataSource (demo-data ingestion source)."""

import json
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

from datahub.cli.datapack.loader import IndexFileEntry
from datahub.cli.datapack.models import DataPackInfo, TrustTier
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.run.pipeline_config import PipelineConfig
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

    @patch("datahub.cli.datapack.loader.ingest_datapack_file_entries")
    @patch("datahub.cli.datapack.registry.get_pack")
    @patch("datahub.cli.datapack.loader.download_pack")
    @patch("datahub.cli.datapack.loader.check_trust")
    def test_default_loads_bootstrap(
        self, mock_trust, mock_download, mock_get_pack, mock_ingest
    ):
        """Default config resolves 'bootstrap' pack from registry."""
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

            ctx = PipelineContext(run_id="demo-run")
            config = DemoDataConfig()
            source = DemoDataSource(ctx, config)

            mock_get_pack.assert_called_once_with("bootstrap")
            mock_trust.assert_called_once()
            mock_download.assert_called_once()
            mock_ingest.assert_called_once()
            assert source.file_source is not None
            assert mock_ingest.call_args.kwargs.get("server") is None
            assert mock_ingest.call_args.kwargs.get("token") is None

    @patch("datahub.cli.datapack.loader.ingest_datapack_file_entries")
    @patch("datahub.cli.datapack.loader.download_pack")
    @patch("datahub.cli.datapack.loader.check_trust")
    def test_pack_url_creates_custom_pack(self, mock_trust, mock_download, mock_ingest):
        """pack_url creates an ad-hoc custom pack without hitting registry."""
        with tempfile.NamedTemporaryFile(suffix=".json", mode="w") as f:
            json.dump([], f)
            f.flush()
            mock_download.return_value = [IndexFileEntry(path=Path(f.name))]

            ctx = PipelineContext(run_id="demo-run")
            config = DemoDataConfig(pack_url="https://example.com/custom.json")
            _source = DemoDataSource(ctx, config)

            # Should NOT call get_pack when pack_url is provided
            mock_trust.assert_called_once()
            trust_call_pack = mock_trust.call_args[0][0]
            assert trust_call_pack.name == "custom"
            assert trust_call_pack.trust.value == "custom"
            mock_ingest.assert_called_once()

    @patch("datahub.cli.datapack.loader.ingest_datapack_file_entries")
    @patch("datahub.cli.datapack.registry.get_pack")
    @patch("datahub.cli.datapack.loader.download_pack")
    @patch("datahub.cli.datapack.loader.check_trust")
    def test_time_shift_when_enabled(
        self, mock_trust, mock_download, mock_get_pack, mock_ingest
    ):
        """Time-shifting is invoked when no_time_shift=False and pack has reference_timestamp."""
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

            ctx = PipelineContext(run_id="test-run-id")
            config = DemoDataConfig(pack_name="test", no_time_shift=False)
            _source = DemoDataSource(ctx, config)

            mock_ingest.assert_called_once()
            assert mock_ingest.call_args.kwargs["no_time_shift"] is False

    @patch("datahub.cli.datapack.loader.ingest_datapack_file_entries")
    @patch("datahub.cli.datapack.registry.get_pack")
    @patch("datahub.cli.datapack.loader.download_pack")
    @patch("datahub.cli.datapack.loader.check_trust")
    def test_no_time_shift_by_default(
        self, mock_trust, mock_download, mock_get_pack, mock_ingest
    ):
        """Default config (no_time_shift=True) skips time-shifting."""
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

            ctx = PipelineContext(run_id="demo-run")
            config = DemoDataConfig()  # default no_time_shift=True
            _source = DemoDataSource(ctx, config)

            mock_ingest.assert_called_once()
            assert mock_ingest.call_args.kwargs["no_time_shift"] is True

    @patch("datahub.cli.datapack.loader.ingest_datapack_file_entries")
    @patch("datahub.cli.datapack.registry.get_pack")
    @patch("datahub.cli.datapack.loader.download_pack")
    @patch("datahub.cli.datapack.loader.check_trust")
    def test_as_of_passed_to_ingest(
        self, mock_trust, mock_download, mock_get_pack, mock_ingest
    ):
        mock_get_pack.return_value = DataPackInfo(
            name="test",
            description="test",
            url="https://example.com/test.json",
            trust=TrustTier.VERIFIED,
        )

        with tempfile.NamedTemporaryFile(suffix=".json", mode="w") as f:
            json.dump([], f)
            f.flush()
            mock_download.return_value = [IndexFileEntry(path=Path(f.name))]

            ctx = PipelineContext(run_id="demo-run")
            config = DemoDataConfig(as_of="2024-06-01T12:00:00+00:00")
            DemoDataSource(ctx, config)

            mock_ingest.assert_called_once()
            as_of = mock_ingest.call_args.kwargs["as_of"]
            assert as_of is not None
            assert as_of.year == 2024
            assert as_of.month == 6

    @patch("datahub.cli.datapack.loader.ingest_datapack_file_entries")
    @patch("datahub.cli.datapack.registry.get_pack")
    @patch("datahub.cli.datapack.loader.download_pack")
    @patch("datahub.cli.datapack.loader.check_trust")
    def test_server_and_token_passed_as_client_config(
        self, mock_trust, mock_download, mock_get_pack, mock_ingest
    ):
        mock_get_pack.return_value = DataPackInfo(
            name="bootstrap",
            description="test",
            url="https://example.com/bootstrap.json",
            trust=TrustTier.VERIFIED,
        )

        with tempfile.NamedTemporaryFile(suffix=".json", mode="w") as f:
            json.dump([], f)
            f.flush()
            mock_download.return_value = [IndexFileEntry(path=Path(f.name))]

            ctx = PipelineContext(run_id="demo-run")
            config = DemoDataConfig(
                server="http://localhost:8080",
                token="sample-token",
            )
            DemoDataSource(ctx, config)

            mock_ingest.assert_called_once()
            assert mock_ingest.call_args.kwargs["server"] == "http://localhost:8080"
            assert mock_ingest.call_args.kwargs["token"] == "sample-token"

    @patch("datahub.cli.datapack.loader.ingest_datapack_file_entries")
    @patch("datahub.cli.datapack.registry.get_pack")
    @patch("datahub.cli.datapack.loader.download_pack")
    @patch("datahub.cli.datapack.loader.check_trust")
    def test_token_without_server_is_passed(
        self, mock_trust, mock_download, mock_get_pack, mock_ingest
    ):
        mock_get_pack.return_value = DataPackInfo(
            name="bootstrap",
            description="test",
            url="https://example.com/bootstrap.json",
            trust=TrustTier.VERIFIED,
        )

        with tempfile.NamedTemporaryFile(suffix=".json", mode="w") as f:
            json.dump([], f)
            f.flush()
            mock_download.return_value = [IndexFileEntry(path=Path(f.name))]

            ctx = PipelineContext(run_id="demo-run")
            DemoDataSource(ctx, DemoDataConfig(token="sample-token"))

            mock_ingest.assert_called_once()
            assert mock_ingest.call_args.kwargs.get("server") is None
            assert mock_ingest.call_args.kwargs["token"] == "sample-token"

    @patch("datahub.cli.datapack.loader.ingest_datapack_file_entries")
    @patch("datahub.cli.datapack.registry.get_pack")
    @patch("datahub.cli.datapack.loader.download_pack")
    @patch("datahub.cli.datapack.loader.check_trust")
    def test_get_workunits_and_report(
        self, mock_trust, mock_download, mock_get_pack, mock_ingest
    ):
        mock_get_pack.return_value = DataPackInfo(
            name="bootstrap",
            description="test",
            url="https://example.com/bootstrap.json",
            trust=TrustTier.VERIFIED,
        )

        with tempfile.NamedTemporaryFile(suffix=".json", mode="w") as f:
            json.dump([], f)
            f.flush()
            mock_download.return_value = [IndexFileEntry(path=Path(f.name))]

            ctx = PipelineContext(run_id="demo-run")
            source = DemoDataSource(ctx, DemoDataConfig())

            assert list(source.get_workunits()) == []
            assert source.get_report() is not None


def _recipe_ctx(
    sink: Optional[Dict[str, Any]] = None,
    datahub_api: Optional[Dict[str, Any]] = None,
    dry_run: bool = False,
) -> PipelineContext:
    recipe: Dict[str, Any] = {"source": {"type": "demo-data", "config": {}}}
    if sink is not None:
        recipe["sink"] = sink
    if datahub_api is not None:
        recipe["datahub_api"] = datahub_api
    return PipelineContext(
        run_id="demo-run",
        dry_run=dry_run,
        pipeline_config=PipelineConfig.from_dict(recipe),
    )


@patch("datahub.cli.datapack.loader.ingest_datapack_file_entries")
@patch("datahub.cli.datapack.registry.get_pack")
@patch("datahub.cli.datapack.loader.download_pack")
@patch("datahub.cli.datapack.loader.check_trust")
class TestDemoDataSourceRecipeTarget:
    @staticmethod
    def _setup(
        tmp_path: Path, mock_download: MagicMock, mock_get_pack: MagicMock
    ) -> None:
        mock_get_pack.return_value = DataPackInfo(
            name="bootstrap",
            description="test",
            url="https://example.com/bootstrap.json",
            trust=TrustTier.VERIFIED,
        )
        pack_file = tmp_path / "pack.json"
        pack_file.write_text("[]")
        mock_download.return_value = [IndexFileEntry(path=pack_file)]

    def test_dry_run_skips_ingest(
        self, mock_trust, mock_download, mock_get_pack, mock_ingest, tmp_path
    ):
        self._setup(tmp_path, mock_download, mock_get_pack)
        ctx = _recipe_ctx(
            sink={"type": "datahub-rest", "config": {"server": "http://gms:8080"}},
            dry_run=True,
        )

        source = DemoDataSource(ctx, DemoDataConfig())

        mock_ingest.assert_not_called()
        assert list(source.get_workunits()) == []

    def test_uses_rest_sink_target(
        self, mock_trust, mock_download, mock_get_pack, mock_ingest, tmp_path
    ):
        self._setup(tmp_path, mock_download, mock_get_pack)
        ctx = _recipe_ctx(
            sink={
                "type": "datahub-rest",
                "config": {"server": "http://gms:8080", "token": "sink-token"},
            }
        )

        DemoDataSource(ctx, DemoDataConfig())

        client_config = mock_ingest.call_args.kwargs["client_config"]
        assert client_config.server == "http://gms:8080"
        assert client_config.token == "sink-token"

    def test_datahub_api_preferred_over_sink(
        self, mock_trust, mock_download, mock_get_pack, mock_ingest, tmp_path
    ):
        self._setup(tmp_path, mock_download, mock_get_pack)
        ctx = _recipe_ctx(
            sink={"type": "datahub-rest", "config": {"server": "http://sink:8080"}},
            datahub_api={"server": "http://api:8080"},
        )

        DemoDataSource(ctx, DemoDataConfig())

        assert mock_ingest.call_args.kwargs["client_config"].server == (
            "http://api:8080"
        )

    def test_source_token_overrides_sink_token(
        self, mock_trust, mock_download, mock_get_pack, mock_ingest, tmp_path
    ):
        self._setup(tmp_path, mock_download, mock_get_pack)
        ctx = _recipe_ctx(
            sink={
                "type": "datahub-rest",
                "config": {"server": "http://gms:8080", "token": "sink-token"},
            }
        )

        DemoDataSource(ctx, DemoDataConfig(token="source-token"))

        client_config = mock_ingest.call_args.kwargs["client_config"]
        assert client_config.server == "http://gms:8080"
        assert client_config.token == "source-token"

    def test_source_server_overrides_sink(
        self, mock_trust, mock_download, mock_get_pack, mock_ingest, tmp_path
    ):
        self._setup(tmp_path, mock_download, mock_get_pack)
        ctx = _recipe_ctx(
            sink={"type": "datahub-rest", "config": {"server": "http://sink:8080"}}
        )

        DemoDataSource(ctx, DemoDataConfig(server="http://source:8080"))

        assert mock_ingest.call_args.kwargs["client_config"] is None
        assert mock_ingest.call_args.kwargs["server"] == "http://source:8080"

    def test_non_rest_sink_falls_back_to_default_resolution(
        self, mock_trust, mock_download, mock_get_pack, mock_ingest, tmp_path
    ):
        self._setup(tmp_path, mock_download, mock_get_pack)
        ctx = _recipe_ctx(
            sink={"type": "file", "config": {"filename": str(tmp_path / "out.json")}}
        )

        DemoDataSource(ctx, DemoDataConfig())

        assert mock_ingest.call_args.kwargs["client_config"] is None
