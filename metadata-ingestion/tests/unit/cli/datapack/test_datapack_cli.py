"""Tests for the datapack CLI commands."""

import json
import pathlib
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from datahub.cli.datapack.datapack_cli import datapack
from datahub.cli.datapack.models import LoadRecord


class TestListCommand:
    def test_list_table_output(self) -> None:
        runner = CliRunner()
        result = runner.invoke(datapack, ["list"])
        assert result.exit_code == 0
        assert "bootstrap" in result.output
        assert "showcase-ecommerce" in result.output

    def test_list_json_output(self) -> None:
        runner = CliRunner()
        result = runner.invoke(datapack, ["list", "--format", "json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "bootstrap" in data

    def test_list_filter_by_tag(self) -> None:
        runner = CliRunner()
        result = runner.invoke(datapack, ["list", "--tag", "showcase"])
        assert result.exit_code == 0
        assert "showcase-ecommerce" in result.output
        assert "bootstrap" not in result.output

    def test_list_filter_by_nonexistent_tag(self) -> None:
        runner = CliRunner()
        result = runner.invoke(datapack, ["list", "--tag", "nonexistent"])
        assert result.exit_code == 0
        assert "No data packs found" in result.output

    def test_list_shows_showcase_ecommerce(self) -> None:
        runner = CliRunner()
        result = runner.invoke(datapack, ["list"])
        assert result.exit_code == 0
        assert "showcase-ecommerce" in result.output

    def test_list_shows_trust_tier(self) -> None:
        runner = CliRunner()
        result = runner.invoke(datapack, ["list"])
        assert result.exit_code == 0
        assert "verified" in result.output


class TestInfoCommand:
    def test_info_known_pack(self) -> None:
        runner = CliRunner()
        with (
            patch("datahub.cli.datapack.loader.is_cached", return_value=False),
            patch("datahub.cli.datapack.loader.get_load_record", return_value=None),
        ):
            result = runner.invoke(datapack, ["info", "bootstrap"])
        assert result.exit_code == 0
        assert "bootstrap" in result.output
        assert "verified" in result.output
        assert "Cached:          no" in result.output
        assert "Loaded:          no" in result.output

    def test_info_shows_loaded_status(self) -> None:
        record = LoadRecord(
            pack_name="bootstrap",
            run_id="datapack-bootstrap-123",
            loaded_at="2026-03-21T12:00:00Z",
            pack_url="https://example.com",
        )
        runner = CliRunner()
        with (
            patch("datahub.cli.datapack.loader.is_cached", return_value=True),
            patch("datahub.cli.datapack.loader.get_load_record", return_value=record),
        ):
            result = runner.invoke(datapack, ["info", "bootstrap"])
        assert result.exit_code == 0
        assert "Cached:          yes" in result.output
        assert "datapack-bootstrap-123" in result.output

    def test_info_shows_reference_time(self) -> None:
        runner = CliRunner()
        with (
            patch("datahub.cli.datapack.loader.is_cached", return_value=False),
            patch("datahub.cli.datapack.loader.get_load_record", return_value=None),
        ):
            result = runner.invoke(datapack, ["info", "bootstrap"])
        assert result.exit_code == 0
        assert "Reference time:" in result.output

    def test_info_unknown_pack(self) -> None:
        runner = CliRunner()
        result = runner.invoke(datapack, ["info", "nonexistent"])
        assert result.exit_code != 0
        assert "Unknown data pack" in result.output


class TestLoadCommand:
    @patch("datahub.cli.datapack.loader.load_pack_into_datahub")
    @patch("datahub.cli.datapack.loader.download_pack")
    @patch("datahub.cli.datapack.loader.check_version_compatibility")
    @patch("datahub.cli.datapack.loader.check_trust")
    def test_load_dry_run(
        self,
        mock_trust: MagicMock,
        mock_version: MagicMock,
        mock_download: MagicMock,
        mock_load: MagicMock,
    ) -> None:
        mock_download.return_value = pathlib.Path("/tmp/fake.json")
        mock_load.return_value = "datapack-bootstrap-123"

        runner = CliRunner()
        result = runner.invoke(datapack, ["load", "bootstrap", "--dry-run"])
        assert result.exit_code == 0
        mock_trust.assert_called_once()
        # Version check skipped in dry-run
        mock_version.assert_not_called()

    def test_load_unknown_pack(self) -> None:
        runner = CliRunner()
        result = runner.invoke(datapack, ["load", "nonexistent"])
        assert result.exit_code != 0
        assert "Unknown data pack" in result.output

    @patch("datahub.cli.datapack.loader.load_pack_into_datahub")
    @patch("datahub.cli.datapack.loader.download_pack")
    @patch("datahub.cli.datapack.loader.check_version_compatibility")
    @patch("datahub.cli.datapack.loader.check_trust")
    def test_load_custom_url_sets_custom_trust(
        self,
        mock_trust: MagicMock,
        mock_version: MagicMock,
        mock_download: MagicMock,
        mock_load: MagicMock,
    ) -> None:
        mock_download.return_value = pathlib.Path("/tmp/fake.json")
        mock_load.return_value = "run-123"

        runner = CliRunner()
        result = runner.invoke(
            datapack,
            [
                "load",
                "custom-pack",
                "--url",
                "https://example.com/data.json",
                "--trust-custom",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0
        # Check that trust was called with a custom pack
        call_args = mock_trust.call_args
        pack_arg = call_args[0][0]
        assert pack_arg.trust.value == "custom"

    @patch("datahub.cli.datapack.loader.load_pack_into_datahub")
    @patch("datahub.cli.datapack.loader.download_pack")
    @patch("datahub.cli.datapack.loader.check_version_compatibility")
    @patch("datahub.cli.datapack.loader.check_trust")
    def test_load_passes_time_shift_flags(
        self,
        mock_trust: MagicMock,
        mock_version: MagicMock,
        mock_download: MagicMock,
        mock_load: MagicMock,
    ) -> None:
        mock_download.return_value = pathlib.Path("/tmp/fake.json")
        mock_load.return_value = "run-123"

        runner = CliRunner()
        result = runner.invoke(
            datapack, ["load", "bootstrap", "--no-time-shift", "--dry-run"]
        )
        assert result.exit_code == 0
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["no_time_shift"] is True


class TestUnloadCommand:
    @patch("datahub.cli.datapack.loader.unload_pack")
    def test_unload_calls_unload_pack(self, mock_unload: MagicMock) -> None:
        runner = CliRunner()
        result = runner.invoke(datapack, ["unload", "bootstrap"])
        assert result.exit_code == 0
        mock_unload.assert_called_once_with(
            pack_name="bootstrap", hard=False, dry_run=False
        )

    @patch("datahub.cli.datapack.loader.unload_pack")
    def test_unload_hard_flag(self, mock_unload: MagicMock) -> None:
        runner = CliRunner()
        result = runner.invoke(datapack, ["unload", "bootstrap", "--hard"])
        assert result.exit_code == 0
        mock_unload.assert_called_once_with(
            pack_name="bootstrap", hard=True, dry_run=False
        )

    @patch("datahub.cli.datapack.loader.unload_pack")
    def test_unload_dry_run(self, mock_unload: MagicMock) -> None:
        runner = CliRunner()
        result = runner.invoke(datapack, ["unload", "bootstrap", "--dry-run"])
        assert result.exit_code == 0
        mock_unload.assert_called_once_with(
            pack_name="bootstrap", hard=False, dry_run=True
        )

    def test_unload_no_record_shows_error(self) -> None:
        runner = CliRunner()
        with patch("datahub.cli.datapack.loader.get_load_record", return_value=None):
            result = runner.invoke(datapack, ["unload", "nonexistent"])
        assert result.exit_code != 0
        assert "No load record" in result.output
