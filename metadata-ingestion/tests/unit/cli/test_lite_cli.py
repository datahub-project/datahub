from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from click.testing import CliRunner

from datahub.cli import config_utils, lite_cli


@pytest.fixture
def config_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / ".datahubenv"
    monkeypatch.setattr(config_utils, "DATAHUB_CONFIG_PATH", str(path))
    monkeypatch.setattr(lite_cli, "DATAHUB_ROOT_FOLDER", str(tmp_path))
    return path


def test_get_lite_config_defaults_without_config(
    config_path: Path,
) -> None:
    lite_config = lite_cli.get_lite_config()

    assert lite_config.type == "duckdb"
    assert lite_config.config["file"] == str(
        config_path.parent / "lite" / "datahub.duckdb"
    )


def test_get_lite_config_reads_lite_only_config(
    config_path: Path,
) -> None:
    expected = {
        "type": "duckdb",
        "config": {"file": str(config_path.parent / "custom.duckdb")},
    }
    config_path.write_text(yaml.safe_dump({"lite": expected}))

    assert lite_cli.get_lite_config().model_dump() == expected


def test_write_lite_config_creates_missing_config(
    config_path: Path,
) -> None:
    lite_config = lite_cli.get_lite_config()

    lite_cli.write_lite_config(lite_config)

    assert yaml.safe_load(config_path.read_text()) == {"lite": lite_config.model_dump()}


def test_init_persists_default_config_when_missing(
    config_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(lite_cli.telemetry, "telemetry_instance", MagicMock())
    lite = MagicMock()
    lite.location.return_value = str(config_path.parent / "lite" / "datahub.duckdb")
    monkeypatch.setattr(lite_cli, "_get_datahub_lite", lambda: lite)

    result = CliRunner().invoke(lite_cli.lite, ["init"])

    assert result.exit_code == 0, result.output
    assert config_path.exists()
    config = yaml.safe_load(config_path.read_text())
    assert config["lite"]["type"] == "duckdb"
