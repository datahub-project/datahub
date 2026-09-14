import json
from pathlib import Path
from unittest.mock import patch

import run as run_module


@patch("run.fetch_gms_config")
@patch("run.run_single_target")
def test_output_dir_writes_manifest(mock_run, mock_gms, tmp_path: Path) -> None:
    from lib.gms_config import GmsConfigSnapshot

    mock_gms.return_value = GmsConfigSnapshot(
        gms_host="a:8080",
        gms_version="v0.14.0",
        gms_commit="abc",
        server_type="quickstart",
    )
    mock_run.return_value = None
    exit_code = run_module.main(
        [
            "--env-file",
            str(_write_env(tmp_path, "staging", "http://a:8080")),
            "--env-file",
            str(_write_env(tmp_path, "prod", "http://b:8080")),
            "--output-dir",
            str(tmp_path / "results"),
            "--personas",
            "persona-admin",
            "--iterations",
            "1",
            "--warmup",
            "0",
            "--skip-load",
        ]
    )
    assert exit_code == 0
    assert mock_run.call_count == 2
    manifest = json.loads((tmp_path / "results" / "manifest.json").read_text())
    assert len(manifest["runs"]) == 2
    assert manifest["runs"][0]["status"] == "ok"


def _write_env(tmp_path: Path, name: str, server: str) -> Path:
    import yaml

    path = tmp_path / f"{name}.datahubenv"
    path.write_text(
        yaml.dump({"gms": {"server": server, "token": "tok"}}),
        encoding="utf-8",
    )
    return path


@patch("run.fetch_gms_config")
@patch("run.run_single_target")
def test_fail_fast_stops_on_error(mock_run, mock_gms, tmp_path: Path) -> None:
    from lib.gms_config import GmsConfigSnapshot

    mock_gms.return_value = GmsConfigSnapshot(
        gms_host="a:8080",
        gms_version="v0.14.0",
        gms_commit="abc",
        server_type="quickstart",
    )
    mock_run.side_effect = [None, RuntimeError("boom")]
    exit_code = run_module.main(
        [
            "--target",
            "name=a,gms_url=http://a:8080,token=t",
            "--target",
            "name=b,gms_url=http://b:8080,token=t",
            "--output-dir",
            str(tmp_path / "results"),
            "--personas",
            "persona-admin",
            "--iterations",
            "1",
            "--warmup",
            "0",
            "--skip-load",
            "--fail-fast",
        ]
    )
    assert exit_code == 1
    assert mock_run.call_count == 2
    manifest = json.loads((tmp_path / "results" / "manifest.json").read_text())
    assert manifest["runs"][0]["status"] == "ok"
    assert manifest["runs"][1]["status"] == "error"
