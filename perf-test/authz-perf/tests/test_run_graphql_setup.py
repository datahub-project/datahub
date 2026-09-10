from pathlib import Path
from unittest.mock import MagicMock, patch

import run as run_module


@patch("run.run_all_personas", return_value=[])
@patch("run.setup_graphql_queries")
@patch("run.ensure_datapack")
@patch("run.fetch_gms_config")
@patch("run.resolve_credentials")
def test_run_single_target_generates_queries(
    mock_creds,
    mock_gms_config,
    mock_datapack,
    mock_setup_graphql,
    mock_run_all,
    tmp_path: Path,
) -> None:
    from lib.credentials import AuthzPerfCredentials
    from lib.gms_config import GmsConfigSnapshot

    mock_creds.return_value = AuthzPerfCredentials(
        gms_url="http://localhost:8080",
        frontend_url="http://localhost:9002",
        token="local-token",
    )
    mock_gms_config.return_value = GmsConfigSnapshot(
        gms_host="localhost:8080",
        gms_version="v0.0.0",
        gms_commit="abc",
        server_type="quickstart",
    )
    mock_setup_graphql.return_value = MagicMock()

    from lib.targets import TargetSpec
    from lib.variants import RunVariant

    shared = run_module._build_shared_args(
        run_module.parse_args(
            [
                "--output",
                str(tmp_path / "out.jsonl"),
                "--skip-load",
                "--personas",
                "persona-admin",
            ]
        )
    )
    target = TargetSpec(name="local", gms_url="http://localhost:8080")
    variant = RunVariant(run_label=None, docker_tag=None)

    run_module.run_single_target(
        target,
        variant,
        shared,
        tmp_path / "out.jsonl",
    )

    mock_setup_graphql.assert_called_once()
    assert mock_setup_graphql.call_args.args[0] == "http://localhost:8080"
