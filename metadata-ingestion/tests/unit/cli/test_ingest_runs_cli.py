"""`datahub ingest list-runs` and `datahub ingest rollback` build their Rest.li
URLs from `client.config.server`, which made them the commands that broke on
DataHub Cloud whenever the configured URL was the bare instance hostname:
the bare hostname is the frontend, which does not forward `/runs`, so the
request got a 404 while commands routed through the emitter worked.

These tests drive the real commands and pin the URL they post to, for a
Cloud hostname and a self-hosted one alike.
"""

from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from datahub.cli.ingest_cli import ingest
from datahub.ingestion.graph.config import DatahubClientConfig

CLOUD = ("https://abc.acryl.io", "https://abc.acryl.io/gms")
SELF_HOSTED = ("http://localhost:8080", "http://localhost:8080")


def _graph(server: str) -> MagicMock:
    """A graph client whose config is the real model (so normalisation runs)
    and whose session records what the command posts to."""
    graph = MagicMock()
    graph.config = DatahubClientConfig(server=server, token="test-token")
    graph._session = MagicMock()
    return graph


def _restli_response(value: Any) -> MagicMock:
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"value": value}
    return response


@pytest.mark.parametrize("configured, expected_gms", [CLOUD, SELF_HOSTED])
@patch("datahub.upgrade.upgrade.check_upgrade_post")
@patch("datahub.cli.ingest_cli.get_default_graph")
def test_list_runs_posts_to_the_gms_url(
    mock_get_graph: MagicMock,
    _check_upgrade: MagicMock,
    configured: str,
    expected_gms: str,
) -> None:
    graph = _graph(configured)
    mock_get_graph.return_value = graph
    rows: List[Dict[str, Any]] = [
        {"runId": "run-1", "rows": 3, "timestamp": 1_792_000_000_000}
    ]
    graph._session.post.return_value = _restli_response(rows)

    result = CliRunner().invoke(ingest, ["list-runs"])

    assert result.exit_code == 0, result.output
    assert graph._session.post.call_args[0][0] == f"{expected_gms}/runs?action=list"
    assert "run-1" in result.output


@pytest.mark.parametrize("configured, expected_gms", [CLOUD, SELF_HOSTED])
@patch("datahub.upgrade.upgrade.check_upgrade_post")
@patch("datahub.cli.ingest_cli.get_default_graph")
def test_rollback_posts_to_the_gms_url(
    mock_get_graph: MagicMock,
    _check_upgrade: MagicMock,
    configured: str,
    expected_gms: str,
) -> None:
    graph = _graph(configured)
    mock_get_graph.return_value = graph
    summary: Dict[str, Any] = {
        "aspectRowSummaries": [],
        "entitiesAffected": 0,
        "aspectsReverted": 0,
        "aspectsAffected": 0,
        "unsafeEntitiesCount": 0,
        "unsafeEntities": [],
    }
    graph._session.post.return_value = _restli_response(summary)

    result = CliRunner().invoke(ingest, ["rollback", "--run-id", "run-1", "--dry-run"])

    assert result.exit_code == 0, result.output
    assert graph._session.post.call_args[0][0] == f"{expected_gms}/runs?action=rollback"
