"""`datahub get` hands `client.config.server` to `get_aspects_for_entity`, the
same raw-config path that broke `ingest list-runs` and `ingest rollback` on
DataHub Cloud: the bare instance hostname is the frontend, which does not
forward `/entitiesV2`, so the request got a 404 unless the configured URL
already ended in `/gms`.

This test drives the real command and pins the URL it requests, for a Cloud
hostname and a self-hosted one alike, so the normalisation on
`DatahubClientConfig` is proven at the command level for the most-used of
the affected commands and not only for the `ingest` ones.
"""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from datahub.cli.get_cli import get
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.utilities.urns.urn import Urn

CLOUD = ("https://abc.acryl.io", "https://abc.acryl.io/gms")
SELF_HOSTED = ("http://localhost:8080", "http://localhost:8080")
URN = "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users,PROD)"


def _graph(server: str) -> MagicMock:
    """A graph client whose config is the real model (so normalisation runs)
    and whose session records what the command requests."""
    graph = MagicMock()
    graph.config = DatahubClientConfig(server=server, token="test-token")
    graph._session = MagicMock()
    return graph


def _entity_response() -> MagicMock:
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "urn": URN,
        "aspects": {
            "datasetProperties": {
                "name": "datasetProperties",
                "version": 0,
                "type": "VERSIONED",
                "value": {"description": "users fact table"},
            }
        },
    }
    return response


@pytest.mark.parametrize("configured, expected_gms", [CLOUD, SELF_HOSTED])
@patch("datahub.upgrade.upgrade.check_upgrade_post")
@patch("datahub.cli.get_cli.get_default_graph")
def test_get_requests_the_gms_url(
    mock_get_graph: MagicMock,
    _check_upgrade: MagicMock,
    configured: str,
    expected_gms: str,
) -> None:
    graph = _graph(configured)
    mock_get_graph.return_value = graph
    graph._session.get.return_value = _entity_response()

    result = CliRunner().invoke(get, ["urn", "--urn", URN])

    assert result.exit_code == 0, result.output
    expected_url = f"{expected_gms}/entitiesV2/{Urn.url_encode(URN)}"
    assert graph._session.get.call_args[0][0] == expected_url
    assert "users fact table" in result.output
