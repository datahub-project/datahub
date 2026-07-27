from unittest.mock import MagicMock, patch

from lib.gms_config import fetch_gms_config, gms_host_from_url


def test_gms_host_from_url() -> None:
    assert gms_host_from_url("http://staging.example:8080") == "staging.example:8080"


@patch("lib.gms_config.requests.get")
def test_fetch_gms_config(mock_get: MagicMock) -> None:
    mock_get.return_value = MagicMock(
        status_code=200,
        json=lambda: {
            "versions": {
                "acryldata/datahub": {
                    "version": "v0.14.1",
                    "commit": "abc123",
                }
            },
            "datahub": {"serverType": "quickstart"},
        },
    )
    mock_get.return_value.raise_for_status = MagicMock()

    snapshot = fetch_gms_config("http://localhost:8080")
    assert snapshot.gms_version == "v0.14.1"
    assert snapshot.gms_commit == "abc123"
    assert snapshot.server_type == "quickstart"
    assert snapshot.error is None


@patch("lib.gms_config.requests.get")
def test_fetch_gms_config_failure(mock_get: MagicMock) -> None:
    mock_get.side_effect = ConnectionError("refused")

    snapshot = fetch_gms_config("http://localhost:8080")
    assert snapshot.gms_version == "unknown"
    assert snapshot.error is not None
