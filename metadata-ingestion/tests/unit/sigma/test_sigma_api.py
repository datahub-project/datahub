from unittest.mock import MagicMock, patch

from datahub.ingestion.source.sigma.config import SigmaSourceConfig, SigmaSourceReport
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI


def _create_sigma_api() -> SigmaAPI:
    config = SigmaSourceConfig(
        client_id="test_client_id",
        client_secret="test_secret",
    )
    report = SigmaSourceReport()

    with patch.object(SigmaAPI, "_generate_token"):
        api = SigmaAPI(config=config, report=report)
    return api


class TestTokenRefreshOn401:
    def test_refreshes_token_and_retries_on_401(self) -> None:
        api = _create_sigma_api()
        api.refresh_token = "valid_refresh_token"

        unauthorized_response = MagicMock(status_code=401)
        ok_response = MagicMock(status_code=200)
        api.session.get = MagicMock(side_effect=[unauthorized_response, ok_response])

        with patch.object(api, "_refresh_access_token") as mock_refresh:
            result = api._get_api_call("https://api.example.com/test")

        mock_refresh.assert_called_once()
        assert result.status_code == 200
        assert api.session.get.call_count == 2

    def test_skips_refresh_when_no_refresh_token(self) -> None:
        api = _create_sigma_api()
        api.refresh_token = None

        unauthorized_response = MagicMock(status_code=401)
        api.session.get = MagicMock(return_value=unauthorized_response)

        with patch.object(api, "_refresh_access_token") as mock_refresh:
            result = api._get_api_call("https://api.example.com/test")

        mock_refresh.assert_not_called()
        assert result.status_code == 401
        assert api.session.get.call_count == 1

    def test_non_401_response_returned_directly(self) -> None:
        api = _create_sigma_api()
        api.refresh_token = "valid_refresh_token"

        ok_response = MagicMock(status_code=200)
        api.session.get = MagicMock(return_value=ok_response)

        with patch.object(api, "_refresh_access_token") as mock_refresh:
            result = api._get_api_call("https://api.example.com/test")

        mock_refresh.assert_not_called()
        assert result.status_code == 200
        assert api.session.get.call_count == 1
