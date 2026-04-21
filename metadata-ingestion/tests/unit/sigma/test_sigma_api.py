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

        with (
            patch.object(
                api.session, "get", side_effect=[unauthorized_response, ok_response]
            ) as mock_get,
            patch.object(api, "_refresh_access_token") as mock_refresh,
        ):
            result = api._get_api_call("https://api.example.com/test")

        mock_refresh.assert_called_once()
        assert result.status_code == 200
        assert mock_get.call_count == 2

    def test_skips_refresh_when_no_refresh_token(self) -> None:
        api = _create_sigma_api()
        api.refresh_token = None

        unauthorized_response = MagicMock(status_code=401)

        with (
            patch.object(
                api.session, "get", return_value=unauthorized_response
            ) as mock_get,
            patch.object(api, "_refresh_access_token") as mock_refresh,
        ):
            result = api._get_api_call("https://api.example.com/test")

        mock_refresh.assert_not_called()
        assert result.status_code == 401
        assert mock_get.call_count == 1

    def test_non_401_response_returned_directly(self) -> None:
        api = _create_sigma_api()
        api.refresh_token = "valid_refresh_token"

        ok_response = MagicMock(status_code=200)

        with (
            patch.object(api.session, "get", return_value=ok_response) as mock_get,
            patch.object(api, "_refresh_access_token") as mock_refresh,
        ):
            result = api._get_api_call("https://api.example.com/test")

        mock_refresh.assert_not_called()
        assert result.status_code == 200
        assert mock_get.call_count == 1


class TestGetElementUpstreamSources:
    def test_sheet_node_missing_element_id_is_skipped_with_warning(self) -> None:
        api = _create_sigma_api()
        element = MagicMock(elementId="elem1", name="My Chart")
        workbook = MagicMock(workbookId="wb1", name="My Workbook")

        lineage_response = MagicMock(status_code=200)
        lineage_response.json.return_value = {
            "dependencies": {
                "tgt_node": {
                    "nodeId": "tgt_node",
                    "elementId": "elem1",
                    "name": "My Chart",
                    "type": "sheet",
                },
                "bad_sheet_node": {
                    "nodeId": "bad_sheet_node",
                    # no elementId key — API contract violation
                    "name": "Upstream Without Id",
                    "type": "sheet",
                },
            },
            "edges": [
                {"source": "bad_sheet_node", "target": "tgt_node", "type": "source"}
            ],
        }

        with patch.object(api, "_get_api_call", return_value=lineage_response):
            result = api._get_element_upstream_sources(element, workbook)

        assert result == {}
        assert len(api.report.warnings) == 1

    def test_unknown_node_type_is_skipped_with_warning(self) -> None:
        api = _create_sigma_api()
        element = MagicMock(elementId="elem1", name="My Chart")
        workbook = MagicMock(workbookId="wb1", name="My Workbook")

        lineage_response = MagicMock(status_code=200)
        lineage_response.json.return_value = {
            "dependencies": {
                "tgt_node": {
                    "nodeId": "tgt_node",
                    "elementId": "elem1",
                    "name": "My Chart",
                    "type": "sheet",
                },
                "unknown_node": {
                    "nodeId": "unknown_node",
                    "name": "Future Node Type",
                    "type": "formula",  # hypothetical future Sigma node type
                },
            },
            "edges": [
                {"source": "unknown_node", "target": "tgt_node", "type": "source"}
            ],
        }

        with patch.object(api, "_get_api_call", return_value=lineage_response):
            result = api._get_element_upstream_sources(element, workbook)

        assert result == {}
        assert len(api.report.warnings) == 1
