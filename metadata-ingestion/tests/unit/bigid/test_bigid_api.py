"""Unit tests for BigIDClient."""

from unittest.mock import MagicMock, patch

import pytest
import requests
import requests.exceptions

from datahub.ingestion.source.bigid.bigid_api import (
    PAGE_SIZE,
    BigIDAPIError,
    BigIDClient,
)
from datahub.ingestion.source.bigid.bigid_utils import IDSoRAttributeInfo

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_with_access_token() -> BigIDClient:
    return BigIDClient(bigid_url="https://x.com", access_token="tok")


def _client_with_user_token() -> BigIDClient:
    return BigIDClient(bigid_url="https://x.com", user_token="user-tok")


def _http_error(status: int) -> requests.exceptions.HTTPError:
    mock_resp = MagicMock()
    mock_resp.status_code = status
    return requests.exceptions.HTTPError(response=mock_resp)


# ---------------------------------------------------------------------------
# 1. Constructor guard
# ---------------------------------------------------------------------------


def test_constructor_requires_token():
    with pytest.raises(BigIDAPIError):
        BigIDClient(bigid_url="https://x.com")


# ---------------------------------------------------------------------------
# 2. _get_access_token — pre-set access_token skips HTTP
# ---------------------------------------------------------------------------


def test_get_access_token_returns_immediately_when_set():
    client = _client_with_access_token()
    with patch.object(client.session, "get") as mock_get:
        result = client._get_access_token()
    assert result == "tok"
    mock_get.assert_not_called()


# ---------------------------------------------------------------------------
# 3. _get_access_token — user_token successful exchange
# ---------------------------------------------------------------------------


def test_get_access_token_exchanges_user_token():
    client = _client_with_user_token()
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"systemToken": "new-tok"}
    with patch.object(client.session, "get", return_value=mock_resp):
        result = client._get_access_token()
    assert result == "new-tok"
    assert client._access_token == "new-tok"


# ---------------------------------------------------------------------------
# 4. _get_access_token — HTTP error during exchange raises BigIDAPIError
# ---------------------------------------------------------------------------


def test_get_access_token_http_error_raises():
    client = _client_with_user_token()
    with patch.object(client.session, "get", side_effect=_http_error(503)), pytest.raises(BigIDAPIError, match="Token refresh failed"):
        client._get_access_token()


# ---------------------------------------------------------------------------
# 5. _get_access_token — response missing all token keys raises BigIDAPIError
# ---------------------------------------------------------------------------


def test_get_access_token_missing_token_key_raises():
    client = _client_with_user_token()
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"status": "ok"}
    with patch.object(client.session, "get", return_value=mock_resp), pytest.raises(BigIDAPIError):
        client._get_access_token()


# ---------------------------------------------------------------------------
# 6. _request — success
# ---------------------------------------------------------------------------


def test_request_success():
    client = _client_with_access_token()
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"results": []}
    with patch.object(client.session, "get", return_value=mock_resp):
        result = client._request("/api/v1/something")
    assert result == {"results": []}


# ---------------------------------------------------------------------------
# 7. _request — non-401 HTTP error raises BigIDAPIError("HTTP 500")
# ---------------------------------------------------------------------------


def test_request_http_500_raises():
    client = _client_with_access_token()
    with patch.object(client.session, "get", side_effect=_http_error(500)), pytest.raises(BigIDAPIError, match="HTTP 500"):
        client._request("/api/v1/something")


# ---------------------------------------------------------------------------
# 8. _request — timeout raises BigIDAPIError("Timeout")
# ---------------------------------------------------------------------------


def test_request_timeout_raises():
    client = _client_with_access_token()
    with patch.object(client.session, "get", side_effect=requests.exceptions.Timeout), pytest.raises(BigIDAPIError, match="Timeout"):
        client._request("/api/v1/something")


# ---------------------------------------------------------------------------
# 9. _request — 401 with user_token retries once and succeeds
# ---------------------------------------------------------------------------


def test_request_401_retries_with_user_token():
    client = _client_with_user_token()
    client._access_token = "stale-tok"

    ok_resp = MagicMock()
    ok_resp.json.return_value = {"ok": True}

    # refresh-access-token call returns a new token; subsequent data call succeeds
    refresh_resp = MagicMock()
    refresh_resp.json.return_value = {"systemToken": "fresh-tok"}

    call_count = 0

    def side_effect(url, **kwargs):
        nonlocal call_count
        call_count += 1
        if "refresh-access-token" in url:
            return refresh_resp
        if call_count == 1:
            raise _http_error(401)
        return ok_resp

    with patch.object(client.session, "get", side_effect=side_effect):
        result = client._request("/api/v1/data")

    assert result == {"ok": True}
    assert call_count == 3  # first data call (401) + refresh + retry data call


# ---------------------------------------------------------------------------
# 10. _request — 401 with access_token only raises immediately, no retry
# ---------------------------------------------------------------------------


def test_request_401_no_user_token_raises_immediately():
    client = _client_with_access_token()
    with patch.object(client.session, "get", side_effect=_http_error(401)) as mock_get, pytest.raises(BigIDAPIError, match="HTTP 401"):
        client._request("/api/v1/data")
    assert mock_get.call_count == 1


# ---------------------------------------------------------------------------
# 11. get_columns — list response returned as-is
# ---------------------------------------------------------------------------


def test_get_columns_list_response():
    client = _client_with_access_token()
    rows = [{"col": "a", "objectName": "my_table"}]
    with patch.object(client, "_request", return_value=rows):
        result = client.get_columns("my_table", "my_source")
    assert result == rows


# ---------------------------------------------------------------------------
# 12. get_columns — dict with "results" key unwrapped
# ---------------------------------------------------------------------------


def test_get_columns_results_key():
    client = _client_with_access_token()
    rows = [{"col": "b", "objectName": "my_table"}]
    with patch.object(client, "_request", return_value={"results": rows}):
        result = client.get_columns("my_table", "my_source")
    assert result == rows


# ---------------------------------------------------------------------------
# 13. get_columns — dict with "data" key unwrapped
# ---------------------------------------------------------------------------


def test_get_columns_data_key():
    client = _client_with_access_token()
    rows = [{"col": "c", "objectName": "my_table"}]
    with patch.object(client, "_request", return_value={"data": rows}):
        result = client.get_columns("my_table", "my_source")
    assert result == rows


# ---------------------------------------------------------------------------
# 14a. get_columns — FQN filter isolates same-named tables in different schemas
# ---------------------------------------------------------------------------


def test_get_columns_fqn_isolates_same_named_tables():
    client = _client_with_access_token()
    # Two tables share objectName but differ by schema; FQN picks the right one
    rows = [
        {"col": "x", "objectName": "finance_customers", "fullyQualifiedName": "Finance.demo_env.finance_customers"},
        {"col": "y", "objectName": "finance_customers", "fullyQualifiedName": "Finance.dptest.finance_customers"},
    ]
    with patch.object(client, "_request", return_value=rows):
        result = client.get_columns("finance_customers", "Finance", fqn="Finance.demo_env.finance_customers")
    assert result == [rows[0]]


# ---------------------------------------------------------------------------
# 14b. get_columns — fallback objectName filter excludes substring siblings
# ---------------------------------------------------------------------------


def test_get_columns_excludes_substring_siblings():
    client = _client_with_access_token()
    # BigID substring-matches "my_table" against "my_table2"; client must drop the sibling
    rows = [
        {"col": "x", "objectName": "my_table"},
        {"col": "y", "objectName": "my_table2"},
    ]
    with patch.object(client, "_request", return_value=rows):
        result = client.get_columns("my_table", "my_source")
    assert result == [{"col": "x", "objectName": "my_table"}]


# ---------------------------------------------------------------------------
# 14. get_glossary_items — non-list response raises BigIDAPIError
# ---------------------------------------------------------------------------


def test_get_glossary_items_non_list_raises():
    client = _client_with_access_token()
    with patch.object(client, "_request", return_value={"error": "bad"}), pytest.raises(BigIDAPIError):
        client.get_glossary_items()


# ---------------------------------------------------------------------------
# 15. get_connections — non-dict response raises BigIDAPIError
# ---------------------------------------------------------------------------


def test_get_connections_non_dict_raises():
    client = _client_with_access_token()
    with patch.object(client, "_request", return_value=[1, 2, 3]), pytest.raises(BigIDAPIError):
        client.get_connections()


# ---------------------------------------------------------------------------
# 16. get_columns — filter expression format is correct for normal names
# ---------------------------------------------------------------------------


def test_get_columns_filter_expression_format():
    client = _client_with_access_token()
    captured: list[dict] = []

    def fake_request(endpoint, params=None):
        captured.append(params or {})
        return []

    with patch.object(client, "_request", side_effect=fake_request):
        client.get_columns("my_table", "my_source")

    filter_val = captured[0].get("filter", "")
    assert filter_val == 'objectName = "my_table" AND source = "my_source"'


# ---------------------------------------------------------------------------
# 17. get_catalog_objects — pagination stops when last page < PAGE_SIZE
# ---------------------------------------------------------------------------


def test_get_catalog_objects_pagination():
    client = _client_with_access_token()

    page1 = {"results": [{"id": i} for i in range(PAGE_SIZE)]}
    page2 = {"results": [{"id": i} for i in range(3)]}
    pages = [page1, page2]
    call_count = 0

    def fake_request(endpoint, params=None):
        nonlocal call_count
        result = pages[call_count]
        call_count += 1
        return result

    with patch.object(client, "_request", side_effect=fake_request):
        items = list(client.get_catalog_objects())

    assert len(items) == PAGE_SIZE + 3
    assert call_count == 2


# ---------------------------------------------------------------------------
# 18. get_idsor_attribute_map — parsing edge cases
# ---------------------------------------------------------------------------


def _idsor_response(attributes: list) -> dict:
    return {"data": {"attributes": attributes}}


def test_get_idsor_attribute_map_normal():
    client = _client_with_access_token()
    payload = _idsor_response([
        {
            "attributeType": "IDSoR Attribute",
            "attributeName": "full_name",
            "displayName": "Full Name",
            "friendlyName": {"friendlyName": "Full Name", "glossaryId": "fn_item_abc"},
        }
    ])
    with patch.object(client, "_request", return_value=payload):
        result = client.get_idsor_attribute_map()

    assert "full_name" in result
    info = result["full_name"]
    assert isinstance(info, IDSoRAttributeInfo)
    assert info.friendly_name == "Full Name"
    assert info.glossary_id == "fn_item_abc"


def test_get_idsor_attribute_map_empty_attribute_name_skipped():
    client = _client_with_access_token()
    payload = _idsor_response([
        {
            "attributeType": "IDSoR Attribute",
            "attributeName": "",  # empty — must be skipped
            "displayName": "Should Not Appear",
            "friendlyName": {},
        }
    ])
    with patch.object(client, "_request", return_value=payload):
        result = client.get_idsor_attribute_map()

    assert result == {}


def test_get_idsor_attribute_map_empty_friendly_name_obj():
    """When friendlyName is {} (uncurated), fall back to displayName."""
    client = _client_with_access_token()
    payload = _idsor_response([
        {
            "attributeType": "IDSoR Attribute",
            "attributeName": "passport_number",
            "displayName": "Passport Number",
            "friendlyName": {},
        }
    ])
    with patch.object(client, "_request", return_value=payload):
        result = client.get_idsor_attribute_map()

    assert "passport_number" in result
    info = result["passport_number"]
    assert info.friendly_name == "Passport Number"
    assert info.glossary_id is None


def test_get_idsor_attribute_map_non_dict_response_raises():
    """A non-dict response (e.g. a list) must raise BigIDAPIError."""
    client = _client_with_access_token()
    with patch.object(client, "_request", return_value=[]), pytest.raises(BigIDAPIError):
        client.get_idsor_attribute_map()


def test_get_idsor_attribute_map_non_idsor_types_excluded():
    """Only IDSoR Attribute entries should appear in the map; Classification entries are skipped."""
    client = _client_with_access_token()
    payload = _idsor_response([
        {
            "attributeType": "Classification",
            "attributeName": "email_classifier",
            "displayName": "Email",
            "friendlyName": {},
        },
        {
            "attributeType": "IDSoR Attribute",
            "attributeName": "email",
            "displayName": "Email",
            "friendlyName": {"friendlyName": "Email", "glossaryId": None},
        },
    ])
    with patch.object(client, "_request", return_value=payload):
        result = client.get_idsor_attribute_map()

    assert "email_classifier" not in result
    assert "email" in result
