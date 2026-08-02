import pytest
import requests
import requests_mock

from datahub.ingestion.source.sap_mdg.config import SapMdgSourceConfig
from datahub.ingestion.source.sap_mdg.odata_client import SapMdgODataClient
from datahub.ingestion.source.sap_mdg.report import SapMdgSourceReport

_BASE_URL = "https://sap-gw.example.com:44300"
_SERVICE = "/sap/opu/odata/sap/ZMDG_DEMO_SRV"


def _client(**overrides: object) -> SapMdgODataClient:
    config = SapMdgSourceConfig.model_validate(
        {
            "base_url": _BASE_URL,
            "services": [_SERVICE],
            "token": "secret-token",
            **overrides,
        }
    )
    return SapMdgODataClient(config, SapMdgSourceReport())


def test_metadata_url_strips_slashes_and_appends_document():
    client = _client()
    # Regardless of leading/trailing slashes on the service path, the URL joins
    # cleanly with a single separator and the $metadata document suffix.
    assert (
        client._metadata_url("//sap/opu/odata/sap/ZMDG_DEMO_SRV/")
        == f"{_BASE_URL}/sap/opu/odata/sap/ZMDG_DEMO_SRV/$metadata"
    )


def test_query_params_include_sap_client_when_set():
    assert _client(sap_client="100")._query_params() == {"sap-client": "100"}


def test_query_params_empty_without_sap_client():
    assert _client()._query_params() == {}


def test_bearer_token_sets_authorization_header():
    client = _client(token="my-token")
    assert client.session.headers["Authorization"] == "Bearer my-token"
    assert client.session.auth is None


def test_basic_auth_sets_session_auth():
    client = _client(token=None, username="user", password="pass")
    assert client.session.auth == ("user", "pass")
    assert "Authorization" not in client.session.headers


def test_client_certificate_and_key_set_cert_tuple():
    client = _client(
        client_certificate_path="/certs/client.pem",
        client_key_path="/certs/client.key",
    )
    assert client.session.cert == ("/certs/client.pem", "/certs/client.key")


def test_client_certificate_only_set_as_string():
    client = _client(client_certificate_path="/certs/bundle.pem")
    assert client.session.cert == "/certs/bundle.pem"


def test_ca_certificate_path_overrides_verify_flag():
    # A CA bundle path takes precedence over the boolean verify flag.
    client = _client(ca_certificate_path="/certs/ca.pem", verify_ssl=False)
    assert client.session.verify == "/certs/ca.pem"


def test_verify_ssl_flag_used_without_ca_path():
    assert _client(verify_ssl=False).session.verify is False


def test_fetch_metadata_returns_content():
    client = _client(sap_client="100")
    url = f"{_BASE_URL}{_SERVICE}/$metadata"
    with requests_mock.Mocker() as mock:
        mock.get(url, content=b"<edmx/>")
        assert client.fetch_metadata(_SERVICE) == b"<edmx/>"
        assert mock.last_request.qs["sap-client"] == ["100"]


def test_fetch_metadata_raises_on_http_error():
    client = _client()
    url = f"{_BASE_URL}{_SERVICE}/$metadata"
    with requests_mock.Mocker() as mock:
        mock.get(url, status_code=401)
        with pytest.raises(requests.exceptions.HTTPError):
            client.fetch_metadata(_SERVICE)


def test_test_connection_uses_first_service():
    client = _client(services=[_SERVICE, "/sap/opu/odata/sap/OTHER_SRV"])
    url = f"{_BASE_URL}{_SERVICE}/$metadata"
    with requests_mock.Mocker() as mock:
        mock.get(url, content=b"<edmx/>")
        client.test_connection()
        assert mock.last_request.url.startswith(url)


_TABLE_SERVICE = "/sap/opu/odata/sap/ZTABLE_READ_SRV"
_ROWS_URL = f"{_BASE_URL}{_TABLE_SERVICE}/DRFC_APPL"


def _drf_client(**overrides: object) -> SapMdgODataClient:
    return _client(
        drf={
            "enabled": True,
            "table_read_service": _TABLE_SERVICE,
            **overrides,
        }
    )


def test_fetch_rows_follows_the_v2_next_link():
    # SAP Gateway caps a collection response and returns "__next" inside "d".
    # Without following it the DRF customizing tables are read partially and every
    # lineage edge that depends on a later row silently disappears.
    client = _drf_client()
    with requests_mock.Mocker() as mock:
        mock.get(
            _ROWS_URL,
            json={
                "d": {
                    "results": [{"APPL": "M1"}],
                    "__next": f"{_ROWS_URL}?$skiptoken=1",
                }
            },
        )
        mock.get(f"{_ROWS_URL}?$skiptoken=1", json={"d": {"results": [{"APPL": "M2"}]}})
        rows = client._fetch_rows("DRFC_APPL")

    assert [row["APPL"] for row in rows] == ["M1", "M2"]


def test_fetch_rows_follows_a_relative_v4_next_link():
    client = _drf_client()
    with requests_mock.Mocker() as mock:
        mock.get(
            _ROWS_URL,
            json={"value": [{"APPL": "M1"}], "@odata.nextLink": "DRFC_APPL?page=2"},
        )
        mock.get(f"{_ROWS_URL}?page=2", json={"value": [{"APPL": "M2"}]})
        rows = client._fetch_rows("DRFC_APPL")

    assert [row["APPL"] for row in rows] == ["M1", "M2"]


def test_fetch_rows_stops_when_the_next_link_repeats():
    client = _drf_client()
    with requests_mock.Mocker() as mock:
        mock.get(
            _ROWS_URL, json={"d": {"results": [{"APPL": "M1"}], "__next": _ROWS_URL}}
        )
        rows = client._fetch_rows("DRFC_APPL")

    assert [row["APPL"] for row in rows] == ["M1"]
    assert len(client.report.warnings) == 1


def test_fetch_rows_single_page_makes_one_request():
    client = _drf_client()
    with requests_mock.Mocker() as mock:
        mock.get(_ROWS_URL, json={"d": {"results": [{"APPL": "M1"}]}})
        assert client._fetch_rows("DRFC_APPL") == [{"APPL": "M1"}]
        assert mock.call_count == 1
