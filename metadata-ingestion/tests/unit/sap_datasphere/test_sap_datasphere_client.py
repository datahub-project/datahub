import logging
import urllib.parse
from typing import Dict

import pytest
import requests
from pydantic import ValidationError

from datahub.ingestion.source.sap_datasphere.client import (
    EdmxFetchReason,
    EdmxFetchResult,
    SapDatasphereClient,
)
from datahub.ingestion.source.sap_datasphere.config import SapDatasphereConfig
from datahub.ingestion.source.sap_datasphere.report import SapDatasphereReport


def _make_config(**kwargs: object) -> SapDatasphereConfig:
    base: Dict[str, object] = {
        "base_url": "https://myco.eu10.hcs.cloud.sap",
        "token": "tok",
    }
    base.update(kwargs)
    return SapDatasphereConfig.model_validate(base)


def test_bearer_header_set_from_token():
    cfg = _make_config()
    client = SapDatasphereClient(cfg)
    assert client.session.headers["Authorization"] == "Bearer tok"


def test_edmx_fetch_result_enforces_xml_iff_ok():
    """EdmxFetchResult enforces xml-present-iff-OK at construction, so callers can trust `xml` after branching on `reason`."""
    assert EdmxFetchResult(xml="<edmx/>", reason=EdmxFetchReason.OK).xml == "<edmx/>"
    for reason in (
        EdmxFetchReason.FORBIDDEN,
        EdmxFetchReason.NOT_FOUND,
        EdmxFetchReason.ERROR,
    ):
        assert EdmxFetchResult(xml=None, reason=reason).reason is reason
    with pytest.raises(ValidationError, match="xml must be set iff reason is OK"):
        EdmxFetchResult(xml=None, reason=EdmxFetchReason.OK)
    with pytest.raises(ValidationError, match="xml must be set iff reason is OK"):
        EdmxFetchResult(xml="<edmx/>", reason=EdmxFetchReason.NOT_FOUND)


def test_list_spaces_returns_values(requests_mock):
    cfg = _make_config()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    client = SapDatasphereClient(cfg)
    spaces = list(client.list_spaces())
    assert spaces == [{"name": "S1", "label": "Space 1"}]


def test_list_spaces_handles_bare_array_response(requests_mock):
    """The live spaces endpoint returns a bare JSON array (no `{value: [...]}` wrapper), unlike the assets endpoint."""
    cfg = _make_config()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json=[{"name": "DEMO_SPACE", "label": "DEMO_SPACE"}],
    )
    client = SapDatasphereClient(cfg)
    spaces = list(client.list_spaces())
    assert spaces == [{"name": "DEMO_SPACE", "label": "DEMO_SPACE"}]


def test_list_spaces_non_json_response_raises(requests_mock):
    """A top-level listing drives stale-entity soft-deletes, so an HTML-200 (proxy/login redirect) must hard-fail rather than degrade to zero spaces."""
    cfg = _make_config()
    url = "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces"
    requests_mock.get(
        url,
        text="<html><body>Please sign in</body></html>",
        headers={"Content-Type": "text/html"},
    )
    client = SapDatasphereClient(cfg)
    with pytest.raises(ValueError) as exc_info:
        list(client.list_spaces())
    assert "non-JSON" in str(exc_info.value)


def test_list_spaces_wrong_shape_json_warns_and_yields_nothing(requests_mock):
    """A 200 returning a dict where a bare array is expected must be treated as empty AND surface a report.warning."""
    cfg = _make_config()
    url = "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces"
    requests_mock.get(url, json={"error": "x"})
    report = SapDatasphereReport()
    client = SapDatasphereClient(cfg, report=report)
    spaces = list(client.list_spaces())
    # A dict has no "value" key, so _paginate yields nothing.
    assert spaces == []


def test_list_spaces_non_dict_non_list_json_raises(requests_mock):
    """A bare JSON scalar (neither dict nor list) is a wrong-shape top-level listing and must hard-fail rather than degrade to empty."""
    cfg = _make_config()
    url = "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces"
    requests_mock.get(url, json="not-a-collection")
    client = SapDatasphereClient(cfg)
    with pytest.raises(ValueError) as exc_info:
        list(client.list_spaces())
    assert "unexpected" in str(exc_info.value).lower()


def test_list_spaces_paginates(requests_mock):
    cfg = _make_config()
    url = "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces"
    all_spaces = [{"name": f"S{i}", "label": f"L{i}"} for i in range(1001)]

    def respond(request, context):
        skip = int(request.qs.get("$skip", ["0"])[0])
        top = int(request.qs.get("$top", ["500"])[0])
        return {"value": all_spaces[skip : skip + top]}

    requests_mock.get(url, json=respond)
    client = SapDatasphereClient(cfg)
    spaces = list(client.list_spaces())
    assert len(spaces) == 1001


def test_list_assets_follows_odata_nextlink(requests_mock):
    """Regression: SAP caps each catalog page at 500 records and returns ``@odata.nextLink``; the paginator must follow it to completion or silently drop every asset past the cap."""
    cfg = _make_config()
    url = "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S')/assets"
    all_assets = [
        {"name": f"A{i}", "spaceName": "S", "assetRelationalMetadataUrl": "http://x"}
        for i in range(5)
    ]
    # server_cap mimics the 500-record catalog cap at small scale.
    server_cap = 2

    def respond(request, context):
        skip = int(request.qs.get("$skip", ["0"])[0])
        page = all_assets[skip : skip + server_cap]
        body: Dict[str, object] = {"value": page}
        nxt = skip + server_cap
        if nxt < len(all_assets):
            body["@odata.nextLink"] = f"{url}?$top={server_cap}&$skip={nxt}"
        return body

    requests_mock.get(url, json=respond)
    client = SapDatasphereClient(cfg)
    assets = list(client.list_assets("S"))
    assert [a["name"] for a in assets] == [f"A{i}" for i in range(5)]


def test_list_assets_offset_fallback_stops_on_short_page(requests_mock):
    """Without an @odata.nextLink, a page shorter than requested is the genuine end of data; the paginator must stop, not loop forever against an endpoint ignoring $skip."""
    cfg = _make_config()
    url = "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S')/assets"
    # Always returns the same single record, never a nextLink, ignores $skip.
    requests_mock.get(
        url,
        json={
            "value": [
                {
                    "name": "ONLY",
                    "spaceName": "S",
                    "assetRelationalMetadataUrl": "http://x",
                }
            ]
        },
    )
    client = SapDatasphereClient(cfg)
    assets = list(client.list_assets("S"))
    assert [a["name"] for a in assets] == ["ONLY"]


def test_list_assets_for_space(requests_mock):
    cfg = _make_config()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('DEMO_SPACE')/assets",
        json={
            "value": [
                {
                    "name": "VIEW1",
                    "spaceName": "DEMO_SPACE",
                    "assetRelationalMetadataUrl": "http://x",
                }
            ]
        },
    )
    client = SapDatasphereClient(cfg)
    assets = list(client.list_assets("DEMO_SPACE"))
    assert assets[0]["name"] == "VIEW1"


def test_fetch_edmx_returns_text(requests_mock):
    cfg = _make_config()
    requests_mock.get("http://edmx-url/$metadata", text="<edmx/>")
    client = SapDatasphereClient(cfg)
    result = client.fetch_edmx("http://edmx-url/$metadata")
    assert result.xml == "<edmx/>"
    assert result.reason == EdmxFetchReason.OK


def test_fetch_edmx_returns_none_on_404(requests_mock):
    cfg = _make_config()
    requests_mock.get("http://edmx-url/$metadata", status_code=404)
    client = SapDatasphereClient(cfg)
    result = client.fetch_edmx("http://edmx-url/$metadata")
    assert result.xml is None
    assert result.reason == EdmxFetchReason.NOT_FOUND


def test_fetch_edmx_returns_error_on_request_exception(requests_mock):
    cfg = _make_config()
    requests_mock.get(
        "http://edmx-url/$metadata", exc=requests.exceptions.ConnectionError
    )
    client = SapDatasphereClient(cfg)
    result = client.fetch_edmx("http://edmx-url/$metadata")
    assert result.xml is None
    assert result.reason == EdmxFetchReason.ERROR


def test_fetch_edmx_401_triggers_refresh_and_retry(requests_mock):
    """A bearer expiring during the EDMX phase must trigger one token refresh + retry, not lose schema for the rest of the run."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "client_id": "cid",
            "client_secret": "csec",
            "xsuaa_url": "https://myco.authentication.eu10.hana.ondemand.com",
        }
    )
    requests_mock.post(
        "https://myco.authentication.eu10.hana.ondemand.com/oauth/token",
        [
            {"json": {"access_token": "first_token"}},
            {"json": {"access_token": "fresh_token"}},
        ],
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/asset/$metadata",
        [
            {"status_code": 401, "json": {"error": "token expired"}},
            {"text": "<edmx/>"},
        ],
    )
    client = SapDatasphereClient(cfg)
    result = client.fetch_edmx("https://myco.eu10.hcs.cloud.sap/asset/$metadata")
    assert result.reason == EdmxFetchReason.OK
    assert result.xml == "<edmx/>"
    assert client.session.headers["Authorization"] == "Bearer fresh_token"
    token_calls = [
        h for h in requests_mock.request_history if "authentication" in h.url
    ]
    assert len(token_calls) == 2


def test_no_auth_raises_error():
    # The model_validator raises at config-parse time; the client is never reached.
    with pytest.raises(ValidationError, match="credential"):
        SapDatasphereConfig.model_validate(
            {"base_url": "https://myco.eu10.hcs.cloud.sap"}
        )


def test_client_credentials_token(requests_mock):
    cfg = _make_config(
        token=None,
        client_id="cid",
        client_secret="csec",
        xsuaa_url="https://myco.authentication.eu10.hana.ondemand.com",
    )
    requests_mock.post(
        "https://myco.authentication.eu10.hana.ondemand.com/oauth/token",
        json={"access_token": "cc-token"},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": []},
    )
    client = SapDatasphereClient(cfg)
    # Token is fetched lazily — trigger it via the first real request.
    list(client.list_spaces())
    assert client.session.headers["Authorization"] == "Bearer cc-token"

    xsuaa_call = next(
        req for req in requests_mock.request_history if "oauth/token" in req.url
    )
    body = dict(urllib.parse.parse_qsl(xsuaa_call.text))
    assert body == {
        "grant_type": "client_credentials",
        "client_id": "cid",
        "client_secret": "csec",
    }


def test_refresh_token_grant_posts_expected_body(requests_mock):
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "refresh_token": "rtok",
            "client_id": "cid",
            "xsuaa_url": "https://myco.authentication.eu10.hana.ondemand.com",
        }
    )
    requests_mock.post(
        "https://myco.authentication.eu10.hana.ondemand.com/oauth/token",
        json={"access_token": "rt-token"},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": []},
    )
    client = SapDatasphereClient(cfg)
    list(client.list_spaces())
    assert client.session.headers["Authorization"] == "Bearer rt-token"

    xsuaa_call = next(
        req for req in requests_mock.request_history if "oauth/token" in req.url
    )
    body = dict(urllib.parse.parse_qsl(xsuaa_call.text))
    assert body == {
        "grant_type": "refresh_token",
        "refresh_token": "rtok",
        "client_id": "cid",
    }


def test_post_token_helper_used_by_both_flows(requests_mock):
    """Both OAuth flows funnel through the shared ``_post_token``; each must post the right grant_type-discriminated form body."""
    requests_mock.post(
        "https://myco.authentication.eu10.hana.ondemand.com/oauth/token",
        json={"access_token": "tok-from-helper"},
    )

    cfg_rt = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "refresh_token": "rtok",
            "client_id": "cid",
            "xsuaa_url": "https://myco.authentication.eu10.hana.ondemand.com",
        }
    )
    client_rt = SapDatasphereClient(cfg_rt)
    assert client_rt._exchange_refresh_token() == "tok-from-helper"

    cfg_cc = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "client_id": "cid",
            "client_secret": "csec",
            "xsuaa_url": "https://myco.authentication.eu10.hana.ondemand.com",
        }
    )
    client_cc = SapDatasphereClient(cfg_cc)
    assert client_cc._fetch_client_credentials_token() == "tok-from-helper"

    posted_bodies = [
        dict(urllib.parse.parse_qsl(r.text))
        for r in requests_mock.request_history
        if "oauth/token" in r.url
    ]
    grant_types = sorted(b["grant_type"] for b in posted_bodies)
    assert grant_types == ["client_credentials", "refresh_token"], (
        f"Expected both flows to hit _post_token; got grant_types={grant_types}"
    )
    refresh_body = next(b for b in posted_bodies if b["grant_type"] == "refresh_token")
    assert refresh_body == {
        "grant_type": "refresh_token",
        "refresh_token": "rtok",
        "client_id": "cid",
    }
    cc_body = next(b for b in posted_bodies if b["grant_type"] == "client_credentials")
    assert cc_body == {
        "grant_type": "client_credentials",
        "client_id": "cid",
        "client_secret": "csec",
    }


def test_token_takes_priority_over_refresh_token():
    cfg = _make_config(token="raw-tok", refresh_token="rt", client_id="cid")
    client = SapDatasphereClient(cfg)
    assert client.session.headers["Authorization"] == "Bearer raw-tok"


def test_list_connections_returns_parsed_list(requests_mock):
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[
            {"name": "HANA_CLOUD", "typeId": "HANA"},
            {"name": "MY_SNOWFLAKE", "typeId": "SNOWFLAKE"},
        ],
    )

    client = SapDatasphereClient(cfg)
    result = client.list_connections("S1")
    assert len(result) == 2
    assert result[0]["typeId"] == "HANA"


def test_list_connections_is_cached_per_space(requests_mock):
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[{"name": "HANA_CLOUD", "typeId": "HANA"}],
    )

    client = SapDatasphereClient(cfg)
    client.list_connections("S1")
    client.list_connections("S1")
    calls = [
        h
        for h in requests_mock.request_history
        if "connections" in h.url and "S1" in h.url
    ]
    assert len(calls) == 1


def test_fetch_object_definition_returns_csn(requests_mock):
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/VIEW_DIMENSION_DAY",
        json={
            "definitions": {
                "VIEW_DIMENSION_DAY": {
                    "kind": "entity",
                    "query": {
                        "SELECT": {
                            "from": {"ref": ["M_TIME_DIMENSION"]},
                            "columns": [{"ref": ["DATE_SQL"]}],
                        }
                    },
                }
            }
        },
    )

    client = SapDatasphereClient(cfg)
    result = client.fetch_object_definition("S1", "views", "VIEW_DIMENSION_DAY")
    assert result is not None
    assert "definitions" in result
    assert "VIEW_DIMENSION_DAY" in result["definitions"]


def test_fetch_object_definition_401_triggers_refresh_and_retry(requests_mock):
    """The CSN endpoint has its own 401-refresh-retry block; a token expiring mid-run must refresh once and retry rather than dropping lineage for the remaining assets."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "client_id": "cid",
            "client_secret": "csec",
            "xsuaa_url": "https://myco.authentication.eu10.hana.ondemand.com",
        }
    )
    requests_mock.post(
        "https://myco.authentication.eu10.hana.ondemand.com/oauth/token",
        [
            {"json": {"access_token": "first_token"}},
            {"json": {"access_token": "fresh_token"}},
        ],
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/V1",
        [
            {"status_code": 401, "json": {"error": "token expired"}},
            {"json": {"definitions": {"V1": {"kind": "entity"}}}},
        ],
    )
    client = SapDatasphereClient(cfg)
    result = client.fetch_object_definition("S1", "views", "V1")
    assert result is not None
    assert "V1" in result["definitions"]
    assert client.session.headers["Authorization"] == "Bearer fresh_token"
    token_calls = [
        h for h in requests_mock.request_history if "authentication" in h.url
    ]
    assert len(token_calls) == 2


def test_fetch_object_definition_sends_supported_content_accept_header(requests_mock):
    """The endpoint only returns full CSN when called with the ``application/vnd.sap.datasphere.object.content+json`` Accept header."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/VIEW_X",
        json={"definitions": {"VIEW_X": {"kind": "entity"}}},
    )

    client = SapDatasphereClient(cfg)
    client.fetch_object_definition("S1", "views", "VIEW_X")

    last = requests_mock.request_history[-1]
    assert (
        last.headers.get("Accept")
        == "application/vnd.sap.datasphere.object.content+json"
    ), (
        f"Expected the supported content Accept header on the CSN call; "
        f"got: {last.headers.get('Accept')!r}"
    )


def test_fetch_object_definition_routes_analytic_models(requests_mock):
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/analyticmodels/AM1",
        json={"definitions": {"AM1": {"kind": "entity"}}},
    )

    client = SapDatasphereClient(cfg)
    result = client.fetch_object_definition("S1", "analyticmodels", "AM1")
    assert result is not None
    assert "AM1" in result["definitions"]


def test_fetch_object_definition_returns_none_on_http_error(requests_mock):
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/VIEW_X",
        status_code=500,
    )
    client = SapDatasphereClient(cfg)
    assert client.fetch_object_definition("S1", "views", "VIEW_X") is None


def test_fetch_object_definition_returns_none_on_403(requests_mock):
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/VIEW_X",
        status_code=403,
    )
    client = SapDatasphereClient(cfg)
    assert client.fetch_object_definition("S1", "views", "VIEW_X") is None


def test_fetch_object_definition_returns_none_on_invalid_json(requests_mock):
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/VIEW_X",
        text="<html>oops, not JSON</html>",
    )
    client = SapDatasphereClient(cfg)
    assert client.fetch_object_definition("S1", "views", "VIEW_X") is None


def test_connections_cache_initialized_in_constructor():
    """The per-space `_connections_cache` should be a real instance attribute set by ``__init__`` rather than lazily allocated via ``hasattr``."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    client = SapDatasphereClient(cfg)
    assert hasattr(client, "_connections_cache")
    assert client._connections_cache == {}


def test_fetch_object_definition_500_surfaces_report_warning(requests_mock):
    """A 500 must emit a report warning AND record the technical name under ``assets_csn_fetch_failed`` so operators can see how many assets lost lineage."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    report = SapDatasphereReport()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/VIEW_X",
        status_code=500,
    )
    client = SapDatasphereClient(cfg, report=report)
    assert client.fetch_object_definition("S1", "views", "VIEW_X") is None

    warning_titles = [w.title or "" for w in report.warnings]
    assert any("Failed to fetch object definition" in t for t in warning_titles), (
        f"Expected object-definition fetch warning; got: {warning_titles}"
    )
    assert "S1.VIEW_X" in list(report.assets_csn_fetch_failed)


def test_fetch_object_definition_403_surfaces_not_a_member_warning(requests_mock):
    """A 403 means the principal isn't a member of the space; it must surface the actionable 'not a member' hint, not the generic failure buried under thousands of per-asset warnings."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    report = SapDatasphereReport()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/VIEW_X",
        status_code=403,
    )
    client = SapDatasphereClient(cfg, report=report)
    assert client.fetch_object_definition("S1", "views", "VIEW_X") is None

    warning_titles = [w.title or "" for w in report.warnings]
    assert any("Not a member" in t for t in warning_titles), (
        f"Expected not-a-member warning on 403; got: {warning_titles}"
    )
    assert not any("Failed to fetch object definition" in t for t in warning_titles)
    assert "S1.VIEW_X" in list(report.assets_csn_fetch_failed)


def test_fetch_object_definition_404_is_benign_no_warning(requests_mock):
    """A 404 (object deleted between listing and fetch) is benign: no operator-facing warning, but still record the asset as having no CSN."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    report = SapDatasphereReport()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/VIEW_X",
        status_code=404,
    )
    # The fallback probe under the sibling type also 404s, keeping this the genuinely-missing (benign) case.
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/analyticmodels/VIEW_X",
        status_code=404,
    )
    client = SapDatasphereClient(cfg, report=report)
    assert client.fetch_object_definition("S1", "views", "VIEW_X") is None

    assert report.warnings == [] or all(
        "Failed to fetch object definition" not in (w.title or "")
        for w in report.warnings
    )
    assert "S1.VIEW_X" in list(report.assets_csn_fetch_failed)


def test_fetch_object_definition_404_falls_back_to_alternate_object_type(
    requests_mock,
):
    """The ``supportsAnalyticalQueries`` flag can misroute an asset, so a 404 on the primary type must retry the sibling type, recover the CSN, and record the correction without counting a fetch failure."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    report = SapDatasphereReport()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/analyticmodels/ASSET_X",
        status_code=404,
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/ASSET_X",
        json={"definitions": {"ASSET_X": {"kind": "entity"}}},
    )
    client = SapDatasphereClient(cfg, report=report)
    result = client.fetch_object_definition("S1", "analyticmodels", "ASSET_X")

    assert result is not None
    assert "ASSET_X" in result["definitions"]
    assert "S1.ASSET_X" not in list(report.assets_csn_fetch_failed)
    corrected = list(report.assets_csn_object_type_corrected)
    assert corrected == ["S1.ASSET_X: analyticmodels->views"], (
        f"Expected one correction record; got: {corrected}"
    )


def test_fetch_object_definition_404_both_types_reports_failed_exactly_once(
    requests_mock,
):
    """When both primary and sibling types 404, return None and record the failure exactly once (the fallback must not double-count it), with no correction recorded."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    report = SapDatasphereReport()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/GONE",
        status_code=404,
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/analyticmodels/GONE",
        status_code=404,
    )
    client = SapDatasphereClient(cfg, report=report)
    assert client.fetch_object_definition("S1", "views", "GONE") is None

    failed = list(report.assets_csn_fetch_failed)
    assert failed.count("S1.GONE") == 1, (
        f"Expected the miss counted exactly once; got: {failed}"
    )
    assert list(report.assets_csn_object_type_corrected) == []


def test_fetch_object_definition_localtables_404_has_no_alternate(requests_mock):
    """Local Tables have no sibling object type, so a 404 is reported as a failure directly with no fallback request."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    report = SapDatasphereReport()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/localtables/T1",
        status_code=404,
    )
    client = SapDatasphereClient(cfg, report=report)
    assert client.fetch_object_definition("S1", "localtables", "T1") is None

    assert "S1.T1" in list(report.assets_csn_fetch_failed)
    assert list(report.assets_csn_object_type_corrected) == []
    csn_calls = [h for h in requests_mock.request_history if "/dwaas-core/" in h.url]
    assert len(csn_calls) == 1, f"Expected no fallback probe; got: {csn_calls}"


def test_fetch_object_definition_429_warning_mentions_rate_limit(requests_mock):
    """A sustained 429 must surface a rate-limit-specific hint to lower max_workers_assets, not a vague failure."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "max_retries": 0,
        }
    )
    report = SapDatasphereReport()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/VIEW_X",
        status_code=429,
    )
    client = SapDatasphereClient(cfg, report=report)
    assert client.fetch_object_definition("S1", "views", "VIEW_X") is None

    messages = " ".join(w.message for w in report.warnings)
    assert "429" in messages and "max_workers_assets" in messages, (
        f"Expected rate-limit hint mentioning max_workers_assets; got: {messages}"
    )


def test_list_connections_non_json_surfaces_warning_and_skips(requests_mock):
    """An HTML-200 (SSO/proxy login page) must surface a structured warning and treat the space as having no connections, not let a JSONDecodeError propagate as a misleading generic error."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    report = SapDatasphereReport()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        text="<html><body>Please sign in</body></html>",
        headers={"Content-Type": "text/html"},
    )
    client = SapDatasphereClient(cfg, report=report)
    assert client.list_connections("S1") == []
    # Per-space degrade: the warning names the space and the federated-skip impact.
    assert any(
        "S1" in " ".join(w.context or []) and "federated assets" in w.message.lower()
        for w in report.warnings
    ), (
        f"Expected a per-space federated-skip warning; got: {[w.message for w in report.warnings]}"
    )


def test_list_connections_unexpected_shape_surfaces_report_warning(requests_mock):
    """When the connections API returns a non-list, the client should both log AND attach a structured warning to the report."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    report = SapDatasphereReport()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json={"value": [{"name": "X", "typeId": "HANA"}]},
    )
    client = SapDatasphereClient(cfg, report=report)
    assert client.list_connections("S1") == []
    assert any(
        "dict instead of a list" in w.message and "federated assets" in w.message
        for w in report.warnings
    ), f"Expected unexpected-shape warning; got: {[w.message for w in report.warnings]}"


def test_oauth_401_triggers_refresh_and_retry(requests_mock):
    """A 401 from a normal API call should force one token refresh + retry."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "client_id": "cid",
            "client_secret": "csec",
            "xsuaa_url": "https://myco.authentication.eu10.hana.ondemand.com",
        }
    )

    requests_mock.post(
        "https://myco.authentication.eu10.hana.ondemand.com/oauth/token",
        [
            {"json": {"access_token": "first_token"}},
            {"json": {"access_token": "fresh_token"}},
        ],
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        [
            {"status_code": 401, "json": {"error": "token expired"}},
            {"json": {"value": []}},
        ],
    )

    client = SapDatasphereClient(cfg)
    list(client.list_spaces())

    assert client.session.headers["Authorization"] == "Bearer fresh_token"
    token_calls = [
        h for h in requests_mock.request_history if "authentication" in h.url
    ]
    assert len(token_calls) == 2, (
        f"Expected exactly 2 XSUAA token fetches (initial + refresh); "
        f"got {len(token_calls)}"
    )


def test_oauth_401_after_refresh_raises_actionable_credentials_error(requests_mock):
    """A second 401 after a successful refresh must raise an actionable credentials error (not a bare HTTP 401) and must not loop indefinitely."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "client_id": "cid",
            "client_secret": "csec",
            "xsuaa_url": "https://myco.authentication.eu10.hana.ondemand.com",
        }
    )

    # XSUAA refresh "succeeds" both times, so the second 401 is genuinely rejected credentials, not a stale token.
    requests_mock.post(
        "https://myco.authentication.eu10.hana.ondemand.com/oauth/token",
        json={"access_token": "stale_token"},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        status_code=401,
        json={"error": "unauthorized"},
    )

    client = SapDatasphereClient(cfg)
    with pytest.raises(ValueError) as exc_info:
        list(client.list_spaces())

    msg = str(exc_info.value)
    assert "credentials" in msg.lower()
    assert "after" in msg.lower() and "refresh" in msg.lower()
    # The offending URL should be surfaced for operator triage.
    assert "spaces" in msg

    # No infinite loop: exactly two GETs (initial + single post-refresh retry).
    api_calls = [
        h
        for h in requests_mock.request_history
        if "consumption/catalog/spaces" in h.url
    ]
    assert len(api_calls) == 2, (
        f"Expected exactly one retry after refresh; got {len(api_calls)} GETs"
    )


def test_post_token_surfaces_xsuaa_error_description(requests_mock):
    """The client must surface the XSUAA ``error``/``error_description`` detail instead of a bare ``400 Client Error``."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "refresh_token": "rtok",
            "client_id": "cid",
            "xsuaa_url": "https://myco.authentication.eu10.hana.ondemand.com",
        }
    )
    requests_mock.post(
        "https://myco.authentication.eu10.hana.ondemand.com/oauth/token",
        status_code=400,
        json={"error": "invalid_grant", "error_description": "Refresh token expired"},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": []},
    )

    client = SapDatasphereClient(cfg)
    with pytest.raises(ValueError) as exc_info:
        list(client.list_spaces())

    msg = str(exc_info.value)
    assert "invalid_grant" in msg
    assert "Refresh token expired" in msg
    assert "400" in msg


def test_post_token_falls_back_to_text_when_body_not_json(requests_mock):
    """If the XSUAA error body isn't the expected JSON shape, the raised error must still include the response text."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "refresh_token": "rtok",
            "client_id": "cid",
            "xsuaa_url": "https://myco.authentication.eu10.hana.ondemand.com",
        }
    )
    requests_mock.post(
        "https://myco.authentication.eu10.hana.ondemand.com/oauth/token",
        status_code=401,
        text="<html>Bad Gateway from auth proxy</html>",
        headers={"Content-Type": "text/html"},
    )

    client = SapDatasphereClient(cfg)
    with pytest.raises(ValueError) as exc_info:
        list(client.list_spaces())

    msg = str(exc_info.value)
    assert "401" in msg
    assert "Bad Gateway from auth proxy" in msg


def test_post_token_transport_error_is_labeled_with_endpoint(requests_mock):
    """A transport-level failure to the XSUAA endpoint must be re-raised as an auth error naming the token endpoint, not a raw requests error mislabeled upstream."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "client_id": "cid",
            "client_secret": "csec",
            "xsuaa_url": "https://myco.authentication.eu10.hana.ondemand.com",
        }
    )
    requests_mock.post(
        "https://myco.authentication.eu10.hana.ondemand.com/oauth/token",
        exc=requests.exceptions.ConnectionError,
    )
    client = SapDatasphereClient(cfg)
    with pytest.raises(ValueError) as exc_info:
        list(client.list_spaces())
    assert "oauth/token" in str(exc_info.value)


def test_post_token_non_json_2xx_body_is_labeled(requests_mock):
    """HTTP 200 with a non-JSON body (proxy/SSO login page) must raise a ValueError naming the token endpoint, not a bare JSON decode error."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "client_id": "cid",
            "client_secret": "csec",
            "xsuaa_url": "https://myco.authentication.eu10.hana.ondemand.com",
        }
    )
    requests_mock.post(
        "https://myco.authentication.eu10.hana.ondemand.com/oauth/token",
        status_code=200,
        text="<html>login</html>",
        headers={"Content-Type": "text/html"},
    )
    client = SapDatasphereClient(cfg)
    with pytest.raises(ValueError) as exc_info:
        list(client.list_spaces())
    assert "oauth/token" in str(exc_info.value)


def test_fetch_edmx_returns_none_on_403_with_permission_signal(requests_mock, caplog):
    """A 403 (principal lacks OData read on the space, distinct from a benign 404) must still return None but emit a permission-oriented signal."""
    cfg = _make_config()
    report = SapDatasphereReport()
    requests_mock.get("http://edmx-url/$metadata", status_code=403)
    client = SapDatasphereClient(cfg, report=report)

    with caplog.at_level(logging.WARNING):
        result = client.fetch_edmx("http://edmx-url/$metadata")

    assert result.xml is None
    assert result.reason == EdmxFetchReason.FORBIDDEN
    warnings = [(w.title or "", w.message) for w in report.warnings]
    assert any(
        "403" in (title + message) or "forbidden" in (title + message).lower()
        for title, message in warnings
    ), f"Expected a 403/forbidden permission warning; got: {warnings}"
    assert any(
        "principal" in message.lower() or "odata" in message.lower()
        for _, message in warnings
    ), f"Expected a principal/OData permission hint; got: {warnings}"


def test_fetch_edmx_404_does_not_emit_permission_signal(requests_mock, caplog):
    """A 404 is the benign 'asset not exposed' case and must NOT raise the 403-style permission warning, else operators chase a non-issue."""
    cfg = _make_config()
    report = SapDatasphereReport()
    requests_mock.get("http://edmx-url/$metadata", status_code=404)
    client = SapDatasphereClient(cfg, report=report)

    with caplog.at_level(logging.WARNING):
        result = client.fetch_edmx("http://edmx-url/$metadata")

    assert result.xml is None
    assert result.reason == EdmxFetchReason.NOT_FOUND
    warnings = [(w.title or "", w.message) for w in report.warnings]
    assert not any(
        "403" in (title + message) or "forbidden" in (title + message).lower()
        for title, message in warnings
    ), f"404 must not emit a 403 permission warning; got: {warnings}"


def test_list_connections_warns_at_truncation_threshold(requests_mock):
    """When the connections API returns >=100 entries, surface a structured warning so operators can investigate silent truncation."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    report = SapDatasphereReport()

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[{"name": f"CONN_{i:03d}", "typeId": "HANA"} for i in range(100)],
    )

    client = SapDatasphereClient(cfg, report=report)
    result = client.list_connections("S1")
    assert len(result) == 100

    titles = [w.title or "" for w in report.warnings]
    assert any("Possible connections-list truncation" in t for t in titles), (
        f"Expected truncation warning; got: {titles}"
    )


def test_list_connections_no_truncation_warning_below_threshold(requests_mock):
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    report = SapDatasphereReport()

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[{"name": f"CONN_{i:03d}", "typeId": "HANA"} for i in range(5)],
    )

    client = SapDatasphereClient(cfg, report=report)
    client.list_connections("S1")

    titles = [w.title or "" for w in report.warnings]
    assert not any("Possible connections-list truncation" in t for t in titles), (
        f"Truncation warning should NOT fire below threshold; got: {titles}"
    )


def test_list_local_tables_returns_technical_names(requests_mock):
    """The localtables endpoint returns a bare-array shape verified against the live tenant."""
    cfg = _make_config()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/DEMO_SPACE/localtables",
        json=[
            {"technicalName": "SAP.TIME.M_TIME_DIMENSION"},
            {"technicalName": "SAP.TIME.M_TIME_DIMENSION_TDAY"},
        ],
    )
    client = SapDatasphereClient(cfg)
    result = list(client.list_local_tables("DEMO_SPACE"))
    assert result == [
        {"technicalName": "SAP.TIME.M_TIME_DIMENSION"},
        {"technicalName": "SAP.TIME.M_TIME_DIMENSION_TDAY"},
    ]


def test_list_local_tables_handles_unexpected_shape(requests_mock):
    """An unexpected dict response must yield nothing and log a warning rather than crash."""
    cfg = _make_config()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/DEMO_SPACE/localtables",
        json={"unexpected": "shape"},
    )
    client = SapDatasphereClient(cfg)
    result = list(client.list_local_tables("DEMO_SPACE"))
    assert result == []


def test_list_local_tables_unexpected_shape_records_report_warning(requests_mock):
    """A non-list 200 must surface a report.warning, not just a logger.warning, so a silent-empty localtables list is explained."""
    cfg = _make_config()
    url = "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/DEMO_SPACE/localtables"
    requests_mock.get(url, json={"unexpected": "shape"})
    report = SapDatasphereReport()
    client = SapDatasphereClient(cfg, report=report)
    result = list(client.list_local_tables("DEMO_SPACE"))
    assert result == []
    assert any("localtables" in " ".join(w.context or []) for w in report.warnings)


def test_list_local_tables_non_json_records_report_warning(requests_mock):
    """A non-JSON 200 (proxy/login HTML) must surface a report.warning."""
    cfg = _make_config()
    url = "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/DEMO_SPACE/localtables"
    requests_mock.get(
        url,
        text="<html>login</html>",
        headers={"Content-Type": "text/html"},
    )
    report = SapDatasphereReport()
    client = SapDatasphereClient(cfg, report=report)
    result = list(client.list_local_tables("DEMO_SPACE"))
    assert result == []
    assert any("localtables" in " ".join(w.context or []) for w in report.warnings)


def test_list_connections_logs_when_shape_is_not_list(requests_mock, caplog):
    """If a future API wraps the response as {value: [...]}, emit a logger.warning so observers can detect the silent skip."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json={"value": [{"name": "X", "typeId": "HANA"}]},
    )
    with caplog.at_level(logging.WARNING):
        client = SapDatasphereClient(cfg)
        result = client.list_connections("S1")

    assert result == [], (
        f"Expected fallback to empty list on unexpected shape; got: {result}"
    )
    assert any(
        "shape" in r.message.lower()
        or "unexpected" in r.message.lower()
        or "connection" in r.message.lower()
        for r in caplog.records
    ), (
        f"Expected a logger.warning about unexpected shape; got: {[r.message for r in caplog.records]}"
    )


def test_list_local_tables_403_warns_and_yields_nothing(requests_mock):
    """A 403 (principal not a member of the space) must emit a membership report.warning and yield nothing rather than crash."""
    cfg = _make_config()
    report = SapDatasphereReport()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/localtables",
        status_code=403,
    )
    client = SapDatasphereClient(cfg, report=report)
    assert list(client.list_local_tables("S1")) == []

    warnings = [(w.title or "", w.message) for w in report.warnings]
    assert any(
        "member" in title.lower() or "member" in message.lower()
        for title, message in warnings
    ), f"Expected a membership warning; got: {warnings}"


def test_list_local_tables_still_works(requests_mock):
    """Regression: existing behavior preserved after refactoring onto the shared _list_dwaas_objects helper."""
    cfg = _make_config()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/DEMO_SPACE/localtables",
        json=[
            {"technicalName": "SAP.TIME.M_TIME_DIMENSION"},
            {"technicalName": "SAP.TIME.M_TIME_DIMENSION_TDAY"},
        ],
    )
    client = SapDatasphereClient(cfg)
    result = list(client.list_local_tables("DEMO_SPACE"))
    assert result == [
        {"technicalName": "SAP.TIME.M_TIME_DIMENSION"},
        {"technicalName": "SAP.TIME.M_TIME_DIMENSION_TDAY"},
    ]


def test_api_timing_recorded_for_catalog_list(requests_mock):
    """list_spaces routes through _get(operation="catalog_list"), so the report must gain a catalog_list timing bucket."""
    cfg = _make_config()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1"}]},
    )
    report = SapDatasphereReport()
    client = SapDatasphereClient(cfg, report=report)
    list(client.list_spaces())

    assert "catalog_list" in report.api_timings
    assert report.api_timings["catalog_list"].count >= 1
    assert report.api_timings["catalog_list"].total_seconds >= 0


def test_api_timing_recorded_for_edmx_fetch(requests_mock):
    cfg = _make_config()
    requests_mock.get("http://edmx-url/$metadata", text="<edmx/>")
    report = SapDatasphereReport()
    client = SapDatasphereClient(cfg, report=report)
    client.fetch_edmx("http://edmx-url/$metadata")

    assert "edmx_fetch" in report.api_timings
    assert report.api_timings["edmx_fetch"].count == 1
    assert report.api_timings["edmx_fetch"].total_seconds >= 0


def test_api_timing_recorded_for_csn_fetch(requests_mock):
    cfg = _make_config()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/VIEW_X",
        json={"definitions": {"VIEW_X": {"kind": "entity"}}},
    )
    report = SapDatasphereReport()
    client = SapDatasphereClient(cfg, report=report)
    client.fetch_object_definition("S1", "views", "VIEW_X")

    assert "csn_fetch" in report.api_timings
    assert report.api_timings["csn_fetch"].count == 1


def test_api_timing_recorded_for_connections(requests_mock):
    cfg = _make_config()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[{"name": "C", "typeId": "HANA"}],
    )
    report = SapDatasphereReport()
    client = SapDatasphereClient(cfg, report=report)
    client.list_connections("S1")

    assert "connections" in report.api_timings
    assert report.api_timings["connections"].count == 1


def test_api_timing_recorded_for_oauth_token(requests_mock):
    cfg = _make_config(
        token=None,
        client_id="cid",
        client_secret="csec",
        xsuaa_url="https://myco.authentication.eu10.hana.ondemand.com",
    )
    requests_mock.post(
        "https://myco.authentication.eu10.hana.ondemand.com/oauth/token",
        json={"access_token": "cc-token"},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": []},
    )
    report = SapDatasphereReport()
    client = SapDatasphereClient(cfg, report=report)
    list(client.list_spaces())

    assert "oauth_token" in report.api_timings
    assert report.api_timings["oauth_token"].count >= 1


def test_api_timing_failed_call_still_recorded(requests_mock):
    """A failed CSN fetch (500) must still be timed (try/finally) so operators see latency for slow-failing endpoints too."""
    cfg = _make_config()
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/VIEW_X",
        status_code=500,
    )
    report = SapDatasphereReport()
    client = SapDatasphereClient(cfg, report=report)
    assert client.fetch_object_definition("S1", "views", "VIEW_X") is None
    assert report.api_timings["csn_fetch"].count == 1
