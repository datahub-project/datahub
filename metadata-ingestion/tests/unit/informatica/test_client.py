import io
import json
import zipfile
from typing import Any, Callable, Dict, Iterator, List
from unittest.mock import MagicMock, patch

import pytest
import requests

from datahub.ingestion.source.informatica.client import (
    InformaticaClient,
    _parse_taskflow_export_package,
    _resolve_successor_id,
    parse_saas_connection_ref,
    parse_taskflow_definition,
    parse_taskflow_xml,
)
from datahub.ingestion.source.informatica.config import InformaticaSourceConfig
from datahub.ingestion.source.informatica.models import (
    ExportJobState,
    InformaticaApiError,
    InformaticaLoginError,
    InformaticaSourceReport,
)


def _build_response(
    status_code: int = 200, payload: Any = None, raw_bytes: bytes = b""
) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = payload if payload is not None else {}
    resp.text = json.dumps(payload) if payload is not None else ""
    resp.content = raw_bytes
    resp.iter_content = lambda chunk_size=8192: iter(
        [raw_bytes[i : i + chunk_size] for i in range(0, len(raw_bytes), chunk_size)]
    )
    resp.raise_for_status = MagicMock()
    return resp


@pytest.fixture
def client() -> InformaticaClient:
    config = InformaticaSourceConfig.model_validate(
        {
            "username": "svc",
            "password": "pw",
            "login_url": "https://dm-us.informaticacloud.com",
            "page_size": 2,
            "export_poll_interval_secs": 1,
            "export_poll_timeout_secs": 30,
        }
    )
    return InformaticaClient(config, InformaticaSourceReport())


class TestLogin:
    def test_login_success_caches_session_and_base_url(self, client):
        login_resp = _build_response(
            200,
            {
                "icSessionId": "session-abc",
                "serverUrl": "https://pod.informaticacloud.com/saas/",
                "orgId": "org-1",
            },
        )
        with patch.object(client._session, "post", return_value=login_resp) as mp:
            session_id = client.login()
        assert session_id == "session-abc"
        assert client._session_id == "session-abc"
        assert client._base_url == "https://pod.informaticacloud.com/saas"
        assert client.report.api_call_count == 1
        assert mp.call_args.kwargs["json"]["username"] == "svc"

    def test_login_http_error_raises_typed(self, client):
        with (
            patch.object(
                client._session,
                "post",
                return_value=_build_response(401, {"msg": "bad"}),
            ),
            pytest.raises(InformaticaLoginError, match="IDMC login failed"),
        ):
            client.login()

    def test_login_error_payload_raises_typed(self, client):
        resp = _build_response(200, {"@type": "error", "description": "invalid creds"})
        with (
            patch.object(client._session, "post", return_value=resp),
            pytest.raises(InformaticaLoginError, match="invalid creds"),
        ):
            client.login()

    def test_login_missing_session_id_raises_typed(self, client):
        resp = _build_response(200, {"serverUrl": "https://pod/saas"})
        with (
            patch.object(client._session, "post", return_value=resp),
            pytest.raises(InformaticaLoginError, match="missing icSessionId"),
        ):
            client.login()

    def test_login_non_json_body_raises_typed(self, client):
        # 200 with an HTML/empty body (observed when a reverse proxy sits in
        # front of IDMC) must raise InformaticaLoginError rather than crash
        # with an uncaught JSONDecodeError.
        resp = MagicMock()
        resp.status_code = 200
        resp.text = "<html>Maintenance</html>"
        resp.url = "https://dm-us/login"
        resp.json.side_effect = json.JSONDecodeError("x", "", 0)
        with (
            patch.object(client._session, "post", return_value=resp),
            pytest.raises(InformaticaLoginError, match="non-JSON body"),
        ):
            client.login()


class TestSessionValidation:
    """Unexpected-response branches of ``_validate_session`` must fire a
    one-shot report warning; expected paths (expired token) stay silent.
    """

    def _prep(self, client: InformaticaClient) -> None:
        client._session_id = "sess"
        client._base_url = "https://pod/saas"
        # Recent-enough timestamp to force the HTTP validation probe
        # instead of short-circuiting on TTL.
        client._session_last_validated_at = 1e12

    def test_non_200_fires_warning_once_across_consecutive_calls(self, client):
        self._prep(client)
        bad = _build_response(500, {"error": "upstream"})
        with patch.object(client._session, "post", return_value=bad):
            assert client._validate_session() is False
            assert client._validate_session() is False
            assert client._validate_session() is False
        assert len(client.report.warnings) == 1
        warning = client.report.warnings[0]
        assert warning.title == (
            "IDMC session validation endpoint returned unexpected response"
        )
        assert "HTTP 500" in "".join(warning.context or [])

    def test_non_json_200_body_also_fires_warning_once(self, client):
        self._prep(client)
        resp = _build_response(200)
        resp.json.side_effect = json.JSONDecodeError("x", "", 0)
        with patch.object(client._session, "post", return_value=resp):
            assert client._validate_session() is False
            assert client._validate_session() is False
        assert len(client.report.warnings) == 1
        assert "non-JSON body" in "".join(client.report.warnings[0].context or [])

    def test_expired_token_is_silent(self, client):
        # ``isValidToken: false`` → automatic re-login; no operator action.
        self._prep(client)
        expired = _build_response(200, {"isValidToken": False})
        with patch.object(client._session, "post", return_value=expired):
            assert client._validate_session() is False
        assert client.report.warnings == []


class TestAuthFailureDetection:
    def test_401_is_auth_failure(self):
        assert InformaticaClient._is_auth_failure(_build_response(401)) is True

    def test_403_with_auth_01_is_auth_failure(self):
        resp = _build_response(403, {"error": {"code": "AUTH_01"}})
        assert InformaticaClient._is_auth_failure(resp) is True

    def test_403_without_auth_01_is_not_auth_failure(self):
        resp = _build_response(403, {"error": {"code": "FORBIDDEN"}})
        assert InformaticaClient._is_auth_failure(resp) is False

    def test_200_is_not_auth_failure(self):
        assert InformaticaClient._is_auth_failure(_build_response(200)) is False


class TestDecodeJson:
    def test_raises_informatica_api_error_on_invalid_json(self):
        # 200 with a non-JSON body (e.g., HTML error page, empty response)
        # must be converted to InformaticaApiError so the pipeline reports
        # a clean warning instead of crashing with JSONDecodeError.
        resp = MagicMock()
        resp.json.side_effect = json.JSONDecodeError("x", "", 0)
        resp.text = "<html>Server Error</html>"
        resp.status_code = 200
        resp.url = "https://pod/api/v2/thing"

        with pytest.raises(InformaticaApiError, match="non-JSON body"):
            InformaticaClient._decode_json(resp)

    def test_passes_through_valid_json(self):
        resp = _build_response(200, payload={"key": "value"})
        assert InformaticaClient._decode_json(resp) == {"key": "value"}


class TestRequestRetryOnAuthFailure:
    def test_request_retries_once_on_401(self, client):
        client._session_id = "old"
        client._base_url = "https://pod/saas"
        client._session_last_validated_at = 1e12

        fail = _build_response(401)
        success = _build_response(200, {"ok": True})
        login_resp = _build_response(
            200, {"icSessionId": "new", "serverUrl": "https://pod/saas"}
        )

        with (
            patch.object(
                client._session, "request", side_effect=[fail, success]
            ) as req,
            patch.object(client._session, "post", return_value=login_resp),
        ):
            resp = client._request("GET", "https://pod/saas/api/v2/thing")

        assert resp is success
        assert req.call_count == 2
        assert client._session_id == "new"

    def test_still_failing_auth_after_retry_raises(self, client):
        client._session_id = "old"
        client._base_url = "https://pod/saas"
        client._session_last_validated_at = 1e12

        fail1 = _build_response(401)
        fail2 = _build_response(401)
        login_resp = _build_response(
            200, {"icSessionId": "new", "serverUrl": "https://pod/saas"}
        )

        with (
            patch.object(client._session, "request", side_effect=[fail1, fail2]),
            patch.object(client._session, "post", return_value=login_resp),
            pytest.raises(InformaticaLoginError, match="rejected after retry"),
        ):
            client._request("GET", "https://pod/saas/api/v2/thing")

    def test_request_returns_first_response_if_not_auth_failure(self, client):
        client._session_id = "sess"
        client._base_url = "https://pod/saas"
        client._session_last_validated_at = 1e12

        ok = _build_response(200, {"ok": True})
        with patch.object(client._session, "request", return_value=ok) as req:
            resp = client._request("GET", "https://pod/saas/api/v2/thing")
        assert resp is ok
        assert req.call_count == 1


class TestListObjectsPagination:
    def test_stops_when_page_shorter_than_limit(self, client):
        client._session_id = "s"
        client._base_url = "https://pod/saas"
        client._session_last_validated_at = 1e12

        page1 = _build_response(
            200,
            {
                "objects": [
                    {"id": "1", "name": "a", "path": "/Explore/P1"},
                    {"id": "2", "name": "b", "path": "/Explore/P1"},
                ]
            },
        )
        page2 = _build_response(
            200,
            {"objects": [{"id": "3", "name": "c", "path": "/Explore/P2"}]},
        )

        with patch.object(client._session, "request", side_effect=[page1, page2]):
            results = list(client.list_objects("Project"))

        assert [o.id for o in results] == ["1", "2", "3"]

    def test_stops_on_empty_page(self, client):
        client._session_id = "s"
        client._base_url = "https://pod/saas"
        client._session_last_validated_at = 1e12

        page1 = _build_response(
            200,
            {
                "objects": [
                    {"id": "1", "name": "a", "path": "/Explore/P"},
                    {"id": "2", "name": "b", "path": "/Explore/P"},
                ]
            },
        )
        page2 = _build_response(200, {"objects": []})

        with patch.object(client._session, "request", side_effect=[page1, page2]):
            results = list(client.list_objects("Project"))
        assert len(results) == 2

    def test_tag_filter_is_not_sent_to_server(self, client):
        # IDMC's v3 objects API rejects `tag==` in the server-side `q` query,
        # so filtering must be done client-side.
        client._session_id = "s"
        client._base_url = "https://pod/saas"
        client._session_last_validated_at = 1e12

        resp = _build_response(200, {"objects": []})
        with patch.object(client._session, "request", return_value=resp) as req:
            list(client.list_objects("TASKFLOW", tag="pii"))
        params = req.call_args.kwargs["params"]
        assert params["q"] == "type=='TASKFLOW'"

    def test_tag_filter_applied_client_side(self, client):
        # page_size=2 in the fixture — returning 3 items on a single page
        # would leave the pagination loop unable to terminate (len >= limit),
        # so cap the first page to 2 and signal end-of-data with an empty page.
        client._session_id = "s"
        client._base_url = "https://pod/saas"
        client._session_last_validated_at = 1e12

        first_page = _build_response(
            200,
            {
                "objects": [
                    {"id": "1", "name": "a", "path": "/Explore/P", "tags": ["pii"]},
                    {"id": "2", "name": "b", "path": "/Explore/P", "tags": ["other"]},
                ]
            },
        )
        empty_page = _build_response(200, {"objects": []})
        with patch.object(
            client._session, "request", side_effect=[first_page, empty_page]
        ):
            results = list(client.list_objects("TASKFLOW", tag="pii"))
        assert [o.id for o in results] == ["1"]


class TestParseV3Object:
    def test_parses_flat_response(self):
        obj = InformaticaClient._parse_v3_object(
            {"id": "x", "name": "n", "path": "/Explore/P", "documentType": "Folder"},
            "Project",
        )
        assert obj.object_type == "Folder"

    def test_parses_properties_style_response(self):
        obj = InformaticaClient._parse_v3_object(
            {"properties": [{"name": "id", "value": "p1"}]}, "DTEMPLATE"
        )
        assert obj.id == "p1"
        assert obj.object_type == "DTEMPLATE"

    def test_derives_name_from_path_when_name_missing(self):
        # IDMC sometimes returns objects with no name field at all; the last
        # path component is a reliable fallback so the entity gets a usable name.
        obj = InformaticaClient._parse_v3_object(
            {"id": "tf-1", "path": "/Explore/Sales/my_flow"},
            "TASKFLOW",
        )
        assert obj.name == "my_flow"

    def test_properties_style_falls_back_to_top_level_name_and_path(self):
        # Informatica v3 API can return name/path at the top level even when a
        # properties array is present. Without the fallback, obj.name and obj.path
        # are empty, causing DataHub to display the v3 GUID as the entity name.
        obj = InformaticaClient._parse_v3_object(
            {
                "id": "abc123",
                "name": "My Mapping",
                "path": "/Explore/Sales/Mappings",
                "properties": [
                    {"name": "id", "value": "abc123"},
                    {"name": "description", "value": "some desc"},
                ],
            },
            "DTEMPLATE",
        )
        assert obj.name == "My Mapping"
        assert obj.path == "/Explore/Sales/Mappings"


class TestParseInnerDtemplateZip:
    @staticmethod
    def _make_inner_zip(bin_bytes: bytes) -> zipfile.ZipFile:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("bin/@3.bin", bin_bytes)
        buf.seek(0)
        return zipfile.ZipFile(buf)

    def test_invalid_json_body_reports_to_source(self, client):
        # Corrupted @3.bin payload (valid UTF-8 but truncated JSON): the
        # decoder must land a failure in the report so it shows up in the
        # ingestion summary rather than disappearing into debug logs.
        inner = self._make_inner_zip(b'{"content": {"name": "m"')

        result = client._parse_inner_dtemplate_zip(inner, "Sales__Map.zip", "g-1")

        assert result is None
        assert any(
            "Sales__Map.zip" in entry for entry in client.report.objects_failed
        ), f"expected failure reported; got {list(client.report.objects_failed)}"

    def test_invalid_utf8_body_reports_to_source(self, client):
        # Same contract for a UnicodeDecodeError (binary garbage in @3.bin).
        inner = self._make_inner_zip(b"\xff\xfe\xff\xfe\xff")

        result = client._parse_inner_dtemplate_zip(inner, "Sales__Map.zip", "g-1")

        assert result is None
        assert any("Sales__Map.zip" in entry for entry in client.report.objects_failed)


class TestWaitForExport:
    def test_returns_successful_immediately(self, client):
        client._session_id = "s"
        client._base_url = "https://pod/saas"
        client._session_last_validated_at = 1e12

        resp = _build_response(200, {"status": {"state": "SUCCESSFUL", "message": ""}})
        with patch.object(client._session, "request", return_value=resp):
            status = client.wait_for_export("job-1")
        assert status.state == ExportJobState.SUCCESSFUL

    def test_returns_failure_state(self, client):
        client._session_id = "s"
        client._base_url = "https://pod/saas"
        client._session_last_validated_at = 1e12

        resp = _build_response(200, {"status": {"state": "FAILED", "message": "boom"}})
        with patch.object(client._session, "request", return_value=resp):
            status = client.wait_for_export("job-1")
        assert status.state == ExportJobState.FAILED
        assert any("job-1" in e for e in client.report.export_jobs_failed)

    def test_unknown_state_returns_unknown_enum(self, client):
        client._session_id = "s"
        client._base_url = "https://pod/saas"
        client._session_last_validated_at = 1e12

        resp = _build_response(200, {"status": {"state": "BOGUS_STATE", "message": ""}})
        with patch.object(client._session, "request", return_value=resp):
            status = client.wait_for_export("job-unknown")
        assert status.state == ExportJobState.UNKNOWN

    def test_times_out_and_warns(self, client):
        client.config.export_poll_timeout_secs = 0
        status = client.wait_for_export("job-timeout")
        assert status.state == ExportJobState.TIMEOUT
        assert any("job-timeout" in e for e in client.report.export_jobs_failed)


class TestSubmitExportJob:
    def test_raises_typed_error_on_missing_id(self, client):
        client._session_id = "s"
        client._base_url = "https://pod/saas"
        client._session_last_validated_at = 1e12

        resp = _build_response(200, {"no_id": True})
        with (
            patch.object(client._session, "request", return_value=resp),
            pytest.raises(InformaticaApiError, match="no ID"),
        ):
            client.submit_export_job(["m1"])


class TestListMappingTasksEnrichment:
    def test_v2_detail_lookup_populates_mapping_fields(self, client):
        client._session_id = "s"
        client._base_url = "https://pod/saas"
        client._session_last_validated_at = 1e12

        # v3 MTT listing page + v2 detail payload per MT
        v3_page = _build_response(
            200,
            {
                "objects": [
                    {
                        "id": "mt-guid-1",
                        "name": "nightly_copy",
                        "path": "/Explore/Sales/ETL/nightly_copy",
                        "documentType": "MTT",
                    }
                ]
            },
        )
        empty_page = _build_response(200, {"objects": []})
        v2_detail = _build_response(
            200,
            {
                "id": "v2-task-1",
                "name": "nightly_copy",
                "mappingId": "m-1",
                "mappingName": "copy_mapping",
                "connectionId": "c-1",
            },
        )

        # Order: v3 page → enrichment call for each item → next v3 page.
        with patch.object(
            client._session,
            "request",
            side_effect=[v3_page, v2_detail, empty_page],
        ):
            tasks = list(client.list_mapping_tasks())

        assert len(tasks) == 1
        mt = tasks[0]
        assert mt.v2_id == "mt-guid-1"  # v3 id comes through from_idmc_object
        assert mt.mapping_id == "m-1"
        assert mt.mapping_name == "copy_mapping"
        assert mt.connection_id == "c-1"

    def test_v2_detail_lookup_failure_reports_warning_and_yields(self, client):
        client._session_id = "s"
        client._base_url = "https://pod/saas"
        client._session_last_validated_at = 1e12

        v3_page = _build_response(
            200,
            {
                "objects": [
                    {
                        "id": "mt-1",
                        "name": "nightly",
                        "path": "/Explore/P/F/nightly",
                        "documentType": "MTT",
                    }
                ]
            },
        )
        empty_page = _build_response(200, {"objects": []})
        # v2 detail responds 404 → raise_for_status raises HTTPError
        v2_404 = _build_response(404)
        v2_404.raise_for_status = MagicMock(
            side_effect=requests.HTTPError("404 Not Found")
        )

        with patch.object(
            client._session,
            "request",
            side_effect=[v3_page, v2_404, empty_page],
        ):
            tasks = list(client.list_mapping_tasks())

        # Task still yielded — enrichment failure must not drop the MT.
        # mapping_id stays ``None`` so downstream code can distinguish
        # "enrichment failed / not yet run" from "enriched but blank".
        assert len(tasks) == 1
        assert tasks[0].mapping_id is None
        assert any(
            "enrich IDMC mapping task" in (w.title or "")
            for w in client.report.warnings
        )


class TestExtractLineage:
    def _make_3bin_bytes(self, content: Dict[str, Any]) -> bytes:
        return json.dumps({"content": content}).encode("utf-8")

    def _make_inner_zip(self, bin_bytes: bytes) -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("bin/@2.bin", b"preview-image-placeholder")
            zf.writestr("bin/@3.bin", bin_bytes)
        return buf.getvalue()

    def test_extracts_sources_and_targets_by_class(self, client):
        content = {
            "name": "my_mapping",
            "transformations": [
                {
                    "name": "SRC_orders",
                    "$$class": 9,
                    "dataAdapter": {
                        "object": {"name": "ORDERS", "dbSchema": "SALES"},
                        "connectionId": "saas:@fed-src",
                    },
                },
                {
                    "name": "TGT_orders_copy",
                    "$$class": 8,
                    "dataAdapter": {
                        "object": {"name": "ORDERS_COPY", "dbSchema": "ANALYTICS"},
                        "connectionId": "saas:@fed-tgt",
                    },
                },
            ],
        }
        lineage = client._extract_lineage_from_3bin(
            {"content": content}, "mapping.DTEMPLATE.zip", mapping_id="guid-1"
        )
        assert lineage is not None
        assert lineage.mapping_id == "guid-1"
        assert lineage.source_tables[0].connection_federated_id == "fed-src"
        assert lineage.target_tables[0].connection_federated_id == "fed-tgt"

    def test_classifies_by_name_when_class_missing(self, client):
        content = {
            "name": "m",
            "transformations": [
                {
                    "name": "source_orders",
                    "dataAdapter": {
                        "object": {"name": "O"},
                        "connectionId": "saas:@f1",
                    },
                },
                {
                    "name": "target_copies",
                    "dataAdapter": {
                        "object": {"name": "C"},
                        "connectionId": "saas:@f2",
                    },
                },
            ],
        }
        lineage = client._extract_lineage_from_3bin(
            {"content": content}, "m.zip", mapping_id="g"
        )
        assert lineage is not None
        assert len(lineage.source_tables) == 1
        assert len(lineage.target_tables) == 1

    def test_returns_none_when_no_sources_or_targets(self, client):
        content = {"name": "m", "transformations": []}
        assert client._extract_lineage_from_3bin({"content": content}, "x", "g") is None

    def test_reports_when_content_missing(self, client):
        assert client._extract_lineage_from_3bin({}, "entry-name", "g") is None
        assert len(client.report.warnings) > 0

    def test_download_and_parse_export_threads_mapping_id(self, client):
        client._session_id = "s"
        client._base_url = "https://pod/saas"
        client._session_last_validated_at = 1e12

        inner_zip_bytes = self._make_inner_zip(
            self._make_3bin_bytes(
                {
                    "name": "my_mapping",
                    "transformations": [
                        {
                            "name": "SRC",
                            "$$class": 9,
                            "dataAdapter": {
                                "object": {"name": "T"},
                                "connectionId": "saas:@fed-1",
                            },
                        }
                    ],
                }
            )
        )
        outer_buf = io.BytesIO()
        with zipfile.ZipFile(outer_buf, "w") as zf:
            zf.writestr("guid-abc.DTEMPLATE.zip", inner_zip_bytes)
            zf.writestr("readme.txt", b"ignored")
        resp = _build_response(200, raw_bytes=outer_buf.getvalue())

        with patch.object(client._session, "request", return_value=resp):
            lineages: List = list(
                client.download_and_parse_export("job-42", ["guid-abc"])
            )

        assert len(lineages) == 1
        assert lineages[0].mapping_id == "guid-abc"


class TestResolveEntryMappingId:
    def test_prefers_substring_match(self):
        assert (
            InformaticaClient._resolve_entry_mapping_id(
                "guid-42.DTEMPLATE.zip", ["guid-1", "guid-42", "guid-7"], index=0
            )
            == "guid-42"
        )

    def test_falls_back_to_positional_when_no_match(self):
        assert (
            InformaticaClient._resolve_entry_mapping_id(
                "opaque.DTEMPLATE.zip", ["guid-1", "guid-2"], index=1
            )
            == "guid-2"
        )

    def test_returns_empty_when_no_candidates(self):
        assert (
            InformaticaClient._resolve_entry_mapping_id("x.DTEMPLATE.zip", [], 0) == ""
        )


class TestParseSaasConnectionRef:
    def test_strips_saas_prefix(self):
        assert parse_saas_connection_ref("saas:@fed-123") == "fed-123"

    def test_passes_through_plain_id(self):
        assert parse_saas_connection_ref("01ABC") == "01ABC"

    def test_handles_empty(self):
        assert parse_saas_connection_ref("") == ""


class TestResolveSuccessorId:
    """IDMC Taskflow JSON shape drift: ``nextSteps`` can be bare strings
    or ``{"id": ...}`` dicts. The helper returns ``None`` for anything
    else so schema drift doesn't leak into the step-id list.
    """

    def test_bare_string_returns_as_is(self):
        assert _resolve_successor_id("step-42") == "step-42"

    def test_dict_with_id_key_extracts_id(self):
        assert _resolve_successor_id({"id": "step-42"}) == "step-42"
        assert _resolve_successor_id({"stepId": "step-42"}) == "step-42"
        assert _resolve_successor_id({"name": "step-42"}) == "step-42"

    def test_dict_id_is_coerced_to_str(self):
        # Older payloads returned numeric ids; coerce for uniform keying.
        assert _resolve_successor_id({"id": 42}) == "42"

    def test_dict_without_id_returns_none(self):
        assert _resolve_successor_id({"label": "step-42"}) is None

    def test_unknown_shape_returns_none(self):
        assert _resolve_successor_id(42) is None
        assert _resolve_successor_id(["step-42"]) is None
        assert _resolve_successor_id(None) is None


class TestParseTaskflowDefinition:
    def test_parses_linear_steps_with_next_edges(self):
        # IDMC's taskflow JSON typically exposes edges via ``nextSteps`` on
        # each step. The parser inverts those into ``predecessor_step_ids``.
        data = {
            "id": "tf-1",
            "name": "daily_refresh",
            "steps": [
                {
                    "id": "s1",
                    "displayName": "Copy orders",
                    "type": "data",
                    "taskType": "MTT",
                    "taskName": "nightly_copy_orders",
                    "taskId": "01DM-1",
                    "nextSteps": ["s2"],
                },
                {
                    "id": "s2",
                    "displayName": "Enrich",
                    "type": "data",
                    "taskType": "MTT",
                    "taskName": "enrich_customers",
                    "taskId": "01DM-2",
                    "nextSteps": [],
                },
            ],
        }
        definition = parse_taskflow_definition(data)
        assert definition is not None
        assert definition.taskflow_name == "daily_refresh"
        assert [s.step_id for s in definition.steps] == ["s1", "s2"]
        s1, s2 = definition.steps
        assert s1.task_ref_id == "01DM-1"
        assert s1.task_ref_name == "nightly_copy_orders"
        # s1 has no predecessor; s2 has s1 as predecessor (derived from s1.nextSteps).
        assert s1.predecessor_step_ids == []
        assert s2.predecessor_step_ids == ["s1"]

    def test_parses_steps_with_explicit_predecessors(self):
        # Some taskflow shapes expose ``dependsOn`` directly.
        data = {
            "name": "tf_2",
            "steps": [
                {"id": "a", "name": "A", "type": "command"},
                {"id": "b", "name": "B", "type": "command", "dependsOn": ["a"]},
            ],
        }
        definition = parse_taskflow_definition(data)
        assert definition is not None
        assert definition.steps[1].predecessor_step_ids == ["a"]

    def test_non_dict_returns_none(self):
        assert parse_taskflow_definition({"something": "else"}) is None  # type: ignore[arg-type]

    def test_falls_back_to_name_when_explicit_id_missing(self):
        # Some IDMC taskflow shapes only expose ``name`` on each step; the
        # parser uses name as a fallback step id so predecessor linking works.
        data = {
            "name": "tf",
            "steps": [
                {"name": "step_alpha"},
                {"id": "real", "name": "Real", "type": "data"},
            ],
        }
        definition = parse_taskflow_definition(data)
        assert definition is not None
        assert [s.step_id for s in definition.steps] == ["step_alpha", "real"]

    def test_skips_truly_empty_step_entries(self):
        # A step with no id and no name can't be addressed by predecessors,
        # so it's dropped.
        data = {"name": "tf", "steps": [{}, {"id": "real", "name": "Real"}]}
        definition = parse_taskflow_definition(data)
        assert definition is not None
        assert [s.step_id for s in definition.steps] == ["real"]


class TestParseTaskflowXml:
    EMPTY_TASKFLOW_XML = b"""\
<aetgt:getResponse xmlns:aetgt="http://schemas.active-endpoints.com/appmodules/repository/2010/10/avrepository.xsd"
                   xmlns:types1="http://schemas.active-endpoints.com/appmodules/repository/2010/10/avrepository.xsd">
   <types1:Item>
      <types1:Entry>
         <taskflow xmlns="http://schemas.active-endpoints.com/appmodules/screenflow/2010/10/avosScreenflow.xsd"
                   GUID="l8V6vf71iAxfOscobAR8it"
                   name="Taskflow1">
            <flow id="a">
               <start id="b"><link id="l1" targetId="c"/></start>
               <end id="c"/>
            </flow>
         </taskflow>
      </types1:Entry>
   </types1:Item>
</aetgt:getResponse>
"""

    # Two-step DAG: start → DataTask1 → DataTask2 → end, mirroring the real
    # Taskflow2 export payload shape (eventContainer + service + link).
    LINEAR_TASKFLOW_XML = b"""\
<aetgt:getResponse xmlns:aetgt="http://schemas.active-endpoints.com/appmodules/repository/2010/10/avrepository.xsd">
   <types1:Item xmlns:types1="http://schemas.active-endpoints.com/appmodules/repository/2010/10/avrepository.xsd">
      <types1:Entry>
         <taskflow xmlns="http://schemas.active-endpoints.com/appmodules/screenflow/2010/10/avosScreenflow.xsd"
                   GUID="2zwQ7UmP2ublD6Mfxz0Ssa"
                   name="Taskflow2">
            <flow id="a">
               <start id="b"><link id="l0" targetId="step1"/></start>
               <eventContainer id="step1">
                  <service id="svc1">
                     <title>Data Task 1</title>
                     <serviceName>ICSExecuteDataTask</serviceName>
                     <serviceInput>
                        <parameter name="Task Name">MappingTask3</parameter>
                        <parameter name="GUID">3WD6PD2FNhRbiv93ryBMM2</parameter>
                        <parameter name="Task Type">MCT</parameter>
                     </serviceInput>
                  </service>
                  <link id="l1" targetId="step2"/>
               </eventContainer>
               <eventContainer id="step2">
                  <service id="svc2">
                     <title>Data Task 2</title>
                     <serviceName>ICSExecuteDataTask</serviceName>
                     <serviceInput>
                        <parameter name="Task Name">MappingTask2</parameter>
                        <parameter name="GUID">499OiDLUgICjfllnYEUDqy</parameter>
                        <parameter name="Task Type">MCT</parameter>
                     </serviceInput>
                  </service>
                  <link id="l2" targetId="c"/>
               </eventContainer>
               <end id="c"/>
            </flow>
         </taskflow>
      </types1:Entry>
   </types1:Item>
</aetgt:getResponse>
"""

    def test_empty_taskflow_yields_zero_steps(self):
        # Taskflow with only start/end markers — produces no emittable steps.
        definition = parse_taskflow_xml(self.EMPTY_TASKFLOW_XML)
        assert definition is not None
        assert definition.taskflow_name == "Taskflow1"
        assert definition.taskflow_id == "l8V6vf71iAxfOscobAR8it"
        assert definition.steps == []

    def test_linear_two_step_dag(self):
        definition = parse_taskflow_xml(self.LINEAR_TASKFLOW_XML)
        assert definition is not None
        assert definition.taskflow_name == "Taskflow2"
        assert [s.step_id for s in definition.steps] == ["step1", "step2"]
        s1, s2 = definition.steps
        # Service metadata pulled through from <service><serviceInput>
        assert s1.step_name == "Data Task 1"
        assert s1.step_type == "data"  # ICSExecuteDataTask → "data"
        assert s1.task_ref_name == "MappingTask3"
        assert s1.task_ref_id == "3WD6PD2FNhRbiv93ryBMM2"
        assert s1.task_type == "MCT"
        # Predecessors: step1 has none (its only incoming edge is from <start>,
        # which is a marker and excluded); step2's predecessor is step1.
        assert s1.predecessor_step_ids == []
        assert s2.predecessor_step_ids == ["step1"]

    def test_malformed_xml_returns_none(self):
        assert parse_taskflow_xml(b"not-valid-xml") is None

    def test_xml_without_taskflow_element_returns_none(self):
        assert parse_taskflow_xml(b"<root><other/></root>") is None

    def test_rejects_billion_laughs_entity_expansion(self):
        # Without defusedxml this payload expands to ~10^9 "lol"s; with
        # it, EntitiesForbidden fires on the <!ENTITY> declaration.
        bomb = (
            b'<?xml version="1.0"?>\n'
            b"<!DOCTYPE lolz [\n"
            b'  <!ENTITY lol "lol">\n'
            b'  <!ENTITY lol2 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">\n'
            b'  <!ENTITY lol3 "&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;">\n'
            b"]>\n"
            b"<lolz>&lol3;</lolz>"
        )
        assert parse_taskflow_xml(bomb) is None

    def test_rejects_external_entity_reference(self):
        # XXE payload that would read /etc/hostname without defusedxml.
        xxe = (
            b'<?xml version="1.0"?>\n'
            b"<!DOCTYPE foo [\n"
            b'  <!ENTITY xxe SYSTEM "file:///etc/hostname">\n'
            b"]>\n"
            b"<foo>&xxe;</foo>"
        )
        assert parse_taskflow_xml(xxe) is None


def _iter_bytes(
    data: bytes,
) -> "Callable[..., Iterator[bytes]]":
    """Fake ``requests.Response.iter_content``: chunk_size-keyed bytes iterator."""

    def _iter(chunk_size: int = 8192) -> Iterator[bytes]:
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]

    return _iter


def _zip_with(entries: Dict[str, bytes]) -> bytes:
    """Build an in-memory ZIP with the given ``{name: bytes}`` entries."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in entries.items():
            zf.writestr(name, content)
    return buf.getvalue()


_VALID_TASKFLOW_XML = (
    b'<?xml version="1.0" encoding="UTF-8"?>'
    b'<taskflow GUID="tf-guid-1" name="TF1"><flow id="f">'
    b'<start id="s"><link targetId="n1"/></start>'
    b'<eventContainer id="n1"/>'
    b'<end id="e"/>'
    b"</flow></taskflow>"
)


class TestParseTaskflowExportPackage:
    """Batched ZIP parse: happy path, bad-ZIP warning, and per-entry
    parse failures rolled up into one warning (not one per entry).
    """

    def test_returns_parsed_definitions_on_happy_path(self):
        report = InformaticaSourceReport()
        zip_bytes = _zip_with({"Explore/Default/TF1.TASKFLOW.xml": _VALID_TASKFLOW_XML})
        defs = _parse_taskflow_export_package(_iter_bytes(zip_bytes), report=report)
        assert set(defs.keys()) == {"tf-guid-1"}
        assert report.warnings == []

    def test_bad_zip_emits_single_warning_and_returns_empty(self):
        report = InformaticaSourceReport()
        defs = _parse_taskflow_export_package(_iter_bytes(b"not-a-zip"), report=report)
        assert defs == {}
        assert len(report.warnings) == 1
        assert (
            report.warnings[0].title
            == "IDMC Taskflow export package is not a valid ZIP"
        )

    def test_malformed_entry_produces_one_rollup_warning_not_per_entry(self):
        report = InformaticaSourceReport()
        zip_bytes = _zip_with(
            {
                "good.TASKFLOW.xml": _VALID_TASKFLOW_XML,
                "bad1.TASKFLOW.xml": b"not-xml",
                "bad2.TASKFLOW.xml": b"<a><b></a>",  # unbalanced
                "not-a-taskflow.txt": b"ignored",  # suffix filter skips this
            }
        )
        defs = _parse_taskflow_export_package(_iter_bytes(zip_bytes), report=report)
        assert set(defs.keys()) == {"tf-guid-1"}
        assert len(report.warnings) == 1
        w = report.warnings[0]
        assert w.title == "IDMC Taskflow XML parse failed for some entries"
        assert "parse_failures=2" in "".join(w.context or [])

    def test_oversized_download_emits_warning_and_returns_empty(self):
        report = InformaticaSourceReport()
        zip_bytes = _zip_with({"Explore/Default/TF1.TASKFLOW.xml": _VALID_TASKFLOW_XML})
        with patch(
            "datahub.ingestion.source.informatica.client._MAX_TASKFLOW_ZIP_BYTES", 1
        ):
            defs = _parse_taskflow_export_package(_iter_bytes(zip_bytes), report=report)
        assert defs == {}
        assert len(report.warnings) == 1
        assert (
            report.warnings[0].title
            == "IDMC Taskflow export package exceeds size limit"
        )

    def test_no_report_argument_is_silent(self):
        zip_bytes = _zip_with({"x.TASKFLOW.xml": b"not-xml"})
        defs = _parse_taskflow_export_package(_iter_bytes(zip_bytes))
        assert defs == {}
