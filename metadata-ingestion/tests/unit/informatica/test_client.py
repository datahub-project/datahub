import io
import json
import zipfile
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.informatica.client import (
    InformaticaClient,
    parse_saas_connection_ref,
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
    config = InformaticaSourceConfig.parse_obj(
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

    def test_still_failing_auth_after_retry_is_reported(self, client):
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
        ):
            client._request("GET", "https://pod/saas/api/v2/thing")

        assert len(client.report.failures) > 0

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

    def test_tag_filter_adds_to_query(self, client):
        client._session_id = "s"
        client._base_url = "https://pod/saas"
        client._session_last_validated_at = 1e12

        resp = _build_response(200, {"objects": []})
        with patch.object(client._session, "request", return_value=resp) as req:
            list(client.list_objects("Project", tag="pii"))
        params = req.call_args.kwargs["params"]
        assert "type=='Project'" in params["q"]
        assert "tag=='pii'" in params["q"]


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
