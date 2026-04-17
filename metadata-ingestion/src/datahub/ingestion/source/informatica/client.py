import io
import json
import logging
import tempfile
import time
import zipfile
from typing import Any, Dict, Iterator, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.informatica.config import InformaticaSourceConfig
from datahub.ingestion.source.informatica.models import (
    ExportJobStatus,
    IdmcConnection,
    IdmcMapping,
    IdmcMappingTask,
    IdmcObject,
    InformaticaSourceReport,
    LineageTable,
    MappingLineageInfo,
)

logger = logging.getLogger(__name__)

# IDMC sessions expire after 30 min; validate near expiry instead of every request.
_SESSION_VALIDATION_TTL_SECS = 25 * 60

# backoff_factor=1.0 → delays of 1s, 2s, 4s.
_RETRY_POLICY = Retry(
    total=3,
    backoff_factor=1.0,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset({"GET", "POST"}),
    respect_retry_after_header=True,
    raise_on_status=False,
)


class InformaticaClient:
    """REST API client for Informatica Cloud (IDMC)."""

    def __init__(
        self,
        config: InformaticaSourceConfig,
        report: InformaticaSourceReport,
    ):
        self.config = config
        self.report = report
        self._session = requests.Session()
        adapter = HTTPAdapter(max_retries=_RETRY_POLICY)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)
        self._session_id: Optional[str] = None
        self._base_url: Optional[str] = None
        self._session_last_validated_at: Optional[float] = None

    def login(self) -> str:
        """Authenticate via the v2 login endpoint. Returns the session id."""
        resp = self._session.post(
            f"{self.config.login_url}/ma/api/v2/user/login",
            json={
                "username": self.config.username,
                "password": self.config.password.get_secret_value(),
            },
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        self.report.report_api_call()
        if resp.status_code != 200:
            raise Exception(
                f"IDMC login failed (HTTP {resp.status_code}): {resp.text}. "
                f"Check login_url ({self.config.login_url}), username, and password."
            )
        data = resp.json()
        if "error" in data or ("@type" in data and data.get("@type") == "error"):
            error_msg = data.get("error", {}).get("message") or data.get(
                "description", str(data)
            )
            raise Exception(f"IDMC login failed: {error_msg}")
        session_id = data.get("icSessionId")
        base_url = (data.get("serverUrl") or "").rstrip("/")
        if not session_id or not base_url:
            raise Exception(
                "IDMC login succeeded but response is missing icSessionId "
                f"or serverUrl. Keys returned: {list(data.keys())}"
            )
        self._session_id = session_id
        self._base_url = base_url
        self._session_last_validated_at = time.time()
        logger.info(
            "IDMC login successful. Base URL: %s, Org: %s",
            base_url,
            data.get("orgId", "unknown"),
        )
        return session_id

    def _validate_session(self) -> bool:
        """POST /api/v2/user/validSessionId. False on any failure or expired token."""
        if not self._session_id or not self._base_url:
            return False
        try:
            resp = self._session.post(
                f"{self._base_url}/api/v2/user/validSessionId",
                json={
                    "@type": "validatedToken",
                    "userName": self.config.username,
                    "icToken": self._session_id,
                },
                headers={
                    "Content-Type": "application/json",
                    "icSessionId": self._session_id,
                },
                timeout=10,
            )
            self.report.report_api_call()
        except requests.RequestException as e:
            logger.warning("Session validation request failed: %s", e)
            return False
        if resp.status_code != 200:
            return False
        data = resp.json()
        if not data.get("isValidToken", False):
            return False
        logger.debug(
            "Session valid, %s minutes remaining", data.get("timeUntilExpire", "?")
        )
        self._session_last_validated_at = time.time()
        return True

    def _ensure_authenticated(self, *, clear_session: bool = False) -> str:
        """Return a known-good session id. `clear_session` forces a fresh login."""
        if clear_session:
            self._session_id = None
            self._session_last_validated_at = None
        if self._session_id and not self._session_stale():
            return self._session_id
        if self._session_id and self._validate_session():
            return self._session_id
        logger.info("IDMC session missing or invalid, authenticating...")
        return self.login()

    def _session_stale(self) -> bool:
        if self._session_last_validated_at is None:
            return True
        return (
            time.time() - self._session_last_validated_at
        ) > _SESSION_VALIDATION_TTL_SECS

    @staticmethod
    def _is_auth_failure(resp: requests.Response) -> bool:
        """True for a session/auth failure: 401, or 403 with error code AUTH_01."""
        if resp.status_code == 401:
            return True
        if resp.status_code != 403:
            return False
        try:
            return resp.json().get("error", {}).get("code") == "AUTH_01"
        except ValueError:
            return False

    def _request(
        self,
        method: str,
        url: str,
        *,
        is_v3: bool = False,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        stream: bool = False,
    ) -> requests.Response:
        """Execute an authenticated request; retry once on 401/AUTH_01."""
        resp = self._do_send(
            self._ensure_authenticated(), method, url, is_v3, params, json_body, stream
        )
        if self._is_auth_failure(resp):
            logger.info("IDMC session rejected by server, re-authenticating")
            resp = self._do_send(
                self._ensure_authenticated(clear_session=True),
                method,
                url,
                is_v3,
                params,
                json_body,
                stream,
            )
        return resp

    def _do_send(
        self,
        session_id: str,
        method: str,
        url: str,
        is_v3: bool,
        params: Optional[Dict[str, Any]],
        json_body: Optional[Any],
        stream: bool,
    ) -> requests.Response:
        headers: Dict[str, str] = {
            "INFA-SESSION-ID" if is_v3 else "icSessionId": session_id
        }
        if json_body is not None:
            headers["Content-Type"] = "application/json"
        resp = self._session.request(
            method,
            url,
            headers=headers,
            params=params,
            json=json_body,
            stream=stream,
            timeout=60,
        )
        self.report.report_api_call()
        return resp

    def _get_v2(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        resp = self._request("GET", f"{self._base_url}{path}", params=params)
        resp.raise_for_status()
        return resp.json()

    def _get_v3(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        resp = self._request(
            "GET", f"{self._base_url}{path}", is_v3=True, params=params
        )
        resp.raise_for_status()
        return resp.json()

    def _post_v3(self, path: str, json_body: Any) -> Any:
        resp = self._request(
            "POST", f"{self._base_url}{path}", is_v3=True, json_body=json_body
        )
        resp.raise_for_status()
        return resp.json()

    def list_objects(
        self,
        object_type: str,
        tag: Optional[str] = None,
    ) -> Iterator[IdmcObject]:
        """Paginate through v3 objects of a given type."""
        skip = 0
        limit = self.config.page_size
        while True:
            q_parts = [f"type=='{object_type}'"]
            if tag:
                q_parts.append(f"tag=='{tag}'")
            data = self._get_v3(
                "/public/core/v3/objects",
                params={"q": " and ".join(q_parts), "limit": limit, "skip": skip},
            )
            objects = data if isinstance(data, list) else data.get("objects", [])
            if not objects:
                break
            for obj in objects:
                yield self._parse_v3_object(obj, object_type)
            if len(objects) < limit:
                break
            skip += limit

    @staticmethod
    def _parse_v3_object(data: Dict[str, Any], object_type: str) -> IdmcObject:
        # Some endpoints return nested OData-style `properties`; others return flat JSON.
        if "properties" in data:
            props = {
                p.get("name", ""): p.get("value")
                for p in data.get("properties", [])
                if isinstance(p, dict)
            }
            return IdmcObject(
                id=props.get("id") or data.get("id", ""),
                name=props.get("name") or "",
                path=props.get("path") or "",
                object_type=props.get("documentType") or object_type,
                description=props.get("description") or "",
                updated_by=props.get("lastUpdatedBy") or "",
                created_by=props.get("createdBy") or "",
                create_time=props.get("createdTime") or "",
                update_time=props.get("lastUpdatedTime") or "",
                raw=data,
            )
        return IdmcObject(
            id=data.get("id", ""),
            name=data.get("name", ""),
            path=data.get("path", ""),
            object_type=data.get("documentType", object_type),
            description=data.get("description", ""),
            updated_by=data.get("lastUpdatedBy", data.get("updatedBy", "")),
            created_by=data.get("createdBy", ""),
            create_time=data.get("createdTime", data.get("createTime", "")),
            update_time=data.get("lastUpdatedTime", data.get("updateTime", "")),
            raw=data,
        )

    def list_mappings(self) -> List[IdmcMapping]:
        data = self._get_v2("/api/v2/mapping")
        items = data if isinstance(data, list) else [data]
        return [self._parse_v2_mapping(m) for m in items if isinstance(m, dict)]

    def get_mapping(self, v2_id: str) -> IdmcMapping:
        return self._parse_v2_mapping(self._get_v2(f"/api/v2/mapping/{v2_id}"))

    @staticmethod
    def _parse_v2_mapping(data: Dict[str, Any]) -> IdmcMapping:
        return IdmcMapping(
            v2_id=data.get("id", ""),
            name=data.get("name", ""),
            asset_frs_guid=data.get("assetFrsGuid", ""),
            description=data.get("description", ""),
            created_by=data.get("createdBy", ""),
            updated_by=data.get("updatedBy", ""),
            create_time=data.get("createTime", ""),
            update_time=data.get("updateTime", ""),
            document_type=data.get("documentType", ""),
            valid=data.get("valid", True),
            parameters=data.get("parameters", []),
            references=data.get("references", []),
            raw=data,
        )

    def list_connections(self) -> List[IdmcConnection]:
        data = self._get_v2("/api/v2/connection")
        items = data if isinstance(data, list) else [data]
        return [
            IdmcConnection.from_api_response(c) for c in items if isinstance(c, dict)
        ]

    def get_connection(self, connection_id: str) -> IdmcConnection:
        return IdmcConnection.from_api_response(
            self._get_v2(f"/api/v2/connection/{connection_id}")
        )

    def list_mapping_tasks(self) -> List[IdmcMappingTask]:
        data = self._get_v2("/api/v2/mttask")
        items = data if isinstance(data, list) else [data]
        return [self._parse_v2_mttask(mt) for mt in items if isinstance(mt, dict)]

    @staticmethod
    def _parse_v2_mttask(data: Dict[str, Any]) -> IdmcMappingTask:
        return IdmcMappingTask(
            v2_id=data.get("id", ""),
            name=data.get("name", ""),
            description=data.get("description", ""),
            mapping_id=data.get("mappingId", ""),
            mapping_name=data.get("mappingName", ""),
            connection_id=data.get("connectionId", ""),
            created_by=data.get("createdBy", ""),
            updated_by=data.get("updatedBy", ""),
            create_time=data.get("createTime", ""),
            update_time=data.get("updateTime", ""),
            raw=data,
        )

    def get_activity_log(
        self,
        rows: int = 100,
        run_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"rows": rows}
        if run_id:
            params["runId"] = run_id
        data = self._get_v2("/api/v2/activity/activityLog", params=params)
        return data if isinstance(data, list) else []

    def submit_export_job(self, object_ids: List[str]) -> str:
        body = {
            "objects": [
                {"id": obj_id, "includeDependencies": False} for obj_id in object_ids
            ]
        }
        data = self._post_v3("/public/core/v3/export", body)
        job_id = data.get("id", "")
        if not job_id:
            raise Exception(f"Export job submission returned no ID: {data}")
        self.report.export_jobs_submitted += 1
        logger.info("Export job submitted: %s (%d objects)", job_id, len(object_ids))
        return job_id

    def poll_export_job(self, job_id: str) -> ExportJobStatus:
        data = self._get_v3(f"/public/core/v3/export/{job_id}")
        status = data.get("status", {})
        return ExportJobStatus(
            job_id=job_id,
            state=status.get("state", "UNKNOWN"),
            message=status.get("message", ""),
        )

    def wait_for_export(self, job_id: str) -> ExportJobStatus:
        """Poll until complete or timeout."""
        deadline = time.time() + self.config.export_poll_timeout_secs
        while time.time() < deadline:
            status = self.poll_export_job(job_id)
            if status.state == "SUCCESSFUL":
                return status
            if status.state not in ("IN_PROGRESS", "QUEUED"):
                self.report.report_export_failed(job_id, status.message)
                return status
            time.sleep(self.config.export_poll_interval_secs)
        self.report.report_export_failed(job_id, "Timed out waiting for export")
        return ExportJobStatus(
            job_id=job_id, state="TIMEOUT", message="Export poll timed out"
        )

    def download_and_parse_export(self, job_id: str) -> Iterator[MappingLineageInfo]:
        """Download export ZIP and yield one MappingLineageInfo per mapping."""
        resp = self._request(
            "GET",
            f"{self._base_url}/public/core/v3/export/{job_id}/package",
            is_v3=True,
            stream=True,
        )
        resp.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix=".zip") as tmp:
            for chunk in resp.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp.flush()
            tmp.seek(0)
            try:
                outer_zip = zipfile.ZipFile(tmp)
            except zipfile.BadZipFile:
                logger.error("Export package for job %s is not a valid ZIP", job_id)
                self.report.report_export_failed(job_id, "Invalid ZIP file")
                return
            for entry_name in outer_zip.namelist():
                if not entry_name.endswith(".DTEMPLATE.zip"):
                    continue
                try:
                    inner_zip = zipfile.ZipFile(io.BytesIO(outer_zip.read(entry_name)))
                    lineage = self._parse_inner_dtemplate_zip(inner_zip, entry_name)
                    if lineage:
                        yield lineage
                    inner_zip.close()
                except Exception:
                    logger.warning(
                        "Failed to parse inner ZIP %s in export %s",
                        entry_name,
                        job_id,
                        exc_info=True,
                    )
            outer_zip.close()

    def _parse_inner_dtemplate_zip(
        self,
        inner_zip: zipfile.ZipFile,
        entry_name: str,
    ) -> Optional[MappingLineageInfo]:
        # @3.bin is the mapping design; @2.bin is a preview image we skip.
        bin_candidates = [n for n in inner_zip.namelist() if n.startswith("bin/@")]
        bin_file = next((n for n in bin_candidates if n != "bin/@2.bin"), None)
        if not bin_file:
            logger.debug("No @3.bin found in %s", entry_name)
            return None
        raw_bytes = inner_zip.read(bin_file)
        try:
            data = json.loads(raw_bytes.decode("utf-8"))
        except UnicodeDecodeError:
            data = json.loads(raw_bytes.decode("utf-8-sig"))
        return self._extract_lineage_from_3bin(data, entry_name)

    @staticmethod
    def _extract_lineage_from_3bin(
        data: Dict[str, Any],
        source_label: str,
    ) -> Optional[MappingLineageInfo]:
        content = data.get("content")
        if not content:
            logger.warning("Missing 'content' key in @3.bin from %s", source_label)
            return None
        sources: List[LineageTable] = []
        targets: List[LineageTable] = []
        for tx in content.get("transformations", []):
            adapter = tx.get("dataAdapter")
            if not adapter:
                continue
            obj = adapter.get("object", {})
            conn_id = adapter.get("connectionId", "")
            # connectionId format: "saas:@{federatedId}" — take the part after '@'.
            fed_id = conn_id.split("@", 1)[-1] if "@" in conn_id else conn_id
            lt = LineageTable(
                table_name=obj.get("name")
                or obj.get("objectName")
                or obj.get("label")
                or "",
                schema_name=obj.get("dbSchema") or "",
                connection_federated_id=fed_id,
                transformation_name=tx.get("name", ""),
            )
            tx_class = tx.get("$$class")
            tx_name = tx.get("name", "").lower()
            if tx_class == 9 or "source" in tx_name:
                sources.append(lt)
            elif tx_class == 8 or "target" in tx_name:
                targets.append(lt)
        if not sources and not targets:
            return None
        return MappingLineageInfo(
            mapping_id="",
            mapping_name=content.get("name", source_label),
            source_tables=sources,
            target_tables=targets,
        )
