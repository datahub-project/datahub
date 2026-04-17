import io
import json
import logging
import tempfile
import time
import zipfile
from typing import Any, Dict, Iterator, List, Optional

import requests

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


class InformaticaClient:
    """REST API client for Informatica Cloud (IDMC).

    Handles authentication (v2 login), dual v2/v3 API calls, automatic
    re-authentication on 401, pagination, and retry with backoff.
    """

    def __init__(
        self,
        config: InformaticaSourceConfig,
        report: InformaticaSourceReport,
    ):
        self.config = config
        self.report = report

        self._session = requests.Session()
        self._session_id: Optional[str] = None
        self._base_url: Optional[str] = None  # serverUrl from login response
        self._session_created_at: float = 0.0

        self._max_retries = 3
        self._backoff_factor = 2.0

    def login(self) -> None:
        """Authenticate via the v2 user login endpoint."""
        url = f"{self.config.login_url}/ma/api/v2/user/login"
        payload = {
            "username": self.config.username,
            "password": self.config.password.get_secret_value(),
        }

        resp = self._session.post(
            url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        self.report.report_api_call()

        if resp.status_code != 200:
            body = resp.text
            raise Exception(
                f"IDMC login failed (HTTP {resp.status_code}): {body}. "
                f"Check login_url ({self.config.login_url}), username, and password."
            )

        data = resp.json()

        if "error" in data or "@type" in data and data.get("@type") == "error":
            error_msg = data.get("error", {}).get("message") or data.get(
                "description", str(data)
            )
            raise Exception(f"IDMC login failed: {error_msg}")

        self._session_id = data.get("icSessionId")
        self._base_url = data.get("serverUrl", "").rstrip("/")
        self._session_created_at = time.time()

        if not self._session_id or not self._base_url:
            raise Exception(
                "IDMC login succeeded but response is missing icSessionId "
                f"or serverUrl. Keys returned: {list(data.keys())}"
            )

        logger.info(
            "IDMC login successful. Base URL: %s, Org: %s",
            self._base_url,
            data.get("orgId", "unknown"),
        )

    # IDMC sessions expire after 30 minutes of inactivity.
    # We proactively validate when the session is older than this threshold
    # to avoid sending requests that will definitely fail with 401.
    _SESSION_VALIDATE_AFTER_SECS = 25 * 60  # 25 minutes

    def _is_session_valid(self) -> bool:
        """Check if the current session is still valid via the v2 validSessionId endpoint."""
        if not self._session_id or not self._base_url:
            return False

        try:
            url = f"{self._base_url}/api/v2/user/validSessionId"
            resp = self._session.post(
                url,
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
            if resp.status_code == 200:
                data = resp.json()
                is_valid = data.get("isValidToken", False)
                if is_valid:
                    remaining = data.get("timeUntilExpire", "?")
                    logger.debug("Session valid, %s minutes remaining", remaining)
                return bool(is_valid)
        except Exception:
            logger.debug("Session validation check failed", exc_info=True)

        return False

    def _ensure_authenticated(self) -> None:
        """Ensure we have a valid session.

        - No session → login
        - Session older than 25 min → validate via validSessionId, re-login if expired
        - Session younger than 25 min → skip validation (fast path)
        - The 401 re-auth in _request() is kept as a safety net for edge cases
        """
        if self._session_id:
            session_age = time.time() - self._session_created_at
            if session_age < self._SESSION_VALIDATE_AFTER_SECS:
                return
            if self._is_session_valid():
                return
            logger.info(
                "Session expired after %.0f minutes, re-authenticating...",
                session_age / 60,
            )
            self._session_id = None

        self.login()

        if not self._session_id:
            logger.error("Authentication failed: session ID is still None after login")
            raise Exception(
                "IDMC authentication failed — no session ID after login. "
                "Check login_url, username, and password."
            )

    def _request(
        self,
        method: str,
        url: str,
        *,
        is_v3: bool = False,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        stream: bool = False,
        _retry_count: int = 0,
    ) -> requests.Response:
        """Execute an authenticated HTTP request with retry and re-auth logic."""
        self._ensure_authenticated()
        session_id: str = self._session_id  # type: ignore[assignment]

        headers: Dict[str, str] = {}
        if is_v3:
            headers["INFA-SESSION-ID"] = session_id
        else:
            headers["icSessionId"] = session_id

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

        # Re-auth on 401/403 with AUTH_01
        if resp.status_code in (401, 403) and _retry_count == 0:
            try:
                body = resp.json()
            except Exception:
                body = {}
            error_code = body.get("error", {}).get("code", "")
            if resp.status_code == 401 or error_code == "AUTH_01":
                logger.info("Session expired, re-authenticating...")
                self._session_id = None
                self.login()
                return self._request(
                    method,
                    url,
                    is_v3=is_v3,
                    params=params,
                    json_body=json_body,
                    stream=stream,
                    _retry_count=_retry_count + 1,
                )

        # Retry on 429 / 5xx with backoff
        if resp.status_code in (429, 500, 502, 503, 504):
            if _retry_count < self._max_retries:
                wait = self._backoff_factor**_retry_count
                logger.warning(
                    "HTTP %d from %s — retrying in %.1fs (attempt %d/%d)",
                    resp.status_code,
                    url,
                    wait,
                    _retry_count + 1,
                    self._max_retries,
                )
                time.sleep(wait)
                return self._request(
                    method,
                    url,
                    is_v3=is_v3,
                    params=params,
                    json_body=json_body,
                    stream=stream,
                    _retry_count=_retry_count + 1,
                )

        return resp

    def _get_v2(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """GET from the v2 API. `path` is relative to base_url (e.g., '/api/v2/mapping')."""
        url = f"{self._base_url}{path}"
        resp = self._request("GET", url, is_v3=False, params=params)
        resp.raise_for_status()
        return resp.json()

    def _get_v3(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """GET from the v3 API. `path` is relative to base_url (e.g., '/public/core/v3/objects')."""
        url = f"{self._base_url}{path}"
        resp = self._request("GET", url, is_v3=True, params=params)
        resp.raise_for_status()
        return resp.json()

    def _post_v3(
        self,
        path: str,
        json_body: Any,
    ) -> Any:
        """POST to the v3 API."""
        url = f"{self._base_url}{path}"
        resp = self._request("POST", url, is_v3=True, json_body=json_body)
        resp.raise_for_status()
        return resp.json()

    def list_objects(
        self,
        object_type: str,
        tag: Optional[str] = None,
    ) -> Iterator[IdmcObject]:
        """Paginate through v3 objects of a given type. Yields IdmcObject instances."""
        skip = 0
        limit = self.config.page_size

        while True:
            q_parts = [f"type=='{object_type}'"]
            if tag:
                q_parts.append(f"tag=='{tag}'")
            q = " and ".join(q_parts)

            data = self._get_v3(
                "/public/core/v3/objects",
                params={"q": q, "limit": limit, "skip": skip},
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
        """Parse a v3 object response into an IdmcObject."""
        # v3 objects use OData-like nested property format
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
        # Flat JSON format (some endpoints)
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
        """List all mappings via the v2 API."""
        data = self._get_v2("/api/v2/mapping")
        items = data if isinstance(data, list) else [data]
        return [self._parse_v2_mapping(m) for m in items if isinstance(m, dict)]

    def get_mapping(self, v2_id: str) -> IdmcMapping:
        """Get a single mapping by v2 ID."""
        data = self._get_v2(f"/api/v2/mapping/{v2_id}")
        return self._parse_v2_mapping(data)

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
        """List all connections via the v2 API."""
        data = self._get_v2("/api/v2/connection")
        items = data if isinstance(data, list) else [data]
        return [
            IdmcConnection.from_api_response(c) for c in items if isinstance(c, dict)
        ]

    def get_connection(self, connection_id: str) -> IdmcConnection:
        """Get a single connection by v2 ID."""
        data = self._get_v2(f"/api/v2/connection/{connection_id}")
        return IdmcConnection.from_api_response(data)

    def list_mapping_tasks(self) -> List[IdmcMappingTask]:
        """List all mapping tasks via the v2 API."""
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
        """Get activity log entries via the v2 API."""
        params: Dict[str, Any] = {"rows": rows}
        if run_id:
            params["runId"] = run_id
        data = self._get_v2("/api/v2/activity/activityLog", params=params)
        return data if isinstance(data, list) else []

    def submit_export_job(self, object_ids: List[str]) -> str:
        """Submit a v3 export job for the given object IDs. Returns the job ID."""
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
        """Poll the status of an export job."""
        data = self._get_v3(f"/public/core/v3/export/{job_id}")
        status = data.get("status", {})
        return ExportJobStatus(
            job_id=job_id,
            state=status.get("state", "UNKNOWN"),
            message=status.get("message", ""),
        )

    def wait_for_export(self, job_id: str) -> ExportJobStatus:
        """Poll an export job until it completes or times out."""
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
        """Download an export ZIP and yield MappingLineageInfo for each mapping."""
        self._ensure_authenticated()
        assert self._session_id is not None

        url = f"{self._base_url}/public/core/v3/export/{job_id}/package"
        resp = self._request("GET", url, is_v3=True, stream=True)
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
                    inner_bytes = outer_zip.read(entry_name)
                    inner_zip = zipfile.ZipFile(io.BytesIO(inner_bytes))
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
        """Parse a .DTEMPLATE.zip to extract lineage from @3.bin."""
        bin_candidates = [n for n in inner_zip.namelist() if n.startswith("bin/@")]
        # @3.bin is the mapping design; @2.bin is a preview image
        bin_file = next(
            (n for n in bin_candidates if n != "bin/@2.bin"),
            None,
        )
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
        """Extract source/target tables from an @3.bin mapping definition."""
        content = data.get("content")
        if not content:
            logger.warning("Missing 'content' key in @3.bin from %s", source_label)
            return None

        mapping_name = content.get("name", source_label)
        sources: List[LineageTable] = []
        targets: List[LineageTable] = []

        for tx in content.get("transformations", []):
            adapter = tx.get("dataAdapter")
            if not adapter:
                continue

            obj = adapter.get("object", {})
            table = obj.get("name") or obj.get("objectName") or obj.get("label") or ""
            schema = obj.get("dbSchema") or ""
            conn_id = adapter.get("connectionId", "")
            # connectionId format: "saas:@{federatedId}" → extract the federatedId
            fed_id = conn_id.split("@", 1)[-1] if "@" in conn_id else conn_id

            lt = LineageTable(
                table_name=table,
                schema_name=schema,
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
            mapping_id="",  # Filled later by caller
            mapping_name=mapping_name,
            source_tables=sources,
            target_tables=targets,
        )
