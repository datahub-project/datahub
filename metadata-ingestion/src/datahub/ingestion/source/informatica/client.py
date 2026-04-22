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
    ExportJobState,
    ExportJobStatus,
    IdmcConnection,
    IdmcMapping,
    IdmcMappingTask,
    IdmcObject,
    IdmcObjectType,
    InformaticaApiError,
    InformaticaLoginError,
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
            raise InformaticaLoginError(
                f"IDMC login failed (HTTP {resp.status_code}): {resp.text[:500]}. "
                f"Check login_url ({self.config.login_url}), username, and password."
            )
        try:
            data = resp.json()
        except json.JSONDecodeError as e:
            raise InformaticaLoginError(
                f"IDMC login returned 200 with non-JSON body "
                f"(url={resp.url}): {resp.text[:200]!r}"
            ) from e
        if "error" in data or data.get("@type") == "error":
            error_msg = data.get("error", {}).get("message") or data.get(
                "description", str(data)
            )
            raise InformaticaLoginError(f"IDMC login failed: {error_msg}")
        session_id = data.get("icSessionId")
        base_url = (data.get("serverUrl") or "").rstrip("/")
        if not session_id or not base_url:
            raise InformaticaLoginError(
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
            self.report.warning(
                title="IDMC session validation request failed",
                message="Will attempt to re-login; ingestion will continue if re-login succeeds.",
                context=str(self._base_url),
                exc=e,
            )
            return False
        if resp.status_code != 200:
            return False
        try:
            data = resp.json()
        except json.JSONDecodeError:
            # 200 with unparseable body → force a re-login instead of crashing.
            return False
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
            if self._is_auth_failure(resp):
                self.report.failure(
                    title="IDMC authentication rejected after retry",
                    message="The server refused the request even after re-authentication. Check credentials and token permissions.",
                    context=f"{method} {url}",
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

    @staticmethod
    def _decode_json(resp: requests.Response) -> Any:
        """Parse a 2xx response as JSON, raising InformaticaApiError on empty
        or malformed bodies. ``raise_for_status`` only covers HTTP status —
        a 200 with broken JSON would otherwise crash the whole pipeline with
        an uncaught JSONDecodeError.
        """
        try:
            return resp.json()
        except json.JSONDecodeError as e:
            body_preview = (resp.text or "")[:200]
            raise InformaticaApiError(
                f"IDMC returned {resp.status_code} with non-JSON body "
                f"(url={resp.url}): {body_preview!r}"
            ) from e

    def _get_v2(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        resp = self._request("GET", f"{self._base_url}{path}", params=params)
        resp.raise_for_status()
        return self._decode_json(resp)

    def _get_v3(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        resp = self._request(
            "GET", f"{self._base_url}{path}", is_v3=True, params=params
        )
        resp.raise_for_status()
        return self._decode_json(resp)

    def _post_v3(self, path: str, json_body: Any) -> Any:
        resp = self._request(
            "POST", f"{self._base_url}{path}", is_v3=True, json_body=json_body
        )
        resp.raise_for_status()
        return self._decode_json(resp)

    def list_objects(
        self,
        object_type: IdmcObjectType,
        tag: Optional[str] = None,
    ) -> Iterator[IdmcObject]:
        """Paginate through v3 objects of a given type.

        The `tag` argument, if set, filters the results client-side by the
        object's ``tags`` field. IDMC's v3 objects API rejects ``tag==`` in its
        server-side ``q`` parameter (HTTP 400).

        Every page is logged to the report (method, URL, status proxy via
        item count, total accumulated) so silent empty responses surface
        in the end-of-run summary rather than hiding in DEBUG logs.
        """
        skip = 0
        limit = self.config.page_size
        total_returned = 0
        total_yielded = 0
        page = 0
        while True:
            params = {
                "q": f"type=='{object_type}'",
                "limit": limit,
                "skip": skip,
            }
            url = f"{self._base_url}/public/core/v3/objects?{_encode_params(params)}"
            data = self._get_v3("/public/core/v3/objects", params=params)
            objects = data if isinstance(data, list) else data.get("objects", [])
            page += 1
            self.report.report_api_response(
                method="GET",
                url=url,
                status=200,
                item_count=len(objects),
                extra=f"type={object_type} tag={tag or '-'} page={page}",
            )
            logger.debug(
                "list_objects type=%s tag=%s page=%d returned %d items",
                object_type,
                tag,
                page,
                len(objects),
            )
            if not objects:
                break
            for obj in objects:
                parsed = self._parse_v3_object(obj, object_type)
                total_returned += 1
                # Record under both query type and actual documentType so items
                # returned under a different type (e.g. MAPPLET via DTEMPLATE) surface correctly.
                self.report.report_raw_object(object_type, parsed.path)
                if parsed.object_type and parsed.object_type != object_type:
                    self.report.report_raw_object(parsed.object_type, parsed.path)
                if tag and tag not in parsed.tags:
                    self.report.report_filtered("tag", object_type, parsed.path)
                    continue
                total_yielded += 1
                yield parsed
            if len(objects) < limit:
                break
            skip += limit
        logger.info(
            "list_objects type=%s tag=%s: returned=%d yielded=%d across %d page(s)",
            object_type,
            tag,
            total_returned,
            total_yielded,
            page,
        )

    @staticmethod
    def _parse_v3_object(data: Dict[str, Any], object_type: str) -> IdmcObject:
        # Some endpoints return nested OData-style `properties`; others return flat JSON.
        if "properties" in data:
            return IdmcObject.from_properties(data, object_type)
        return IdmcObject.from_flat(data, object_type)

    def list_mappings(self) -> List[IdmcMapping]:
        data = self._get_v2("/api/v2/mapping")
        items = data if isinstance(data, list) else [data]
        mappings = [
            IdmcMapping.from_api_response(m) for m in items if isinstance(m, dict)
        ]
        self.report.report_api_response(
            method="GET",
            url=f"{self._base_url}/api/v2/mapping",
            status=200,
            item_count=len(mappings),
            extra="v2-mappings",
        )
        return mappings

    def get_mapping(self, v2_id: str) -> IdmcMapping:
        return IdmcMapping.from_api_response(self._get_v2(f"/api/v2/mapping/{v2_id}"))

    # IDMC mapplet endpoint has moved across versions — probe each shape.
    _V2_MAPPLET_ENDPOINTS: List[str] = [
        "/api/v2/mapplet",
        "/api/v2/mapplets",
        "/api/v2/Mapplet",
    ]

    def list_mapplets_v2(self) -> List[IdmcObject]:
        """Fallback mapplet listing via the v2 API.

        Used when the v3 ``type=='MAPPLET'`` query returns zero items.
        Probes the known endpoint shapes and returns the first 2xx response,
        adapted to :class:`IdmcObject`. Empty list when every probe fails.
        """
        for path in self._V2_MAPPLET_ENDPOINTS:
            try:
                data = self._get_v2(path)
            except (InformaticaApiError, requests.HTTPError) as e:
                # _get_v2 raises HTTPError on 404/5xx, or wraps as InformaticaApiError.
                self.report.report_api_response(
                    method="GET",
                    url=f"{self._base_url}{path}",
                    status=0,
                    item_count=0,
                    extra=f"v2-mapplets-probe-failed: {e}",
                )
                continue
            mapplets = self._parse_v2_mapplets(data)
            self.report.report_api_response(
                method="GET",
                url=f"{self._base_url}{path}",
                status=200,
                item_count=len(mapplets),
                extra="v2-mapplets",
            )
            return mapplets
        return []

    @staticmethod
    def _parse_v2_mapplets(data: Any) -> List[IdmcObject]:
        """Coerce a v2 mapplet-list response into :class:`IdmcObject` rows.

        Handles both array and single-object shapes, skipping non-dict items.
        """
        items = data if isinstance(data, list) else [data]
        mapplets: List[IdmcObject] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            name = item.get("name", "")
            parent = item.get("location") or item.get("folderPath") or ""
            path = f"{parent.rstrip('/')}/{name}" if parent and name else name
            mapplets.append(
                IdmcObject(
                    # v3 GUID aligns URNs with v3-sourced content.
                    id=item.get("assetFrsGuid") or item.get("id", ""),
                    name=name,
                    path=path,
                    object_type="MAPPLET",
                    description=item.get("description") or None,
                    created_by=item.get("createdBy") or None,
                    updated_by=item.get("updatedBy") or None,
                    create_time=item.get("createTime") or None,
                    update_time=item.get("updateTime") or None,
                )
            )
        return mapplets

    def list_connections(self) -> List[IdmcConnection]:
        data = self._get_v2("/api/v2/connection")
        items = data if isinstance(data, list) else [data]
        connections = [
            IdmcConnection.from_api_response(c) for c in items if isinstance(c, dict)
        ]
        self.report.report_api_response(
            method="GET",
            url=f"{self._base_url}/api/v2/connection",
            status=200,
            item_count=len(connections),
            extra="v2-connections",
        )
        return connections

    def get_connection(self, connection_id: str) -> IdmcConnection:
        return IdmcConnection.from_api_response(
            self._get_v2(f"/api/v2/connection/{connection_id}")
        )

    def list_mapping_tasks(self) -> Iterator[IdmcMappingTask]:
        """Iterate mapping tasks via the v3 objects API (type=MTT).

        Delegates to :meth:`list_objects` so MTT entries pass through the
        same flat/properties-style parser as other v3 object types. The v2
        ``/api/v2/mttask`` endpoint cannot be used — it requires an ID or
        name and does not list.

        Add-On Bundle tasks are skipped. Paths can arrive as
        ``"/Explore/Add-On Bundles/..."``, ``"/Add-On Bundles/..."``, or
        ``"Add-On Bundles/..."`` depending on IDMC version, so match any
        path segment rather than a fixed prefix.
        """
        for obj in self.list_objects("MTT"):
            if (
                obj.updated_by == "bundle-license-notifier"
                or obj.name == "Add-On Bundles"
                or any(seg == "Add-On Bundles" for seg in obj.path.split("/") if seg)
            ):
                self.report.report_filtered("bundle", "MTT", obj.path)
                logger.debug("Skipping bundle MTT: id=%s path=%s", obj.id, obj.path)
                continue
            yield IdmcMappingTask.from_idmc_object(obj)

    def submit_export_job(self, object_ids: List[str]) -> str:
        body = {
            "objects": [
                {"id": obj_id, "includeDependencies": False} for obj_id in object_ids
            ]
        }
        data = self._post_v3("/public/core/v3/export", body)
        job_id = data.get("id", "")
        if not job_id:
            raise InformaticaApiError(f"Export job submission returned no ID: {data}")
        self.report.export_jobs_submitted += 1
        logger.info("Export job submitted: %s (%d objects)", job_id, len(object_ids))
        return job_id

    def poll_export_job(self, job_id: str) -> ExportJobStatus:
        data = self._get_v3(f"/public/core/v3/export/{job_id}")
        status = data.get("status", {})
        return ExportJobStatus(
            job_id=job_id,
            state=ExportJobState.from_api_value(status.get("state")),
            message=status.get("message", ""),
        )

    def wait_for_export(self, job_id: str) -> ExportJobStatus:
        """Poll until complete or timeout."""
        deadline = time.time() + self.config.export_poll_timeout_secs
        while time.time() < deadline:
            status = self.poll_export_job(job_id)
            if status.state == ExportJobState.SUCCESSFUL:
                return status
            if status.state not in (ExportJobState.IN_PROGRESS, ExportJobState.QUEUED):
                self.report.report_export_failed(job_id, status.message)
                return status
            time.sleep(self.config.export_poll_interval_secs)
        self.report.report_export_failed(job_id, "Timed out waiting for export")
        self.report.warning(
            title="IDMC export job timed out",
            message="Lineage for this export batch will be missing. Consider increasing export_poll_timeout_secs or reducing export_batch_size.",
            context=f"job_id={job_id}",
        )
        return ExportJobStatus(
            job_id=job_id,
            state=ExportJobState.TIMEOUT,
            message="Export poll timed out",
        )

    def download_and_parse_export(
        self,
        job_id: str,
        submitted_ids: List[str],
    ) -> Iterator[MappingLineageInfo]:
        """Download export ZIP and yield one MappingLineageInfo per mapping.

        `submitted_ids` is the list of v3 GUIDs submitted to the export job, in
        submission order. The export package contains one `*.DTEMPLATE.zip`
        entry per submitted object; we align entries to ids positionally, with
        a fallback to searching the entry name for any of the submitted ids.
        """
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
            except zipfile.BadZipFile as e:
                self.report.warning(
                    title="IDMC export package is not a valid ZIP",
                    message="Lineage for this batch will be missing.",
                    context=f"job_id={job_id}",
                    exc=e,
                )
                self.report.report_export_failed(job_id, "Invalid ZIP file")
                return
            dtemplate_entries = [
                n for n in outer_zip.namelist() if n.endswith(".DTEMPLATE.zip")
            ]
            for idx, entry_name in enumerate(dtemplate_entries):
                mapping_id = self._resolve_entry_mapping_id(
                    entry_name, submitted_ids, idx
                )
                try:
                    inner_zip = zipfile.ZipFile(io.BytesIO(outer_zip.read(entry_name)))
                    lineage = self._parse_inner_dtemplate_zip(
                        inner_zip, entry_name, mapping_id
                    )
                    if lineage:
                        yield lineage
                    inner_zip.close()
                except Exception as e:
                    self.report.warning(
                        title="Failed to parse IDMC mapping export entry",
                        message="Lineage for this mapping will be missing.",
                        context=f"job_id={job_id}, entry={entry_name}",
                        exc=e,
                    )
                    self.report.report_object_failed(entry_name, str(e))
            outer_zip.close()

    @staticmethod
    def _resolve_entry_mapping_id(
        entry_name: str, submitted_ids: List[str], index: int
    ) -> str:
        """Match an export entry to its submitted mapping id.

        Prefer a substring match against the entry name (more robust against
        server-side ordering changes); fall back to positional alignment.
        """
        for sid in submitted_ids:
            if sid and sid in entry_name:
                return sid
        if 0 <= index < len(submitted_ids):
            return submitted_ids[index]
        return ""

    def _parse_inner_dtemplate_zip(
        self,
        inner_zip: zipfile.ZipFile,
        entry_name: str,
        mapping_id: str,
    ) -> Optional[MappingLineageInfo]:
        # @3.bin is the mapping design; @2.bin is a preview image we skip.
        bin_candidates = [n for n in inner_zip.namelist() if n.startswith("bin/@")]
        bin_file = next((n for n in bin_candidates if n != "bin/@2.bin"), None)
        if not bin_file:
            logger.debug("No @3.bin found in %s", entry_name)
            return None
        raw_bytes = inner_zip.read(bin_file)
        # Decode/parse failures are reported explicitly so they surface in the
        # summary instead of being absorbed by the caller's batch-level except.
        try:
            try:
                decoded = raw_bytes.decode("utf-8")
            except UnicodeDecodeError:
                decoded = raw_bytes.decode("utf-8-sig")
            data = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            self.report.report_object_failed(
                entry_name, f"invalid mapping design @3.bin: {e}"
            )
            logger.warning(
                "Failed to parse mapping design JSON for %s: %s", entry_name, e
            )
            return None
        return self._extract_lineage_from_3bin(data, entry_name, mapping_id)

    def _extract_lineage_from_3bin(
        self,
        data: Dict[str, Any],
        source_label: str,
        mapping_id: str,
    ) -> Optional[MappingLineageInfo]:
        content = data.get("content")
        if not content:
            self.report.warning(
                title="Malformed IDMC mapping export",
                message="The @3.bin file is missing the 'content' key; lineage for this mapping will be missing.",
                context=source_label,
            )
            self.report.report_object_failed(source_label, "Missing 'content' key")
            return None
        sources: List[LineageTable] = []
        targets: List[LineageTable] = []
        for tx in content.get("transformations", []):
            adapter = tx.get("dataAdapter")
            if not adapter:
                continue
            obj = adapter.get("object", {})
            conn_id = adapter.get("connectionId", "")
            fed_id = parse_saas_connection_ref(conn_id)
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
            mapping_id=mapping_id,
            mapping_name=content.get("name", source_label),
            source_tables=sources,
            target_tables=targets,
        )


def parse_saas_connection_ref(conn_id: str) -> str:
    """Parse an IDMC connection reference of the form 'saas:@{federatedId}'.

    Returns the part after the '@' if present, otherwise the input unchanged.
    """
    return conn_id.split("@", 1)[-1] if "@" in conn_id else conn_id


def _encode_params(params: Dict[str, Any]) -> str:
    """Render query params in a human-readable form for the report log.

    Not used for actual HTTP — requests builds that itself — just for log
    lines so the report shows ``q=type=='MAPPLET'`` style strings that the
    user can eyeball to diagnose a bad filter.
    """
    return "&".join(f"{k}={v}" for k, v in params.items())
