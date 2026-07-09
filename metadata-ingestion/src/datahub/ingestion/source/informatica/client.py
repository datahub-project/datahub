import io
import json
import logging
import random
import tempfile
import time
import xml.etree.ElementTree as ET
import zipfile
from enum import IntEnum
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    TypedDict,
)
from urllib.parse import urlencode

import defusedxml.ElementTree
import requests
from defusedxml.common import DefusedXmlException
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.informatica.config import (
    DEFAULT_LOGIN_TIMEOUT_SECS,
    DEFAULT_SESSION_VALIDATION_TIMEOUT_SECS,
    InformaticaSourceConfig,
)
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
    TaskflowDefinition,
    TaskflowStep,
    TaskflowStepType,
)

logger = logging.getLogger(__name__)


class TxClass(IntEnum):
    # IDMC transformation ``$$class`` values, reverse-engineered from
    # exported DTEMPLATE @3.bin — not publicly documented.
    TARGET = 8
    SOURCE = 9


# IDMC sessions expire after 30 min; validate near expiry, not per-call.
_SESSION_VALIDATION_TTL_SECS = 25 * 60

# Cap decompressed size of a single DTEMPLATE entry — guards against
# malformed / hostile exports. Real mapping exports are sub-megabyte.
_MAX_INNER_ZIP_BYTES = 100 * 1024 * 1024

# Cap the outer export ZIP streamed to /tmp — guards against unbounded writes
# that could exhaust pod disk space.
_MAX_OUTER_ZIP_BYTES = 500 * 1024 * 1024

# Cap for the taskflow export package — same rationale as above.
_MAX_TASKFLOW_ZIP_BYTES = 500 * 1024 * 1024

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
        self._taskflow_definition_cache: Dict[str, TaskflowDefinition] = {}
        # One-shot guard so a persistently broken ``/validSessionId`` surfaces
        # once per run instead of spamming the report on every API call.
        self._session_validation_warned: bool = False

    def close(self) -> None:
        self._session.close()

    def login(self) -> str:
        """Authenticate via the v2 login endpoint. Returns the session id."""
        resp = self._session.post(
            f"{self.config.login_url}/ma/api/v2/user/login",
            json={
                "username": self.config.username,
                "password": self.config.password.get_secret_value(),
            },
            headers={"Content-Type": "application/json"},
            timeout=DEFAULT_LOGIN_TIMEOUT_SECS,
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
                timeout=DEFAULT_SESSION_VALIDATION_TIMEOUT_SECS,
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
            self._warn_once_on_session_validation_failure(
                reason=f"HTTP {resp.status_code}"
            )
            return False
        try:
            data = resp.json()
        except json.JSONDecodeError:
            # 200 with unparseable body → force a re-login instead of crashing.
            self._warn_once_on_session_validation_failure(reason="non-JSON body")
            return False
        if not data.get("isValidToken", False):
            # Expected path when the token has simply expired — don't warn.
            return False
        logger.debug(
            "Session valid, %s minutes remaining", data.get("timeUntilExpire", "?")
        )
        self._session_last_validated_at = time.time()
        return True

    def _warn_once_on_session_validation_failure(self, *, reason: str) -> None:
        if self._session_validation_warned:
            return
        self._session_validation_warned = True
        self.report.warning(
            title="IDMC session validation endpoint returned unexpected response",
            message=(
                "Re-login will be triggered on every API call until this is "
                "resolved. Verify the validSessionId endpoint is reachable "
                "and the service account has the correct permissions."
            ),
            context=f"base_url={self._base_url} reason={reason}",
        )

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
                # Typed error so the caller aborts cleanly instead of the 4xx
                # being downgraded to per-endpoint warnings by ``_safe_list``.
                raise InformaticaLoginError(
                    f"IDMC authentication rejected after retry: {method} {url}"
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
            timeout=self.config.request_timeout_secs,
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

        ``tag`` filters client-side; IDMC's v3 ``q`` parameter rejects
        ``tag==`` server-side (HTTP 400).
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
                # Record both the query type and the actual documentType so
                # types returned under a different wrapper (e.g. MAPPLET
                # served via DTEMPLATE) still surface in report counts.
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

    def list_mapping_tasks(self) -> Iterator[IdmcMappingTask]:
        """Iterate mapping tasks via the v3 objects API (type=MTT).

        Each v3 MTT row is enriched with a v2 ``/api/v2/mttask/name/<name>``
        lookup to pull in ``mappingId`` / ``mappingName`` / ``connectionId`` —
        the v3 listing doesn't include the mapping reference.

        Add-On Bundle tasks are filtered out via ``IdmcObject.is_bundle``
        (shared with other listing paths so report counts are consistent).
        """
        for obj in self.list_objects("MTT"):
            if obj.is_bundle():
                self.report.report_filtered("bundle", "MTT", obj.path)
                logger.debug("Skipping bundle MTT: id=%s path=%s", obj.id, obj.path)
                continue
            mt = IdmcMappingTask.from_idmc_object(obj)
            self._enrich_mapping_task_with_v2_details(mt)
            yield mt

    def _enrich_mapping_task_with_v2_details(self, mt: IdmcMappingTask) -> None:
        """Populate ``mapping_id`` / ``mapping_name`` / ``connection_id`` via v2."""
        if not mt.name:
            return
        try:
            data = self._get_v2(f"/api/v2/mttask/name/{mt.name}")
        except (InformaticaApiError, requests.HTTPError) as e:
            self.report.warning(
                title="Failed to enrich IDMC mapping task with v2 details",
                message=(
                    "mappingId / mappingName / connectionId will be missing for "
                    "this task; the task→mapping link will not be emitted."
                ),
                context=f"name={mt.name!r} v3_id={mt.v2_id!r}",
                exc=e,
            )
            return
        if isinstance(data, dict):
            mt.merge_v2_details(data)

    # Taskflow step shape varies by CDI version; probe known URLs in order
    # and use the first that parses. ``/api/v2/workflow/`` is the legacy
    # PowerCenter path — 403 on most modern tenants but kept for old pods.
    _TASKFLOW_DEFINITION_ENDPOINTS = (
        "/public/core/v3/objects/{guid}",
        "/api/v2/taskflow/name/{name}",
        "/api/v2/workflow/name/{name}",
    )

    def get_taskflow_definition(
        self,
        taskflow_name: str,
        taskflow_v3_guid: str = "",
    ) -> Optional[TaskflowDefinition]:
        """Resolve a Taskflow's step DAG.

        Tries cheap REST endpoints first; on failure falls back to the
        authoritative v3 Export API, which returns the full IDMC
        taskflowModel XML.
        """
        if not taskflow_name and not taskflow_v3_guid:
            return None

        if taskflow_v3_guid and taskflow_v3_guid in self._taskflow_definition_cache:
            return self._taskflow_definition_cache[taskflow_v3_guid]

        for template in self._TASKFLOW_DEFINITION_ENDPOINTS:
            path = template.format(guid=taskflow_v3_guid, name=taskflow_name)
            try:
                data = (
                    self._get_v3(path)
                    if path.startswith("/public/core/v3")
                    else self._get_v2(path)
                )
            except (InformaticaApiError, requests.HTTPError) as e:
                logger.debug("Taskflow endpoint %s errored: %s", path, e)
                continue
            if not isinstance(data, dict):
                continue
            parsed = parse_taskflow_definition(data)
            if parsed and parsed.steps:
                return parsed

        # REST endpoints fail on modern CDI pods; fall back to a one-off v3
        # export. Callers that know the full GUID set should use
        # ``prefetch_taskflow_definitions`` (single batched job).
        if taskflow_v3_guid:
            definitions = self._download_taskflow_export_definitions([taskflow_v3_guid])
            if taskflow_v3_guid in definitions:
                parsed = definitions[taskflow_v3_guid]
                self._taskflow_definition_cache[taskflow_v3_guid] = parsed
                return parsed if parsed.steps else None
        return None

    def prefetch_taskflow_definitions(self, taskflow_v3_guids: Iterable[str]) -> None:
        """Batch-export N Taskflows in a single job and cache the parsed DAGs.

        Idempotent — already-cached GUIDs are skipped.
        """
        guids = [
            g
            for g in taskflow_v3_guids
            if g and g not in self._taskflow_definition_cache
        ]
        if not guids:
            return
        definitions = self._download_taskflow_export_definitions(guids)
        self._taskflow_definition_cache.update(definitions)
        logger.info(
            "Prefetched %d/%d Taskflow definitions via batched v3 export",
            len(definitions),
            len(guids),
        )

    def _download_taskflow_export_definitions(
        self, guids: List[str]
    ) -> Dict[str, TaskflowDefinition]:
        """Export ``guids`` as one job, download the package, parse every
        ``.TASKFLOW.xml`` entry. Empty dict on any failure so callers
        degrade gracefully; failures are surfaced as report warnings.
        """
        try:
            job_id = self.submit_export_job(guids)
        except (InformaticaApiError, requests.HTTPError) as e:
            self.report.warning(
                title="IDMC Taskflow batch export submit failed",
                message=(
                    "Taskflow step DAGs will be missing for this batch. "
                    "Common cause: service account lacks 'Asset - export' "
                    "privilege. Taskflows will still be emitted as "
                    "DataFlow-only entities."
                ),
                context=f"{len(guids)} taskflow(s)",
                exc=e,
            )
            return {}
        status = self.wait_for_export(job_id)
        if status.state != ExportJobState.SUCCESSFUL:
            # TIMEOUT is already reported inside wait_for_export; other
            # non-success states get a warning with IDMC's status.message.
            if status.state != ExportJobState.TIMEOUT:
                self.report.warning(
                    title="IDMC Taskflow batch export did not succeed",
                    message=(
                        f"Export ended in state {status.state}. "
                        "Taskflow step DAGs will be missing for this batch."
                    ),
                    context=(f"job_id={job_id} message={status.message or '<none>'}"),
                )
            return {}
        try:
            resp = self._request(
                "GET",
                f"{self._base_url}/public/core/v3/export/{job_id}/package",
                is_v3=True,
                stream=True,
            )
            resp.raise_for_status()
        except (InformaticaApiError, requests.HTTPError) as e:
            self.report.warning(
                title="IDMC Taskflow export package download failed",
                message="Taskflow step DAGs will be missing for this batch.",
                context=f"job_id={job_id}",
                exc=e,
            )
            return {}
        return _parse_taskflow_export_package(resp.iter_content, report=self.report)

    def submit_export_job(self, object_ids: Sequence[str]) -> str:
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
            try:
                status = self.poll_export_job(job_id)
            except (InformaticaApiError, requests.HTTPError) as e:
                logger.warning("Transient error polling export job %s: %s", job_id, e)
                time.sleep(
                    self.config.export_poll_interval_secs + random.uniform(-1, 1)
                )
                continue
            if status.state == ExportJobState.SUCCESSFUL:
                return status
            if status.state not in (ExportJobState.IN_PROGRESS, ExportJobState.QUEUED):
                self.report.report_export_failed(job_id, status.message)
                return status
            time.sleep(self.config.export_poll_interval_secs + random.uniform(-1, 1))
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
        submitted_ids: Sequence[str],
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
            total_bytes = 0
            for chunk in resp.iter_content(chunk_size=8192):
                total_bytes += len(chunk)
                if total_bytes > _MAX_OUTER_ZIP_BYTES:
                    self.report.warning(
                        title="IDMC export package exceeds size limit",
                        message=f"Download aborted after {_MAX_OUTER_ZIP_BYTES} bytes. Lineage for this batch will be missing.",
                        context=f"job_id={job_id}",
                    )
                    return
                tmp.write(chunk)
            tmp.flush()
            tmp.seek(0)
            try:
                outer_zip_ctx = zipfile.ZipFile(tmp)
            except zipfile.BadZipFile as e:
                self.report.warning(
                    title="IDMC export package is not a valid ZIP",
                    message="Lineage for this batch will be missing.",
                    context=f"job_id={job_id}",
                    exc=e,
                )
                self.report.report_export_failed(job_id, "Invalid ZIP file")
                return
            with outer_zip_ctx as outer_zip:
                dtemplate_entries = [
                    n for n in outer_zip.namelist() if n.endswith(".DTEMPLATE.zip")
                ]
                assigned_ids: set[str] = set()
                for idx, entry_name in enumerate(dtemplate_entries):
                    mapping_id = self._resolve_entry_mapping_id(
                        entry_name, submitted_ids, idx, assigned_ids
                    )
                    if mapping_id:
                        assigned_ids.add(mapping_id)
                    lineage = self._parse_dtemplate_entry(
                        outer_zip, entry_name, mapping_id, job_id
                    )
                    if lineage is not None:
                        yield lineage

    def _parse_dtemplate_entry(
        self,
        outer_zip: zipfile.ZipFile,
        entry_name: str,
        mapping_id: str,
        job_id: str,
    ) -> Optional[MappingLineageInfo]:
        """Parse one ``*.DTEMPLATE.zip`` entry with zip-bomb + malformed
        payload guards. Returns ``None`` on failure so the caller's loop
        over sibling entries continues; failures are surfaced via
        ``report.warning`` / ``report_object_failed``.
        """
        try:
            info = outer_zip.getinfo(entry_name)
            if info.file_size > _MAX_INNER_ZIP_BYTES:
                self.report.warning(
                    title="IDMC export entry exceeds zip-bomb guard",
                    message="Skipping suspiciously large mapping export entry.",
                    context=(
                        f"job_id={job_id}, entry={entry_name}, "
                        f"decompressed_size={info.file_size}"
                    ),
                )
                self.report.report_object_failed(
                    entry_name, f"exceeds {_MAX_INNER_ZIP_BYTES} bytes"
                )
                return None
            with zipfile.ZipFile(io.BytesIO(outer_zip.read(entry_name))) as inner_zip:
                return self._parse_inner_dtemplate_zip(
                    inner_zip, entry_name, mapping_id
                )
        except (
            zipfile.BadZipFile,
            UnicodeDecodeError,
            json.JSONDecodeError,
            KeyError,
            ValueError,
        ) as e:
            self.report.warning(
                title="Failed to parse IDMC mapping export entry",
                message="Lineage for this mapping will be missing.",
                context=f"job_id={job_id}, entry={entry_name}",
                exc=e,
            )
            self.report.report_object_failed(entry_name, str(e))
            return None

    @staticmethod
    def _resolve_entry_mapping_id(
        entry_name: str,
        submitted_ids: Sequence[str],
        index: int,
        assigned_ids: Optional[set[str]] = None,
    ) -> str:
        """Match an export entry to its submitted mapping id.

        Prefer a substring match against the entry name (more robust against
        server-side ordering changes); fall back to positional alignment.
        ``assigned_ids`` dedups substring matches across entries so a single
        id cannot be reused when another entry also contains its substring —
        and skips the positional fallback if the aligned id has already been
        claimed by an earlier entry.
        """
        already_assigned = assigned_ids or set()
        for sid in submitted_ids:
            if sid and sid in entry_name and sid not in already_assigned:
                return sid
        if 0 <= index < len(submitted_ids):
            positional = submitted_ids[index]
            if positional not in already_assigned:
                return positional
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
            if tx_class == TxClass.SOURCE or "source" in tx_name:
                sources.append(lt)
            elif tx_class == TxClass.TARGET or "target" in tx_name:
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
    # IDMC uses ``saas:@{federatedId}`` for SaaS connection refs.
    return conn_id.split("@", 1)[-1] if "@" in conn_id else conn_id


# Field-name fallbacks — IDMC's Taskflow JSON shape varies across API versions.
_TASKFLOW_STEP_ARRAY_KEYS = ("steps", "stepList", "tfSteps", "taskflowSteps")
_TASKFLOW_STEP_ID_KEYS = ("id", "stepId", "name")
_TASKFLOW_STEP_NAME_KEYS = ("displayName", "stepName", "name")
_TASKFLOW_STEP_TYPE_KEYS = ("type", "stepType", "kind", "@type")
_TASKFLOW_STEP_TASK_TYPE_KEYS = ("taskType", "dataTaskType", "mttType")
_TASKFLOW_STEP_TASK_NAME_KEYS = ("taskName", "mttaskName", "mappingTaskName")
_TASKFLOW_STEP_TASK_ID_KEYS = ("taskId", "mttaskId", "mappingTaskId")
_TASKFLOW_STEP_NEXT_KEYS = ("nextSteps", "next", "onSuccess", "successors")
_TASKFLOW_STEP_PREV_KEYS = ("predecessors", "prev", "dependsOn")


def _first_present(data: Dict[str, Any], keys: Iterable[str]) -> Any:
    for k in keys:
        if k in data and data[k] is not None:
            return data[k]
    return None


def _resolve_successor_id(succ: Any) -> Optional[str]:
    """Extract a step id from a ``nextSteps`` entry.

    IDMC's Taskflow JSON varies across API versions: ``nextSteps`` can
    be a list of bare step-id strings or a list of ``{"id": "..."}``
    dicts. Return ``None`` for anything else (unknown shape).
    """
    if isinstance(succ, str):
        return succ
    if isinstance(succ, dict):
        succ_id = _first_present(succ, _TASKFLOW_STEP_ID_KEYS)
        return str(succ_id) if succ_id else None
    return None


def parse_taskflow_definition(data: Dict[str, Any]) -> Optional[TaskflowDefinition]:
    """Convert a Taskflow JSON payload into a :class:`TaskflowDefinition`.

    Returns ``None`` if the top-level shape doesn't contain a step array.
    Predecessors: use explicit ``predecessors`` if present, otherwise
    invert ``nextSteps`` edges (A.nextSteps=[B] → B.predecessors+=A).
    """
    steps_raw = _first_present(data, _TASKFLOW_STEP_ARRAY_KEYS)
    if not isinstance(steps_raw, list) or not steps_raw:
        return None

    steps: List[TaskflowStep] = []
    successors: Dict[str, List[str]] = {}
    for raw in steps_raw:
        if not isinstance(raw, dict):
            continue
        step_id = _first_present(raw, _TASKFLOW_STEP_ID_KEYS) or ""
        if not step_id:
            continue
        step_name = _first_present(raw, _TASKFLOW_STEP_NAME_KEYS) or step_id
        step_type = _normalize_step_type(
            (_first_present(raw, _TASKFLOW_STEP_TYPE_KEYS) or "").lower()
        )
        predecessors_raw = _first_present(raw, _TASKFLOW_STEP_PREV_KEYS) or []
        next_raw = _first_present(raw, _TASKFLOW_STEP_NEXT_KEYS) or []
        if isinstance(next_raw, list):
            for succ in next_raw:
                resolved = _resolve_successor_id(succ)
                if resolved:
                    successors.setdefault(step_id, []).append(resolved)
        steps.append(
            TaskflowStep(
                step_id=str(step_id),
                step_name=str(step_name),
                step_type=step_type,
                task_type=_first_present(raw, _TASKFLOW_STEP_TASK_TYPE_KEYS),
                task_ref_name=_first_present(raw, _TASKFLOW_STEP_TASK_NAME_KEYS),
                task_ref_id=_first_present(raw, _TASKFLOW_STEP_TASK_ID_KEYS),
                predecessor_step_ids=(
                    [str(p) for p in predecessors_raw if isinstance(p, (str, int))]
                ),
            )
        )

    # Fall back to inverting ``nextSteps`` when explicit predecessors
    # are missing.
    if successors:
        by_id = {s.step_id: s for s in steps}
        for src_id, dsts in successors.items():
            for dst_id in dsts:
                dst = by_id.get(dst_id)
                if dst is not None and src_id not in dst.predecessor_step_ids:
                    dst.predecessor_step_ids.append(src_id)

    tf_id = data.get("id") or data.get("taskflowId") or ""
    tf_name = data.get("name") or data.get("taskflowName") or ""
    return TaskflowDefinition(
        taskflow_id=str(tf_id),
        taskflow_name=str(tf_name),
        steps=steps,
    )


# Control-flow markers inside ``<flow>``; their ``<link>`` children still
# contribute to successor edges but the markers themselves aren't steps.
_TASKFLOW_MARKER_TAGS = {"start", "end"}


def _local_tag(elem: ET.Element) -> str:
    return elem.tag.split("}", 1)[-1] if "}" in elem.tag else elem.tag


def _first_child(elem: ET.Element, local_name: str) -> Optional[ET.Element]:
    for child in elem:
        if _local_tag(child) == local_name:
            return child
    return None


_KNOWN_STEP_TYPES: frozenset = frozenset(
    {"data", "command", "decision", "assignment", "notification", "subtaskflow"}
)


def _normalize_step_type(raw: str) -> TaskflowStepType:
    # Anything not in the closed set becomes "unknown".
    if raw in _KNOWN_STEP_TYPES:
        return raw  # type: ignore[return-value]
    return "unknown"


def _classify_step_type(service_name: str, fallback: str) -> TaskflowStepType:
    """Map an IDMC ``<service><serviceName>`` to a closed step-type label."""
    name = service_name or ""
    if "ExecuteDataTask" in name or "RunTask" in name:
        return "data"
    if "Command" in name:
        return "command"
    if "Notification" in name or "Email" in name:
        return "notification"
    if "Decision" in name:
        return "decision"
    if "Assignment" in name:
        return "assignment"
    if "Subtaskflow" in name or "SubTaskFlow" in name:
        return "subtaskflow"
    if fallback in _KNOWN_STEP_TYPES:
        return fallback  # type: ignore[return-value]
    return "unknown"


class TaskflowStepServiceData(TypedDict):
    """Structured result of parsing a Taskflow ``<service>`` XML element.

    Fields are all Optional since IDMC's XML omits absent attributes.
    """

    step_name: Optional[str]
    step_type: Optional[str]
    task_type: Optional[str]
    task_ref_name: Optional[str]
    task_ref_id: Optional[str]


def _parse_taskflow_step_service(
    service: ET.Element, fallback_type: TaskflowStepType
) -> TaskflowStepServiceData:
    """Extract step name + type + MT references from a ``<service>`` element."""
    result: TaskflowStepServiceData = {
        "step_name": None,
        "step_type": fallback_type,
        "task_type": None,
        "task_ref_name": None,
        "task_ref_id": None,
    }
    title = _first_child(service, "title")
    if title is not None and title.text:
        result["step_name"] = title.text.strip()
    svc_name_elem = _first_child(service, "serviceName")
    svc_name = (
        svc_name_elem.text.strip()
        if svc_name_elem is not None and svc_name_elem.text
        else ""
    )
    result["step_type"] = _classify_step_type(svc_name, fallback=fallback_type)
    svc_input = _first_child(service, "serviceInput")
    if svc_input is not None:
        for param in svc_input:
            if _local_tag(param) != "parameter":
                continue
            pname = param.get("name", "")
            pvalue = (param.text or "").strip()
            if not pvalue:
                continue
            if pname == "Task Name":
                result["task_ref_name"] = pvalue
            elif pname == "GUID":
                result["task_ref_id"] = pvalue
            elif pname == "Task Type":
                result["task_type"] = pvalue
    return result


def _collect_taskflow_edges(
    flow_elem: ET.Element,
) -> Dict[str, List[str]]:
    """Collect outgoing ``<link targetId="..."/>`` edges for each node in a flow."""
    successors: Dict[str, List[str]] = {}
    for node in flow_elem:
        node_id = node.get("id", "")
        if not node_id:
            continue
        for child in node:
            if _local_tag(child) == "link":
                target = child.get("targetId")
                if target:
                    successors.setdefault(node_id, []).append(target)
    return successors


def _xml_node_to_step(node: ET.Element) -> Optional[TaskflowStep]:
    """Convert one ``<flow>`` child into a :class:`TaskflowStep`.

    Returns ``None`` for control-flow markers (``<start>``, ``<end>``)
    or nodes without an ``id``.
    """
    tag = _local_tag(node)
    node_id = node.get("id", "")
    if not node_id or tag in _TASKFLOW_MARKER_TAGS:
        return None
    fallback_type: TaskflowStepType = "data" if tag == "eventContainer" else "unknown"
    service = _first_child(node, "service")
    if service is not None:
        svc = _parse_taskflow_step_service(service, fallback_type=fallback_type)
    else:
        svc = TaskflowStepServiceData(
            step_name=None,
            step_type=fallback_type,
            task_type=None,
            task_ref_name=None,
            task_ref_id=None,
        )
    step_type_value = svc["step_type"] or fallback_type
    return TaskflowStep(
        step_id=node_id,
        step_name=svc["step_name"] or node_id,
        step_type=_normalize_step_type(str(step_type_value)),
        task_type=svc["task_type"],
        task_ref_name=svc["task_ref_name"],
        task_ref_id=svc["task_ref_id"],
    )


def parse_taskflow_xml(xml_bytes: bytes) -> Optional[TaskflowDefinition]:
    """Parse IDMC's v3-exported ``.TASKFLOW.xml`` into a :class:`TaskflowDefinition`.

    Structure expected (namespaces elided):

        <aetgt:getResponse>
          <types1:Item>
            <types1:Entry>
              <taskflow GUID="..." name="...">
                <flow id="...">
                  <start id="b"><link targetId="..."/></start>
                  <eventContainer id="...">
                    <service>
                      <title>Data Task 1</title>
                      <serviceName>ICSExecuteDataTask</serviceName>
                      <serviceInput>
                        <parameter name="Task Name">MappingTask3</parameter>
                        <parameter name="GUID">3WD6PD2FNhRbiv93ryBMM2</parameter>
                        <parameter name="Task Type">MCT</parameter>
                      </serviceInput>
                    </service>
                    <link targetId="..."/>
                  </eventContainer>
                  <end id="c"/>
                </flow>
              </taskflow>
            </types1:Entry>
          </types1:Item>
        </aetgt:getResponse>

    ``<start>`` / ``<end>`` are skipped (flow markers, not emitted as steps).
    Each ``<link targetId="X"/>`` child of a node is an outgoing edge; we
    invert them into :attr:`TaskflowStep.predecessor_step_ids`.
    """
    try:
        # ``defusedxml`` blocks billion-laughs / external-entity / DTD
        # attacks; returns a stdlib ``ET.Element`` on success.
        root = defusedxml.ElementTree.fromstring(xml_bytes)
    except ET.ParseError as e:
        logger.debug("Taskflow XML parse error: %s", e)
        return None
    except DefusedXmlException as e:
        logger.warning(
            "Rejected potentially malicious Taskflow XML "
            "(entity expansion or external reference): %s",
            e,
        )
        return None

    tf_elem: Optional[ET.Element] = None
    for elem in root.iter():
        if _local_tag(elem) == "taskflow":
            tf_elem = elem
            break
    if tf_elem is None:
        return None

    tf_guid = tf_elem.get("GUID", "")
    tf_name = tf_elem.get("name") or tf_elem.get("displayName") or ""

    flow_elem = _first_child(tf_elem, "flow")
    if flow_elem is None:
        return TaskflowDefinition(taskflow_id=tf_guid, taskflow_name=tf_name, steps=[])

    successors = _collect_taskflow_edges(flow_elem)
    steps: List[TaskflowStep] = []
    for node in flow_elem:
        step = _xml_node_to_step(node)
        if step is not None:
            steps.append(step)

    # Invert successors → predecessors; skip control-flow marker nodes.
    by_id = {s.step_id: s for s in steps}
    for src_id, dsts in successors.items():
        if src_id not in by_id:
            continue
        for dst_id in dsts:
            dst = by_id.get(dst_id)
            if dst is not None and src_id not in dst.predecessor_step_ids:
                dst.predecessor_step_ids.append(src_id)

    return TaskflowDefinition(taskflow_id=tf_guid, taskflow_name=tf_name, steps=steps)


def _parse_taskflow_export_package(
    iter_content: Callable[..., Iterator[bytes]],
    report: Optional[InformaticaSourceReport] = None,
) -> Dict[str, TaskflowDefinition]:
    """Stream an export package ZIP to a temp file and parse every
    ``.TASKFLOW.xml`` entry.

    ``report`` is optional so unit tests can skip the report wiring.
    """
    definitions: Dict[str, TaskflowDefinition] = {}
    with tempfile.NamedTemporaryFile(suffix=".zip") as tmp:
        total_bytes = 0
        for chunk in iter_content(chunk_size=8192):
            total_bytes += len(chunk)
            if total_bytes > _MAX_TASKFLOW_ZIP_BYTES:
                if report is not None:
                    report.warning(
                        title="IDMC Taskflow export package exceeds size limit",
                        message=f"Download aborted after {_MAX_TASKFLOW_ZIP_BYTES} bytes. Taskflow step DAGs in this batch will be missing.",
                    )
                return definitions
            tmp.write(chunk)
        tmp.flush()
        tmp.seek(0)
        try:
            zf_ctx = zipfile.ZipFile(tmp)
        except zipfile.BadZipFile as e:
            logger.debug("Taskflow export package is not a valid ZIP: %s", e)
            if report is not None:
                report.warning(
                    title="IDMC Taskflow export package is not a valid ZIP",
                    message="All Taskflow step DAGs in this batch will be missing.",
                    context=str(e),
                )
            return definitions
        with zf_ctx as zf:
            parse_failures = 0
            for name in zf.namelist():
                if not name.endswith(".TASKFLOW.xml"):
                    continue
                try:
                    xml_bytes = zf.read(name)
                except KeyError:
                    continue
                parsed = parse_taskflow_xml(xml_bytes)
                if parsed is not None and parsed.taskflow_id:
                    definitions[parsed.taskflow_id] = parsed
                else:
                    parse_failures += 1
            if parse_failures and report is not None:
                report.warning(
                    title="IDMC Taskflow XML parse failed for some entries",
                    message=(
                        f"{parse_failures} Taskflow .TASKFLOW.xml entr(y/ies) "
                        "could not be parsed; step DAGs will be missing. "
                        "This usually indicates IDMC schema drift."
                    ),
                    context=f"parse_failures={parse_failures}",
                )
    return definitions


def _encode_params(params: Dict[str, Any]) -> str:
    # Used only for readable report-log URLs, not actual HTTP. ``safe``
    # keeps ``q=type=='X'`` readable instead of percent-escaped.
    return urlencode(params, safe="=',")
