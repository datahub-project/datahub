import logging
import time
from typing import Any, Dict, Iterable, List, Optional, Set, Type, TypeVar

import requests
from pydantic import BaseModel, ValidationError

from datahub.ingestion.source.microstrategy.config import (
    MicroStrategyConfig,
    MicroStrategyGuestAuth,
    MicroStrategyPasswordAuth,
)
from datahub.ingestion.source.microstrategy.constants import (
    MSTR_LOGIN_MODE_GUEST,
    MSTR_LOGIN_MODE_STANDARD,
    MSTR_OBJECT_TYPE_DASHBOARD,
    MSTR_OBJECT_TYPE_REPORT,
)
from datahub.ingestion.source.microstrategy.models import (
    Datasource,
    DatasourceConnection,
    MSTRObject,
    Project,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport

logger = logging.getLogger(__name__)

_ModelT = TypeVar("_ModelT", bound=BaseModel)

# Retry only genuinely transient statuses; other 4xx errors fail fast so bad
# credentials or missing objects are not re-hammered max_retries times.
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Cap backoff so a hostile Retry-After header or large max_retries cannot
# stall ingestion for minutes per request.
_MAX_RETRY_DELAY_SECONDS = 60


class MicroStrategyAPIError(RuntimeError):
    pass


class MicroStrategyAuthError(RuntimeError):
    """Authentication was lost mid-run and could not be re-established.

    Deliberately not a MicroStrategyAPIError subclass: per-object error
    boundaries swallow API errors to keep the run going, but once auth is
    irrecoverably dead every remaining call would fail, so this must
    propagate to the top-level project loop and abort the run.
    """


class MicroStrategyClient:
    def __init__(self, config: MicroStrategyConfig, report: MicroStrategyReport):
        self.config = config
        self.report = report
        self.base_url = config.base_url
        self._auth_dead = False
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    def login(self) -> None:
        auth = self.config.auth
        payload: Dict[str, Any]
        if isinstance(auth, MicroStrategyGuestAuth):
            payload = {"loginMode": MSTR_LOGIN_MODE_GUEST}
        elif isinstance(auth, MicroStrategyPasswordAuth):
            payload = {
                "loginMode": MSTR_LOGIN_MODE_STANDARD,
                "username": auth.username,
                "password": auth.password.get_secret_value(),
            }
        else:
            raise MicroStrategyAPIError(f"Unsupported auth config {type(auth)}")

        response = self._request("POST", "/api/auth/login", json=payload)
        token = response.headers.get("X-MSTR-AuthToken")
        if not token:
            raise MicroStrategyAPIError("MicroStrategy login did not return auth token")
        self.session.headers.update({"X-MSTR-AuthToken": token})

    def close(self) -> None:
        try:
            self._request("POST", "/api/auth/logout")
        except Exception:
            logger.debug("MicroStrategy logout failed", exc_info=True)
        self.session.close()

    def _parse_model(
        self,
        model_cls: Type[_ModelT],
        item: Dict[str, Any],
        context: str,
    ) -> Optional[_ModelT]:
        """Validate one API object, reporting and skipping malformed items so a
        single bad object cannot abort the whole ingestion run."""
        try:
            return model_cls.model_validate(item)
        except ValidationError as error:
            self.report.report_malformed_object(
                f"{context}: {model_cls.__name__} id={item.get('id') or item.get('objectId')!r}"
            )
            self.report.warning(
                title="Skipped malformed MicroStrategy object",
                message="An API object did not match the expected shape and was skipped.",
                context=f"{context}, model={model_cls.__name__}, keys={sorted(item)[:10]}",
                exc=error,
            )
            return None

    def _parse_models(
        self,
        model_cls: Type[_ModelT],
        items: Iterable[Any],
        context: str,
    ) -> List[_ModelT]:
        parsed = [
            self._parse_model(model_cls, item, context)
            for item in items
            if isinstance(item, dict)
        ]
        return [item for item in parsed if item is not None]

    def list_projects(self) -> List[Project]:
        payload = self._get_json("/api/projects")
        projects: Any
        if isinstance(payload, list):
            projects = payload
        elif isinstance(payload, dict):
            projects = payload.get("projects") or payload.get("result") or []
        else:
            projects = []
        if not projects:
            self._warn_if_unrecognized_shape(
                payload, "/api/projects", recognized_keys={"projects", "result"}
            )
        return self._parse_models(Project, projects, "GET /api/projects")

    def list_datasources(self, project_id: str) -> List[Datasource]:
        path = "/api/datasources"
        payload = self._get_json(path, project_id=project_id)
        datasources = self._extract_list(payload, "datasources")
        if not datasources:
            self._warn_if_unrecognized_shape(
                payload, path, recognized_keys={"datasources", "result", "items"}
            )
        return self._parse_models(
            Datasource, datasources, f"GET {path} project_id={project_id}"
        )

    def list_project_datasources(self, project_id: str) -> List[Datasource]:
        path = f"/api/projects/{project_id}/datasources"
        payload = self._get_json(path, project_id=project_id)
        datasources = self._extract_list(payload, "datasources")
        if not datasources:
            self._warn_if_unrecognized_shape(
                payload, path, recognized_keys={"datasources", "result", "items"}
            )
        return self._parse_models(Datasource, datasources, f"GET {path}")

    def get_datasource_connection(
        self, connection_id: str, project_id: Optional[str] = None
    ) -> DatasourceConnection:
        payload = self._get_json(
            f"/api/datasources/connections/{connection_id}",
            project_id=project_id,
        )
        result = payload.get("result") if isinstance(payload, dict) else None
        try:
            return DatasourceConnection.model_validate(
                result if isinstance(result, dict) else payload
            )
        except ValidationError as error:
            raise MicroStrategyAPIError(
                f"MicroStrategy datasource connection {connection_id} response did "
                "not match the expected shape"
            ) from error

    def search_dashboards(self, project_id: str) -> Iterable[MSTRObject]:
        yield from self._search_typed_objects(
            project_id, MSTR_OBJECT_TYPE_DASHBOARD, "dashboard search"
        )

    def search_reports(self, project_id: str) -> Iterable[MSTRObject]:
        yield from self._search_typed_objects(
            project_id, MSTR_OBJECT_TYPE_REPORT, "report search"
        )

    def get_report_object(
        self,
        project_id: str,
        report_id: str,
    ) -> Optional[MSTRObject]:
        """Object info for one report, including the folder ancestors array.

        Lets the report pass fetch dashboard-linked reports directly by id
        instead of paginating the project's entire report library.
        """
        payload = self._get_json(
            f"/api/objects/{report_id}",
            project_id=project_id,
            params={"type": MSTR_OBJECT_TYPE_REPORT},
        )
        item = payload
        if "id" not in item and isinstance(item.get("result"), dict):
            item = item["result"]
        if not item:
            self._warn_if_unrecognized_shape(
                payload, f"/api/objects/{report_id}", recognized_keys={"id", "result"}
            )
            return None
        return self._parse_model(
            MSTRObject,
            item,
            f"report object info project_id={project_id}, report_id={report_id}",
        )

    def _search_typed_objects(
        self,
        project_id: str,
        object_type: int,
        context: str,
    ) -> Iterable[MSTRObject]:
        for item in self._metadata_search(
            project_id=project_id,
            type_filter=str(object_type),
        ):
            parsed = self._parse_model(
                MSTRObject, item, f"{context} project_id={project_id}"
            )
            if parsed is not None:
                yield parsed

    def get_object_dependencies(
        self,
        project_id: str,
        object_id: str,
        object_type: str,
    ) -> List[MSTRObject]:
        create_response = self._get_json(
            "/api/metadataSearches/results",
            project_id=project_id,
            params={
                "domain": 2,
                "usedByObject": f"{object_id};{object_type}",
                "usedByRecursive": "false",
            },
            method="POST",
        )
        search_id = self._extract_search_id(create_response)
        if not search_id:
            raise MicroStrategyAPIError(
                "MicroStrategy metadata search response did not include search id "
                f"for object_id={object_id}, object_type={object_type}"
            )
        result = self._get_json(
            "/api/metadataSearches/results",
            project_id=project_id,
            params={"searchId": search_id, "offset": 0, "limit": -1},
        )
        return self._parse_models(
            MSTRObject,
            self._extract_search_results(result),
            f"object dependencies object_id={object_id}",
        )

    def get_metric_model(self, project_id: str, metric_id: str) -> Dict[str, Any]:
        """Raw model JSON; shape varies by MicroStrategy version and is parsed
        by the lineage helpers."""
        return self._get_json(
            f"/api/model/metrics/{metric_id}",
            project_id=project_id,
            params={"showExpressionAs": "tokens"},
        )

    def get_model_document(self, project_id: str, document_id: str) -> Dict[str, Any]:
        """Raw modeling JSON for a document/dossier; exposes per-dataset derived
        metrics/attributes whose embedded object ids identify which dataset a
        visualization reads. Parsed by the lineage helpers."""
        return self._get_json(
            f"/api/model/documents/{document_id}",
            project_id=project_id,
        )

    def list_model_tables(
        self,
        project_id: str,
        limit: int = 1,
        offset: int = 0,
        fields: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if fields:
            params["fields"] = fields
        return self._get_json(
            "/api/model/tables",
            project_id=project_id,
            params=params,
        )

    def get_dossier_definition(
        self, project_id: str, dossier_id: str
    ) -> Dict[str, Any]:
        """Raw definition JSON; shape varies by MicroStrategy version and is
        parsed by DashboardDefinition.from_api_response."""
        return self._get_json(
            f"/api/v2/dossiers/{dossier_id}/definition",
            project_id=project_id,
        )

    def get_document_definition(
        self, project_id: str, document_id: str
    ) -> Dict[str, Any]:
        return self._get_json(
            f"/api/documents/{document_id}/definition",
            project_id=project_id,
        )

    def get_report_definition(self, project_id: str, report_id: str) -> Dict[str, Any]:
        return self._get_json(
            f"/api/v2/reports/{report_id}",
            project_id=project_id,
        )

    def create_dossier_instance(self, project_id: str, dossier_id: str) -> str:
        response = self._get_json(
            f"/api/dossiers/{dossier_id}/instances",
            project_id=project_id,
            method="POST",
            json={},
            timeout_seconds=self.config.warehouse_lineage_sql_timeout_seconds,
            # Instance execution and SQL-view calls are expensive (the server
            # runs the dashboard/report); retrying a 180s timeout multiplies
            # a slow report into 10+ silent minutes, so they get one attempt.
            max_attempts=1,
        )
        instance_id = self._extract_instance_id(response)
        if not instance_id:
            raise MicroStrategyAPIError(
                "MicroStrategy dossier instance response did not include "
                f"an instance id for {dossier_id}"
            )
        return instance_id

    def create_document_instance(self, project_id: str, document_id: str) -> str:
        response = self._get_json(
            f"/api/documents/{document_id}/instances",
            project_id=project_id,
            method="POST",
            json={},
            timeout_seconds=self.config.warehouse_lineage_sql_timeout_seconds,
            # Instance execution and SQL-view calls are expensive (the server
            # runs the dashboard/report); retrying a 180s timeout multiplies
            # a slow report into 10+ silent minutes, so they get one attempt.
            max_attempts=1,
        )
        instance_id = self._extract_instance_id(response)
        if not instance_id:
            raise MicroStrategyAPIError(
                "MicroStrategy document instance response did not include "
                f"an instance id for {document_id}"
            )
        return instance_id

    def create_report_instance(self, project_id: str, report_id: str) -> str:
        response = self._get_json(
            f"/api/v2/reports/{report_id}/instances",
            project_id=project_id,
            params={"executionStage": "resolve_prompts"},
            method="POST",
            json={},
            timeout_seconds=self.config.warehouse_lineage_sql_timeout_seconds,
            # Instance execution and SQL-view calls are expensive (the server
            # runs the dashboard/report); retrying a 180s timeout multiplies
            # a slow report into 10+ silent minutes, so they get one attempt.
            max_attempts=1,
        )
        instance_id = self._extract_instance_id(response)
        if not instance_id:
            raise MicroStrategyAPIError(
                "MicroStrategy report instance response did not include "
                f"an instance id for {report_id}"
            )
        return instance_id

    def get_dossier_datasets_sql(
        self,
        project_id: str,
        dossier_id: str,
        instance_id: str,
    ) -> List[Dict[str, Any]]:
        payload = self._get_json(
            f"/api/dossiers/{dossier_id}/instances/{instance_id}/datasets/sqlView",
            project_id=project_id,
            timeout_seconds=self.config.warehouse_lineage_sql_timeout_seconds,
            # Instance execution and SQL-view calls are expensive (the server
            # runs the dashboard/report); retrying a 180s timeout multiplies
            # a slow report into 10+ silent minutes, so they get one attempt.
            max_attempts=1,
        )
        datasets = self._extract_list(payload, "datasets")
        if datasets:
            return [dataset for dataset in datasets if isinstance(dataset, dict)]
        if isinstance(payload, dict) and any(
            key in payload for key in ("sql", "sqlStatement", "statement")
        ):
            return [payload]
        return []

    def get_report_sql_view(
        self,
        project_id: str,
        report_id: str,
        instance_id: str,
    ) -> Dict[str, Any]:
        return self._get_json(
            f"/api/v2/reports/{report_id}/instances/{instance_id}/sqlView",
            project_id=project_id,
            timeout_seconds=self.config.warehouse_lineage_sql_timeout_seconds,
            # Instance execution and SQL-view calls are expensive (the server
            # runs the dashboard/report); retrying a 180s timeout multiplies
            # a slow report into 10+ silent minutes, so they get one attempt.
            max_attempts=1,
        )

    def delete_dossier_instance(
        self,
        project_id: str,
        dossier_id: str,
        instance_id: str,
    ) -> bool:
        response = self._request(
            "DELETE",
            f"/api/dossiers/{dossier_id}/instances/{instance_id}",
            project_id=project_id,
            expected_statuses={200, 202, 204, 404, 405},
        )
        return response.status_code not in {404, 405}

    def delete_document_instance(
        self,
        project_id: str,
        document_id: str,
        instance_id: str,
    ) -> bool:
        response = self._request(
            "DELETE",
            f"/api/documents/{document_id}/instances/{instance_id}",
            project_id=project_id,
            expected_statuses={200, 202, 204, 404},
        )
        return response.status_code != 404

    def delete_report_instance(
        self,
        project_id: str,
        report_id: str,
        instance_id: str,
    ) -> bool:
        response = self._request(
            "DELETE",
            f"/api/v2/reports/{report_id}/instances/{instance_id}",
            project_id=project_id,
            expected_statuses={200, 202, 204, 404},
        )
        return response.status_code != 404

    def get_dossier_visualization(
        self,
        project_id: str,
        dossier_id: str,
        instance_id: str,
        chapter_key: str,
        visualization_key: str,
    ) -> Dict[str, Any]:
        return self._get_json(
            (
                f"/api/v2/dossiers/{dossier_id}/instances/{instance_id}"
                f"/chapters/{chapter_key}/visualizations/{visualization_key}"
            ),
            project_id=project_id,
        )

    def get_cube_definition(self, project_id: str, cube_id: str) -> Dict[str, Any]:
        """Raw cube definition JSON with availableObjects (attributes with
        forms, metrics); the usage extractor resolves objects by name from it."""
        return self._get_json(
            f"/api/v2/cubes/{cube_id}",
            project_id=project_id,
        )

    def execute_cube_query(
        self,
        project_id: str,
        cube_id: str,
        body: Dict[str, Any],
        limit: int,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Execute a cube query (requestedObjects + viewFilter) and return the
        first page of grid data."""
        return self._get_json(
            f"/api/v2/cubes/{cube_id}/instances",
            project_id=project_id,
            params={"limit": limit, "offset": offset},
            method="POST",
            json=body,
            timeout_seconds=self.config.usage_query_timeout_seconds,
            # Cube execution is server-side work; retrying a long timeout
            # multiplies a slow query into silent minutes, so one attempt.
            max_attempts=1,
        )

    def get_cube_instance_page(
        self,
        project_id: str,
        cube_id: str,
        instance_id: str,
        limit: int,
        offset: int,
    ) -> Dict[str, Any]:
        """Fetch a subsequent page of a previously executed cube query."""
        return self._get_json(
            f"/api/v2/cubes/{cube_id}/instances/{instance_id}",
            project_id=project_id,
            params={"limit": limit, "offset": offset},
            timeout_seconds=self.config.usage_query_timeout_seconds,
        )

    def search_objects_by_name(
        self,
        project_id: str,
        object_type: int,
        name: str,
    ) -> Iterable[Dict[str, Any]]:
        """Quick search filtered by object type and name pattern."""
        yield from self._metadata_search(
            project_id=project_id,
            type_filter=str(object_type),
            name=name,
        )

    def _metadata_search(
        self,
        project_id: str,
        type_filter: Optional[str] = None,
        name: Optional[str] = None,
    ) -> Iterable[Dict[str, Any]]:
        offset = 0
        total: Optional[int] = None
        while True:
            payload: Dict[str, Any] = {
                "limit": self.config.page_size,
                "offset": offset,
                "showHidden": self.config.include_hidden,
                # Ancestors carry the folder path used for folder containers.
                "getAncestors": True,
            }
            if type_filter:
                payload["type"] = type_filter
            if name:
                payload["name"] = name

            result = self._get_json(
                "/api/searches/results",
                project_id=project_id,
                params=payload,
            )
            items = self._extract_search_results(result)
            if total is None:
                total = self._extract_total(result)
            if not items:
                if offset == 0:
                    self._warn_if_unrecognized_shape(result, "/api/searches/results")
                break
            # Progress per page: large libraries take many slow searches and
            # the log stream must not go silent while paginating them.
            logger.info(
                "Metadata search progress: %d/%s objects (type=%s, project=%s)",
                offset + len(items),
                total if total is not None else "?",
                type_filter,
                project_id,
            )
            yield from items
            offset += len(items)
            if total is not None:
                if offset >= total:
                    break
            elif len(items) < self.config.page_size:
                # Without a total count, a short page is the only end-of-results
                # signal; servers that cap page size below the requested limit
                # should be reporting totalItems.
                break

    def _warn_if_unrecognized_shape(
        self,
        response: Any,
        path: str,
        recognized_keys: Optional[Set[str]] = None,
    ) -> None:
        """Surface response-shape drift instead of silently returning nothing."""
        if not isinstance(response, dict) or not response:
            return
        recognized = recognized_keys or {
            "result",
            "results",
            "objects",
            "items",
            "totalItems",
            "totalCount",
        }
        if recognized.isdisjoint(response):
            self.report.warning(
                title="Unrecognized MicroStrategy API response shape",
                message=(
                    "The API returned a non-empty payload with no recognized result "
                    "keys; entities may be silently missing from ingestion."
                ),
                context=f"path={path}, keys={sorted(response)[:10]}",
            )

    @staticmethod
    def _extract_total(response: Any) -> Optional[int]:
        if not isinstance(response, dict):
            return None
        for source in (response, response.get("result")):
            if not isinstance(source, dict):
                continue
            for key in ("totalItems", "totalCount"):
                value = source.get(key)
                if isinstance(value, int) and value >= 0:
                    return value
        return None

    @staticmethod
    def _extract_search_results(response: Any) -> List[Dict[str, Any]]:
        if isinstance(response, list):
            return [item for item in response if isinstance(item, dict)]
        if not isinstance(response, dict):
            return []
        for key in ("result", "results", "objects", "items"):
            value = response.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
            if isinstance(value, dict):
                nested = value.get("items") or value.get("objects")
                if isinstance(nested, list):
                    return [item for item in nested if isinstance(item, dict)]
        return []

    @staticmethod
    def _extract_search_id(response: Any) -> Optional[str]:
        if not isinstance(response, dict):
            return None
        for key in ("id", "searchId"):
            value = response.get(key)
            if isinstance(value, str) and value:
                return value
        result = response.get("result")
        if isinstance(result, dict):
            return MicroStrategyClient._extract_search_id(result)
        return None

    @staticmethod
    def _extract_list(response: Any, key: str) -> List[Any]:
        if isinstance(response, list):
            return response
        if not isinstance(response, dict):
            return []
        value = response.get(key)
        if isinstance(value, list):
            return value
        result = response.get("result")
        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            nested = result.get(key) or result.get("items")
            if isinstance(nested, list):
                return nested
        items = response.get("items")
        if isinstance(items, list):
            return items
        return []

    def _get_json(
        self,
        path: str,
        project_id: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET",
        json: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
        max_attempts: Optional[int] = None,
    ) -> Dict[str, Any]:
        response = self._request(
            method,
            path,
            project_id=project_id,
            params=params,
            json=json,
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
        )
        if not response.content:
            return {}
        try:
            value = response.json()
        except ValueError as error:
            self.report.report_api_error()
            raise MicroStrategyAPIError(
                "MicroStrategy API returned a non-JSON response: "
                f"{method} {path} status={response.status_code}"
            ) from error
        return value if isinstance(value, dict) else {"result": value}

    @staticmethod
    def _extract_instance_id(response: Any) -> Optional[str]:
        if not isinstance(response, dict):
            return None
        for key in ("instanceId", "instanceID", "mid", "id"):
            value = response.get(key)
            if isinstance(value, str) and value:
                return value
        result = response.get("result")
        if isinstance(result, dict):
            return MicroStrategyClient._extract_instance_id(result)
        return None

    def _request(
        self,
        method: str,
        path: str,
        project_id: Optional[str] = None,
        expected_statuses: Optional[Set[int]] = None,
        **kwargs: Any,
    ) -> requests.Response:
        if self._auth_dead:
            # A failed re-login means every further call would 401; refusing
            # immediately lets the source abort the run instead of failing
            # every remaining project one API call at a time.
            raise MicroStrategyAuthError(
                "MicroStrategy authentication was lost and could not be "
                f"re-established; refusing request: {method} {path}"
            )
        url = f"{self.base_url}{path}"
        headers = kwargs.pop("headers", {})
        if project_id:
            headers["X-MSTR-ProjectID"] = project_id
        timeout_seconds = kwargs.pop("timeout_seconds", None)
        max_attempts = kwargs.pop("max_attempts", None)

        attempts = max_attempts or self.config.max_retries + 1
        last_error: Optional[Exception] = None
        reauth_attempted = False
        attempt = 0
        while attempt < attempts:
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    timeout=timeout_seconds or self.config.timeout_seconds,
                    verify=self.config.verify_ssl,
                    **kwargs,
                )
            except requests.RequestException as error:
                # Network-level failures (connection errors, timeouts) are
                # transient; HTTP error statuses are handled below so that
                # non-retryable 4xx responses fail fast.
                last_error = error
                if attempt < attempts - 1:
                    time.sleep(min(2**attempt, _MAX_RETRY_DELAY_SECONDS))
                    attempt += 1
                    continue
                self.report.report_api_error()
                raise MicroStrategyAPIError(
                    f"MicroStrategy API request failed: {method} {path}: {error}"
                ) from error

            if expected_statuses and response.status_code in expected_statuses:
                return response
            if response.status_code == 401 and not path.startswith("/api/auth/"):
                # Session tokens can be invalidated at any moment (idle or
                # absolute timeouts, concurrent-session limits, admin action),
                # so one re-login + replay recovers long runs. The replay does
                # not consume a retry attempt: this is auth recovery, not a
                # retry of a failing endpoint, and must work even for
                # max_attempts=1 calls.
                self._reauthenticate_or_die(method, path, reauth_attempted)
                reauth_attempted = True
                continue
            if (
                response.status_code in _RETRYABLE_STATUS_CODES
                and attempt < attempts - 1
            ):
                time.sleep(self._retry_delay(response, attempt))
                attempt += 1
                continue
            try:
                response.raise_for_status()
            except requests.HTTPError as error:
                self.report.report_api_error()
                raise MicroStrategyAPIError(
                    f"MicroStrategy API request failed: {method} {path}: {error}"
                ) from error
            return response

        raise MicroStrategyAPIError(
            f"MicroStrategy API request failed: {method} {path}: {last_error}"
        )

    def _reauthenticate_or_die(
        self,
        method: str,
        path: str,
        reauth_attempted: bool,
    ) -> None:
        """Re-login after a mid-run 401, or mark auth as irrecoverably dead.

        A second 401 after a successful re-login means the credentials no
        longer grant access, so retrying further requests is pointless.
        """
        if reauth_attempted:
            self._auth_dead = True
            self.report.report_api_error()
            raise MicroStrategyAuthError(
                "MicroStrategy rejected the request again immediately after "
                f"a successful re-login; aborting: {method} {path}"
            )
        logger.info(
            "MicroStrategy session was rejected (401) on %s %s; "
            "re-authenticating and replaying the request",
            method,
            path,
        )
        try:
            self.login()
        except Exception as error:
            self._auth_dead = True
            self.report.report_api_error()
            raise MicroStrategyAuthError(
                "MicroStrategy session was invalidated and re-login failed: "
                f"{method} {path}: {error}"
            ) from error
        self.report.report_session_reauthenticated()

    @staticmethod
    def _retry_delay(response: requests.Response, attempt: int) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(max(float(retry_after), 1.0), _MAX_RETRY_DELAY_SECONDS)
            except ValueError:
                pass
        return float(min(2**attempt, _MAX_RETRY_DELAY_SECONDS))
