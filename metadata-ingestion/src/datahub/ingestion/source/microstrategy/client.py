"""
MicroStrategy REST API Client.

Handles:
- Authentication and session management
- Request retry logic and error handling
- Pagination
- Token refresh for long-running ingestions
- Guaranteed session cleanup
"""

import logging
import threading
import types
from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional, Type, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.microstrategy.config import (
    MicroStrategyConnectionConfig,
)
from datahub.ingestion.source.microstrategy.constants import (
    ISERVER_PROJECT_UNAVAILABLE,
    ISERVER_PROJECT_UNAVAILABLE_DETAIL,
    SEARCH_OBJECT_TYPE_WAREHOUSE_TABLE,
)

logger = logging.getLogger(__name__)


class MicroStrategyAPIError(Exception):
    """Base exception for MicroStrategy API errors."""

    pass


class MicroStrategyAuthenticationError(MicroStrategyAPIError):
    """Raised when authentication fails."""

    pass


class MicroStrategyPermissionError(MicroStrategyAPIError):
    """Raised when API returns permission denied."""

    pass


class MicroStrategyProjectUnavailableError(MicroStrategyAPIError):
    """Raised when IServer reports the project as unavailable / not loaded."""

    def __init__(
        self,
        message: str,
        *,
        i_server_code: int,
        ticket_id: str = "",
        endpoint: str = "",
    ) -> None:
        super().__init__(message)
        self.i_server_code = i_server_code
        self.ticket_id = ticket_id
        self.endpoint = endpoint


class MicroStrategyClient:
    """
    Client for MicroStrategy REST API.

    Features:
    - Automatic session management (login/logout)
    - Token refresh before expiration
    - Exponential backoff retry logic
    - Pagination support
    - Context manager for guaranteed cleanup

    Usage:
        with MicroStrategyClient(config) as client:
            projects = client.get_projects()
            for project in projects:
                objects = client.search_objects(project["id"], object_type=55)
    """

    # API Endpoints
    AUTH_LOGIN = "/api/auth/login"
    AUTH_LOGOUT = "/api/auth/logout"
    SESSIONS = "/api/sessions"

    # Session timeout defaults (MicroStrategy standard is 30 minutes)
    DEFAULT_SESSION_TIMEOUT = timedelta(minutes=30)
    REFRESH_THRESHOLD = timedelta(minutes=5)

    def __init__(self, config: MicroStrategyConnectionConfig):
        self.config = config
        self.base_url = config.base_url.rstrip("/")

        # Session state
        self.auth_token: Optional[str] = None
        self.token_created_at: Optional[datetime] = None
        self.session_timeout = self.DEFAULT_SESSION_TIMEOUT
        self._token_lock = threading.Lock()

        # HTTP session with retry logic
        self.session = requests.Session()
        # Retry on gateway-level errors only.  500 is intentionally excluded —
        # MicroStrategy returns 500 for permanent application errors (ClassCast,
        # missing objects, internal assertion failures) and retrying amplifies
        # server load without recovery.  429 (rate limit) uses backoff.
        retry_strategy = Retry(
            total=config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT", "DELETE"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _response_json_dict(response: requests.Response) -> Optional[Dict[str, Any]]:
        try:
            if not response.content:
                return None
            data = response.json()
            return data if isinstance(data, dict) else None
        except (ValueError, requests.exceptions.JSONDecodeError) as e:
            logger.warning("Failed to parse response JSON from %s: %s", response.url, e)
            return None

    def _raise_if_iserver_project_unavailable(
        self, response: requests.Response, endpoint: str
    ) -> None:
        body = self._response_json_dict(response)
        if body and body.get("iServerCode") == ISERVER_PROJECT_UNAVAILABLE:
            msg = str(body.get("message") or ISERVER_PROJECT_UNAVAILABLE_DETAIL)
            raise MicroStrategyProjectUnavailableError(
                msg,
                i_server_code=int(body["iServerCode"]),
                ticket_id=str(body.get("ticketId", "")),
                endpoint=endpoint,
            )

    @staticmethod
    def _is_legacy_classcast_message(body: Union[Dict[str, Any], str, None]) -> bool:
        if body is None:
            return False
        text = str(body.get("message", "")) if isinstance(body, dict) else str(body)
        lower = text.lower()
        return "cannot be cast" in lower or "classcast" in lower

    @staticmethod
    def _project_headers(project_id: str) -> Dict[str, str]:
        """Return per-request headers for a project-scoped API call.

        These are passed via ``extra_headers`` to ``_request`` / ``_paginated_request``
        and merged per-request without mutating ``session.headers``, making all calls
        safe to issue from multiple threads concurrently.
        """
        return {"X-MSTR-ProjectID": project_id}

    # ── Context manager ───────────────────────────────────────────────────────

    def __enter__(self) -> "MicroStrategyClient":
        self.login()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        try:
            self.logout()
        except Exception as e:
            logger.error("Session cleanup error during exit: %s", e)
            if exc_type is None:
                raise

    def close(self) -> None:
        """Release the underlying HTTP connection pool."""
        self.session.close()

    # ── Authentication ────────────────────────────────────────────────────────

    def login(self) -> None:
        logger.info("Authenticating to MicroStrategy...")
        login_mode = 8 if self.config.use_anonymous else 1
        login_data: Dict[str, Any] = {"loginMode": login_mode}
        if not self.config.use_anonymous:
            if not self.config.username or not self.config.password:
                raise MicroStrategyAuthenticationError(
                    "Username and password required for non-anonymous access"
                )
            login_data["username"] = self.config.username
            login_data["password"] = self.config.password.get_secret_value()
        try:
            response = self.session.post(
                f"{self.base_url}{self.AUTH_LOGIN}",
                json=login_data,
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            self.auth_token = response.headers.get("X-MSTR-AuthToken")
            if not self.auth_token:
                raise MicroStrategyAuthenticationError(
                    "Authentication response missing X-MSTR-AuthToken header"
                )
            self.token_created_at = datetime.now()
            self.session.headers.update({"X-MSTR-AuthToken": self.auth_token})
            self._activate_session()
            logger.info("Successfully authenticated to MicroStrategy")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [401, 403]:
                logger.debug("Authentication failed response body: %s", e.response.text)
                raise MicroStrategyAuthenticationError(
                    f"Authentication failed: HTTP {e.response.status_code}"
                ) from e
            raise MicroStrategyAPIError(f"Login request failed: {e}") from e
        except requests.exceptions.RequestException as e:
            raise MicroStrategyAPIError(
                f"Failed to connect to MicroStrategy: {e}"
            ) from e

    def _activate_session(self) -> None:
        try:
            response = self.session.put(
                f"{self.base_url}{self.SESSIONS}",
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            logger.debug("Session activated successfully")
        except Exception as e:
            logger.warning("Failed to activate session (non-fatal): %s", e)

    def _ensure_valid_token(self) -> None:
        with self._token_lock:
            if not self.auth_token or not self.token_created_at:
                logger.info("No active session, logging in...")
                self.login()
                return
            elapsed = datetime.now() - self.token_created_at
            if elapsed >= (self.session_timeout - self.REFRESH_THRESHOLD):
                logger.info("Token nearing expiration, refreshing session...")
                try:
                    self._activate_session()
                    self.token_created_at = datetime.now()
                    logger.info("Session refreshed successfully")
                except Exception as e:
                    logger.warning("Token refresh failed, re-authenticating: %s", e)
                    self.auth_token = None
                    self.login()

    def logout(self) -> None:
        if not self.auth_token:
            logger.debug("No active session to logout")
            return
        try:
            response = self.session.post(
                f"{self.base_url}{self.AUTH_LOGOUT}",
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            logger.debug("Successfully logged out MicroStrategy session")
        except Exception as e:
            logger.error("Failed to logout MicroStrategy session: %s", e)
            raise MicroStrategyAPIError(f"Logout failed: {e}") from e
        finally:
            self.auth_token = None
            self.token_created_at = None
            self.session.headers.pop("X-MSTR-AuthToken", None)

    # ── Core request machinery ────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        *,
        extra_headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        legacy_classcast_500_returns_empty: bool = False,
    ) -> Any:
        # Return type is Any because callers have heterogeneous return contracts:
        # some methods declare -> Dict[str, Any], others -> List[Any], others -> str.
        # Narrowing here would require 13+ overloads or generic plumbing — the
        # individual method annotations provide the effective contracts.
        """
        Make authenticated API request with automatic token refresh.

        Args:
            method: HTTP method
            endpoint: API endpoint path
            params: Query parameters
            json: Request body
            extra_headers: Per-request headers merged with session headers for this
                call only — session.headers is never mutated, making this safe to
                use from multiple threads concurrently.
            timeout: Override default timeout (seconds). Use for slow endpoints
                     such as document instance creation which can take 60–90s.
            legacy_classcast_500_returns_empty: Return {} on ClassCast 500 instead of raising.

        Returns:
            Parsed JSON (dict or list) or empty dict / empty list.

        Raises:
            MicroStrategyPermissionError: 403
            MicroStrategyProjectUnavailableError: iServerCode project unavailable
            MicroStrategyAPIError: Other API errors
        """
        self._ensure_valid_token()
        url = f"{self.base_url}{endpoint}"
        effective_timeout = (
            timeout if timeout is not None else self.config.timeout_seconds
        )

        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json,
                # extra_headers are merged per-request without mutating session.headers,
                # making these calls safe to issue from multiple threads concurrently.
                headers=extra_headers,
                timeout=effective_timeout,
            )

            if response.status_code == 403:
                logger.debug(
                    "Permission denied response body for %s: %s",
                    endpoint,
                    response.text,
                )
                raise MicroStrategyPermissionError(f"Permission denied for {endpoint}")

            self._raise_if_iserver_project_unavailable(response, endpoint)

            if legacy_classcast_500_returns_empty and response.status_code == 500:
                body = self._response_json_dict(response)
                if self._is_legacy_classcast_message(body):
                    logger.warning(
                        "Ignoring legacy MicroStrategy ClassCast error for %s", endpoint
                    )
                    return {}

            response.raise_for_status()
            return response.json() if response.content else {}

        except MicroStrategyProjectUnavailableError:
            raise
        except requests.exceptions.HTTPError as e:
            self._raise_if_iserver_project_unavailable(e.response, endpoint)
            if e.response.status_code == 404:
                logger.debug("Resource not found: %s", url)
                return {}
            logger.debug(
                "HTTP %s error response body for %s: %s",
                e.response.status_code,
                endpoint,
                e.response.text,
            )
            raise MicroStrategyAPIError(
                f"HTTP {e.response.status_code} error for {endpoint}"
            ) from e
        except requests.exceptions.Timeout:
            raise MicroStrategyAPIError(f"Request timeout for {endpoint}") from None
        except requests.exceptions.RequestException as e:
            raise MicroStrategyAPIError(f"Request failed for {endpoint}: {e}") from e

    def _paginated_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_size: int = 1000,
        *,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Yield items from all pages of a paginated endpoint."""
        params = params or {}
        params["limit"] = page_size
        offset = 0
        while True:
            params["offset"] = offset
            response = self._request(
                "GET", endpoint, params=params, extra_headers=extra_headers
            )
            items: List[Dict[str, Any]] = []
            if isinstance(response, list):
                items = response
            elif isinstance(response, dict):
                items = (
                    response.get("result")
                    or response.get("value")
                    or response.get("data")
                    or response.get("items")
                    or []
                )
            if not items:
                break
            for item in items:
                yield item
            if len(items) < page_size:
                break
            offset += page_size

    # ── Connection test ───────────────────────────────────────────────────────

    def test_connection(self) -> bool:
        try:
            with self:
                pass
            return True
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False

    # ── Project / folder / user endpoints ────────────────────────────────────

    def get_projects(self) -> List[Dict[str, Any]]:
        response = self._request("GET", "/api/projects")
        projects = response if isinstance(response, list) else response.get("value", [])
        logger.info("Found %s projects", len(projects))
        return projects

    def get_project(self, project_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/v2/projects/{project_id}")

    def get_folders(self, project_id: str) -> List[Dict[str, Any]]:
        return list(
            self._paginated_request(
                f"/api/v2/projects/{project_id}/folders",
                extra_headers=self._project_headers(project_id),
            )
        )

    def get_folder(self, folder_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/v2/folders/{folder_id}")

    def get_users(self) -> List[Dict[str, Any]]:
        return list(self._paginated_request("/api/v2/users"))

    def get_user(self, user_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/v2/users/{user_id}")

    # ── Admin / datasource endpoints ──────────────────────────────────────────

    def get_datasources(self) -> List[Dict[str, Any]]:
        """
        Return all datasource connection objects from the environment.

        GET /api/datasources — environment-level admin endpoint; does NOT take
        X-MSTR-ProjectID (project-scoped header would cause a 400).

        Each entry carries:
          database.type   — MSTR internal DBMS string (e.g. 'teradata', 'snow_flake')
          dbms.name       — human-readable name      (e.g. 'Teradata Database 16.20')
          tablePrefix     — default schema prefix     (useful for table name qualification)
          datasourceType  — 'normal' = project warehouse, 'data_import' = upload source

        Returns empty list (not an error) on 403 — the service account may lack
        the "Configure Database Connections" admin privilege.  The caller should
        fall back to SQL quoting-style inference in that case.
        """
        # No extra_headers needed — this is an env-level endpoint that must NOT
        # receive X-MSTR-ProjectID (would cause a 400).  Since project headers
        # are now passed per-request via extra_headers, session.headers never
        # contains X-MSTR-ProjectID, so no stripping is required.
        try:
            data = self._request("GET", "/api/datasources")
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return data.get("datasources") or data.get("result") or []
            return []
        except MicroStrategyPermissionError:
            logger.info(
                "GET /api/datasources returned 403 — service account lacks admin privilege. "
                "Platform will be inferred from SQL quoting style instead."
            )
            return []
        except Exception as e:
            logger.warning(
                "GET /api/datasources failed (%s: %s) — "
                "platform will be inferred from SQL quoting style instead.",
                type(e).__name__,
                e,
            )
            return []

    def get_project_datasources(self, project_id: str) -> List[Dict[str, Any]]:
        """
        Return datasources directly associated with a specific project.

        GET /api/projects/{projectId}/datasources — project-scoped endpoint.
        Project ID is in the URL path; do NOT set X-MSTR-ProjectID header here
        (that would cause a 400).

        Unlike GET /api/datasources (env-wide, all configured sources), this
        returns only the DSNs attached to the given project — typically just one
        or a handful, making platform detection unambiguous.

        Returns empty list on 403/404 or any failure — caller falls back to the
        env-level endpoint.
        """
        try:
            data = self._request("GET", f"/api/projects/{project_id}/datasources")
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return data.get("datasources", [])
            return []
        except MicroStrategyPermissionError:
            logger.info(
                "GET /api/projects/%s/datasources returned 403 — "
                "falling back to env-level /api/datasources.",
                project_id,
            )
            return []
        except Exception as e:
            logger.warning("get_project_datasources failed for %s: %s", project_id, e)
            return []

    # ── Object search (primary discovery method) ──────────────────────────────

    def search_objects(
        self, project_id: str, object_type: int, limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Search for objects using /api/searches/results.

        This is the authoritative discovery method — the /v2/projects/{id}/dashboards
        and similar endpoints often return empty results, while the search API
        reliably returns all objects.

        Object type codes:
            3   = Report + Intelligent Cube (subtype 776 = cube)
            8   = Folder
            53  = Warehouse table
            55  = Dashboard/Dossier/Document (subtype 14081=legacy, 14336=modern)
            776 = Intelligent Cube (search API, subtype of type 3)
        """
        objects = list(
            self._paginated_request(
                "/api/searches/results",
                params={"type": object_type},
                page_size=limit,
                extra_headers=self._project_headers(project_id),
            )
        )
        logger.info(
            "Found %s objects (type=%s) in project %s",
            len(objects),
            object_type,
            project_id,
        )
        return objects

    def get_object(
        self, object_id: str, object_type: int, project_id: str
    ) -> Dict[str, Any]:
        data = self._request(
            "GET",
            f"/api/objects/{object_id}",
            params={"type": object_type},
            extra_headers=self._project_headers(project_id),
        )
        return data if isinstance(data, dict) else {}

    def search_warehouse_tables(
        self, project_id: str, limit: int = 1000
    ) -> List[Dict[str, Any]]:
        return self.search_objects(
            project_id, SEARCH_OBJECT_TYPE_WAREHOUSE_TABLE, limit=limit
        )

    def get_table_definition(self, table_id: str, project_id: str) -> Dict[str, Any]:
        """
        Return the full definition of a logical table object.

        GET /api/v2/tables/{id} with X-MSTR-ProjectID set.

        The response includes a primaryDataSource field that references the
        project-scoped DBRole (datasource) this table is connected to:

            {
              "primaryDataSource": {
                "objectId": "A1B2C3D4...",
                "subType":  "db_role",
                "name":     "XRBIA_Teradata_DSN"
              },
              ...
            }

        The objectId can then be passed to get_datasource_by_id() to resolve
        the database.type (e.g. 'teradata') for the platform mapping.
        """
        try:
            return self._request(
                "GET",
                f"/api/v2/tables/{table_id}",
                extra_headers=self._project_headers(project_id),
            )
        except Exception as e:
            logger.info("get_table_definition failed for %s: %s", table_id, e)
            return {}

    def list_model_tables(
        self, project_id: str, *, limit: int = 200, offset: int = 0
    ) -> Dict[str, Any]:
        """
        Return one page of model Table objects from the Modeling Service.

        GET /api/model/tables  (with X-MSTR-ProjectID header, paginated)

        Each entry has an information.objectId and information.name.
        IMPORTANT: these model Table IDs are NOT the same as the type-53 "DB Table"
        IDs returned by GET /api/searches/results?type=53 — they are different
        objects in the MSTR metadata layer.  Use this method to find the correct
        model Table ID by name, then pass it to get_model_table_definition().

        Returns empty dict on 403 (requires 'Use Architect Editors' privilege in
        some MSTR versions) or any other failure.
        """
        try:
            data = self._request(
                "GET",
                "/api/model/tables",
                params={"limit": limit, "offset": offset},
                extra_headers=self._project_headers(project_id),
            )
            return data if isinstance(data, dict) else {}
        except MicroStrategyPermissionError:
            logger.info(
                "GET /api/model/tables returned 403 for project %s — "
                "falling back to next detection tier",
                project_id,
            )
            return {}
        except Exception as e:
            logger.info("list_model_tables failed for project %s: %s", project_id, e)
            return {}

    def get_model_table_definition(
        self, model_table_id: str, project_id: str
    ) -> Dict[str, Any]:
        """
        Return the full definition of a model Table object.

        GET /api/model/tables/{id}  (with X-MSTR-ProjectID header)

        The response includes primaryDataSource (objectId, name, subType) which
        identifies the project-scoped DBRole (datasource) for this table.

        Use list_model_tables() to find the correct model Table ID — type-53
        DB Table IDs from /api/searches/results are NOT valid here and will
        return 500 with a 'type mismatch' error.
        """
        try:
            return self._request(
                "GET",
                f"/api/model/tables/{model_table_id}",
                extra_headers=self._project_headers(project_id),
            )
        except Exception as e:
            logger.info(
                "get_model_table_definition failed for %s: %s", model_table_id, e
            )
            return {}

    def get_datasource_by_id(
        self, datasource_id: str, project_id: str
    ) -> Dict[str, Any]:
        """
        Return a single datasource object by its ID.

        GET /api/datasources/{id} with X-MSTR-ProjectID set so that
        project-scoped DSNs (DBRoles) are visible — unlike the env-level
        GET /api/datasources which only surfaces globally-registered sources.

        Response shape matches the per-item shape from /api/datasources:
            {
              "id":             "A1B2C3D4...",
              "name":           "XRBIA_Teradata_DSN",
              "datasourceType": "normal",
              "database": {
                "type":    "teradata",
                "version": "teradata_16"
              },
              ...
            }

        Returns empty dict on 403/404 or any other failure — caller should
        fall back to SQL inference in that case.
        """
        try:
            return self._request(
                "GET",
                f"/api/datasources/{datasource_id}",
                extra_headers=self._project_headers(project_id),
            )
        except MicroStrategyPermissionError:
            logger.debug(
                "GET /api/datasources/%s returned 403 with project header",
                datasource_id,
            )
            return {}
        except Exception as e:
            logger.debug("get_datasource_by_id failed for %s: %s", datasource_id, e)
            return {}

    def get_datasource_connection(self, connection_id: str) -> Dict[str, Any]:
        """
        Return the connection object for a datasource connection.

        GET /api/datasources/connections/{connectionId}

        The response includes a ``connectionString`` field containing the
        JDBC/ODBC connection string, which typically encodes database, schema,
        warehouse, and other parameters.  For example a Snowflake JDBC string:

            JDBC;DRIVER={net.snowflake.client.jdbc.SnowflakeDriver};
            URL={jdbc:snowflake://acct.snowflakecomputing.com/
            ?warehouse=WH&db=MY_DB&schema=MY_SCHEMA&role=MY_ROLE};

        Returns empty dict on 403/404 or any other failure.
        """
        try:
            return self._request("GET", f"/api/datasources/connections/{connection_id}")
        except MicroStrategyPermissionError:
            logger.debug(
                "GET /api/datasources/connections/%s returned 403", connection_id
            )
            return {}
        except Exception as e:
            logger.debug(
                "get_datasource_connection failed for %s: %s", connection_id, e
            )
            return {}

    def get_attribute_expression(self, attribute_id: str, project_id: str) -> str:
        """
        Return a human-readable expression string for a MicroStrategy attribute.

        GET /api/model/attributes/{id} → forms[].expressions[].expression.text

        Returns the first expression text found across all forms, or empty string.
        For multi-expression attributes the expressions are joined with " | ".
        """
        try:
            data = self._request(
                "GET",
                f"/api/model/attributes/{attribute_id}",
                extra_headers=self._project_headers(project_id),
            )
            parts = []
            for form in data.get("forms", []):
                for expr in form.get("expressions", []):
                    text = expr.get("expression", {}).get("text", "")
                    if text and text not in parts:
                        parts.append(text)
            return " | ".join(parts)
        except Exception as e:
            logger.debug("get_attribute_expression failed for %s: %s", attribute_id, e)
            return ""

    def get_metric_expression(self, metric_id: str, project_id: str) -> str:
        """
        Return the formula expression string for a MicroStrategy metric.

        GET /api/model/metrics/{id} → expression.text

        Examples:
          "Sum(NET_SLS_RTL_AMT)"
          "({Net Sales Retail Amt} - {Net Sales Retail Amt LY}) / Abs({Net Sales Retail Amt LY}) * 100"
        """
        try:
            data = self._request(
                "GET",
                f"/api/model/metrics/{metric_id}",
                extra_headers=self._project_headers(project_id),
            )
            return data.get("expression", {}).get("text", "")
        except Exception as e:
            logger.debug("get_metric_expression failed for %s: %s", metric_id, e)
            return ""

    # ── Cube endpoints ────────────────────────────────────────────────────────

    def get_cube(self, cube_id: str, project_id: str) -> Dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v2/cubes/{cube_id}",
            extra_headers=self._project_headers(project_id),
            legacy_classcast_500_returns_empty=True,
        )

    def get_cube_sql_view(self, cube_id: str, project_id: str) -> str:
        """
        Get the SQL generated by MicroStrategy for an Intelligent Cube.

        Returns the raw SQL string for warehouse lineage extraction.
        Returns empty string if cube has no SQL view (not published, dynamic sourcing).

        Confirmed working via live testing (16 tables from XRBIA_DM schema, JCP).
        Does NOT require 'Use Architect Editors' privilege — only standard access.

        Note: _request wraps all non-404 HTTP errors as MicroStrategyAPIError, so
        400 responses (cube not published) propagate as MicroStrategyAPIError to
        callers. _prefetch_cube handles this with a broad except.
        """
        data = self._request(
            "GET",
            f"/api/v2/cubes/{cube_id}/sqlView",
            extra_headers=self._project_headers(project_id),
        )
        return data.get("sqlStatement", "") if isinstance(data, dict) else ""

    # ── Report endpoints ──────────────────────────────────────────────────────

    def get_report(self, report_id: str, project_id: str) -> Dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v2/reports/{report_id}",
            extra_headers=self._project_headers(project_id),
        )

    def create_report_instance(self, report_id: str, project_id: str) -> Optional[str]:
        """
        Create a report instance for SQL view retrieval.

        Uses executionStage=resolve_prompts so the query plan is built without
        executing the report against the warehouse — fast and safe for ingestion.
        Requires DssXmlPrivilegesWebReportSQL privilege on the service account.

        Returns instanceId string, or None if creation fails.
        """
        try:
            data = self._request(
                "POST",
                f"/api/v2/reports/{report_id}/instances",
                params={"executionStage": "resolve_prompts"},
                json={},
                extra_headers=self._project_headers(project_id),
            )
            if isinstance(data, dict):
                return data.get("instanceId")
            return None
        except MicroStrategyPermissionError:
            logger.warning(
                "Cannot create report instance for %s — missing DssXmlPrivilegesWebReportSQL "
                "privilege. Grant 'Web Report SQL' in MSTR Security Roles.",
                report_id,
            )
            return None
        except Exception as e:
            logger.debug("Failed to create report instance for %s: %s", report_id, e)
            return None

    def get_report_sql_view(
        self, report_id: str, instance_id: str, project_id: str
    ) -> str:
        """
        Get the SQL statement for a report instance.

        Returns the raw SQL string, or empty string on failure.
        Confirmed working: 8 source tables parsed from 'Spring OOS Date Receipt
        Cst by Week Div 5' on jcpenney-qa.cloud.strategy.com.
        """
        try:
            data = self._request(
                "GET",
                f"/api/v2/reports/{report_id}/instances/{instance_id}/sqlView",
                extra_headers=self._project_headers(project_id),
            )
            return data.get("sqlStatement", "") if isinstance(data, dict) else ""
        except Exception as e:
            logger.debug(
                "Failed to get report SQL view for %s/%s: %s",
                report_id,
                instance_id,
                e,
            )
            return ""

    def delete_report_instance(
        self, report_id: str, instance_id: str, project_id: str
    ) -> None:
        """Delete a report instance. Best-effort — errors are logged but not raised."""
        try:
            self._request(
                "DELETE",
                f"/api/v2/reports/{report_id}/instances/{instance_id}",
                extra_headers=self._project_headers(project_id),
            )
        except Exception as e:
            logger.debug("Failed to delete report instance %s: %s", instance_id, e)

    # ── Document/Dossier endpoints ────────────────────────────────────────────

    def get_document_definition(
        self, document_id: str, project_id: str
    ) -> Dict[str, Any]:
        """
        Get legacy document definition (subtype 14081).

        Uses GET /api/documents/{id}/definition which returns:
          { "datasets": [ { "id": ..., "name": ..., "availableObjects": [...] } ] }

        This is different from /api/v2/dossiers/{id}/definition which returns
        chapters/pages/visualizations and returns ClassCastException 500 for subtype 14081.
        """
        return self._request(
            "GET",
            f"/api/documents/{document_id}/definition",
            extra_headers=self._project_headers(project_id),
            legacy_classcast_500_returns_empty=True,
        )

    def get_dossier_definition(
        self, dossier_id: str, project_id: str
    ) -> Dict[str, Any]:
        """
        Get modern dossier definition (subtype 14336).

        Uses GET /api/v2/dossiers/{id}/definition which returns:
          { "chapters": [ { "pages": [ { "visualizations": [...] } ] } ] }

        Note the pages layer — source.py traverses chapters → pages → visualizations.
        """
        return self._request(
            "GET",
            f"/api/v2/dossiers/{dossier_id}/definition",
            extra_headers=self._project_headers(project_id),
            legacy_classcast_500_returns_empty=True,
        )

    def create_document_instance(
        self, document_id: str, project_id: str
    ) -> Optional[str]:
        """
        Create an instance of a legacy document (subtype 14081).

        MSTR asymmetry (confirmed via live testing):
          - Create via POST /api/documents/{id}/instances  → returns {"mid": "..."}
          - Fetch sqlView via GET /api/dossiers/{id}/instances/{mid}/datasets/sqlView
          - Delete via DELETE /api/dossiers/{id}/instances/{mid}

        Uses a 90-second timeout — legacy documents execute all embedded
        datasets on instantiation and can take 60–90s to respond.

        Returns mid (instance ID) string, or None on failure.
        """
        try:
            data = self._request(
                "POST",
                f"/api/documents/{document_id}/instances",
                json={},
                extra_headers=self._project_headers(project_id),
                timeout=90,  # legacy docs are slow — they execute all datasets on load
            )
            if isinstance(data, dict):
                # Legacy documents return "mid", not "instanceId"
                return data.get("mid") or data.get("instanceId")
            return None
        except requests.exceptions.Timeout:
            logger.warning(
                "Document instance creation timed out for %s after 90s. "
                "The document may have too many datasets. "
                "Consider targeting a modern dossier (subtype 14336) instead.",
                document_id,
            )
            return None
        except Exception as e:
            logger.debug(
                "Failed to create document instance for %s: %s", document_id, e
            )
            return None

    def create_dossier_instance(
        self, dossier_id: str, project_id: str
    ) -> Optional[str]:
        """
        Create an instance of a modern dossier (subtype 14336).

        POST /api/dossiers/{id}/instances → returns {"mid": "..."} or {"instanceId": "..."}

        Returns instance ID string, or None on failure.
        """
        try:
            data = self._request(
                "POST",
                f"/api/dossiers/{dossier_id}/instances",
                json={},
                extra_headers=self._project_headers(project_id),
                timeout=60,
            )
            if isinstance(data, dict):
                return data.get("mid") or data.get("instanceId")
            return None
        except Exception as e:
            logger.debug("Failed to create dossier instance for %s: %s", dossier_id, e)
            return None

    def get_dossier_datasets_sql(
        self, dossier_id: str, instance_id: str, project_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get SQL statements for all datasets in a document or dossier instance.

        MSTR asymmetry (confirmed via live testing):
          - Always use /api/dossiers/{id}/instances/{iid}/datasets/sqlView
          - This works for BOTH subtype 14081 (legacy document) and 14336 (modern dossier)
          - Using /api/documents/{id}/instances/{iid}/datasets/sqlView returns 404 for 14081

        Confirmed working: 97 unique warehouse tables from 15 datasets in
        'Salon Sales To Plan' (subtype 14081) on jcpenney-qa.cloud.strategy.com.

        Returns list of dicts: [{"name": "DATASET NAME", "sqlStatement": "SELECT ..."}, ...]
        Response shape may vary — handles both list and {"datasets": [...]} wrapper.

        Timeout is 120s — legacy documents with 15+ datasets can be slow to generate SQL.
        """
        try:
            data = self._request(
                "GET",
                f"/api/dossiers/{dossier_id}/instances/{instance_id}/datasets/sqlView",
                extra_headers=self._project_headers(project_id),
                timeout=120,
            )
        except MicroStrategyPermissionError:
            logger.warning(
                "Cannot fetch dataset SQL for %s — missing DssXmlPrivilegesWebReportSQL "
                "privilege. Grant 'Web Report SQL' in MSTR Security Roles.",
                dossier_id,
            )
            return []
        except Exception as e:
            logger.debug(
                "Failed to get dataset SQL for dossier %s/%s: %s",
                dossier_id,
                instance_id,
                e,
            )
            return []
        # Log raw response shape at INFO so we can diagnose partial-return issues
        if isinstance(data, list):
            logger.info(
                "datasets/sqlView returned list of %s items for dossier %s",
                len(data),
                dossier_id,
            )
            return data
        if isinstance(data, dict):
            top_keys = sorted(data.keys())
            logger.info(
                "datasets/sqlView returned dict with keys %s for dossier %s",
                top_keys,
                dossier_id,
            )
            # Try all known wrapper shapes in priority order
            result = (
                data.get("datasets")
                or data.get("sqlStatements")
                or data.get("result")
                or ([data] if data.get("sqlStatement") else [])
            )
            if result:
                logger.info(
                    "datasets/sqlView resolved to %s dataset SQL entries",
                    len(result),
                )
            else:
                logger.warning(
                    "datasets/sqlView dict had no recognisable SQL list key — "
                    "keys were %s — full response logged at DEBUG",
                    top_keys,
                )
                logger.debug("datasets/sqlView full response: %s", data)
            return result
        logger.warning(
            "datasets/sqlView returned unexpected type %s for dossier %s",
            type(data).__name__,
            dossier_id,
        )
        return []

    def delete_dossier_instance(
        self, dossier_id: str, instance_id: str, project_id: str
    ) -> None:
        """
        Delete a dossier or document instance.

        Uses /api/dossiers/{id}/instances/{iid} for BOTH subtype 14081 (legacy
        document) and 14336 (modern dossier) — the asymmetric cleanup endpoint.
        Best-effort — errors are logged but not raised.
        """
        try:
            self._request(
                "DELETE",
                f"/api/dossiers/{dossier_id}/instances/{instance_id}",
                extra_headers=self._project_headers(project_id),
            )
        except Exception as e:
            logger.debug("Failed to delete dossier instance %s: %s", instance_id, e)

    # ── Dataset endpoints ─────────────────────────────────────────────────────

    def get_datasets(self, project_id: str) -> List[Dict[str, Any]]:
        datasets = list(
            self._paginated_request(
                f"/api/v2/projects/{project_id}/datasets",
                extra_headers=self._project_headers(project_id),
            )
        )
        logger.info("Found %s datasets in project %s", len(datasets), project_id)
        return datasets

    def get_dataset(self, dataset_id: str, project_id: str) -> Dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v2/datasets/{dataset_id}",
            extra_headers=self._project_headers(project_id),
        )
