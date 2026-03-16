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
from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.microstrategy.config import (
    MicroStrategyConnectionConfig,
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
                dashboards = client.get_dashboards(project["id"])
    """

    # API Endpoints
    AUTH_LOGIN = "/api/auth/login"
    AUTH_LOGOUT = "/api/auth/logout"
    SESSIONS = "/api/sessions"

    # Session timeout defaults (MicroStrategy standard is 30 minutes)
    DEFAULT_SESSION_TIMEOUT = timedelta(minutes=30)
    REFRESH_THRESHOLD = timedelta(minutes=5)  # Refresh 5 mins before expiry

    def __init__(self, config: MicroStrategyConnectionConfig):
        """
        Initialize MicroStrategy client.

        Args:
            config: Connection configuration
        """
        self.config = config
        self.base_url = config.base_url.rstrip("/")

        # Session state
        self.auth_token: Optional[str] = None
        self.token_created_at: Optional[datetime] = None
        self.session_timeout = self.DEFAULT_SESSION_TIMEOUT

        # Setup HTTP session with retry logic
        self.session = requests.Session()
        retry_strategy = Retry(
            total=config.max_retries,
            backoff_factor=1,  # 1s, 2s, 4s exponential backoff
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT", "DELETE"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def __enter__(self) -> "MicroStrategyClient":
        """Context manager entry - login."""
        self.login()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - guaranteed logout."""
        try:
            self.logout()
        except Exception as e:
            logger.error(f"Session cleanup error during exit: {e}")
            # Don't suppress original exception if one exists
            if exc_type is None:
                raise
        return False  # Re-raise any exception from the with block

    def login(self) -> None:
        """
        Authenticate and obtain session token.

        Raises:
            MicroStrategyAuthenticationError: If login fails
        """
        logger.info("Authenticating to MicroStrategy...")

        # Determine login mode
        login_mode = 8 if self.config.use_anonymous else 1  # 1=Standard, 8=Anonymous

        # Build login payload
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

            # Extract auth token from response header
            self.auth_token = response.headers.get("X-MSTR-AuthToken")
            if not self.auth_token:
                raise MicroStrategyAuthenticationError(
                    "Authentication response missing X-MSTR-AuthToken header"
                )

            self.token_created_at = datetime.now()

            # Set auth token for all subsequent requests
            self.session.headers.update({"X-MSTR-AuthToken": self.auth_token})

            # Activate session (keep-alive)
            self._activate_session()

            logger.info("Successfully authenticated to MicroStrategy")

        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [401, 403]:
                raise MicroStrategyAuthenticationError(
                    f"Authentication failed: {e.response.text}"
                ) from e
            raise MicroStrategyAPIError(f"Login request failed: {e}") from e
        except requests.exceptions.RequestException as e:
            raise MicroStrategyAPIError(
                f"Failed to connect to MicroStrategy: {e}"
            ) from e

    def _activate_session(self) -> None:
        """
        Activate session with keep-alive request.

        This extends the session timeout.
        """
        try:
            response = self.session.put(
                f"{self.base_url}{self.SESSIONS}",
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            logger.debug("Session activated successfully")
        except Exception as e:
            logger.warning(f"Failed to activate session (non-fatal): {e}")

    def _ensure_valid_token(self) -> None:
        """
        Ensure authentication token is valid.

        Auto-refreshes token if nearing expiration.
        Critical for long-running ingestions.
        """
        if not self.auth_token or not self.token_created_at:
            logger.info("No active session, logging in...")
            self.login()
            return

        # Check if token is nearing expiration
        elapsed = datetime.now() - self.token_created_at
        if elapsed >= (self.session_timeout - self.REFRESH_THRESHOLD):
            logger.info("Token nearing expiration, refreshing session...")
            try:
                # Keep-alive via PUT /sessions extends token lifetime
                self._activate_session()
                self.token_created_at = datetime.now()
                logger.info("Session refreshed successfully")
            except Exception as e:
                logger.warning(f"Token refresh failed, re-authenticating: {e}")
                # Clear token and re-login
                self.auth_token = None
                self.login()

    def logout(self) -> None:
        """
        Terminate session.

        Raises:
            MicroStrategyAPIError: If logout fails (after clearing local state)
        """
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
            logger.error(f"Failed to logout MicroStrategy session: {e}")
            raise MicroStrategyAPIError(f"Logout failed: {e}") from e
        finally:
            # Always clear local state
            self.auth_token = None
            self.token_created_at = None
            if "X-MSTR-AuthToken" in self.session.headers:
                del self.session.headers["X-MSTR-AuthToken"]

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make authenticated API request with automatic token refresh.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (e.g., "/api/v2/projects")
            params: Query parameters
            json: JSON request body

        Returns:
            Response JSON as dictionary

        Raises:
            MicroStrategyPermissionError: If permission denied (403)
            MicroStrategyAPIError: For other API errors
        """
        # Ensure token is valid before request
        self._ensure_valid_token()

        url = f"{self.base_url}{endpoint}"

        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json,
                timeout=self.config.timeout_seconds,
            )

            # Handle specific error cases
            if response.status_code == 403:
                raise MicroStrategyPermissionError(
                    f"Permission denied for {endpoint}: {response.text}"
                )

            response.raise_for_status()

            # Return JSON if present, otherwise empty dict
            if response.content:
                return response.json()
            return {}

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.debug(f"Resource not found: {url}")
                return {}
            raise MicroStrategyAPIError(
                f"HTTP {e.response.status_code} error for {endpoint}: {e.response.text}"
            ) from e
        except requests.exceptions.Timeout as e:
            raise MicroStrategyAPIError(f"Request timeout for {endpoint}") from e
        except requests.exceptions.RequestException as e:
            raise MicroStrategyAPIError(f"Request failed for {endpoint}: {e}") from e

    def _paginated_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_size: int = 1000,
    ) -> Iterator[Dict[str, Any]]:
        """
        Make paginated API request.

        Yields individual items from all pages.

        Args:
            endpoint: API endpoint
            params: Query parameters
            page_size: Number of items per page

        Yields:
            Individual items from response
        """
        params = params or {}
        params["limit"] = page_size
        offset = 0

        while True:
            params["offset"] = offset
            response = self._request("GET", endpoint, params=params)

            # MicroStrategy returns items in various formats
            # Try common patterns
            items: List[Dict[str, Any]] = []
            if isinstance(response, list):
                items = response
            elif isinstance(response, dict):
                # Common response patterns
                items = (
                    response.get("result")  # /searches/results uses "result"
                    or response.get("value")
                    or response.get("data")
                    or response.get("items")
                    or []
                )

            if not items:
                break

            for item in items:
                yield item

            # Check if there are more pages
            if len(items) < page_size:
                break

            offset += page_size

    # API Methods

    def test_connection(self) -> bool:
        """
        Test API connectivity.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self:
                # Just verify we can login/logout
                pass
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def get_projects(self) -> List[Dict[str, Any]]:
        """
        Get all projects.

        Returns:
            List of project dictionaries
        """
        logger.debug("Fetching projects...")
        response = self._request("GET", "/api/projects")

        # Response is a list of projects or wrapped in "value" key
        projects = response if isinstance(response, list) else response.get("value", [])

        logger.info(f"Found {len(projects)} projects")
        return projects

    def get_project(self, project_id: str) -> Dict[str, Any]:
        """
        Get single project details.

        Args:
            project_id: Project ID

        Returns:
            Project dictionary
        """
        return self._request("GET", f"/api/v2/projects/{project_id}")

    def get_folders(self, project_id: str) -> List[Dict[str, Any]]:
        """
        Get folders in project.

        Args:
            project_id: Project ID

        Returns:
            List of folder dictionaries
        """
        logger.debug(f"Fetching folders for project {project_id}...")
        return list(self._paginated_request(f"/api/v2/projects/{project_id}/folders"))

    def get_folder(self, folder_id: str) -> Dict[str, Any]:
        """
        Get single folder details.

        Args:
            folder_id: Folder ID

        Returns:
            Folder dictionary
        """
        return self._request("GET", f"/api/v2/folders/{folder_id}")

    def get_dashboards(
        self, project_id: str, fallback_to_dossiers: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Get dashboards (formerly dossiers) in project.

        Supports both new "dashboards" and legacy "dossiers" endpoints
        for backward compatibility.

        Args:
            project_id: Project ID
            fallback_to_dossiers: Whether to fallback to legacy endpoint

        Returns:
            List of dashboard dictionaries
        """
        logger.debug(f"Fetching dashboards for project {project_id}...")

        # Try new endpoint first
        try:
            dashboards = list(
                self._paginated_request(f"/api/v2/projects/{project_id}/dashboards")
            )
            logger.info(f"Found {len(dashboards)} dashboards in project {project_id}")
            return dashboards
        except MicroStrategyAPIError as e:
            if fallback_to_dossiers and "404" in str(e):
                # Fallback to legacy endpoint for older instances
                logger.info(
                    "Using legacy 'dossiers' endpoint for backward compatibility"
                )
                try:
                    dashboards = list(
                        self._paginated_request(
                            f"/api/v2/projects/{project_id}/dossiers"
                        )
                    )
                    logger.info(
                        f"Found {len(dashboards)} dossiers in project {project_id}"
                    )
                    return dashboards
                except Exception as fallback_error:
                    logger.warning(
                        f"Both dashboards and dossiers endpoints failed: {fallback_error}"
                    )
                    return []
            raise

    def get_dashboard_definition(self, dashboard_id: str) -> Dict[str, Any]:
        """
        Get dashboard definition (visualizations and chapters).

        Args:
            dashboard_id: Dashboard/Dossier ID

        Returns:
            Dashboard definition dictionary
        """
        return self._request("GET", f"/api/v2/dossiers/{dashboard_id}/definition")

    def get_reports(self, project_id: str) -> List[Dict[str, Any]]:
        """
        Get reports in project.

        Args:
            project_id: Project ID

        Returns:
            List of report dictionaries
        """
        logger.debug(f"Fetching reports for project {project_id}...")
        reports = list(
            self._paginated_request(f"/api/v2/projects/{project_id}/reports")
        )
        logger.info(f"Found {len(reports)} reports in project {project_id}")
        return reports

    def get_report(self, report_id: str) -> Dict[str, Any]:
        """
        Get single report details.

        Args:
            report_id: Report ID

        Returns:
            Report dictionary
        """
        return self._request("GET", f"/api/v2/reports/{report_id}")

    def get_cubes(self, project_id: str) -> List[Dict[str, Any]]:
        """
        Get Intelligent Cubes in project.

        Args:
            project_id: Project ID

        Returns:
            List of cube dictionaries
        """
        logger.debug(f"Fetching cubes for project {project_id}...")
        cubes = list(self._paginated_request(f"/api/v2/projects/{project_id}/cubes"))
        logger.info(f"Found {len(cubes)} cubes in project {project_id}")
        return cubes

    def get_cube(self, cube_id: str) -> Dict[str, Any]:
        """
        Get single cube details.

        Args:
            cube_id: Cube ID

        Returns:
            Cube dictionary
        """
        return self._request("GET", f"/api/v2/cubes/{cube_id}")

    def get_cube_schema(self, cube_id: str) -> Dict[str, Any]:
        """
        Get cube schema (attributes and metrics).

        Args:
            cube_id: Cube ID

        Returns:
            Schema dictionary with attributes and metrics
        """
        return self._request("GET", f"/api/v2/cubes/{cube_id}/schema")

    def get_datasets(self, project_id: str) -> List[Dict[str, Any]]:
        """
        Get datasets in project.

        Args:
            project_id: Project ID

        Returns:
            List of dataset dictionaries
        """
        logger.debug(f"Fetching datasets for project {project_id}...")
        datasets = list(
            self._paginated_request(f"/api/v2/projects/{project_id}/datasets")
        )
        logger.info(f"Found {len(datasets)} datasets in project {project_id}")
        return datasets

    def get_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """
        Get single dataset details.

        Args:
            dataset_id: Dataset ID

        Returns:
            Dataset dictionary
        """
        return self._request("GET", f"/api/v2/datasets/{dataset_id}")

    def get_users(self) -> List[Dict[str, Any]]:
        """
        Get all users.

        Returns:
            List of user dictionaries
        """
        logger.debug("Fetching users...")
        users = list(self._paginated_request("/api/v2/users"))
        logger.info(f"Found {len(users)} users")
        return users

    def get_user(self, user_id: str) -> Dict[str, Any]:
        """
        Get single user details.

        Args:
            user_id: User ID

        Returns:
            User dictionary
        """
        return self._request("GET", f"/api/v2/users/{user_id}")

    def search_objects(
        self, project_id: str, object_type: int, limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Search for objects using the search API.

        This is the primary method for discovering content in MicroStrategy projects.
        The /v2/projects/{id}/dashboards endpoints often return empty results,
        while /searches/results reliably finds all objects.

        Args:
            project_id: Project ID to search within
            object_type: Object type code (3=report, 55=dashboard/dossier)
            limit: Maximum number of results per page

        Returns:
            List of object dictionaries

        Object Types (EnumDSSXMLObjectTypes):
            3 = Report
            8 = Folder
            39 = Intelligent Cube
            55 = Dashboard/Dossier (includes documents)
        """
        logger.debug(
            f"Searching for objects type={object_type} in project {project_id}..."
        )

        # Set project header for search context
        original_headers = self.session.headers.copy()
        self.session.headers.update({"X-MSTR-ProjectID": project_id})

        try:
            objects = list(
                self._paginated_request(
                    "/api/searches/results",
                    params={"type": object_type},
                    page_size=limit,
                )
            )
            logger.info(
                f"Found {len(objects)} objects (type={object_type}) in project {project_id}"
            )
            return objects
        finally:
            # Restore original headers
            self.session.headers = original_headers
