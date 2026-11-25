"""API client for Grafana metadata extraction"""

import hashlib
import json
import logging
from typing import Dict, Iterator, List, Optional, Sequence, Set, Union

import requests
import urllib3.exceptions
from pydantic import SecretStr, ValidationError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.grafana.constants import (
    DASHBOARD_FETCH_ERROR_MESSAGE,
    DASHBOARD_FETCH_ERROR_TITLE,
    DASHBOARD_PARSE_ERROR_MESSAGE,
    DASHBOARD_PARSE_ERROR_TITLE,
    DASHBOARD_SEARCH_ERROR_MESSAGE,
    DASHBOARD_SEARCH_ERROR_TITLE,
    DASHBOARD_SEARCH_UNEXPECTED_RESPONSE_MESSAGE,
    FOLDER_FETCH_ERROR_MESSAGE,
    FOLDER_FETCH_ERROR_TITLE,
    FOLDER_PARSE_ERROR_MESSAGE,
    FOLDER_PARSE_ERROR_TITLE,
    FOLDER_UNEXPECTED_RESPONSE_MESSAGE,
    HTTP_NOT_FOUND,
    HTTP_PERMISSION_STATUS_CODES,
    HTTP_RETRY_BACKOFF_FACTOR,
    HTTP_RETRY_MAX_TIMES,
    HTTP_RETRY_STATUS_CODES,
    REPEATED_PAGE_MESSAGE,
    REPEATED_PAGE_TITLE,
    SEARCH_HIT_SKIPPED_MESSAGE,
    SEARCH_HIT_SKIPPED_TITLE,
)
from datahub.ingestion.source.grafana.models import (
    Dashboard,
    DashboardListing,
    DashboardSearchHit,
    Folder,
    FolderListing,
)
from datahub.ingestion.source.grafana.report import GrafanaSourceReport

logger = logging.getLogger(__name__)


def _is_permission_error(e: requests.exceptions.RequestException) -> bool:
    # A requests.Response is falsy for 4xx/5xx, so compare against None.
    return (
        e.response is not None
        and e.response.status_code in HTTP_PERMISSION_STATUS_CODES
    )


class _DashboardNotFoundError(Exception):
    pass


class GrafanaAPIClient:
    """Client for making requests to Grafana API"""

    def __init__(
        self,
        base_url: str,
        token: SecretStr,
        verify_ssl: bool,
        page_size: int,
        report: GrafanaSourceReport,
        skip_text_panels: bool = False,
    ) -> None:
        self.base_url = base_url
        self.verify_ssl = verify_ssl
        self.page_size = page_size
        self.report = report
        self.skip_text_panels = skip_text_panels
        self.session = self._create_session(token)

    def _create_session(self, token: SecretStr) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {token.get_secret_value()}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )
        session.verify = self.verify_ssl
        # Without retries one transient error skips a dashboard, and with stale
        # removal on, a skipped dashboard fails the whole run.
        adapter = HTTPAdapter(
            max_retries=Retry(
                total=HTTP_RETRY_MAX_TIMES,
                status_forcelist=HTTP_RETRY_STATUS_CODES,
                backoff_factor=HTTP_RETRY_BACKOFF_FACTOR,
                raise_on_status=False,
            )
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # If SSL verification is disabled, suppress the warnings
        if not self.verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            self.report.warning(
                title="SSL Configuration Warning",
                message="SSL Verification is recommended.",
            )

        return session

    def get_folders(self) -> FolderListing:
        """Fetch all folders from Grafana with pagination."""
        listing = FolderListing()
        for batch in self._iter_pages(
            path="/api/folders",
            params={},
            error_title=FOLDER_FETCH_ERROR_TITLE,
            error_message=FOLDER_FETCH_ERROR_MESSAGE,
            unexpected_response_message=FOLDER_UNEXPECTED_RESPONSE_MESSAGE,
        ):
            self._parse_folders(batch, listing)
        return listing

    def _parse_folders(self, batch: Sequence[object], listing: FolderListing) -> None:
        """Validate each folder separately so one bad entry costs only itself."""
        for folder in batch:
            try:
                listing.folders.append(Folder.model_validate(folder))
            except ValidationError as e:
                listing.unparseable += 1
                self.report.report_folder_unparseable()
                self.report.warning(
                    title=FOLDER_PARSE_ERROR_TITLE,
                    message=FOLDER_PARSE_ERROR_MESSAGE,
                    context=str(folder),
                    exc=e,
                )

    def get_dashboard(self, uid: str) -> Optional[Dashboard]:
        """Fetch a specific dashboard by UID"""
        try:
            return self._fetch_dashboard(uid)
        except _DashboardNotFoundError as e:
            self.report.warning(
                title=DASHBOARD_FETCH_ERROR_TITLE,
                message=DASHBOARD_FETCH_ERROR_MESSAGE,
                context=uid,
                exc=e,
            )
            return None

    def _fetch_dashboard(self, uid: str) -> Optional[Dashboard]:
        """Return the dashboard, or None if it could not be read. A 404 raises
        _DashboardNotFoundError so the caller decides what it means."""
        try:
            response = self.session.get(f"{self.base_url}/api/dashboards/uid/{uid}")
            response.raise_for_status()
            dashboard_data = response.json()
            if self.skip_text_panels and isinstance(dashboard_data, dict):
                dashboard_data["_skip_text_panels"] = True
            return Dashboard.model_validate(dashboard_data)
        except requests.exceptions.RequestException as e:
            if e.response is not None and e.response.status_code == HTTP_NOT_FOUND:
                raise _DashboardNotFoundError(uid) from e
            self.report.warning(
                title=DASHBOARD_FETCH_ERROR_TITLE,
                message=DASHBOARD_FETCH_ERROR_MESSAGE,
                context=uid,
                exc=e,
            )
            if _is_permission_error(e):
                self.report.report_permission_warning()
            return None
        except ValidationError as e:
            self.report.report_dashboard_unparseable()
            self.report.warning(
                title=DASHBOARD_PARSE_ERROR_TITLE,
                message=DASHBOARD_PARSE_ERROR_MESSAGE,
                context=uid,
                exc=e,
            )
            return None

    def get_dashboards(self) -> DashboardListing:
        """Fetch all dashboards found by /api/search, with pagination."""
        listing = DashboardListing()
        seen_uids: Set[str] = set()
        for batch in self._iter_pages(
            path="/api/search",
            params={"type": "dash-db"},
            error_title=DASHBOARD_SEARCH_ERROR_TITLE,
            error_message=DASHBOARD_SEARCH_ERROR_MESSAGE,
            unexpected_response_message=DASHBOARD_SEARCH_UNEXPECTED_RESPONSE_MESSAGE,
        ):
            for hit in self.parse_search_hits(batch):
                if hit is None:
                    listing.skipped += 1
                    continue
                if hit.uid in seen_uids:
                    continue
                seen_uids.add(hit.uid)
                try:
                    dashboard = self._fetch_dashboard(hit.uid)
                except _DashboardNotFoundError:
                    # Deleted between search and fetch, so it is gone from
                    # Grafana: not a skip, and stale removal may delete it.
                    self.report.report_dashboard_deleted_after_search()
                    logger.info(f"Dashboard {hit.uid} was deleted during ingestion")
                    continue
                if dashboard is None:
                    listing.skipped += 1
                else:
                    listing.dashboards.append(dashboard)
        return listing

    def parse_search_hits(
        self, batch: Sequence[object], require_title_and_url: bool = False
    ) -> List[Optional[DashboardSearchHit]]:
        """Validate each /api/search result; a result that cannot be used is
        reported and returned as None, so callers can count it."""
        hits: List[Optional[DashboardSearchHit]] = []
        for result in batch:
            hit: Optional[DashboardSearchHit] = None
            try:
                hit = DashboardSearchHit.model_validate(result)
            except ValidationError:
                pass
            if (
                hit is not None
                and require_title_and_url
                and not (hit.title and hit.url)
            ):
                hit = None
            if hit is None:
                self.report.report_dashboard_search_hit_skipped()
                self.report.warning(
                    title=SEARCH_HIT_SKIPPED_TITLE,
                    message=SEARCH_HIT_SKIPPED_MESSAGE,
                    context=str(result),
                )
            hits.append(hit)
        return hits

    def _iter_pages(
        self,
        path: str,
        params: Dict[str, Union[str, int]],
        error_title: str,
        error_message: str,
        unexpected_response_message: str,
    ) -> Iterator[List[object]]:
        """Yield each page of a paginated Grafana list endpoint.

        Paging ends at the first empty page. It also ends, with a failure, on a
        page that is not a list or that cannot be fetched, and when Grafana
        returns a full page it has already returned, which means it is ignoring
        the page parameter and later pages may be missing. A failure makes
        stale-entity removal skip the run, so nothing missing is soft-deleted.
        A repeated page shorter than the page size is the whole listing, so
        paging just stops.
        """
        seen_pages: Set[str] = set()
        page = 1
        while True:
            try:
                response = self.session.get(
                    f"{self.base_url}{path}",
                    params={**params, "page": page, "limit": self.page_size},
                )
                response.raise_for_status()
                batch = response.json()
            except requests.exceptions.RequestException as e:
                self.report.failure(
                    title=error_title,
                    message=error_message,
                    context=f"page {page}",
                    exc=e,
                )
                if _is_permission_error(e):
                    self.report.report_permission_warning()
                return

            # Check the shape before emptiness: a non-list page (e.g. a proxy
            # error body) is a failure even when it is {} or null.
            if not isinstance(batch, list):
                self.report.failure(
                    title=error_title,
                    message=unexpected_response_message,
                    context=f"page {page}: {batch}",
                )
                return
            if not batch:
                return

            # Order-insensitive, so a page returned again in another order
            # also counts as repeated.
            fingerprint = hashlib.sha256(
                json.dumps(
                    sorted(
                        json.dumps(item, sort_keys=True, default=str) for item in batch
                    )
                ).encode()
            ).hexdigest()
            if fingerprint in seen_pages:
                if len(batch) >= self.page_size:
                    self.report.failure(
                        title=REPEATED_PAGE_TITLE,
                        message=REPEATED_PAGE_MESSAGE,
                        context=f"{path} page {page}",
                    )
                return
            seen_pages.add(fingerprint)

            yield batch
            page += 1
