import logging
import time
from typing import Any, Dict, Iterable, List, Optional, Type, TypeVar

import requests
import urllib3
from pydantic import BaseModel, ValidationError

from datahub.ingestion.source.workday.config import WorkdayConfig
from datahub.ingestion.source.workday.constants import (
    OAUTH_ACCESS_TOKEN_KEY,
    OAUTH_EXPIRES_IN_KEY,
    PRISM_BASE_PATH,
    PRISM_BUCKETS_PATH,
    PRISM_DATA_SOURCES_PATH,
    PRISM_DATASET_PATH,
    PRISM_DATASETS_PATH,
    PRISM_TABLE_PATH,
    PRISM_TABLES_PATH,
    TOKEN_EXPIRY_SKEW_SECONDS,
    WORKDAY_GRANT_TYPE_CLIENT_CREDENTIALS,
    WORKDAY_OAUTH_TOKEN_PATH,
    WQL_BASE_PATH,
    WQL_CUSTOM_REPORTS_DATA_SOURCE,
    WQL_DATA_SOURCE_FIELDS_PATH,
    WQL_DATA_SOURCES_PATH,
)
from datahub.ingestion.source.workday.models import (
    CustomReport,
    PrismBucket,
    PrismDataset,
    PrismDataSource,
    PrismField,
    PrismTable,
    WqlDataSource,
)
from datahub.ingestion.source.workday.report import WorkdayReport

logger = logging.getLogger(__name__)

_ModelT = TypeVar("_ModelT", bound=BaseModel)

# Retry only genuinely transient statuses; other 4xx errors fail fast so bad
# credentials or missing objects are not re-hammered max_retries times.
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Cap backoff so a hostile Retry-After header or large max_retries cannot stall
# ingestion for minutes per request.
_MAX_RETRY_DELAY_SECONDS = 60


class WorkdayAPIError(RuntimeError):
    pass


class WorkdayAuthError(RuntimeError):
    """Token could not be obtained/refreshed and the run cannot continue.

    Kept distinct from WorkdayAPIError: per-object boundaries swallow API errors
    to keep going, but dead auth must propagate and abort the run.
    """


class WorkdayClient:
    def __init__(self, config: WorkdayConfig, report: WorkdayReport):
        self.config = config
        self.report = report
        self.base_url = config.base_url
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self._access_token: Optional[str] = None
        # Monotonic deadline (time.monotonic seconds) after which the cached
        # token must be refreshed.
        self._token_expiry: float = 0.0
        if not config.verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            self.report.warning(
                title="SSL Verification Disabled",
                message=(
                    "verify_ssl=False is set; TLS certificate validation is off "
                    "for Workday API calls."
                ),
            )

    # -- URL construction ---------------------------------------------------

    def _token_url(self) -> str:
        if self.config.token_url:
            return self.config.token_url
        path = WORKDAY_OAUTH_TOKEN_PATH.format(tenant=self.config.tenant)
        return f"{self.base_url}{path}"

    def _prism_url(self, path: str) -> str:
        base = PRISM_BASE_PATH.format(
            version=self.config.prism_api_version, tenant=self.config.tenant
        )
        return f"{self.base_url}{base}{path}"

    def _wql_url(self, path: str) -> str:
        base = WQL_BASE_PATH.format(tenant=self.config.tenant)
        # WQL versioning lives on the data-source path segment in some tenants;
        # keep it simple and pin the version into the base like Prism does.
        base = base.replace("/v1/", f"/{self.config.wql_api_version}/")
        return f"{self.base_url}{base}{path}"

    # -- Authentication -----------------------------------------------------

    def _ensure_token(self) -> None:
        """Fetch a bearer token if none is cached or the cached one is near expiry.

        The client-credentials grant issues no refresh token, so we simply
        request a fresh token from the client_id/secret whenever needed.
        """
        if self._access_token and time.monotonic() < self._token_expiry:
            return
        self._request_token()

    def _request_token(self) -> None:
        data = {
            "grant_type": WORKDAY_GRANT_TYPE_CLIENT_CREDENTIALS,
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret.get_secret_value(),
        }
        try:
            response = self.session.post(
                self._token_url(),
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=self.config.timeout_seconds,
                verify=self.config.verify_ssl,
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as error:
            self.report.report_api_error()
            raise WorkdayAuthError(
                f"Workday OAuth token request failed: {error}"
            ) from error

        token = payload.get(OAUTH_ACCESS_TOKEN_KEY)
        if not isinstance(token, str) or not token:
            raise WorkdayAuthError(
                "Workday OAuth token response did not contain an access_token"
            )
        self._access_token = token
        self.session.headers.update({"Authorization": f"Bearer {token}"})
        expires_in = payload.get(OAUTH_EXPIRES_IN_KEY)
        lifetime = expires_in if isinstance(expires_in, (int, float)) else 3600
        # Refresh a little early so a request never fires with a token that
        # expires in-flight.
        self._token_expiry = time.monotonic() + max(
            float(lifetime) - TOKEN_EXPIRY_SKEW_SECONDS, 0.0
        )
        self.report.report_token_issued()

    def close(self) -> None:
        self.session.close()

    # -- Request plumbing ---------------------------------------------------

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        self._ensure_token()
        max_attempts = self.config.max_retries + 1
        last_error: Optional[Exception] = None
        reauth_attempted = False
        attempt = 0
        while attempt < max_attempts:
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    timeout=self.config.timeout_seconds,
                    verify=self.config.verify_ssl,
                    **kwargs,
                )
            except requests.RequestException as error:
                # Network-level failures are transient; HTTP error statuses are
                # handled below so non-retryable 4xx responses fail fast.
                last_error = error
                if attempt < max_attempts - 1:
                    time.sleep(min(2**attempt, _MAX_RETRY_DELAY_SECONDS))
                    attempt += 1
                    continue
                self.report.report_api_error()
                raise WorkdayAPIError(
                    f"Workday API request failed: {method} {url}: {error}"
                ) from error

            if response.status_code == 401 and not reauth_attempted:
                # A short-lived token can lapse mid-run; one forced refresh +
                # replay recovers long runs. This does not consume a retry
                # attempt: it is auth recovery, not an endpoint retry.
                logger.info("Workday returned 401 on %s; refreshing token", url)
                self._force_token_refresh(method, url)
                reauth_attempted = True
                continue
            if (
                response.status_code in _RETRYABLE_STATUS_CODES
                and attempt < max_attempts - 1
            ):
                time.sleep(self._retry_delay(response, attempt))
                attempt += 1
                continue
            try:
                response.raise_for_status()
            except requests.HTTPError as error:
                self.report.report_api_error()
                raise WorkdayAPIError(
                    f"Workday API request failed: {method} {url}: {error}"
                ) from error
            return response

        raise WorkdayAPIError(
            f"Workday API request failed: {method} {url}: {last_error}"
        )

    def _force_token_refresh(self, method: str, url: str) -> None:
        self._access_token = None
        self._token_expiry = 0.0
        try:
            self._request_token()
        except WorkdayAuthError:
            raise
        except Exception as error:
            raise WorkdayAuthError(
                f"Workday token refresh failed after 401: {method} {url}: {error}"
            ) from error

    @staticmethod
    def _retry_delay(response: requests.Response, attempt: int) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(max(float(retry_after), 1.0), _MAX_RETRY_DELAY_SECONDS)
            except ValueError:
                pass
        return float(min(2**attempt, _MAX_RETRY_DELAY_SECONDS))

    def _get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        response = self._request("GET", url, params=params)
        if not response.content:
            return {}
        try:
            return response.json()
        except ValueError as error:
            self.report.report_api_error()
            raise WorkdayAPIError(
                f"Workday API returned a non-JSON response: GET {url} "
                f"status={response.status_code}"
            ) from error

    # -- Parsing helpers ----------------------------------------------------

    def _parse_model(
        self, model_cls: Type[_ModelT], item: Dict[str, object], context: str
    ) -> Optional[_ModelT]:
        try:
            return model_cls.model_validate(item)
        except ValidationError as error:
            self.report.report_malformed_object(
                f"{context}: {model_cls.__name__} id={item.get('id')!r}"
            )
            self.report.warning(
                title="Skipped malformed Workday object",
                message="An API object did not match the expected shape and was skipped.",
                context=f"{context}, model={model_cls.__name__}, keys={sorted(item)[:10]}",
                exc=error,
            )
            return None

    def _parse_models(
        self, model_cls: Type[_ModelT], items: Iterable[object], context: str
    ) -> List[_ModelT]:
        parsed = [
            self._parse_model(model_cls, item, context)
            for item in items
            if isinstance(item, dict)
        ]
        return [item for item in parsed if item is not None]

    @staticmethod
    def _extract_items(payload: object) -> List[object]:
        """Pull the collection array from a Workday list response.

        Workday collection endpoints return {"total": N, "data": [...]}; older
        or proxied shapes use "items"/"results". A bare list is returned as-is.
        """
        if isinstance(payload, list):
            return payload
        if not isinstance(payload, dict):
            return []
        for key in ("data", "items", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
        return []

    def _iter_paged(self, url: str) -> Iterable[Dict[str, object]]:
        """Yield raw objects across all pages of a Prism collection endpoint.

        Uses limit/offset paging and stops when a page returns fewer than the
        requested limit, guarding against a server that ignores paging params.
        """
        offset = 0
        limit = self.config.page_size
        while True:
            payload = self._get_json(url, params={"limit": limit, "offset": offset})
            items = self._extract_items(payload)
            for item in items:
                if isinstance(item, dict):
                    yield item
            if len(items) < limit:
                return
            offset += limit

    # -- Prism resources ----------------------------------------------------

    def list_tables(self) -> List[PrismTable]:
        url = self._prism_url(PRISM_TABLES_PATH)
        items = list(self._iter_paged(url))
        return self._parse_models(PrismTable, items, f"GET {PRISM_TABLES_PATH}")

    def get_table(self, table_id: str) -> Optional[PrismTable]:
        url = self._prism_url(PRISM_TABLE_PATH.format(id=table_id))
        payload = self._get_json(url)
        item = payload if isinstance(payload, dict) else None
        if item and "data" in item and isinstance(item["data"], dict):
            item = item["data"]
        if not isinstance(item, dict):
            return None
        return self._parse_model(PrismTable, item, f"GET {PRISM_TABLE_PATH}")

    def list_datasets(self) -> List[PrismDataset]:
        url = self._prism_url(PRISM_DATASETS_PATH)
        items = list(self._iter_paged(url))
        return self._parse_models(PrismDataset, items, f"GET {PRISM_DATASETS_PATH}")

    def get_dataset(self, dataset_id: str) -> Optional[PrismDataset]:
        url = self._prism_url(PRISM_DATASET_PATH.format(id=dataset_id))
        payload = self._get_json(url)
        item = payload if isinstance(payload, dict) else None
        if item and "data" in item and isinstance(item["data"], dict):
            item = item["data"]
        if not isinstance(item, dict):
            return None
        return self._parse_model(PrismDataset, item, f"GET {PRISM_DATASET_PATH}")

    def list_data_sources(self) -> List[PrismDataSource]:
        url = self._prism_url(PRISM_DATA_SOURCES_PATH)
        items = list(self._iter_paged(url))
        return self._parse_models(
            PrismDataSource, items, f"GET {PRISM_DATA_SOURCES_PATH}"
        )

    def list_buckets(self) -> List[PrismBucket]:
        url = self._prism_url(PRISM_BUCKETS_PATH)
        items = list(self._iter_paged(url))
        return self._parse_models(PrismBucket, items, f"GET {PRISM_BUCKETS_PATH}")

    # -- WQL resources (business-object catalog + custom reports) -----------

    def list_business_objects(self) -> List[WqlDataSource]:
        url = self._wql_url(WQL_DATA_SOURCES_PATH)
        items = list(self._iter_paged(url))
        return self._parse_models(WqlDataSource, items, f"GET {WQL_DATA_SOURCES_PATH}")

    def get_business_object_fields(self, data_source_id: str) -> List[PrismField]:
        url = self._wql_url(WQL_DATA_SOURCE_FIELDS_PATH.format(id=data_source_id))
        payload = self._get_json(url)
        items = self._extract_items(payload)
        return self._parse_models(
            PrismField, items, f"GET {WQL_DATA_SOURCE_FIELDS_PATH}"
        )

    def list_custom_reports(self) -> List[CustomReport]:
        """Enumerate custom reports by querying the allCustomReports WQL object.

        ponytail: relies on the tenant exposing the standard allCustomReports
        data source; tenants that restrict it yield an empty list rather than an
        error (the query 4xx is swallowed per-boundary by the caller).
        """
        url = self._wql_url(WQL_DATA_SOURCES_PATH)
        report_url = f"{url}/{WQL_CUSTOM_REPORTS_DATA_SOURCE}/data"
        items = list(self._iter_paged(report_url))
        return self._parse_models(
            CustomReport, items, f"GET {WQL_CUSTOM_REPORTS_DATA_SOURCE}/data"
        )
