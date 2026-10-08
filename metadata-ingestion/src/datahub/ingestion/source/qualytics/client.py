"""HTTP client for the Qualytics REST API.

Kept deliberately separate from `source.py` (`standards/api.md`): this file knows about
HTTP, auth, paging and retries, and nothing about DataHub.

Qualytics deployments are single-tenant and frequently private -- self-hosted, inside a
VPC, or behind a corporate CA -- and DataHub ingestion may be running inside the
customer's network. Hence the TLS knobs and the generous, configurable timeout.
"""

import logging
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlsplit

import requests
from pydantic import ValidationError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.qualytics.config import QualyticsSourceConfig
from datahub.ingestion.source.qualytics.constants import (
    ANOMALIES_PATH,
    CONTAINER_FIELD_PROFILES_PATH,
    CONTAINER_PROFILE_PATH,
    CONTAINERS_PATH,
    DATASTORES_PATH,
    OPENAPI_PATH,
    QUALITY_CHECKS_PATH,
)
from datahub.ingestion.source.qualytics.models import Page

logger = logging.getLogger(__name__)

# Transient conditions worth retrying. 429 is included because Qualytics fronts its API
# with a rate limiter; the rest are ordinary gateway flakiness.
_RETRY_STATUSES = (429, 500, 502, 503, 504)

# The detail Qualytics puts on the 404 for a container that has never been profiled.
_NOT_PROFILED = "has not been profiled"


class QualyticsApiError(Exception):
    """A request to Qualytics failed.

    Callers decide the blast radius -- one item, one container, one datastore. The
    subclass QualyticsAuthError is the exception: it always ends the run.
    """


class QualyticsAuthError(QualyticsApiError):
    """The token was rejected (401/403).

    Distinct from the base error because there is no point continuing: every subsequent
    request will fail the same way, and a per-item warning storm would bury the cause.
    """


class QualyticsNotFoundError(QualyticsApiError):
    """The resource does not exist (404).

    Its own type because Qualytics uses 404 for a state, not only for a bad path:
    ``/containers/{id}/profile`` answers 404 "has not been profiled" for every container
    that has never been profiled. ``body`` keeps the response so callers can tell the
    two apart.
    """

    def __init__(self, message: str, body: str) -> None:
        super().__init__(message)
        self.body = body


class QualyticsClient:
    """Thin, typed wrapper over the Qualytics REST API."""

    def __init__(self, config: QualyticsSourceConfig) -> None:
        self.config = config
        self.base_url = config.base_url.rstrip("/")

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {config.token.get_secret_value()}",
                "Accept": "application/json",
            }
        )

        # urllib3 handles the backoff schedule (1s, 2s, 4s...) and honours Retry-After
        # on 429, which a hand-rolled loop usually forgets.
        retry = Retry(
            total=config.max_retries,
            backoff_factor=1,
            status_forcelist=_RETRY_STATUSES,
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # requests treats verify as "True, False, or a CA bundle path", so a configured
        # ca_cert_path both enables verification and points it at the private CA.
        self._verify: bool | str = config.ca_cert_path or config.verify_ssl

    def close(self) -> None:
        self.session.close()

    # --- requests -----------------------------------------------------------------

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """GET a path relative to `base_url`, returning parsed JSON."""
        url = self._url(path)
        try:
            response = self.session.get(
                url,
                params=params,
                timeout=self.config.timeout_sec,
                verify=self._verify,
            )
        except requests.exceptions.SSLError as e:
            raise QualyticsApiError(
                f"TLS verification failed for {url}. If this deployment is fronted by a "
                f"private or corporate CA, set ca_cert_path to that CA bundle."
            ) from e
        except requests.exceptions.RequestException as e:
            raise QualyticsApiError(f"Request to {url} failed: {e}") from e

        if response.status_code in (401, 403):
            raise QualyticsAuthError(
                f"Qualytics rejected the API token ({response.status_code}) for {url}. "
                f"Check the token is valid and its user can read datastores, "
                f"containers, fields, quality checks and anomalies."
            )
        if response.status_code >= 400:
            message = (
                f"GET {url} returned {response.status_code}: {response.text[:300]}"
            )
            if response.status_code == 404:
                raise QualyticsNotFoundError(message, body=response.text)
            raise QualyticsApiError(message)

        try:
            return response.json()
        except ValueError as e:
            raise QualyticsApiError(f"GET {url} returned a non-JSON body") from e

    def paginate(
        self, path: str, params: dict[str, Any] | None = None
    ) -> Iterator[dict[str, Any]]:
        """Yield every item across the pages of a Qualytics list endpoint.

        A generator rather than a list: some tenants have tens of thousands of quality
        checks, and holding them all in memory to return one list is the memory bug the
        performance standard warns about.

        Qualytics uses the fastapi-pagination envelope -- `?page=N&size=M` returning
        `{items, page, pages, size, total}` -- uniformly across every list endpoint.
        """
        page = 1
        while True:
            payload = self.get(
                path,
                params={**(params or {}), "page": page, "size": self.config.page_size},
            )
            try:
                envelope = Page[dict[str, Any]].model_validate(payload)
            except ValidationError as e:
                # Not cosmetic. The previous `payload.get("items") or []` turned any
                # envelope change into a clean run that emitted nothing, scanned
                # nothing and exited 0 -- indistinguishable from an empty tenant.
                raise QualyticsApiError(
                    f"GET {self._url(path)} did not return a Qualytics page envelope "
                    f"({{items, page, pages, size, total}}): {e}"
                ) from e

            yield from envelope.items

            if page >= envelope.pages:
                return
            page += 1

    # --- capability probes --------------------------------------------------------

    def get_openapi_spec(self) -> dict[str, Any]:
        """Fetch this deployment's own OpenAPI spec.

        Not cached: it is several megabytes parsed, and each caller needs it once --
        get_version at the start of a run, the root-path check in test_connection.
        Holding it for the length of a run would cost memory for nothing.
        """
        spec = self.get(OPENAPI_PATH)
        if not isinstance(spec, dict):
            raise QualyticsApiError("openapi.json did not return an object")
        return spec

    def get_version(self) -> str | None:
        """Best-effort Qualytics build version, for the ingestion report.

        Every deployment runs its own build, so recording which one a run talked to is
        the first thing you want when a mapping behaves unexpectedly.
        """
        try:
            spec = self.get_openapi_spec()
        except QualyticsAuthError:
            # A rejected token is not "version unavailable" -- it is the run's real
            # failure, and this is the first call of the ingestion. Swallowing it
            # discarded the actionable 401 and let the run limp on to fail later.
            raise
        except QualyticsApiError as e:
            logger.warning(
                "Could not read the Qualytics version from openapi.json: %s", e
            )
            return None
        info = spec.get("info")
        return info.get("version") if isinstance(info, dict) else None

    def detect_api_root_path(self) -> str | None:
        """Read the deployment's API root path out of its own spec.

        Each deployment configures its own API root path, so `/api` is a default
        rather than a guarantee, and it may be more than one segment behind a gateway.
        The spec's path keys carry it: whatever precedes the datastores path is the
        root. Anchoring on a path the connector calls, rather than on what the keys
        have in common, keeps one stray top-level route from hiding the answer.
        Returns None when the spec is unavailable or has no datastores path.
        """
        try:
            spec = self.get_openapi_spec()
        except QualyticsAuthError:
            raise
        except QualyticsApiError:
            return None

        paths = spec.get("paths")
        if not isinstance(paths, dict) or not paths:
            return None

        roots = [
            p[: -len(DATASTORES_PATH)]
            for p in paths
            if isinstance(p, str) and p.endswith(DATASTORES_PATH)
        ]
        if not roots:
            return None
        return min(roots, key=len).rstrip("/")

    def check_base_url_root_path(self) -> str | None:
        """Return a human-readable problem with `base_url`'s root path, or None.

        The single most likely recipe mistake is giving the deployment origin without
        the `/api` suffix, which turns every request into a 404. Catching it in
        test_connection beats letting the user watch an empty ingestion run.
        """
        detected = self.detect_api_root_path()
        if detected is None:
            return None

        configured = urlsplit(self.base_url).path.rstrip("/")
        if configured == detected:
            return None
        if not configured:
            return (
                f"base_url has no path, but this deployment serves its API under "
                f"'{detected}'. Use '{self.base_url}{detected}'."
            )
        return (
            f"base_url ends in '{configured}', but this deployment serves its API under "
            f"'{detected}'."
        )

    # --- resources ----------------------------------------------------------------

    def list_datastores(self) -> Iterator[dict[str, Any]]:
        yield from self.paginate(DATASTORES_PATH)

    def list_containers(
        self, datastore_id: int | None = None
    ) -> Iterator[dict[str, Any]]:
        params: dict[str, Any] = {}
        if datastore_id is not None:
            params["datastore"] = datastore_id
        yield from self.paginate(CONTAINERS_PATH, params)

    def get_container_profile(self, container_id: int) -> dict[str, Any] | None:
        """Latest profile for a container, or None if it has never been profiled.

        Qualytics reports "never profiled" as a 404 whose detail says so. Only that 404
        means None: any other -- a route a deployment does not have, say -- is raised,
        so a missing endpoint shows up as failed profiles rather than as a tenant where
        nothing has ever been profiled.
        """
        try:
            payload = self.get(CONTAINER_PROFILE_PATH.format(id=container_id))
        except QualyticsNotFoundError as e:
            if _NOT_PROFILED in e.body:
                return None
            raise
        if not isinstance(payload, dict):
            raise QualyticsApiError(
                f"{CONTAINER_PROFILE_PATH.format(id=container_id)} returned "
                f"{type(payload).__name__}, not a profile object"
            )
        # An empty object is returned as-is, not as None: only the 404 above means
        # "never profiled", and a malformed body should fail validation visibly.
        return payload

    def list_container_field_profiles(
        self, container_id: int
    ) -> Iterator[dict[str, Any]]:
        yield from self.paginate(CONTAINER_FIELD_PROFILES_PATH.format(id=container_id))

    def list_quality_checks(
        self, container_id: int | None = None
    ) -> Iterator[dict[str, Any]]:
        # No `archived` param: the spec types it as Literal["include", "only"], so
        # sending a bool 422s the request. Omitting it already means "exclude
        # archived", which is the intent. archived=False made every quality-check
        # listing fail against a real deployment while requests_mock accepted it --
        # see test_api_params.py, which now checks outgoing params against the spec.
        params: dict[str, Any] = {}
        if container_id is not None:
            params["container"] = container_id
        yield from self.paginate(QUALITY_CHECKS_PATH, params)

    def list_anomalies(
        self,
        container_id: int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        # See list_quality_checks: `archived` is an enum, not a bool.
        params: dict[str, Any] = {}
        if container_id is not None:
            params["container"] = container_id
        if start_date is not None:
            params["start_date"] = start_date
        if end_date is not None:
            params["end_date"] = end_date
        yield from self.paginate(ANOMALIES_PATH, params)


__all__ = [
    "QualyticsApiError",
    "QualyticsAuthError",
    "QualyticsClient",
    "QualyticsNotFoundError",
]
