# src/datahub/ingestion/source/sap_datasphere/client.py
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Dict,
    Generator,
    Iterator,
    List,
    Optional,
    cast,
)
from urllib.parse import quote, urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.sap_datasphere.config import SapDatasphereConfig
from datahub.ingestion.source.sap_datasphere.platform_mapping import ConnectionRecord
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.str_enum import StrEnum

if TYPE_CHECKING:
    from datahub.ingestion.source.sap_datasphere.report import SapDatasphereReport

logger = logging.getLogger(__name__)

# SAP's catalog service caps page size at 500 records — this is both the
# default and the documented MAXIMUM ("Consuming Data Exposed by SAP Datasphere",
# OData API → Pagination). Requesting more is silently capped at 500 by the
# server, so we ask for exactly the documented maximum. The paginator below does
# NOT rely on this value for correctness — it advances by the number of records
# actually returned — but asking for >500 would just waste the over-ask.
_PAGE_SIZE = 500
# SAP-supported catalog API. The legacy `/api/v1/dwc/catalog/` prefix still
# works but is documented as deprecated; the modern `/api/v1/datasphere/` path
# is what's published in SAP's REST API Reference (see SAP API Business Hub).
_CATALOG_BASE = "/api/v1/datasphere/consumption/catalog"
# Threshold beyond which the connections endpoint may be silently truncating.
# The SAP Datasphere connections API is not documented to support pagination;
# 100 is a reasonable yellow-line based on common REST defaults. If your tenant
# legitimately has >=100 connections in a single space, raise a tracking issue
# so we can confirm the API's behaviour and adjust the threshold.
_CONNECTIONS_TRUNCATION_THRESHOLD = 100


class EdmxFetchReason(StrEnum):
    """Why an EDMX fetch produced (or failed to produce) schema XML.

    Lets the caller distinguish a benign 404 (asset simply not OData-exposed)
    from a 403 (permission misconfiguration the client already warned about)
    from a genuine error, instead of collapsing them all into ``None``.
    """

    OK = "ok"
    FORBIDDEN = "forbidden"  # 403 — principal lacks OData read (already warned here)
    NOT_FOUND = "not_found"  # 404 — asset not exposed via OData (benign)
    ERROR = "error"  # network / non-2xx / other


@dataclass(frozen=True)
class EdmxFetchResult:
    xml: Optional[str]
    reason: EdmxFetchReason

    def __post_init__(self) -> None:
        # Invariant: xml is present iff the fetch succeeded. Enforcing it makes
        # the illegal states (OK-with-no-xml, or a non-OK reason carrying xml)
        # unrepresentable, so callers can branch on `reason` and trust `xml`.
        if (self.reason is EdmxFetchReason.OK) != (self.xml is not None):
            raise ValueError(
                f"EdmxFetchResult invariant violated: xml must be set iff "
                f"reason is OK (got reason={self.reason}, xml_is_none="
                f"{self.xml is None})"
            )


class SapDatasphereClient:
    def __init__(
        self,
        config: SapDatasphereConfig,
        report: Optional["SapDatasphereReport"] = None,
    ) -> None:
        self.config = config
        self._report = report
        self.session = self._build_session()
        self._auth_initialized = False
        # Per-space cache for `list_connections`; populated lazily on first call
        # per space.
        self._connections_cache: Dict[str, List[ConnectionRecord]] = {}
        if config.token:
            # Raw token — set Authorization eagerly; no network call needed.
            self.session.headers["Authorization"] = (
                f"Bearer {config.token.get_secret_value()}"
            )
            self._auth_initialized = True

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=self.config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({"Accept": "application/json"})
        return session

    @contextmanager
    def _timed_api(self, operation: str, url: Optional[str] = None) -> Iterator[None]:
        """Time the wrapped outbound HTTP request and attribute it to ``operation``.

        Uses try/finally so the call is timed regardless of outcome (a failed or
        retried request still reflects the latency the operator experienced).
        Overhead per call is a ``perf_counter`` delta plus a dict/heap update.
        """
        timer = PerfTimer()
        timer.start()
        try:
            yield
        finally:
            if self._report is not None:
                self._report.report_api_call(operation, timer.elapsed_seconds(), url)

    def _ensure_auth(self) -> None:
        """Fetch and cache the OAuth token on the first call; no-op thereafter.

        The Authorization header is always re-set from the freshly fetched
        token so that ``_refresh_auth`` (which flips ``_auth_initialized`` back
        to False) reliably replaces an expired bearer.
        """
        if self._auth_initialized:
            return
        token = self._get_token()
        self.session.headers["Authorization"] = f"Bearer {token}"
        self._auth_initialized = True

    def _refresh_auth(self) -> None:
        """Force re-fetch of the OAuth token and update the session header.

        Called on a 401 response — see ``_get``. A second 401 after refresh is
        treated as a hard credentials failure (no infinite retry loop).
        """
        self._auth_initialized = False
        self._ensure_auth()

    def _get_token(self) -> str:
        """Priority: token > refresh_token > client_credentials."""
        cfg = self.config
        if cfg.token:
            return cfg.token.get_secret_value()
        if cfg.refresh_token and cfg.client_id:
            return self._exchange_refresh_token()
        if cfg.client_id and cfg.client_secret:
            return self._fetch_client_credentials_token()
        raise ValueError(
            "No auth configured. Set token, refresh_token+client_id, or client_id+client_secret."
        )

    def _post_token(self, payload: Dict[str, str]) -> str:
        """POST to the XSUAA token endpoint with a form-encoded payload and
        return the ``access_token`` from the response.

        The flow-specific guard rails (missing ``client_id``, missing
        ``refresh_token``, missing ``client_secret``, ...) are enforced by the
        caller; this helper only handles the network round-trip and the shared
        error-shape parsing so the two OAuth grant flows can share code.
        """
        cfg = self.config
        if not cfg.xsuaa_url:
            raise ValueError(
                "xsuaa_url is required for OAuth; set it explicitly or use a standard SAP tenant URL."
            )
        token_url = f"{cfg.xsuaa_url}/oauth/token"
        with self._timed_api("oauth_token", token_url):
            resp = requests.post(
                token_url,
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=cfg.request_timeout_sec,
            )
        if resp.status_code >= 400:
            # XSUAA returns a JSON body with `error`/`error_description`
            # (e.g. invalid_grant: refresh token expired) that raise_for_status
            # would discard. Surface it so the operator gets an actionable cause
            # instead of a bare "400 Client Error".
            detail = resp.text
            try:
                err_body = resp.json()
            except ValueError:
                err_body = None
            if isinstance(err_body, dict) and "error" in err_body:
                error = err_body.get("error")
                error_description = err_body.get("error_description")
                detail = (
                    f"{error}: {error_description}" if error_description else str(error)
                )
            raise ValueError(
                f"OAuth token request to {cfg.xsuaa_url}/oauth/token failed "
                f"({resp.status_code}): {detail}"
            )
        body = resp.json()
        token = body.get("access_token")
        if not token:
            raise ValueError(
                f"OAuth response from {cfg.xsuaa_url}/oauth/token did not include "
                f"`access_token`. Response body: {body}"
            )
        return token

    def _exchange_refresh_token(self) -> str:
        cfg = self.config
        if not cfg.client_id:
            raise ValueError("client_id is required for the refresh_token OAuth flow.")
        if not cfg.refresh_token:
            raise ValueError("refresh_token is not set.")
        return self._post_token(
            {
                "grant_type": "refresh_token",
                "refresh_token": cfg.refresh_token.get_secret_value(),
                "client_id": cfg.client_id,
            }
        )

    def _fetch_client_credentials_token(self) -> str:
        cfg = self.config
        if not cfg.client_id or not cfg.client_secret:
            raise ValueError(
                "client_id and client_secret are required for the client_credentials flow."
            )
        return self._post_token(
            {
                "grant_type": "client_credentials",
                "client_id": cfg.client_id,
                "client_secret": cfg.client_secret.get_secret_value(),
            }
        )

    def _get(
        self, url: str, params: Optional[Dict] = None, *, operation: str = "api"
    ) -> requests.Response:
        # Time the WHOLE _get — including the optional one-shot refresh+retry —
        # because that round-trip total is the latency the operator experiences.
        with self._timed_api(operation, url):
            return self._get_inner(url, params)

    def _get_inner(self, url: str, params: Optional[Dict] = None) -> requests.Response:
        self._ensure_auth()
        resp = self.session.get(
            url, params=params, timeout=self.config.request_timeout_sec
        )
        if resp.status_code == 401:
            # The OAuth bearer may have expired mid-run. Refresh once and retry.
            # Note: _get_token uses requests.post directly (NOT self._get), so a
            # 401 returned by the XSUAA token endpoint surfaces as an HTTPError
            # via raise_for_status inside _refresh_auth — no infinite loop.
            logger.info(
                "Got 401 from %s; refreshing OAuth token and retrying once", url
            )
            self._refresh_auth()
            resp = self.session.get(
                url, params=params, timeout=self.config.request_timeout_sec
            )
            if resp.status_code == 401:
                # A 401 that survives a successful token refresh is not a stale
                # bearer — the credentials/principal are genuinely being
                # rejected. Surface that instead of a misleading bare HTTP 401.
                raise ValueError(
                    "SAP Datasphere rejected the request with 401 even after "
                    "refreshing the token — the credentials appear invalid. "
                    "Verify the OAuth client_id/client_secret or token, and that "
                    "the principal has access. "
                    f"URL: {url}"
                )
        resp.raise_for_status()
        return resp

    def _paginate(
        self, url: str, page_size: int = _PAGE_SIZE
    ) -> Generator[Dict, None, None]:
        """Yield items from a SAP Datasphere list endpoint, transparently paginating.

        Handles BOTH shapes the real tenant uses (discovered via live probe):
          - OData-style ``{"value": [...]}`` — paginated
            (e.g., ``/api/v1/datasphere/consumption/catalog/spaces('X')/assets``)
          - Bare JSON list ``[...]`` — no pagination, single page
            (e.g., ``/api/v1/datasphere/consumption/catalog/spaces``)

        Two pagination mechanisms are used, in priority order:

          1. Server-driven ``@odata.nextLink`` cursor — the robust OData way. The
             server signals end-of-data by omitting the link, so this can never
             loop and stays correct regardless of the server's page-size cap.
             SAP's catalog caps a page at 500 records and returns a nextLink for
             the remainder.
          2. Offset fallback (``$top``/``$skip``) for endpoints that return no
             nextLink. Here a short page IS a reliable end-of-data signal: an
             OData service that truncates a response below the requested size
             must provide a nextLink, so reaching this fallback with a short page
             means there genuinely is no more data. (Advancing blindly until an
             empty page would instead loop forever against an endpoint that
             ignores ``$skip``.)
        """
        skip = 0
        next_url: Optional[str] = None
        while True:
            if next_url is not None:
                resp = self._get(next_url, operation="catalog_list")
            else:
                resp = self._get(
                    url,
                    params={"$top": page_size, "$skip": skip},
                    operation="catalog_list",
                )
            try:
                body = resp.json()
            except ValueError:
                logger.warning("Non-JSON response from %s", url)
                if self._report is not None:
                    self._report.warning(
                        title="Unexpected response from SAP Datasphere list endpoint",
                        message=(
                            "Expected a JSON array but got a non-JSON response "
                            "(possibly a proxy/login HTML page); this list will be "
                            "treated as empty."
                        ),
                        context=url,
                    )
                return
            if isinstance(body, list):
                # Bare-array endpoint — no pagination available.
                yield from body
                return
            if not isinstance(body, dict):
                logger.warning(
                    "Unexpected response shape from %s: got %s, expected dict or list",
                    url,
                    type(body).__name__,
                )
                if self._report is not None:
                    self._report.warning(
                        title="Unexpected response from SAP Datasphere list endpoint",
                        message=(
                            "Expected a JSON array but got a wrong-shape response "
                            "(neither dict nor list); this list will be treated as "
                            "empty."
                        ),
                        context=url,
                    )
                return
            items = body.get("value", []) or []
            if not items:
                return
            yield from items
            next_link = body.get("@odata.nextLink")
            if next_link:
                # Follow the server cursor. Resolve relative links against the
                # endpoint URL (handles full, absolute-path, and relative forms).
                next_url = urljoin(url, next_link)
                continue
            # No server cursor — offset fallback. A short page means end-of-data
            # (see docstring). Advance by the count actually received so the
            # offset stays correct even if the server returned fewer than asked.
            next_url = None
            if len(items) < page_size:
                return
            skip += len(items)

    def list_spaces(self) -> Generator[Dict, None, None]:
        yield from self._paginate(f"{self.config.base_url}{_CATALOG_BASE}/spaces")

    def list_assets(self, space_name: str) -> Generator[Dict, None, None]:
        # OData URL-quote the space name to avoid breaking on names containing apostrophes.
        quoted = quote(space_name, safe="")
        url = f"{self.config.base_url}{_CATALOG_BASE}/spaces('{quoted}')/assets"
        yield from self._paginate(url)

    def fetch_edmx(self, metadata_url: str) -> EdmxFetchResult:
        """Fetch OData EDMX XML.

        Returns an :class:`EdmxFetchResult` so the caller can tell apart the
        distinct no-schema cases instead of collapsing them into ``None``:

          - ``OK``: ``xml`` carries the EDMX document.
          - ``NOT_FOUND`` (404): the asset is legitimately not exposed for OData
            consumption — a benign, quiet skip. ``xml`` is ``None``.
          - ``FORBIDDEN`` (403): the ingestion principal lacks OData read on the
            space — a permission misconfiguration the operator should fix.
            Already surfaced here via ``report.warning`` (and ``logger.warning``)
            so the caller must NOT re-warn. ``xml`` is ``None``.
          - ``ERROR``: network / non-2xx / other failure. ``xml`` is ``None``.
        """
        self._ensure_auth()
        try:
            with self._timed_api("edmx_fetch", metadata_url):
                resp = self.session.get(
                    metadata_url,
                    headers={"Accept": "application/xml"},
                    timeout=self.config.request_timeout_sec,
                )
            if resp.status_code == 403:
                msg = (
                    f"EDMX metadata forbidden (HTTP 403) for {metadata_url}; the "
                    f"ingestion principal may lack OData read on this space. The "
                    f"asset's column-level schema will be unavailable."
                )
                logger.warning(msg)
                if self._report is not None:
                    self._report.warning(
                        title="EDMX metadata forbidden (HTTP 403)",
                        message=msg,
                        context=metadata_url,
                    )
                return EdmxFetchResult(xml=None, reason=EdmxFetchReason.FORBIDDEN)
            if resp.status_code == 404:
                # Benign: the asset simply isn't exposed for OData consumption.
                logger.debug(
                    "EDMX metadata not found (HTTP 404) for %s; asset not exposed "
                    "for OData consumption",
                    metadata_url,
                )
                return EdmxFetchResult(xml=None, reason=EdmxFetchReason.NOT_FOUND)
            resp.raise_for_status()
            return EdmxFetchResult(xml=resp.text, reason=EdmxFetchReason.OK)
        except requests.RequestException as e:
            logger.warning("Failed to fetch EDMX from %s: %s", metadata_url, e)
            return EdmxFetchResult(xml=None, reason=EdmxFetchReason.ERROR)

    def list_connections(self, space: str) -> List[ConnectionRecord]:
        """Return the connections defined in `space`, cached per (client, space)."""
        if space not in self._connections_cache:
            url = (
                f"{self.config.base_url}/api/v1/datasphere/spaces/"
                f"{quote(space, safe='')}/connections"
            )
            resp = self._get(url, operation="connections")
            try:
                data = resp.json()
            except ValueError:
                # A proxy/SSO redirect can return HTTP 200 with an HTML login
                # page; surface it explicitly rather than letting JSONDecodeError
                # propagate as a misleading generic "failed to fetch" upstream.
                logger.warning("Non-JSON response from %s", url)
                if self._report is not None:
                    self._report.warning(
                        title="Unexpected response shape from SAP Datasphere connections API",
                        message=(
                            f"Connections API for space {space} returned a "
                            f"non-JSON response (possibly a proxy/login HTML "
                            f"page). Federated assets in this space will be "
                            f"skipped."
                        ),
                        context=space,
                    )
                self._connections_cache[space] = []
                return self._connections_cache[space]
            if isinstance(data, list):
                if len(data) >= _CONNECTIONS_TRUNCATION_THRESHOLD:
                    msg = (
                        f"Connections list for space {space} has {len(data)} "
                        f"entries. The connections endpoint is not known to "
                        f"support pagination; if your space has additional "
                        f"connections beyond this point, federated assets "
                        f"routed to them will silently be skipped."
                    )
                    logger.warning(msg)
                    if self._report is not None:
                        self._report.warning(
                            title="Possible connections-list truncation",
                            message=msg,
                            context=space,
                        )
                # Cast at the JSON-parse boundary: the SAP API returns extra
                # fields per record, but the connector only reads name + typeId.
                self._connections_cache[space] = cast(List[ConnectionRecord], data)
            else:
                logger.warning(
                    "Unexpected shape from connections API for space %s: got %s, "
                    "expected list. Federated assets in this space will be skipped.",
                    space,
                    type(data).__name__,
                )
                if self._report is not None:
                    self._report.warning(
                        title="Unexpected response shape from SAP Datasphere connections API",
                        message=(
                            f"Connections API for space {space} returned a "
                            f"{type(data).__name__} instead of a JSON array. "
                            f"Federated assets in this space will be skipped."
                        ),
                        context=space,
                    )
                self._connections_cache[space] = []
        return self._connections_cache[space]

    def _list_dwaas_objects(
        self, space: str, object_type: str
    ) -> Generator[Dict, None, None]:
        """List objects of one type via the dwaas-core per-type endpoint
        (``/dwaas-core/api/v1/spaces/{space}/{object_type}``). Returns bare-array
        ``{"technicalName": ...}`` entries. On HTTP 403 the ingestion principal is
        not a member of the space — warn and yield nothing rather than crash.
        """
        quoted = quote(space, safe="")
        url = f"{self.config.base_url}/dwaas-core/api/v1/spaces/{quoted}/{object_type}"
        # ``_get`` calls ``resp.raise_for_status()``, so a 403 surfaces as a
        # requests.HTTPError (with the response attached) — catch it and skip.
        try:
            resp = self._get(url, operation="dwaas_list")
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 403:
                self._warn_not_a_member(space, object_type)
                return
            raise
        try:
            data = resp.json()
        except ValueError:
            logger.warning("Non-JSON response from %s", url)
            if self._report is not None:
                self._report.warning(
                    title="Unexpected response from SAP Datasphere list endpoint",
                    message=(
                        "Expected a JSON array but got a non-JSON response "
                        "(possibly a proxy/login HTML page); this list will be "
                        "treated as empty."
                    ),
                    context=url,
                )
            return
        if not isinstance(data, list):
            logger.warning(
                "Unexpected response shape from %s: got %s, expected list",
                url,
                type(data).__name__,
            )
            if self._report is not None:
                self._report.warning(
                    title="Unexpected response from SAP Datasphere list endpoint",
                    message=(
                        "Expected a JSON array but got a wrong-shape response "
                        "(not a list); this list will be treated as empty."
                    ),
                    context=url,
                )
            return
        yield from data

    def _warn_not_a_member(self, space: str, object_type: str) -> None:
        msg = (
            f"Received HTTP 403 listing {object_type} in space {space!r}. The "
            f"ingestion principal is not a member of this space, so its objects "
            f"will be skipped. Add the ingestion user/client as a member of the "
            f"space in SAP Datasphere to ingest it."
        )
        logger.warning(msg)
        if self._report is not None:
            self._report.warning(
                title="Not a member of SAP Datasphere space",
                message=msg,
                context=space,
            )

    def list_local_tables(self, space: str) -> Generator[Dict, None, None]:
        """Return the Local Tables (base tables not exposed for consumption) defined
        in `space`. Uses the supported `/dwaas-core/api/v1/spaces/X/localtables`
        endpoint — same surface the official `datasphere` CLI uses (see SAP KBA
        #3517441 which marks this as the public API).

        The response is a bare JSON list of ``{"technicalName": "..."}`` entries.
        No schema/columns are available via this endpoint; consumers must treat
        Local Tables as stub entities.
        """
        yield from self._list_dwaas_objects(space, "localtables")

    def fetch_object_definition(
        self,
        space: str,
        object_type: str,
        technical_name: str,
    ) -> Optional[Dict]:
        """Fetch the design-time definition (CSN) of a Datasphere object.

        Uses the SAP-supported per-object-type endpoint under
        ``/dwaas-core/api/v1/spaces/{space}/{object_type}/{technicalName}`` with
        the ``application/vnd.sap.datasphere.object.content+json`` content-type
        that returns full CSN. This is the same surface the official
        ``datasphere`` CLI uses (no policy caveat — replaces the previous
        ``/deepsea/`` dependency per SAP KBA #3517441).

        Args:
            space: Datasphere space (e.g. ``DEMO_SPACE``).
            object_type: ``views`` (for non-analytical views),
                ``analyticmodels`` (for analytic models), or ``localtables``
                (for Local Tables). For views vs analytic models this is
                routed from the asset record's ``supportsAnalyticalQueries``
                field at the caller; for Local Tables the emitter passes
                ``"localtables"`` directly.
            technical_name: The asset's technical name (e.g.
                ``SAP.TIME.VIEW_DIMENSION_DAY``).

        Returns:
            The parsed CSN body (``{"definitions": {...}, ...}``) on success.
            ``None`` on any HTTP failure (the source then emits the dataset
            without lineage rather than aborting ingestion). Failures populate
            ``self._report.assets_csn_fetch_failed``.
        """
        quoted_space = quote(space, safe="")
        quoted_name = quote(technical_name, safe="")
        url = (
            f"{self.config.base_url}/dwaas-core/api/v1/spaces/"
            f"{quoted_space}/{object_type}/{quoted_name}"
        )
        self._ensure_auth()
        try:
            # Time the whole CSN fetch — including the optional refresh+retry —
            # so the recorded latency matches what the operator experiences.
            with self._timed_api("csn_fetch", url):
                resp = self.session.get(
                    url,
                    headers={
                        "Accept": "application/vnd.sap.datasphere.object.content+json"
                    },
                    timeout=self.config.request_timeout_sec,
                )
                if resp.status_code == 401:
                    self._refresh_auth()
                    resp = self.session.get(
                        url,
                        headers={
                            "Accept": (
                                "application/vnd.sap.datasphere.object.content+json"
                            )
                        },
                        timeout=self.config.request_timeout_sec,
                    )
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status == 403:
                # Principal lacks membership on the space — systemic and fixable.
                # Reuse the actionable not-a-member hint (one warning per space).
                self._warn_not_a_member(space, object_type)
                self._report_csn_fetch_failed(space, technical_name)
                return None
            if status == 404:
                # The object genuinely doesn't exist (e.g. deleted between catalog
                # listing and CSN fetch) — benign, debug-log only, no warning.
                logger.debug(
                    "CSN object not found (404) for %s/%s/%s; skipping lineage.",
                    space,
                    object_type,
                    technical_name,
                )
                self._report_csn_fetch_failed(space, technical_name)
                return None
            extra = (
                " (HTTP 429 — rate limited; consider lowering max_workers_assets)"
                if status == 429
                else ""
            )
            self._warn_csn_fetch_failed(space, object_type, technical_name, e, extra)
            return None
        except (requests.RequestException, ValueError) as e:
            self._warn_csn_fetch_failed(space, object_type, technical_name, e, "")
            return None

    def _report_csn_fetch_failed(self, space: str, technical_name: str) -> None:
        if self._report is not None:
            self._report.assets_csn_fetch_failed.append(f"{space}.{technical_name}")

    def _warn_csn_fetch_failed(
        self,
        space: str,
        object_type: str,
        technical_name: str,
        e: Exception,
        extra: str,
    ) -> None:
        if self._report is not None:
            self._report.warning(
                title="Failed to fetch object definition from SAP Datasphere",
                message=(
                    f"Could not retrieve CSN for {space}/{object_type}/"
                    f"{technical_name} from the supported "
                    f"/dwaas-core/api/v1/spaces endpoint. Lineage for this "
                    f"asset will be unavailable.{extra}"
                ),
                context=f"{type(e).__name__}: {e}",
            )
        self._report_csn_fetch_failed(space, technical_name)
