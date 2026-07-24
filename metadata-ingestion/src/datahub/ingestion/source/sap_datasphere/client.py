import logging
from contextlib import contextmanager
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
from datahub.ingestion.source.sap_datasphere.constants import (
    ACCEPT_JSON,
    ACCEPT_XML,
    ALT_OBJECT_TYPE,
    CATALOG_BASE,
    CONNECTIONS_TRUNCATION_THRESHOLD,
    CSN_CONTENT_TYPE,
    DEFAULT_PAGE_SIZE,
    FORM_URLENCODED,
    GRANT_CLIENT_CREDENTIALS,
    GRANT_REFRESH_TOKEN,
    HEADER_CONTENT_TYPE,
    OAUTH_TOKEN_PATH,
    OBJECT_TYPE_LOCAL_TABLES,
    ODATA_NEXT_LINK_KEY,
    ODATA_VALUE_KEY,
    PARAM_CLIENT_ID,
    PARAM_CLIENT_SECRET,
    PARAM_GRANT_TYPE,
    PARAM_REFRESH_TOKEN,
    TOKEN_RESP_ACCESS_TOKEN,
    TOKEN_RESP_ERROR,
    TOKEN_RESP_ERROR_DESCRIPTION,
)
from datahub.ingestion.source.sap_datasphere.models import (
    ConnectionRecord,
    EdmxFetchReason,
    EdmxFetchResult,
)
from datahub.utilities.perf_timer import PerfTimer

if TYPE_CHECKING:
    from datahub.ingestion.source.sap_datasphere.report import SapDatasphereReport

logger = logging.getLogger(__name__)


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
        self._connections_cache: Dict[str, List[ConnectionRecord]] = {}
        if config.token:
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
        session.headers.update({"Accept": ACCEPT_JSON})
        return session

    @contextmanager
    def _timed_api(self, operation: str, url: Optional[str] = None) -> Iterator[None]:
        # try/finally so a failed or retried request still records the latency
        # the operator experienced.
        timer = PerfTimer()
        timer.start()
        try:
            yield
        finally:
            if self._report is not None:
                self._report.report_api_call(operation, timer.elapsed_seconds(), url)

    def _ensure_auth(self) -> None:
        # The header is always re-set from the freshly fetched token so
        # _refresh_auth reliably replaces an expired bearer.
        if self._auth_initialized:
            return
        token = self._get_token()
        self.session.headers["Authorization"] = f"Bearer {token}"
        self._auth_initialized = True

    def _refresh_auth(self) -> None:
        # Called on a 401. A second 401 after refresh is treated as a hard
        # credentials failure (no infinite retry loop).
        self._auth_initialized = False
        self._ensure_auth()

    def _get_token(self) -> str:
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
        # Shared network round-trip + error-shape parsing for both grant flows;
        # the flow-specific guards are enforced by the callers.
        cfg = self.config
        if not cfg.xsuaa_url:
            raise ValueError(
                "xsuaa_url is required for OAuth; set it explicitly or use a standard SAP tenant URL."
            )
        token_url = f"{cfg.xsuaa_url}{OAUTH_TOKEN_PATH}"
        try:
            with self._timed_api("oauth_token", token_url):
                resp = requests.post(
                    token_url,
                    data=payload,
                    headers={HEADER_CONTENT_TYPE: FORM_URLENCODED},
                    timeout=cfg.request_timeout_sec,
                )
        except requests.RequestException as e:
            # Re-raise transport failures as an auth error that names the token
            # endpoint, so they aren't mislabeled by the upstream caller.
            raise ValueError(
                f"OAuth token request to {token_url} failed (transport error): "
                f"{type(e).__name__}: {e}"
            ) from e
        if resp.status_code >= 400:
            # XSUAA returns a JSON error/error_description that raise_for_status
            # would discard; surface it for an actionable cause.
            detail = resp.text
            try:
                err_body = resp.json()
            except ValueError:
                err_body = None
            if isinstance(err_body, dict) and TOKEN_RESP_ERROR in err_body:
                error = err_body.get(TOKEN_RESP_ERROR)
                error_description = err_body.get(TOKEN_RESP_ERROR_DESCRIPTION)
                detail = (
                    f"{error}: {error_description}" if error_description else str(error)
                )
            raise ValueError(
                f"OAuth token request to {token_url} failed "
                f"({resp.status_code}): {detail}"
            )
        try:
            body = resp.json()
        except ValueError as e:
            # HTTP 2xx but a non-JSON body (e.g. an SSO/proxy login HTML page).
            raise ValueError(
                f"OAuth response from {token_url} was not valid JSON "
                f"(possibly a proxy/login HTML page): {e}"
            ) from e
        token = body.get(TOKEN_RESP_ACCESS_TOKEN)
        if not token:
            raise ValueError(
                f"OAuth response from {token_url} did not include "
                f"`{TOKEN_RESP_ACCESS_TOKEN}`. Response body: {body}"
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
                PARAM_GRANT_TYPE: GRANT_REFRESH_TOKEN,
                PARAM_REFRESH_TOKEN: cfg.refresh_token.get_secret_value(),
                PARAM_CLIENT_ID: cfg.client_id,
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
                PARAM_GRANT_TYPE: GRANT_CLIENT_CREDENTIALS,
                PARAM_CLIENT_ID: cfg.client_id,
                PARAM_CLIENT_SECRET: cfg.client_secret.get_secret_value(),
            }
        )

    def _get(
        self, url: str, params: Optional[Dict] = None, *, operation: str = "api"
    ) -> requests.Response:
        # Time the whole _get, including the optional refresh+retry, because that
        # round-trip total is the latency the operator experiences.
        with self._timed_api(operation, url):
            return self._get_inner(url, params)

    def _get_with_refresh(
        self,
        url: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict] = None,
    ) -> requests.Response:
        """GET that transparently refreshes the OAuth bearer once on a 401 and
        retries. Returns the final response (which may itself still be a 401 if
        the refreshed credentials are also rejected); each caller decides how to
        treat a surviving 401 / non-2xx. ``_get_token`` uses ``requests.post``
        directly, so a 401 from the token endpoint can't loop back in here."""
        self._ensure_auth()
        resp = self.session.get(
            url, headers=headers, params=params, timeout=self.config.request_timeout_sec
        )
        if resp.status_code == 401:
            logger.info(
                "Got 401 from %s; refreshing OAuth token and retrying once", url
            )
            self._refresh_auth()
            resp = self.session.get(
                url,
                headers=headers,
                params=params,
                timeout=self.config.request_timeout_sec,
            )
        return resp

    def _get_inner(self, url: str, params: Optional[Dict] = None) -> requests.Response:
        resp = self._get_with_refresh(url, params=params)
        if resp.status_code == 401:
            # A 401 surviving a successful refresh means the credentials are
            # genuinely rejected, not a stale bearer.
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
        self, url: str, page_size: int = DEFAULT_PAGE_SIZE
    ) -> Generator[Dict, None, None]:
        """Yield items from a list endpoint, transparently paginating.

        Handles both shapes the tenant uses: OData ``{"value": [...]}`` (paginated)
        and a bare JSON list (single page). Prefers the server-driven
        ``@odata.nextLink`` cursor (can't loop); falls back to ``$top``/``$skip``
        where no cursor is returned, treating a short page as end-of-data (an
        OData service that truncates below the requested size must return a
        nextLink, so a short page here genuinely means no more data — advancing
        blindly would loop forever against a server that ignores ``$skip``).
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
            items = body.get(ODATA_VALUE_KEY, []) or []
            if not items:
                return
            yield from items
            next_link = body.get(ODATA_NEXT_LINK_KEY)
            if next_link:
                # Resolve relative links against the endpoint URL.
                next_url = urljoin(url, next_link)
                continue
            next_url = None
            if len(items) < page_size:
                return
            skip += len(items)

    def list_spaces(self) -> Generator[Dict, None, None]:
        yield from self._paginate(f"{self.config.base_url}{CATALOG_BASE}/spaces")

    def list_assets(self, space_name: str) -> Generator[Dict, None, None]:
        # OData URL-quote the space name so names containing apostrophes don't break.
        quoted = quote(space_name, safe="")
        url = f"{self.config.base_url}{CATALOG_BASE}/spaces('{quoted}')/assets"
        yield from self._paginate(url)

    def fetch_edmx(self, metadata_url: str) -> EdmxFetchResult:
        """Fetch OData EDMX XML, distinguishing the no-schema cases via
        ``EdmxFetchResult.reason``: OK (xml present), NOT_FOUND (404, benign — not
        OData-exposed), FORBIDDEN (403 — already warned here, caller must not
        re-warn), ERROR (network / non-2xx / other).
        """
        try:
            # Time the whole fetch (incl. refresh+retry) as one sample so the
            # edmx_fetch metric isn't double-counted on a 401 refresh. A token
            # expiring mid-EDMX-phase would otherwise lose schema for every
            # remaining asset — _get_with_refresh recovers it.
            with self._timed_api("edmx_fetch", metadata_url):
                resp = self._get_with_refresh(
                    metadata_url, headers={"Accept": ACCEPT_XML}
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
        if space not in self._connections_cache:
            url = (
                f"{self.config.base_url}/api/v1/datasphere/spaces/"
                f"{quote(space, safe='')}/connections"
            )
            resp = self._get(url, operation="connections")
            try:
                data = resp.json()
            except ValueError:
                # A proxy/SSO redirect can return HTTP 200 with an HTML login page.
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
                if len(data) >= CONNECTIONS_TRUNCATION_THRESHOLD:
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
                # The API returns extra fields per record; the connector only
                # reads name + typeId.
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
        # dwaas-core per-type endpoint returning bare-array {"technicalName": ...}
        # entries. HTTP 403 means the principal is not a member of the space.
        quoted = quote(space, safe="")
        url = f"{self.config.base_url}/dwaas-core/api/v1/spaces/{quoted}/{object_type}"
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
        # Base tables not exposed for consumption; bare-array {"technicalName": ...}.
        # No schema is available via this endpoint — consumers treat them as stubs.
        yield from self._list_dwaas_objects(space, OBJECT_TYPE_LOCAL_TABLES)

    def list_objects(self, space: str, object_type: str) -> Generator[Dict, None, None]:
        # Generic bare-array {"technicalName": ...} listing for any dwaas-core
        # object type (flows, task chains, remote tables). 403 is downgraded to a
        # not-a-member warning by _list_dwaas_objects.
        yield from self._list_dwaas_objects(space, object_type)

    def fetch_flow_definition(
        self, space: str, object_type: str, technical_name: str
    ) -> Optional[Dict]:
        """Fetch a flow / task-chain design-time definition. Same dwaas-core
        surface + CSN content type as fetch_object_definition, but with no
        views<->analyticmodels sibling retry (flows have no sibling) and flow-
        scoped failure reporting. Returns the parsed body or None on any error."""
        quoted_space = quote(space, safe="")
        quoted_name = quote(technical_name, safe="")
        url = (
            f"{self.config.base_url}/dwaas-core/api/v1/spaces/"
            f"{quoted_space}/{object_type}/{quoted_name}"
        )
        try:
            with self._timed_api("flow_fetch", url):
                resp = self._get_with_refresh(url, headers={"Accept": CSN_CONTENT_TYPE})
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status == 403:
                self._warn_not_a_member(space, object_type)
            self._report_flow_fetch_failed(space, object_type, technical_name)
            return None
        except (requests.RequestException, ValueError):
            self._report_flow_fetch_failed(space, object_type, technical_name)
            return None

    def _report_flow_fetch_failed(
        self, space: str, object_type: str, technical_name: str
    ) -> None:
        if self._report is not None:
            self._report.flows_fetch_failed.append(
                f"{space}.{object_type}.{technical_name}"
            )

    def fetch_object_definition(
        self,
        space: str,
        object_type: str,
        technical_name: str,
        _try_alternate: bool = True,
    ) -> Optional[Dict]:
        """Fetch the design-time CSN of a Datasphere object via the dwaas-core
        per-type endpoint. Returns the parsed CSN body on success, or ``None`` on
        any HTTP failure (the source then emits without lineage). Failures
        populate ``report.assets_csn_fetch_failed``.

        ``_try_alternate`` is internal: on the first call it is ``True``; if the
        primary object_type 404s and has a sibling (views <-> analyticmodels), the
        method retries once under the sibling with it set to ``False``. The retry
        reports nothing so a genuinely-missing object is counted exactly once.
        """
        quoted_space = quote(space, safe="")
        quoted_name = quote(technical_name, safe="")
        url = (
            f"{self.config.base_url}/dwaas-core/api/v1/spaces/"
            f"{quoted_space}/{object_type}/{quoted_name}"
        )
        try:
            with self._timed_api("csn_fetch", url):
                resp = self._get_with_refresh(url, headers={"Accept": CSN_CONTENT_TYPE})
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            # A fallback probe stays silent — the primary call owns all reporting
            # so an asset is never double-counted across the two attempts.
            if not _try_alternate:
                return None
            if status == 403:
                self._warn_not_a_member(space, object_type)
                self._report_csn_fetch_failed(space, technical_name)
                return None
            if status == 404:
                # supportsAnalyticalQueries can misroute the type; retry once
                # under the sibling before giving up.
                alt = ALT_OBJECT_TYPE.get(object_type)
                if alt is not None:
                    logger.debug(
                        "CSN 404 for %s/%s/%s; retrying as %s",
                        space,
                        object_type,
                        technical_name,
                        alt,
                    )
                    recovered = self.fetch_object_definition(
                        space, alt, technical_name, _try_alternate=False
                    )
                    if recovered is not None:
                        self._report_csn_object_type_corrected(
                            space, technical_name, object_type, alt
                        )
                        return recovered
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
            if not _try_alternate:
                return None
            self._warn_csn_fetch_failed(space, object_type, technical_name, e, "")
            return None

    def _report_csn_fetch_failed(self, space: str, technical_name: str) -> None:
        if self._report is not None:
            self._report.assets_csn_fetch_failed.append(f"{space}.{technical_name}")

    def _report_csn_object_type_corrected(
        self, space: str, technical_name: str, attempted: str, recovered_as: str
    ) -> None:
        if self._report is not None:
            self._report.assets_csn_object_type_corrected.append(
                f"{space}.{technical_name}: {attempted}->{recovered_as}"
            )

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
