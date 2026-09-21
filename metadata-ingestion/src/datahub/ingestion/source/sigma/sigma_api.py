import functools
import logging
import sys
from collections import deque
from collections.abc import Hashable
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
)
from urllib.parse import quote, urlencode

import requests
from pydantic import BaseModel, ValidationError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.sigma.config import (
    Constant,
    SigmaSourceConfig,
    SigmaSourceReport,
)
from datahub.ingestion.source.sigma.data_classes import (
    ConnectionPath,
    CustomSqlEntry,
    DataModelElementUpstream,
    DatasetUpstream,
    Element,
    ElementUpstream,
    File,
    Page,
    SheetUpstream,
    SigmaDataModel,
    SigmaDataModelColumn,
    SigmaDataModelElement,
    SigmaDataset,
    WarehouseInodeRaw,
    WarehouseTableUpstream,
    Workbook,
    WorkbookLineageTableEntry,
    Workspace,
)

# Logger instance
logger = logging.getLogger(__name__)

# Statuses /datasets/{id}/sources uses for "cannot resolve this dataset". Sigma
# answers 409 inode_archived rather than 404 for an unresolvable dataset
# (verified live), so both are treated the same.
_DATASET_SOURCES_NOT_FOUND_STATUSES = frozenset({404, 409})
# Not-founds with zero successes before escalating from info to a warning.
_DATASET_SOURCES_NOT_FOUND_WARN_THRESHOLD = 3


# Sigma error bodies are short; the cap only guards against an HTML error page
# from a proxy being pasted whole into the ingestion report.
_MAX_ERROR_BODY_CHARS = 400


T = TypeVar("T", bound=BaseModel)

# A ROW this connector cannot read, as opposed to a call Sigma refused.
_UNREADABLE_ROW = (ValidationError,)
# The PAGE's shape is wrong: no `entries`, or an unknown cursor name. Costs
# every later page rather than one entity.
# requests' own JSONDecodeError covers both the stdlib and simplejson cases.
# Added in requests 2.27, hence the floor in setup.py.
_MALFORMED_RESPONSE = (KeyError, requests.exceptions.JSONDecodeError)
# ...but only for keys this client reads off a payload. A KeyError on
# anything else is a bug here, and would wrongly blame the vendor.
_PAYLOAD_KEYS = frozenset(
    {
        Constant.ENTRIES,
        Constant.NEXTPAGE,
        Constant.NEXTPAGETOKEN,
        Constant.PARENTID,
        Constant.ID,
    }
)


def _is_malformed_response(e: BaseException) -> bool:
    if isinstance(e, KeyError):
        return bool(e.args) and e.args[0] in _PAYLOAD_KEYS
    return isinstance(e, _MALFORMED_RESPONSE)


def _http_status(e: BaseException) -> Optional[int]:
    """The status of the failed call, if the failure was an HTTP one."""
    if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
        return int(e.response.status_code)
    return None


def _envelope(body: Any, *, entries_must_be_a_list: bool = False) -> Dict[str, Any]:
    """A listing body, or a :class:`KeyError` classed as a shape change.

    A 200 whose body is a JSON list raises ``TypeError`` on the ``entries``
    subscript. That carries no status, so it reaches the remedy chain
    untagged and is answered "transient, re-run" -- advice for a body that
    will come back identical. It is the same event as a page with no
    ``entries``: the wrong shape.

    ``entries_must_be_a_list`` is for callers that iterate ``entries``
    straight away, where a null costs the same TypeError. The paginated
    helper applies that rule itself, and only for run-wide listings.
    """
    if not isinstance(body, dict):
        raise KeyError(Constant.ENTRIES)
    if entries_must_be_a_list and not isinstance(body.get(Constant.ENTRIES), list):
        raise KeyError(Constant.ENTRIES)
    return body


def _is_transient(e: BaseException) -> bool:
    """Whether re-asking could plausibly get a different answer.

    Only a negative cache should ask this. Latching a node as dead on a
    transport blip or a 5xx costs every SIBLING under it too -- the walk
    short-circuits without ever re-fetching -- and an entity dropped that way
    is soft-deleted by a run that otherwise passes. 500 in particular is
    deliberately absent from the retry status list, so nothing else retries
    it.
    """
    status = _http_status(e)
    if status is not None:
        return status == 429 or status >= 500
    # No HTTP response at all. A reset or a timeout is worth re-asking; a body
    # this connector could not read is the same on every attempt.
    return isinstance(e, requests.exceptions.RequestException) and not (
        _is_malformed_response(e)
    )


def _failed_row(e: BaseException, last_row: Dict[str, Any]) -> str:
    """Name the row only when the ROW is what failed.

    The hand-written listings keep the last row in hand for the whole
    listing, so on an HTTP failure it names whatever parsed fine just before
    -- "id=f-1, http_status=500" points at an innocent row on page 1 when
    page 2 died -- and on a first-page failure it reports no id at all, when
    there was no payload to have one.
    """
    return f"{_row_identity(last_row)}, " if isinstance(e, _UNREADABLE_ROW) else ""


def _exc_text(e: BaseException) -> str:
    """Exception text for a report context, without pydantic's row echo."""
    return _terse(e) if isinstance(e, ValidationError) else str(e)


def _terse(ve: ValidationError) -> str:
    """A validation error without pydantic's echo of the offending row.

    The echo repeats the whole input for EVERY missing field, which fills the
    1000-char context cap on its own and pushes out whatever follows.
    """
    try:
        return "; ".join(
            f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}"
            for err in ve.errors(include_url=False, include_input=False)
        )
    except Exception:
        return str(ve)[:200]


def _row_identity(entry: Dict[str, Any]) -> str:
    """Whatever identifies a row that failed to parse, if anything does.

    A validation error names the FIELD that was wrong, never the object, so
    without this an operator is told a row is unparseable and has no way to
    find it.
    """
    for key in (
        "workbookId",
        "datasetId",
        "dataModelId",
        "workspaceId",
        "elementId",
        "id",
    ):
        value = entry.get(key)
        if isinstance(value, str) and value:
            return f"{key}={value}"
    return "row id not present in the payload"


def _error_payload(response: Optional[requests.Response]) -> Any:
    """The response's parsed JSON, or None. Parsed once per failure.

    ``requests`` re-decodes and re-parses on every ``.json()``, and both
    ``_error_code`` and ``_error_body`` want the payload. One decode still
    happens for a body that is not JSON -- the saving is the repeat, not the
    first attempt.
    """
    if response is None:
        return None
    try:
        return response.json()
    except Exception:
        return None


def _error_detail(response: Optional[requests.Response], *, payload: Any) -> str:
    """The parts of a failed response worth putting in a report entry.

    ``payload`` is REQUIRED, not defaulted: for :func:`_error_body` a missing
    payload means "suppress unconditionally", so a future call site that
    forgot the kwarg would silently drop every body instead of failing.
    """
    if response is None:
        # Not "http_status=None", which reads as a missing field: nothing came
        # back, or a 200 came back that this connector could not read.
        return "no_http_error_response"
    try:
        # Bounded like the body: a server-controlled header should not be
        # echoed into the report at whatever length the server chose.
        retry_after = str(response.headers.get("Retry-After") or "")[:32] or None
    except Exception:
        retry_after = None
    body = _error_body(response, payload=payload)
    return (
        f"http_status={response.status_code}"
        + (f", retry_after={retry_after}" if retry_after else "")
        + (f", body={body}" if body else "")
    )


def _error_body(
    response: Optional[requests.Response], *, payload: Any
) -> Optional[str]:
    """The server's explanation for a failed call, bounded.

    ``requests`` puts only the status line into the exception text, so a 400
    that Sigma explains in its body reads as an unexplained failure. Reading
    the body must never itself raise -- the call has already failed.
    """
    if response is None:
        return None
    if isinstance(payload, dict):
        # Sigma's own message, which is what an operator can act on.
        message = payload.get("message")
        if isinstance(message, str) and message.strip():
            return message.strip()[:_MAX_ERROR_BODY_CHARS].replace("\n", " ")
    try:
        content_type = response.headers.get("Content-Type", "")
    except Exception:
        content_type = ""
    if payload is None or "json" not in content_type.lower():
        # Not Sigma's own JSON error -- typically a proxy's error page, which
        # can carry internal hostnames into a persisted report. Report its
        # size, not its content. Both signals must agree, since Content-Type
        # is set by whatever returned the body. ``.content`` is already
        # buffered; Content-Length is proxy-controlled and absent on chunked
        # responses.
        try:
            size = len(response.content or b"")
        except Exception:
            return None
        if not size:
            return None
        return f"<{size} bytes of non-JSON body suppressed>"
    try:
        text = (response.text or "").strip()
    except Exception:
        return None
    if not text:
        return None
    return text[:_MAX_ERROR_BODY_CHARS].replace("\n", " ")


def _error_code(
    response: Optional[requests.Response], *, payload: Any
) -> Optional[str]:
    """Sigma's own machine-readable classifier for a failed call.

    The HTTP status is too coarse to act on: several unrelated problems share
    one status and need different people to fix them. The ``code`` field
    separates them and is stable enough to aggregate on, where ``message``
    embeds object names and would never group.
    """
    if response is None or not isinstance(payload, dict):
        return None
    code = payload.get("code")
    return code if isinstance(code, str) and code else None


class SigmaAPI:
    def __init__(self, config: SigmaSourceConfig, report: SigmaSourceReport) -> None:
        self.config = config
        self.report = report
        self.workspaces: Dict[str, Workspace] = {}
        # Two sets, like the file-path walk: a transient failure is not
        # remembered as dead, but its loss is still counted once.
        self._workspace_lookup_failed: Set[str] = set()
        self._workspace_loss_counted: Set[str] = set()
        # Ancestors whose /files fetch failed, so a walk reaching one can stop.
        # Separate from what has been COUNTED -- see _note_file_path_loss.
        self._file_path_lookup_failed: Set[str] = set()
        self._file_path_loss_counted: Set[str] = set()
        self.users: Dict[str, str] = {}
        # Track source_type values we've already warned about to keep the
        # report summary readable on large tenants with repeated unknown
        # node types.
        self._unknown_lineage_node_types_warned: Set[str] = set()
        # Sigma's dataset API is deprecated. Once the endpoint is concluded
        # removed -- a 410, or a 404/409 a re-probe confirms -- it is not called
        # again for the rest of the run rather than retried per dataset.
        self._dataset_sources_endpoint_gone = False
        # Set once any /sources call returns 200, which proves the endpoint
        # exists and downgrades a later not-found to a per-dataset miss.
        # A dataset whose /sources answered 200 this run, re-queried to tell
        # "endpoint removed" from "this dataset is gone". Also stands in for
        # "anything has succeeded", so the two cannot drift apart. It is cleared
        # if that dataset later turns out to be archived.
        self._known_good_dataset_id: Optional[str] = None
        # Sticky: unlike _known_good_dataset_id this is never cleared, since a
        # later 404 should not be read as "nothing ever worked".
        self._dataset_sources_succeeded = False
        self._dataset_sources_not_found_warned = False
        self.session = requests.Session()

        # raise_on_status=False must stay False: get_data_model_by_url_id
        # inspects response.status_code to surface 429 explicitly; if True,
        # exhausted retries raise MaxRetryError and bypass that branch.
        retry_strategy = Retry(
            total=3,
            # 500 is deliberately NOT here: Sigma answers 500 from
            # /workbooks/{id}/lineage/elements/{id} as its ordinary "no
            # lineage" reply, once per element, so retrying would cost ~12s of
            # backoff each.
            status_forcelist=[429, 502, 503, 504],
            # Explicit, not inherited: the two POSTs here are the token and
            # refresh calls, which must not be replayed.
            allowed_methods=frozenset({"GET"}),
            backoff_factor=2,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.refresh_token: Optional[str] = None
        # Test connection by generating access token
        logger.info(f"Trying to connect to {self.config.api_url}")
        self._generate_token()

    def _generate_token(self):
        data = {
            "grant_type": "client_credentials",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret.get_secret_value(),
        }
        response = self.session.post(f"{self.config.api_url}/auth/token", data=data)
        response.raise_for_status()
        response_dict = response.json()
        self.refresh_token = response_dict[Constant.REFRESH_TOKEN]
        self.session.headers.update(
            {
                "Authorization": f"Bearer {response_dict[Constant.ACCESS_TOKEN]}",
                "Content-Type": "application/json",
            }
        )

    def _record_enumeration_failure(
        self,
        *,
        what: str,
        context: str,
        optional_feature: Optional[str] = None,
        unparseable_row: bool = False,
        malformed_response: bool = False,
        status: Optional[int] = None,
    ) -> None:
        """A call that ENUMERATES entities failed, so entities are missing.

        The one class of failure that must be a failure, not a warning.
        Stale-entity removal soft-deletes anything the previous run emitted
        and this one did not, so a dead listing makes live objects look
        deleted. ``StaleEntityRemovalHandler`` guards against that only when
        the source reports a failure, which this connector never did.

        Detail failures stay warnings -- a workbook whose element fetch dies
        is still emitted, just thinner. So does a listing scoped to ONE
        parent; see :meth:`_record_child_listing_failure`.
        """
        self.report.entity_enumeration_failed += 1
        # ``toggle_helps`` gates the opt-out sentence, appended once below
        # rather than repeated in every branch. It is False wherever turning
        # the feature off would soft-delete every object of that kind to
        # survive something a re-run or an upgrade fixes.
        if malformed_response:
            # Costs every later page, not one entity. A retired endpoint can
            # announce itself by changing shape rather than by a 404, so the
            # toggle is on offer here but not on a bad row.
            remedy = (
                "Sigma's response did not have the shape this connector "
                "expects, so every later page is missing. Nothing was "
                "refused, so a scope will not help: upgrade the connector."
            )
            toggle_helps = True
        elif unparseable_row:
            # The call SUCCEEDED; a row was unreadable. A scope is wrong
            # (nothing was refused) and so is a toggle (it would soft-delete
            # every object of that kind to avoid losing one).
            remedy = (
                "Sigma returned rows this connector cannot parse. Nothing was "
                "refused, and this repeats every run until the rows or the "
                "connector change: upgrade the connector."
            )
            toggle_helps = False
        elif status == 401:
            # NOT 403: a 401 rejects the credential, not the scope attached
            # to it, and _get_api_call has already refreshed and re-asked
            # where a refresh token is configured.
            remedy = (
                "Sigma rejected this connector's credentials, so a scope will "
                "not help: check client_id and client_secret, and that the "
                "token has not been revoked or expired."
            )
            toggle_helps = False
        elif status == 403:
            remedy = (
                "Sigma refused this call. Grant the token the scope for this endpoint."
            )
            toggle_helps = True
        elif status in (404, 410):
            remedy = "This endpoint is gone or was never present."
            if not optional_feature:
                remedy += " This listing cannot be turned off."
            toggle_helps = True
        elif status is None or status == 429 or status >= 500:
            # 429 belongs here: it says it is temporary, and _is_transient
            # treats it that way for the caches. This branch must stay ABOVE
            # the catch-all, or an outage is answered by soft-deleting
            # everything. An internal error lands here too.
            remedy = (
                "Transient: Sigma failed to answer, answered that it is rate "
                "limited, or did not answer at all. Re-run -- the context "
                "carries Retry-After where Sigma sent one."
            )
            toggle_helps = False
        else:
            remedy = (
                "Neither refused nor transient, so an odd status is more "
                "likely a bug in this connector than anything to configure."
            )
            toggle_helps = True
        if optional_feature and toggle_helps:
            remedy += (
                f" Or set {optional_feature}, which stops the call being made "
                "-- at the cost of soft-deleting the objects you stop "
                "ingesting."
            )

        self.report.failure(
            title="Sigma entity listing failed",
            message="A Sigma listing call failed, so the entities it would "
            "have returned are missing from this run. Stale-entity removal is "
            "suppressed to avoid soft-deleting objects that still exist. "
            "Re-run once the cause is resolved; the context names the listing "
            "and what to do about a failure that repeats.",
            # Remedy FIRST: report_log truncates context at 1000 chars, which
            # a proxied api_url plus Retry-After is enough to reach.
            context=f"{remedy} Could not list {what}: {context}",
        )

    def _record_child_listing_failure(self) -> None:
        """A listing scoped to ONE parent failed -- one workbook's pages, one
        page's elements, one Data Model's elements.

        Counted, not failed. The framework guard is run-wide and
        all-or-nothing, so failing here would let one flaky child call freeze
        soft-deletion for the whole tenant -- and on a tenant with thousands
        of pages, at least one such failure per run is near certain. The loss
        is bounded to one parent, and ``fail_safe_threshold`` still catches
        enough of them failing to look like a mass deletion. Run-wide
        listings do fail the run -- see :meth:`_record_enumeration_failure`.
        """
        self.report.child_entity_listing_failed += 1
        # One grouped entry: without it an operator reading
        # "child_entity_listing_failed: 47" cannot tell why the run passed.
        self.report.info(
            title="Sigma child entity listing failed",
            log=False,
            message="A listing scoped to one parent failed, so that parent's "
            "children are missing from this run. This does NOT fail the run: "
            "the framework's stale-entity guard is run-wide, so failing here "
            "would freeze soft-deletion for the whole tenant whenever a single "
            "child call is flaky. fail_safe_threshold still covers the case "
            "where enough of them fail to look like a mass deletion.",
        )

    def _log_http_error(self, message: str, *, report_warning: bool = True) -> str:
        """Record a failed Sigma API call.

        The terminal handler for most ``except`` blocks here, so anything it
        drops is invisible. It used to log a context-free ``HTTP status-code
        = 404`` at WARNING, put the identifying detail on a DEBUG line, and
        touch the report not at all.

        ``message`` names the resource at every call site, so it becomes the
        warning context. The title is fixed so LossyList groups them, and the
        counters carry the true totals after that list truncates.

        Pass ``report_warning=False`` when the caller emits its own, better
        scoped entry; the counters still fire. Returns the rendered status /
        body detail so that caller can put it on ITS entry.
        """
        _, e, _ = sys.exc_info()
        if e is None:
            # Called outside an `except`: the counter would silently bucket
            # under "NoneType". Deliberately fatal -- a bug to fail a test on,
            # not a condition to degrade around.
            raise AssertionError(
                f"_log_http_error called with no active exception: {message}"
            )
        response = (
            e.response
            if isinstance(e, requests.exceptions.HTTPError) and e.response is not None
            else None
        )
        status = response.status_code if response is not None else None
        key = str(status) if status is not None else type(e).__name__
        self.report.api_call_failures_by_status_or_error[key] = (
            self.report.api_call_failures_by_status_or_error.get(key, 0) + 1
        )
        # Parsed once and threaded through both readers: requests re-decodes
        # and re-parses on every .json().
        payload = _error_payload(response)
        sigma_code = _error_code(response, payload=payload)
        if sigma_code:
            # int_top_k_dict() is a defaultdict(int).
            self.report.api_call_failures_by_sigma_code[sigma_code] += 1
        detail = _error_detail(response, payload=payload)
        if report_warning:
            self.report.warning(
                title="Sigma API call failed",
                message="A Sigma API call failed. The affected objects are "
                "emitted without whatever that call would have provided, or "
                "-- where the call resolved a parent -- dropped entirely; see "
                "api_call_failures_by_status_or_error for the totals.",
                context=f"{message} ({detail})",
            )
        logger.debug(msg=message, exc_info=e)
        return detail

    def _refresh_access_token(self):
        try:
            data = {
                "grant_type": Constant.REFRESH_TOKEN,
                "refresh_token": self.refresh_token,
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret.get_secret_value(),
            }
            post_response = self.session.post(
                f"{self.config.api_url}/auth/token",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data=data,
            )
            post_response.raise_for_status()
            response_dict = post_response.json()
            self.refresh_token = response_dict[Constant.REFRESH_TOKEN]
            self.session.headers.update(
                {
                    "Authorization": f"Bearer {response_dict[Constant.ACCESS_TOKEN]}",
                    "Content-Type": "application/json",
                }
            )
        except Exception as e:
            self._log_http_error(
                message=f"Unable to refresh access token. Exception: {e}"
            )

    def _get_api_call(self, url: str) -> requests.Response:
        """Make an API call with automatic retry on 429/502/503/504 and token
        refresh on 401."""
        get_response = self.session.get(url)

        # Handle token refresh on 401
        if get_response.status_code == 401 and self.refresh_token:
            logger.debug("Access token might expired. Refreshing access token.")
            self._refresh_access_token()
            get_response = self.session.get(url)

        return get_response

    def get_workspace(self, workspace_id: str) -> Optional[Workspace]:
        if workspace_id in self.workspaces:
            return self.workspaces[workspace_id]
        if workspace_id in self._workspace_lookup_failed:
            # Negative cache: a workspace that refuses would otherwise be
            # re-fetched once per child, for an answer that cannot change.
            # Only a non-transient failure latches -- see below.
            return None

        logger.debug(f"Fetching workspace metadata with id '{workspace_id}'")
        try:
            response = self._get_api_call(
                f"{self.config.api_url}/workspaces/{workspace_id}"
            )
            if response.status_code == 403:
                logger.debug(f"Workspace {workspace_id} not accessible.")
                # Latched: inaccessibility is a property of the workspace,
                # not of the lookup, so re-asking only re-pays the round trip.
                self._workspace_lookup_failed.add(workspace_id)
                self.report.non_accessible_workspaces_count += 1
                return None
            response.raise_for_status()
            workspace = Workspace.model_validate(response.json())
            self.workspaces[workspace.workspaceId] = workspace
            return workspace
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch workspace '{workspace_id}'. Exception: {e}"
            )
            # Entity-removing, not decorating: None here drops every workbook
            # and dataset in this workspace when ingest_shared_entities is
            # False. Bounded to one workspace, so it takes the child-listing
            # rule. The 403 above is a steady state, counted separately.
            if not _is_transient(e):
                # Latching a blip would drop the whole workspace where
                # re-asking for the next child could have succeeded.
                self._workspace_lookup_failed.add(workspace_id)
            if (
                # With shared entities ON the caller keeps the workbook or
                # dataset anyway, so nothing was lost.
                not self.config.ingest_shared_entities
                # Per workspace, not per lookup: a transient failure is
                # re-asked, so counting lookups would mix units.
                and workspace_id not in self._workspace_loss_counted
            ):
                self._workspace_loss_counted.add(workspace_id)
                self._record_child_listing_failure()
        return None

    def fill_workspaces(self) -> None:
        logger.debug("Fetching all accessible workspaces metadata.")
        workspace_url = url = f"{self.config.api_url}/workspaces?limit=50"
        last_row: Dict[str, Any] = {}
        try:
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = _envelope(response.json(), entries_must_be_a_list=True)
                for workspace_dict in response_dict[Constant.ENTRIES]:
                    last_row = workspace_dict
                    self.workspaces[workspace_dict[Constant.WORKSPACEID]] = (
                        Workspace.model_validate(workspace_dict)
                    )
                if response_dict[Constant.NEXTPAGE]:
                    url = f"{workspace_url}&page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break
        except Exception as e:
            detail = self._log_http_error(
                message=f"Unable to fetch workspaces. Exception: {e}",
                report_warning=False,
            )
            self._record_enumeration_failure(
                what="workspaces",
                context=f"{_failed_row(e, last_row)}{detail}, exception={_exc_text(e)}",
                # This listing validates rows inside its own try, so one bad
                # row lands here rather than on the per-row path.
                unparseable_row=isinstance(e, _UNREADABLE_ROW),
                malformed_response=_is_malformed_response(e),
                status=_http_status(e),
            )

    @functools.lru_cache()
    def _get_users(self) -> Dict[str, str]:
        logger.debug("Fetching all accessible users metadata.")
        try:
            users: Dict[str, str] = {}
            members_url = url = f"{self.config.api_url}/members?limit=50"
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = response.json()
                for user_dict in response_dict[Constant.ENTRIES]:
                    users[user_dict[Constant.MEMBERID]] = user_dict[Constant.EMAIL]
                if response_dict[Constant.NEXTPAGE]:
                    url = f"{members_url}&page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break
            return users
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch users details. Exception: {e}"
            )
            return {}

    def get_user_name(self, user_id: str) -> Optional[str]:
        return self._get_users().get(user_id)

    def get_workspace_id_from_file_path(
        self, parent_id: str, path: str, entity_removing: bool = True
    ) -> Optional[str]:
        """Walk a file's path up to its workspace id.

        ``entity_removing`` says whether losing the answer costs an entity.
        It does for workbooks and datasets, which are dropped without a
        workspace; not for data models, which fall back to the /dataModels
        payload's own workspaceId. Only the caller knows the file type.

        It is NOT part of the cache key, or a folder holding both would be
        walked twice. The cached walk reports which ancestor died; this
        wrapper decides what that costs.
        """
        workspace_id, failed_ancestor = self._walk_to_workspace(parent_id, path)
        if failed_ancestor is not None:
            self._note_file_path_loss(failed_ancestor, entity_removing)
        return workspace_id

    @functools.lru_cache()
    def _walk_to_workspace(
        self, parent_id: str, path: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """``(workspace_id, None)``, or ``(None, the ancestor that died)``."""
        try:
            path_list = path.split("/")
            while len(path_list) != 1:  # means current parent id is folder's id
                if parent_id in self._file_path_lookup_failed:
                    # Known-broken ancestor. The repeat this prevents is
                    # SIBLING FOLDERS; lru_cache collapses the per-file case.
                    return None, parent_id
                response = self._get_api_call(
                    f"{self.config.api_url}/files/{parent_id}"
                )
                response.raise_for_status()
                parent_id = response.json()[Constant.PARENTID]
                path_list.pop()
            return parent_id, None
        except Exception as e:
            # Through _log_http_error, not logger.error, or the failure misses
            # the counter and no entry names the file.
            self._log_http_error(
                message=f"Unable to find workspace id using file path "
                f"'{path}'. Exception: {e}"
            )
            if not _is_transient(e):
                # Same rule as get_workspace: latching a folder on a blip
                # drops every sibling subtree under it, unretried.
                self._file_path_lookup_failed.add(parent_id)
            return None, parent_id

    def _note_file_path_loss(self, ancestor_id: str, entity_removing: bool) -> None:
        """Count a broken ancestor once, for the callers that lose an entity.

        "Known broken" and "already counted" are separate sets: a Data Model
        walk marks an ancestor without counting -- the DM survives via its own
        payload -- and a later workbook walk short-circuiting on that same
        ancestor must still count, because the workbook IS dropped.
        """
        if not entity_removing or self.config.ingest_shared_entities:
            return
        if ancestor_id not in self._file_path_loss_counted:
            self._file_path_loss_counted.add(ancestor_id)
            self._record_child_listing_failure()

    # Pre-existing cache. It also means a failed fetch is recorded once per
    # file type rather than once per caller, since the partial map is cached.
    @functools.lru_cache
    def _get_files_metadata(self, file_type: str) -> Dict[str, File]:
        logger.debug(f"Fetching file metadata with type {file_type}.")
        file_url = url = (
            f"{self.config.api_url}/files?permissionFilter=view&typeFilters={file_type}"
        )
        # Same rule as the failure below: a missing workspace drops workbooks
        # and datasets, while a data model falls back to its own payload.
        records_failure = file_type in (Constant.DATASET, Constant.WORKBOOK)
        # Kept in hand like the other three listings: a ValidationError names
        # the FIELD, never the object.
        last_row: Dict[str, Any] = {}
        # Outside the try so the failure path can return the pages it already
        # read, like the dataset and workbook listings do.
        files_metadata: Dict[str, File] = {}
        try:
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = _envelope(response.json(), entries_must_be_a_list=True)
                for file_dict in response_dict[Constant.ENTRIES]:
                    last_row = file_dict
                    file = File.model_validate(file_dict)
                    file.workspaceId = self.get_workspace_id_from_file_path(
                        file.parentId,
                        file.path,
                        entity_removing=records_failure,
                    )
                    files_metadata[file_dict[Constant.ID]] = file
                if response_dict[Constant.NEXTPAGE]:
                    url = f"{file_url}&page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break
            self.report.number_of_files_metadata[file_type] = len(files_metadata)
            return files_metadata
        except Exception as e:
            # report_warning=False only where a failure is recorded below:
            # data-model records none, so it would have no entry at all.
            detail = self._log_http_error(
                message=f"Unable to fetch files metadata. Exception: {_exc_text(e)}",
                report_warning=not records_failure,
            )
            # A short map is a deletion, not a thinner result: every workbook
            # and dataset it does not name is dropped for "missing file
            # metadata" with no exception raised. Data Models are exempt --
            # they fall back to the /dataModels payload's own workspaceId.
            if records_failure:
                self._record_enumeration_failure(
                    what=f"file metadata for {file_type}s, which drops every "
                    f"{file_type} the listing did not reach",
                    context=f"{_failed_row(e, last_row)}{detail}, "
                    f"exception={_exc_text(e)}",
                    # A bad row here repeats every run and drops every
                    # workbook, so it needs the advice that clears it.
                    unparseable_row=isinstance(e, _UNREADABLE_ROW),
                    malformed_response=_is_malformed_response(e),
                    status=_http_status(e),
                    # Only reached from get_sigma_datasets, which
                    # ingest_datasets=False skips, so it shares that remedy.
                    optional_feature=(
                        "ingest_datasets=False"
                        if file_type == Constant.DATASET
                        else None
                    ),
                )
            return files_metadata

    def get_connections(self) -> List[Dict[str, Any]]:
        """Fetch all Sigma Connections (paginated). Returns raw API payloads.

        Mapping to SigmaConnectionRecord happens in
        connection_registry.SigmaConnectionRegistry.build().

        Deliberately NOT a run-wide listing failure: a dead /v2/connections
        thins lineage without removing entities, so nothing reads as deleted.
        """
        return self._paginated_raw_entries(
            f"{self.config.api_url}/connections",
            "Unable to fetch Sigma connections.",
        )

    def get_sigma_datasets(self) -> List[SigmaDataset]:
        logger.debug("Fetching all accessible datasets metadata.")
        dataset_url = url = f"{self.config.api_url}/datasets"
        dataset_files_metadata = self._get_files_metadata(file_type=Constant.DATASET)
        datasets: List[SigmaDataset] = []
        # The row in hand when a failure escapes the loop: a ValidationError
        # identifies the FIELD, never the object.
        last_row: Dict[str, Any] = {}
        try:
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = _envelope(response.json(), entries_must_be_a_list=True)
                for dataset_dict in response_dict[Constant.ENTRIES]:
                    last_row = dataset_dict
                    dataset = SigmaDataset.model_validate(dataset_dict)

                    if dataset.datasetId not in dataset_files_metadata:
                        # Counted as well as dropped: _get_files_metadata returns
                        # {} on failure, which silently drops every dataset. The
                        # counter lets the warehouse route tell that apart from a
                        # workspace_pattern exclusion.
                        self.report.datasets_dropped_missing_file_metadata += 1
                        self.report.datasets.dropped(
                            f"{dataset.name} ({dataset.datasetId}) (missing file metadata)"
                        )
                        continue

                    dataset.workspaceId = dataset_files_metadata[
                        dataset.datasetId
                    ].workspaceId

                    dataset.path = dataset_files_metadata[dataset.datasetId].path
                    dataset.badge = dataset_files_metadata[dataset.datasetId].badge

                    workspace = None
                    if dataset.workspaceId:
                        workspace = self.get_workspace(dataset.workspaceId)

                    if workspace:
                        if self.config.workspace_pattern.allowed(workspace.name):
                            self.report.datasets.processed(
                                f"{dataset.name} ({dataset.datasetId}) in {workspace.name}"
                            )
                            datasets.append(dataset)
                        else:
                            self.report.datasets.dropped(
                                f"{dataset.name} ({dataset.datasetId}) in {workspace.name}"
                            )
                    elif self.config.ingest_shared_entities:
                        # If no workspace for dataset we can consider it as shared entity
                        self.report.datasets_without_workspace += 1
                        self.report.datasets.processed(
                            f"{dataset.name} ({dataset.datasetId}) in workspace id {dataset.workspaceId or 'unknown'}"
                        )
                        datasets.append(dataset)
                    else:
                        self.report.datasets.dropped(
                            f"{dataset.name} ({dataset.datasetId}) in workspace id {dataset.workspaceId or 'unknown'}"
                        )

                if response_dict[Constant.NEXTPAGE]:
                    url = f"{dataset_url}?page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break

            return datasets
        except Exception as e:
            # Read by sigma.py to tell "the listing died" apart from "this
            # tenant has no datasets". No warning of its own -- the guidance
            # belongs on the failure below.
            self.report.datasets_listing_failed += 1
            detail = self._log_http_error(
                message=f"Unable to fetch sigma datasets. Exception: {e}",
                report_warning=False,
            )
            self._record_enumeration_failure(
                what="Sigma datasets, so no Sigma Dataset warehouse lineage "
                "can be resolved this run",
                context=f"{_failed_row(e, last_row)}{detail}, exception={_exc_text(e)}",
                # Passed unconditionally; the remedy decides whether it
                # applies.
                optional_feature="ingest_datasets=False",
                unparseable_row=isinstance(e, _UNREADABLE_ROW),
                malformed_response=_is_malformed_response(e),
                status=_http_status(e),
            )
            # Partial rows, not []: the run fails either way, so keeping the
            # pages already read leaves those entities fresh.
            return datasets

    def _process_lineage_node(
        self,
        source_node_id: str,
        source_node: Dict,
        upstream_sources: Dict[str, "ElementUpstream"],
        queue: "Deque[str]",
        element: Element,
        workbook: Workbook,
    ) -> None:
        """Dispatch one BFS node into upstream_sources or re-enqueue it (join)."""
        source_type = source_node.get(Constant.TYPE)
        if source_type == "dataset":
            try:
                upstream_sources[source_node_id] = DatasetUpstream(
                    name=source_node.get(Constant.NAME),
                )
            except ValidationError as e:
                self.report.warning(
                    title="Sigma lineage node parse failed",
                    message="Failed to parse Sigma lineage node",
                    context=f"node={source_node_id}, element={element.name}, workbook={workbook.name}",
                    exc=e,
                )
        elif source_type == "sheet":
            element_id = source_node.get(Constant.ELEMENTID)
            if element_id is None:
                self.report.warning(
                    title="Sigma sheet lineage node missing elementId",
                    message="Sheet upstream node missing elementId",
                    context=f"node={source_node_id}, element={element.name}, workbook={workbook.name}",
                )
                return
            try:
                upstream_sources[source_node_id] = SheetUpstream(
                    name=source_node.get(Constant.NAME),
                    element_id=element_id,
                )
            except ValidationError as e:
                self.report.warning(
                    title="Sigma lineage node parse failed",
                    message="Failed to parse Sigma lineage node",
                    context=f"node={source_node_id}, element={element.name}, workbook={workbook.name}",
                    exc=e,
                )
        elif source_type == "data-model":
            # Node id shape is "<dataModelUrlId>/<opaque_suffix>"; we carry
            # the prefix and the DM-side ``name`` for name-based matching
            # at emit time.
            dm_url_id = (
                source_node_id.split("/")[0]
                if "/" in source_node_id
                else source_node_id
            )
            if not dm_url_id:
                self.report.warning(
                    title="Sigma data-model lineage node missing url-id prefix",
                    message="Sigma data-model lineage node missing url-id prefix",
                    context=(
                        f"node={source_node_id}, element={element.name}, "
                        f"workbook={workbook.name}"
                    ),
                )
                return
            try:
                # Uses the API-reported DM-side name. The edge-only
                # synthesis path below uses the workbook element's own
                # name instead; the two diverge only if the workbook
                # element was renamed after the DM link.
                upstream_sources[source_node_id] = DataModelElementUpstream(
                    name=source_node.get(Constant.NAME),
                    data_model_url_id=dm_url_id,
                )
            except ValidationError as e:
                self.report.warning(
                    title="Sigma lineage node parse failed",
                    message="Failed to parse Sigma lineage node",
                    context=f"node={source_node_id}, element={element.name}, workbook={workbook.name}",
                    exc=e,
                )
        elif source_type == "join":
            queue.append(source_node_id)  # pass-through
        elif source_type == "table":
            # nodeId format: "inode-{urlId}". Strip prefix; name is used for
            # name-based resolution in SigmaSource via wb_warehouse_table_index.
            if not isinstance(source_node_id, str) or not source_node_id.startswith(
                "inode-"
            ):
                self.report.chart_warehouse_table_node_skipped += 1
                logger.debug(
                    "Sigma chart BFS table node has unexpected nodeId format: "
                    "node=%s, element=%s, workbook=%s",
                    source_node_id,
                    element.name,
                    workbook.name,
                )
                return
            url_id = source_node_id[len("inode-") :]
            name = source_node.get(Constant.NAME)
            if not url_id or not isinstance(name, str) or not name:
                self.report.chart_warehouse_table_node_skipped += 1
                logger.debug(
                    "Sigma chart BFS table node missing url_id or name: "
                    "node=%s, name=%r, element=%s, workbook=%s",
                    source_node_id,
                    name,
                    element.name,
                    workbook.name,
                )
                return
            upstream_sources[source_node_id] = WarehouseTableUpstream(
                url_id=url_id,
                name=name,
            )
        elif source_type == "customSQL":
            pass  # handled by _build_workbook_customsql_registry via the workbook-level lineage endpoint
        else:
            # Warn once per unknown source_type to avoid log spam.
            warn_key = source_type if isinstance(source_type, str) else "<non-str>"
            if warn_key not in self._unknown_lineage_node_types_warned:
                self._unknown_lineage_node_types_warned.add(warn_key)
                self.report.warning(
                    title="Unknown Sigma lineage node type",
                    message="Unknown Sigma lineage node type",
                    context=(
                        f"type={source_type!r}, element={element.name}, "
                        f"workbook={workbook.name} (further occurrences of "
                        f"this type will be suppressed)"
                    ),
                )

    def _get_element_upstream_sources(
        self, element: Element, workbook: Workbook
    ) -> Dict[str, ElementUpstream]:
        """Return upstream sources keyed by nodeId, admitting Sigma Dataset
        (``dataset``), intra-workbook element (``sheet``), data-model
        (``data-model``), and direct warehouse table (``table``) nodes. Walks
        through ``join`` pass-through nodes.

        BFS from the queried element's sheet node, following edges in
        reverse (target-to-source), so only reachable upstreams are
        captured (sibling edges in the payload do not leak in).
        """
        upstream_sources: Dict[str, ElementUpstream] = {}

        try:
            response = self._get_api_call(
                f"{self.config.api_url}/workbooks/{workbook.workbookId}/lineage/elements/{element.elementId}"
            )
            if response.status_code == 500:
                logger.debug(
                    f"Lineage metadata not present for element {element.name} of workbook '{workbook.name}'"
                )
                return upstream_sources
            if response.status_code == 403:
                logger.debug(
                    f"Lineage metadata not accessible for element {element.name} of workbook '{workbook.name}'"
                )
                return upstream_sources
            if response.status_code == 400:
                logger.debug(
                    f"Lineage not supported for element {element.name} of workbook '{workbook.name}' (400 Bad Request)"
                )
                return upstream_sources
            response.raise_for_status()
            response_dict = response.json()
        except requests.exceptions.RequestException as e:
            self.report.warning(
                message="Failed to fetch Sigma element lineage",
                context=f"element={element.name}, workbook={workbook.name}",
                exc=e,
            )
            return {}

        try:
            dependencies = response_dict[Constant.DEPENDENCIES]

            # Reverse adjacency (target nodeId -> source nodeIds). A
            # malformed edge skips itself; others still populate.
            edges_by_target: Dict[str, List[str]] = {}
            for edge in response_dict[Constant.EDGES]:
                try:
                    edges_by_target.setdefault(edge[Constant.TARGET], []).append(
                        edge[Constant.SOURCE]
                    )
                except (KeyError, TypeError) as e:
                    self.report.warning(
                        message="Skipping malformed Sigma lineage edge",
                        context=f"edge={edge!r}, element={element.name}, workbook={workbook.name}",
                        exc=e,
                    )

            # Seeds: sheet nodes whose elementId matches the queried
            # element. Sigma typically returns one, but we BFS from all of
            # them defensively.
            seed_node_ids = [
                node_id
                for node_id, node_data in dependencies.items()
                if node_data.get(Constant.TYPE) == "sheet"
                and node_data.get(Constant.ELEMENTID) == element.elementId
            ]

            if not seed_node_ids:
                self.report.warning(
                    message="Could not find sheet node for element in lineage response",
                    context=f"element={element.name}, workbook={workbook.name}",
                )
                return {}

            if len(seed_node_ids) > 1:
                self.report.warning(
                    message="Multiple seed sheet nodes found for element in lineage response",
                    context=f"element={element.name}, workbook={workbook.name}, seed_count={len(seed_node_ids)}",
                )

            # BFS from all seeds, walking edges in reverse (target to source).
            visited: Set[str] = set(seed_node_ids)
            queue: Deque[str] = deque(seed_node_ids)

            while queue:
                current_id = queue.popleft()
                for source_node_id in edges_by_target.get(current_id, []):
                    if source_node_id in visited:
                        continue
                    visited.add(source_node_id)

                    # Workbook-to-DM-element reference: the node
                    # ``<dmUrlId>/<suffix>`` appears only as an edge source
                    # (not as a ``dependencies`` key). Synthesize the
                    # upstream from the edge. The edge carries no DM-side
                    # name, so we fall back to the workbook element's own
                    # name (Sigma's default mirrors the DM element name).
                    # A user rename degrades to
                    # ``element_dm_edge.name_unmatched_but_dm_known``.
                    if source_node_id not in dependencies and "/" in source_node_id:
                        dm_url_id, _, suffix = source_node_id.partition("/")
                        if dm_url_id and suffix:
                            try:
                                upstream_sources[source_node_id] = (
                                    DataModelElementUpstream(
                                        name=element.name,
                                        data_model_url_id=dm_url_id,
                                    )
                                )
                                self.report.element_dm_edge.synthesized_from_edge_only += 1
                            except ValidationError as e:
                                self.report.warning(
                                    title="Sigma DM upstream synthesis from edge-only node failed",
                                    message="Failed to synthesize Sigma DM upstream from edges-only node",
                                    context=(
                                        f"node={source_node_id}, element={element.name}, "
                                        f"workbook={workbook.name}"
                                    ),
                                    exc=e,
                                )
                            continue
                        # Malformed (empty prefix or suffix): fall through
                        # so the legacy dispatch surfaces a warning.

                    # Per-node isolation: a malformed node skips itself.
                    try:
                        source_node = dependencies[source_node_id]
                    except (KeyError, AttributeError, TypeError) as e:
                        self.report.warning(
                            title="Sigma lineage node parse failed",
                            message="Failed to parse Sigma lineage node",
                            context=f"node={source_node_id}, element={element.name}, workbook={workbook.name}",
                            exc=e,
                        )
                        continue

                    try:
                        self._process_lineage_node(
                            source_node_id,
                            source_node,
                            upstream_sources,
                            queue,
                            element,
                            workbook,
                        )
                    except (KeyError, AttributeError, TypeError, ValidationError) as e:
                        # Defence-in-depth; the helper already handles
                        # ValidationError internally.
                        self.report.warning(
                            title="Sigma lineage node parse failed",
                            message="Failed to parse Sigma lineage node",
                            context=f"node={source_node_id}, element={element.name}, workbook={workbook.name}",
                            exc=e,
                        )
        except (KeyError, AttributeError, TypeError, ValidationError) as e:
            # Structural errors in the setup phase (missing keys, malformed
            # edges, non-dict seed entries).
            self.report.warning(
                title="Sigma element lineage response parse failed",
                message="Failed to parse Sigma element lineage response",
                context=f"element={element.name}, workbook={workbook.name}",
                exc=e,
            )
            return {}

        return upstream_sources

    def _get_element_sql_query(
        self, element: Element, workbook: Workbook
    ) -> Optional[str]:
        try:
            response = self._get_api_call(
                f"{self.config.api_url}/workbooks/{workbook.workbookId}/elements/{element.elementId}/query"
            )
            if response.status_code == 404:
                logger.debug(
                    f"Query not present for element {element.name} of workbook '{workbook.name}'"
                )
                return None
            response.raise_for_status()
            response_dict = response.json()
            if "sql" in response_dict:
                return response_dict["sql"]
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch sql query for element {element.name} of workbook '{workbook.name}'. Exception: {e}"
            )
        return None

    def get_workbook_column_formulas(
        self, workbook_id: str
    ) -> Tuple[Dict[str, Dict[str, Optional[str]]], Dict[str, Dict[str, str]]]:
        """Fetch per-element column formulas from GET /workbooks/{id}/columns.

        Returns a tuple:
          - elementId -> {column_name: formula_or_None}
          - elementId -> {column_name: raw_columnId}

        The raw_columnId for warehouse-backed columns is "inode-{tableUrlId}/{NATIVE_NAME}";
        for DM-backed columns it is an opaque hash with no slash.

        The /columns endpoint is the authoritative source for column formulas
        in production; the page-elements endpoint returns columns as plain strings.
        If pagination aborts partway through, a report warning is emitted by
        _paginated_raw_entries and column_formulas_fetch_partial is incremented so
        the partial-data workbook is distinguishable from one with few formulas.
        """
        error_ctx = f"Unable to fetch column formulas for workbook {workbook_id}."
        aborts_before = self.report.pagination_aborted
        result: Dict[str, Dict[str, Optional[str]]] = {}
        col_ids: Dict[str, Dict[str, str]] = {}
        for col in self._paginated_raw_entries(
            f"{self.config.api_url}/workbooks/{workbook_id}/columns",
            error_ctx,
            silent_statuses=(404,),
        ):
            elem_id = col.get(Constant.ELEMENTID)
            name = col.get(Constant.NAME)
            formula: Optional[str] = col.get("formula") or None
            column_id: str = col.get("columnId") or ""
            if elem_id and name:
                result.setdefault(elem_id, {})[name] = formula
                if column_id:
                    col_ids.setdefault(elem_id, {})[name] = column_id
        if self.report.pagination_aborted > aborts_before:
            # Not warnings.total_elements: that counts distinct KEYS, and
            # every abort shares one title+message.
            self.report.column_formulas_fetch_partial += 1
        return result, col_ids

    def get_page_elements(
        self,
        workbook: Workbook,
        page: Page,
        column_formulas_by_element: Optional[
            Dict[str, Dict[str, Optional[str]]]
        ] = None,
        column_ids_by_element: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> List[Element]:
        try:
            elements: List[Element] = []
            response = self._get_api_call(
                f"{self.config.api_url}/workbooks/{workbook.workbookId}/pages/{page.pageId}/elements"
            )
            response.raise_for_status()
            for i, element_dict in enumerate(response.json()[Constant.ENTRIES]):
                # only element of table and visualization type have lineage and sql query supported
                if element_dict.get("type") not in ["table", "visualization"]:
                    logger.debug(
                        f"Skipping lineage and sql query extraction for element {element_dict.get('name')} of type {element_dict.get('type')} of workbook '{workbook.name}'"
                    )
                    continue

                if not element_dict.get(Constant.NAME):
                    element_dict[Constant.NAME] = (
                        f"Element {i + 1} of Page '{page.name}'"
                    )
                element_dict[Constant.URL] = (
                    f"{workbook.url}?:nodeId={element_dict[Constant.ELEMENTID]}&:fullScreen=true"
                )
                element = Element.model_validate(element_dict)
                if column_formulas_by_element is not None:
                    element.column_formulas = column_formulas_by_element.get(
                        element.elementId, {}
                    )
                if column_ids_by_element is not None:
                    element.column_id_by_name = column_ids_by_element.get(
                        element.elementId, {}
                    )
                if (
                    self.config.extract_lineage
                    and self.config.workbook_lineage_pattern.allowed(workbook.name)
                ):
                    element.upstream_sources = self._get_element_upstream_sources(
                        element, workbook
                    )
                    element.query = self._get_element_sql_query(element, workbook)
                elements.append(element)
            return elements
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch elements of page '{page.name}', workbook '{workbook.name}'. Exception: {e}"
            )
            self._record_child_listing_failure()
            return []

    def get_workbook_pages(self, workbook: Workbook) -> List[Page]:
        try:
            pages: List[Page] = []
            column_formulas_by_element: Optional[
                Dict[str, Dict[str, Optional[str]]]
            ] = None
            column_ids_by_element: Optional[Dict[str, Dict[str, str]]] = None
            if (
                self.config.extract_lineage
                and self.config.workbook_lineage_pattern.allowed(workbook.name)
            ):
                column_formulas_by_element, column_ids_by_element = (
                    self.get_workbook_column_formulas(workbook.workbookId)
                )
            response = self._get_api_call(
                f"{self.config.api_url}/workbooks/{workbook.workbookId}/pages"
            )
            response.raise_for_status()
            for page_dict in response.json()[Constant.ENTRIES]:
                page = Page.model_validate(page_dict)
                page.elements = self.get_page_elements(
                    workbook,
                    page,
                    column_formulas_by_element=column_formulas_by_element,
                    column_ids_by_element=column_ids_by_element,
                )
                pages.append(page)
            return pages
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch pages of workbook '{workbook.name}'. Exception: {e}"
            )
            self._record_child_listing_failure()
            return []

    def _paginated_raw_entries(
        self,
        base_url: str,
        error_ctx: str,
        silent_statuses: Tuple[int, ...] = (),
        enumerates_entities: bool = False,
        scoped_to_parent: bool = False,
        optional_feature: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Page through a Sigma list endpoint and return raw ``entries``
        dicts. Handles both pagination shapes (``nextPage`` and
        ``nextPageToken``) and guards against pathological proxies that
        return the same cursor twice in a row (which would otherwise loop
        forever and accumulate duplicates). HTTP/JSON errors abort
        pagination and surface a report warning; partial results
        collected before the failure are preserved. ``silent_statuses``
        lets callers treat expected "no data" statuses (e.g. 404 from
        Sigma's /lineage on empty DMs) as an empty list without emitting
        a warning -- applied only to the first page so later-page
        failures still surface. ``enumerates_entities`` marks a RUN-WIDE listing
        whose rows ARE entities, so an abort fails the run rather than
        warning; ``scoped_to_parent`` marks a listing of one parent's
        children, which is counted instead -- see
        :meth:`_record_enumeration_failure` and
        :meth:`_record_child_listing_failure`.
        """
        # A run-wide listing that swallows a status can report zero rows for
        # a live endpoint. Not a bare `assert`, which -O would strip.
        if silent_statuses and enumerates_entities:
            raise AssertionError(
                "silent_statuses on a run-wide listing would hide a dead "
                f"listing behind an empty result: {error_ctx}"
            )
        # Use ``&`` as the separator if the base URL already has query
        # params (e.g. an ``api_url`` routed through a proxy), so
        # pagination does not collide with existing params.
        separator = "&" if "?" in base_url else "?"
        url = base_url
        raw_entries: List[Dict[str, Any]] = []
        # Cycle protection: a broken proxy (or caching layer) can echo the
        # same ``nextPage`` / ``nextPageToken`` back on every call. Track
        # (kind, value) tuples so a cycle that crosses cursor types is also
        # detected (e.g. page=1 → nextPageToken=1 → page=1 repeating).
        seen_cursors: Set[Tuple[str, str]] = set()
        first_page = True
        pages_read = 0
        try:
            while True:
                response = self._get_api_call(url)
                if first_page and response.status_code in silent_statuses:
                    logger.debug(
                        f"{error_ctx} Swallowed expected status "
                        f"{response.status_code} on first page."
                    )
                    return raw_entries
                first_page = False
                response.raise_for_status()
                # After raise_for_status, so pages_read is pages actually READ:
                # 0 means the first page itself failed.
                pages_read += 1
                response_dict = _envelope(response.json())
                entries = response_dict.get(Constant.ENTRIES)
                if enumerates_entities and not isinstance(entries, list):
                    # `.get(..., [])` made a page with NO `entries` look like
                    # a page with none: zero rows, nothing reported, and
                    # everything soft-deleted.
                    raise KeyError(Constant.ENTRIES)
                for entry in entries or []:
                    if isinstance(entry, dict):
                        raw_entries.append(entry)
                if enumerates_entities and not (
                    Constant.NEXTPAGE in response_dict
                    or Constant.NEXTPAGETOKEN in response_dict
                ):
                    # A RENAMED cursor ends the listing silently: neither key
                    # is present, the loop breaks, and every later page is
                    # missing from a run that passes. The hand-written listings
                    # index NEXTPAGE directly against live Sigma, which is the
                    # evidence that it is always present. Run-wide only: a
                    # truncated detail endpoint costs a thinner entity.
                    raise KeyError(Constant.NEXTPAGE)
                next_page = response_dict.get(Constant.NEXTPAGE)
                next_token = response_dict.get(Constant.NEXTPAGETOKEN)
                if next_page:
                    cursor_key: Tuple[str, str] = ("page", str(next_page))
                    cursor = urlencode({"page": next_page})
                elif next_token:
                    cursor_key = ("nextPageToken", str(next_token))
                    cursor = urlencode({"nextPageToken": next_token})
                else:
                    break
                if cursor_key in seen_cursors:
                    # Truncates the listing as an HTTP abort does, so it
                    # takes the same accounting.
                    self.report.pagination_aborted += 1
                    if enumerates_entities:
                        # One entry: the failure already carries the cursor
                        # and page count, so a warning would repeat them.
                        self._record_enumeration_failure(
                            what="a paginated entity listing",
                            context=f"endpoint={error_ctx}, url={base_url}, "
                            f"repeated_cursor={cursor}, "
                            f"partial_results={len(raw_entries)}, "
                            f"pages_read={pages_read}",
                            optional_feature=optional_feature,
                            # Sigma answered; its pagination came back wrong.
                            # Same class as a page with no `entries`.
                            malformed_response=True,
                        )
                    else:
                        self.report.warning(
                            title="Sigma paginated endpoint aborted",
                            message="Pagination cursor repeated; aborting.",
                            context=f"endpoint={error_ctx}, url={base_url}, "
                            f"cursor={cursor}, "
                            f"partial_results={len(raw_entries)}, "
                            f"pages_read={pages_read}",
                        )
                        if scoped_to_parent:
                            self._record_child_listing_failure()
                    break
                seen_cursors.add(cursor_key)
                url = f"{base_url}{separator}{cursor}"
            return raw_entries
        except Exception as e:
            # _log_http_error alone is debug-level, which would leave the DM
            # looking healthy while its elements are missing. The status goes
            # in ``context``, not ``title``, so LossyList groups all aborts.
            detail = self._log_http_error(
                message=f"{error_ctx} Exception: {e}", report_warning=False
            )
            self.report.pagination_aborted += 1
            # Which page died separates "refused outright" from "served rows
            # and then stopped".
            where = (
                f"endpoint={error_ctx}, url={url}, "
                f"partial_results={len(raw_entries)}, pages_read={pages_read}"
            )
            if enumerates_entities:
                # ONE entry: the rows this call would have returned ARE the
                # entities, so the warning's detail is folded into the
                # failure's context.
                self._record_enumeration_failure(
                    what="a paginated entity listing",
                    context=f"{where}, {detail}, exception={e}",
                    optional_feature=optional_feature,
                    # Without this the only paginated run-wide listing --
                    # /dataModels -- reads a 403 as "transient, re-run".
                    status=_http_status(e),
                    # A missing `entries`, an unknown cursor name and a
                    # non-JSON body are all "the wrong shape".
                    malformed_response=_is_malformed_response(e),
                )
            else:
                # The warning is this failure's only entry -- a child listing
                # gets one grouped info and nothing else.
                self.report.warning(
                    title="Sigma paginated endpoint aborted",
                    message="Pagination aborted; partial results preserved.",
                    context=f"{where}, {detail}",
                    exc=e,
                )
                if scoped_to_parent:
                    self._record_child_listing_failure()
            return raw_entries

    # Cap per-endpoint malformed-entry warnings so a vendor regression
    # that breaks every row cannot flood the report with thousands of
    # identical warnings. Total dropped count is tracked via the
    # ``pagination_malformed_entries_dropped`` counter.
    _MAX_MALFORMED_WARNINGS_PER_ENDPOINT: int = 10

    def _paginated_entries(
        self,
        base_url: str,
        model_cls: Type[T],
        error_ctx: str,
        dedup_key: Optional[Callable[[T], Hashable]] = None,
        enumerates_entities: bool = False,
        scoped_to_parent: bool = False,
        optional_feature: Optional[str] = None,
    ) -> List[T]:
        """Page through a Sigma list endpoint, parsing each entry into
        ``model_cls``. Shares pagination / cycle-protection logic with
        :meth:`_paginated_raw_entries`. Per-entry ``ValidationError``
        drops only that entry (so one malformed row cannot empty the
        whole list). ``dedup_key`` lets callers collapse duplicates by
        a natural key so an echoed pagination cursor (or server-side
        overlap between pages) cannot leak duplicate aspects downstream
        -- matters because the same element/column emitted twice
        double-counts counters and re-upserts the same aspect for the
        same URN.
        """
        results: List[T] = []
        seen_keys: Set[Hashable] = set()
        failures_before = self.report.entity_enumeration_failed
        malformed_warned = 0
        malformed_dropped = 0
        first_malformed = ""
        for entry in self._paginated_raw_entries(
            base_url,
            error_ctx,
            enumerates_entities=enumerates_entities,
            scoped_to_parent=scoped_to_parent,
            optional_feature=optional_feature,
        ):
            try:
                parsed = model_cls.model_validate(entry)
            except ValidationError as ve:
                self.report.pagination_malformed_entries_dropped += 1
                malformed_dropped += 1
                first_malformed = first_malformed or (
                    f"{_row_identity(entry)}, validation_error={_terse(ve)}"
                )
                if malformed_warned < self._MAX_MALFORMED_WARNINGS_PER_ENDPOINT:
                    self.report.warning(
                        message="Dropped malformed entry",
                        context=f"{error_ctx} entry={entry!r}",
                        exc=ve,
                    )
                    malformed_warned += 1
                continue
            if dedup_key is not None:
                key = dedup_key(parsed)
                if key in seen_keys:
                    self.report.pagination_duplicate_entries_dropped += 1
                    continue
                seen_keys.add(key)
            results.append(parsed)
        if (
            enumerates_entities
            and malformed_dropped > len(results)
            # Page 1 of bad rows then page 2 dying is ONE dead listing; a
            # second entry would contradict the first.
            and self.report.entity_enumeration_failed == failures_before
        ):
            # ONE bad row is a bounded loss, counted not failed: it fails
            # identically every run, so failing would freeze soft-deletion
            # tenant-wide with no remedy. MOST rows failing is a different
            # event. `not results` was too weak a line -- 1 good row among 99
            # bad ones passed it, and fail_safe_threshold does not backstop
            # that, since it measures all URNs of every type.
            self._record_enumeration_failure(
                what="most rows of a paginated entity listing, so what came "
                "back is not a usable listing",
                context=f"endpoint={error_ctx}, rows_dropped={malformed_dropped}, "
                f"rows_parsed={len(results)}, "
                f"first={first_malformed}",
                unparseable_row=True,
            )
        return results

    def _get_data_model_elements(
        self, data_model_id: str
    ) -> List[SigmaDataModelElement]:
        logger.debug(f"Fetching elements for data model '{data_model_id}'.")
        return self._paginated_entries(
            f"{self.config.api_url}/dataModels/{data_model_id}/elements",
            SigmaDataModelElement,
            f"Unable to fetch elements for data model '{data_model_id}'.",
            dedup_key=lambda element: element.elementId,
            scoped_to_parent=True,
        )

    def _get_data_model_columns(self, data_model_id: str) -> List[SigmaDataModelColumn]:
        logger.debug(f"Fetching columns for data model '{data_model_id}'.")
        return self._paginated_entries(
            f"{self.config.api_url}/dataModels/{data_model_id}/columns",
            SigmaDataModelColumn,
            f"Unable to fetch columns for data model '{data_model_id}'.",
            # Dedup by (elementId, columnId): Sigma reuses warehouse-native
            # columnIds (e.g. CUSTOMER_ID) across customSQL elements that share
            # a warehouse passthrough column. Keying on columnId alone silently
            # drops all but the first occurrence, removing real passthrough
            # columns from consumer elements' schemaMetadata.
            dedup_key=lambda column: (column.elementId, column.columnId),
        )

    def _get_data_model_lineage_entries(
        self, data_model_id: str
    ) -> List[Dict[str, Any]]:
        """Return raw entries from the DM /lineage endpoint. ``element``
        entries carry ``elementId`` + ``sourceIds`` (intra-DM elementIds
        or ``inode-<suffix>`` for external upstreams); ``dataset`` /
        ``table`` entries carry ``inodeId``. Paginated via
        :meth:`_paginated_raw_entries` so large DMs whose lineage spans
        multiple pages are not silently truncated. Sigma returns
        400/403/404 for DMs with no lineage graph (empty DMs or
        permission-scoped views); those are swallowed silently. 5xx
        responses are *not* in ``silent_statuses`` -- a globally
        degraded Sigma region would otherwise produce zero lineage
        aspects with zero warnings, which is worse than a loud
        warning per affected DM.

        Raw entries are deduped by a shape-aware natural key
        (``elementId`` for elements, ``inodeId`` for dataset/table,
        ``nodeId`` as a last-ditch fallback for any future shape) to
        defuse the same cursor-echo / pagination-overlap concern
        :meth:`_paginated_entries` guards against for typed models.
        Picking the *wrong* key silently collapses multiple real rows
        into one (e.g. keying elements on an absent ``nodeId`` would
        discard every element after the first), so this function is
        intentionally conservative: entries whose expected identifier
        is missing are preserved, not dropped.
        """
        logger.debug(f"Fetching lineage for data model '{data_model_id}'.")
        raw = self._paginated_raw_entries(
            f"{self.config.api_url}/dataModels/{data_model_id}/lineage",
            f"Unable to fetch lineage for data model '{data_model_id}'.",
            silent_statuses=(400, 403, 404),
        )
        deduped: List[Dict[str, Any]] = []
        # Map natural key -> index into ``deduped`` so we can merge on
        # collision rather than silently drop the second occurrence.
        # For ``element`` rows, merging means union of ``sourceIds``;
        # for ``dataset`` / ``table`` rows it's a no-op (there is no
        # payload field we'd want to accumulate across duplicates).
        # The union-on-collision shape handles:
        #   (a) a proxy that echoes the same page cursor -- the two
        #       rows are identical so the union collapses to the same
        #       set.
        #   (b) a future Sigma-side API change that splits one
        #       element's lineage across multiple rows (e.g. versioned
        #       upstreams) -- we accumulate rather than losing the
        #       trailing rows' ``sourceIds``.
        seen_index: Dict[Tuple[str, str], int] = {}
        for entry in raw:
            entry_type = str(entry.get(Constant.TYPE, ""))
            if entry_type == "element":
                identifier = str(entry.get(Constant.ELEMENTID, ""))
            elif entry_type in ("dataset", "table"):
                identifier = str(entry.get("inodeId", ""))
            else:
                identifier = str(entry.get("nodeId", ""))
            # Preserve entries whose natural identifier is absent rather
            # than collapsing them under the same empty-string key --
            # the point of dedup is cursor echo, not data loss.
            if not entry_type or not identifier:
                deduped.append(entry)
                continue
            key = (entry_type, identifier)
            existing_idx = seen_index.get(key)
            if existing_idx is None:
                seen_index[key] = len(deduped)
                deduped.append(entry)
                continue
            self.report.pagination_duplicate_entries_dropped += 1
            if entry_type == "element":
                existing = deduped[existing_idx]
                existing_sources = existing.get("sourceIds") or []
                new_sources = entry.get("sourceIds") or []
                if not isinstance(existing_sources, list) or not isinstance(
                    new_sources, list
                ):
                    # Vendor shape drift: don't attempt a merge on a
                    # non-list sourceIds -- the enclosing
                    # ``_assemble_data_model`` defensively validates
                    # per-element and will simply see the already-stored
                    # row.
                    continue
                # Preserve first-seen order while unioning the second
                # row's additions. Stringify-and-dedupe matches the
                # invariant enforced downstream by
                # ``_assemble_data_model`` (non-string sourceIds are
                # filtered out there anyway).
                seen_sources: Set[str] = {
                    s for s in existing_sources if isinstance(s, str)
                }
                merged = list(existing_sources)
                for s in new_sources:
                    if isinstance(s, str) and s not in seen_sources:
                        merged.append(s)
                        seen_sources.add(s)
                existing["sourceIds"] = merged
        return deduped

    def get_workbook_lineage_entries(self, workbook_id: str) -> List[Dict[str, Any]]:
        """Return raw entries from GET /v2/workbooks/{id}/lineage.

        ``customSQL`` entries carry the SQL definition; ``element`` entries
        carry ``elementId`` + ``sourceIds`` pointing at customSQL names.

        Sigma returns 400 (not 404) for workbooks that have no lineage graph at
        all (empirically observed — workbooks whose only sources are non-SQL
        warehouse tables never have a /lineage endpoint and return 400).
        403/404 cover permission-scoped views and deleted workbooks.  5xx is
        intentionally *not* silenced: a degraded Sigma API would otherwise
        produce zero lineage aspects with zero warnings.
        """
        logger.debug(f"Fetching lineage for workbook '{workbook_id}'.")
        return self._paginated_raw_entries(
            f"{self.config.api_url}/workbooks/{workbook_id}/lineage",
            f"Unable to fetch lineage for workbook '{workbook_id}'.",
            silent_statuses=(400, 403, 404),
        )

    def _assemble_data_model(
        self,
        data_model: SigmaDataModel,
        file_meta: Optional[File],
        resolved_workspace_id: Optional[str] = None,
    ) -> None:
        """Fetch and attach elements, per-element columns, and per-element
        sourceIds. If ``resolved_workspace_id`` is provided by the caller,
        it overrides whatever the ``/dataModels`` payload or ``/files``
        row reported, ensuring filtering (done by the caller) and
        rendering (done below) agree on a single workspace per DM.
        """
        if resolved_workspace_id is not None:
            # Overwrite unconditionally: ``get_data_models`` already
            # resolved ``(file_meta.workspaceId, data_model.workspaceId)``
            # into a single "authoritative" workspace with /files
            # preferred. Leaving the original ``data_model.workspaceId``
            # intact would produce DMs filtered under workspace B but
            # rendered / counted under workspace A when the two
            # disagree (e.g. a DM moved across workspaces, or a
            # Sigma-side inconsistency between the two endpoints).
            data_model.workspaceId = resolved_workspace_id
        if file_meta is not None:
            # These secondary fields are /files-authoritative (the folder
            # tree); fall back to them only when the /dataModels payload
            # did not carry a value.
            if data_model.path is None:
                data_model.path = file_meta.path
            if data_model.badge is None:
                data_model.badge = file_meta.badge
            if data_model.urlId is None and file_meta.urlId:
                data_model.urlId = file_meta.urlId

        elements = self._get_data_model_elements(data_model.dataModelId)
        columns = self._get_data_model_columns(data_model.dataModelId)
        # ``extract_lineage=False`` is a historical opt-out for the
        # privileged ``/workbooks/{id}/lineage`` surface; users who set
        # it don't expect the connector to call *any* ``/lineage``
        # endpoint. Honor that here so ``ingest_data_models=True +
        # extract_lineage=False`` emits DM Containers + element Datasets
        # + ``SchemaMetadata`` (the "catalog without upstreams" shape),
        # rather than silently reaching for the same lineage surface
        # under a different flag.
        if self.config.extract_lineage:
            lineage_entries = self._get_data_model_lineage_entries(
                data_model.dataModelId
            )
        else:
            lineage_entries = []

        columns_by_element: Dict[str, List[SigmaDataModelColumn]] = {}
        for column in columns:
            if column.elementId is None:
                # DM-global calculations: no element to attach to.
                self.report.data_model_columns_without_element_dropped += 1
                continue
            columns_by_element.setdefault(column.elementId, []).append(column)

        # Reset before populating so repeated _assemble_data_model calls
        # (e.g. during alias discovery) cannot accumulate stale entries.
        data_model.source_dm_element_names = {}
        data_model.warehouse_inodes_by_inode_id = {}
        data_model.custom_sql_by_name = {}
        source_ids_by_element: Dict[str, List[str]] = {}
        for entry in lineage_entries:
            entry_type = entry.get(Constant.TYPE)
            if entry_type == "element":
                element_id = entry.get(Constant.ELEMENTID)
                source_ids = entry.get("sourceIds") or []
                if element_id and isinstance(source_ids, list):
                    source_ids_by_element[element_id] = [
                        s for s in source_ids if isinstance(s, str)
                    ]
            elif entry_type == "data-model":
                # Each ``data-model`` entry names the specific element consumed
                # from a source DM.  Stash by source dataModelId so
                # ``_resolve_dm_element_cross_dm_upstream`` can look up the
                # correct source element name without relying on the consuming
                # element sharing that name.
                src_dm_id = entry.get("dataModelId")
                src_name = entry.get("name")
                if (
                    isinstance(src_dm_id, str)
                    and src_dm_id
                    and isinstance(src_name, str)
                    and src_name.strip()
                ):
                    data_model.source_dm_element_names.setdefault(src_dm_id, []).append(
                        src_name.strip()
                    )
            elif entry_type == "table":
                # Stash raw warehouse-table nodes keyed by inodeId so
                # SigmaSource can call /files/{inodeId} for the urlId + path
                # needed to construct fully-qualified warehouse Dataset URNs.
                inode_id = str(entry.get("inodeId") or "")
                conn_id = str(entry.get("connectionId") or "")
                if not (inode_id and conn_id):
                    self.report.dm_element_warehouse_table_entry_incomplete += 1
                    missing = [
                        f
                        for f, v in (
                            ("inodeId", inode_id),
                            ("connectionId", conn_id),
                        )
                        if not v
                    ]
                    self.report.warning(
                        title="Sigma type=table lineage entry is incomplete",
                        message=(
                            "A type=table lineage entry is missing inodeId or "
                            "connectionId. Warehouse upstream skipped."
                        ),
                        context=(
                            f"dm={data_model.dataModelId}, missing_fields={missing}"
                        ),
                    )
                    continue
                raw: WarehouseInodeRaw = {"connectionId": conn_id}
                data_model.warehouse_inodes_by_inode_id[inode_id] = raw
            elif entry_type in ("customSQL", "customSql"):
                name = entry.get("name")
                if isinstance(name, str) and name:
                    # Sigma's API normally guarantees unique entry names within
                    # a DM.  A collision here means a payload anomaly; last
                    # entry wins so downstream elements still resolve (with a
                    # warning so operators can investigate).
                    if name in data_model.custom_sql_by_name:
                        self.report.warning(
                            title="Sigma DM customSQL duplicate entry name",
                            message="Two customSQL lineage entries share the same name; later entry overwrites earlier — elements sourcing the earlier entry will get the wrong SQL.",
                            context=f"dataModelId={data_model.dataModelId!r}, name={name!r}",
                        )
                    data_model.custom_sql_by_name[name] = CustomSqlEntry.model_validate(
                        entry
                    )
                else:
                    self.report.warning(
                        title="Sigma DM customSQL entry missing name",
                        message="A customSQL lineage entry has a missing or non-string name field; it will be skipped and any elements referencing it will have no warehouse lineage.",
                        context=f"dataModelId={data_model.dataModelId!r}, entry_keys={sorted(entry.keys())!r}",
                    )
            # ``type: dataset`` entries (CSV uploads) are terminal.

        for element in elements:
            element.columns = columns_by_element.get(element.elementId, [])
            element.source_ids = source_ids_by_element.get(element.elementId, [])

        data_model.elements = elements

    def _dataset_sources_gone_for(self, dataset_id: str) -> bool:
        """Whether /sources now fails for a dataset that answered 200 earlier.

        410 counts as well as the not-found statuses: it is the strongest
        removal signal the endpoint can give, so a re-probe that returns it must
        not be read as "this dataset is fine".
        """
        url = f"{self.config.api_url}/datasets/{quote(dataset_id, safe='')}/sources"
        try:
            return self._get_api_call(url).status_code in (
                _DATASET_SOURCES_NOT_FOUND_STATUSES | {410}
            )
        except Exception as e:
            # Reported, not just logged: a probe that cannot answer leaves the
            # route on, so an operator seeing missing lineage needs to know the
            # check itself failed rather than concluded "alive".
            self.report.warning(
                title="Sigma dataset sources re-probe failed",
                message=(
                    "Could not re-check /datasets/{id}/sources for a dataset that "
                    "resolved earlier this run, so the endpoint is assumed still "
                    "available. If Sigma has removed it, lineage will be missing "
                    "without the 'endpoint unavailable' warning."
                ),
                context=f"dataset_id={dataset_id}",
                exc=e,
            )
            return False

    def _reference_says_endpoint_gone(self, dataset_id: str) -> bool:
        """Whether a dead reference dataset points at endpoint removal.

        Asked when /sources has stopped answering for the dataset that worked
        earlier. ``GET /v2/datasets/{id}`` then distinguishes three cases:

        - **200** -- the dataset is there but its /sources is not: the endpoint
          went away. Latch.
        - **404/410** -- the dataset API path itself is gone, which is removal a
          step further along. Latch, rather than rotating and reporting that the
          API "still responds" when it plainly does not.
        - **anything else**, notably 409 inode_archived -- the reference was
          archived mid-run, so it says nothing about the endpoint. Rotate. A
          transient 5xx lands here too, which is the safe side: rotating costs
          one dataset, latching wrongly costs every dataset after it.
        """
        url = f"{self.config.api_url}/datasets/{quote(dataset_id, safe='')}"
        try:
            return self._get_api_call(url).status_code in (200, 404, 410)
        except Exception:
            logger.debug(
                "Reference probe failed for %r; rotating rather than latching.",
                dataset_id,
            )
            return False

    def _dataset_api_is_gone(self, dataset_id: str) -> bool:
        """Whether the deprecated dataset API itself has been removed.

        Called only to disambiguate a 404 from /sources. ``GET /v2/datasets/{id}``
        Only 404/410 count: those are path-level. A dataset Sigma cannot resolve
        answers 409 inode_archived (verified live), which proves the API is
        answering and the problem is that one dataset.
        """
        url = f"{self.config.api_url}/datasets/{quote(dataset_id, safe='')}"
        try:
            return self._get_api_call(url).status_code in (404, 410)
        except Exception:
            # Cannot tell; assume alive so one flaky probe does not disable the
            # route for the rest of the run.
            logger.debug("Dataset API probe failed for %r; assuming alive.", dataset_id)
            return False

    def _mark_dataset_sources_gone(self, dataset_id: str, status: int) -> None:
        """Latch the dataset-sources endpoint as removed and warn once."""
        if not self._dataset_sources_endpoint_gone:
            self._dataset_sources_endpoint_gone = True
            self.report.dataset_sources_endpoint_removed += 1
            self.report.warning(
                title="Sigma dataset sources endpoint unavailable",
                # Constant: StructuredLogs keys entries on title+message, and an
                # interpolated status would make each one distinct. The status
                # belongs in context.
                message=(
                    "/datasets/{id}/sources is no longer answering. Sigma retired "
                    "the dataset API on 2026-09-15, so it has most likely been "
                    "removed. No Sigma Dataset will get warehouse upstreamLineage "
                    "for the rest of this run; migrate datasets to Data Models."
                ),
                context=f"dataset_id={dataset_id}, http_status={status}",
            )

    def get_dataset_sources(self, dataset_id: str) -> Optional[List[Dict[str, Any]]]:
        """Fetch the raw ``/datasets/{datasetId}/sources`` entries, or None on failure.

        Shape is ``[{"type": "table", "inodeId": "<uuid>"}, ...]`` -- a bare
        list, not the ``{"entries": [...]}`` envelope other Sigma endpoints use,
        and not paginated. A non-list body is a failure rather than coerced, so
        an envelope change surfaces instead of silently resolving zero sources.

        Sigma marks this endpoint deprecated alongside the rest of the dataset
        API, so this whole route is a stopgap: it exists to keep lineage alive
        for datasets that have not been migrated to Data Models yet, and will
        stop returning anything once Sigma removes it. A 410 therefore latches
        the endpoint as gone; a 404 is disambiguated against
        ``GET /v2/datasets/{id}`` first, since one deleted dataset also 404s.
        """
        if self._dataset_sources_endpoint_gone:
            # Counted so operators can see how much lineage the latch cost.
            self.report.dataset_sources_skipped_endpoint_gone += 1
            return None
        logger.debug("Fetching sources for dataset '%s'.", dataset_id)
        url = f"{self.config.api_url}/datasets/{quote(dataset_id, safe='')}/sources"
        try:
            response = self._get_api_call(url)
            if response.status_code == 410:
                # 410 Gone is unambiguous: the endpoint is retired. Latch at once.
                self._mark_dataset_sources_gone(dataset_id, 410)
                return None
            if response.status_code in _DATASET_SOURCES_NOT_FOUND_STATUSES:
                # Ambiguous on its own: the endpoint may be retired, or just this
                # dataset may have been deleted or re-permissioned since the
                # listing (workbooks are processed long after it). Note Sigma
                # answers 409 inode_archived, not 404, for a dataset it cannot
                # resolve -- verified against a live tenant -- so both statuses
                # land here.
                status = response.status_code
                known_good = self._known_good_dataset_id
                if known_good is not None:
                    # Cheapest decisive check: re-ask for a dataset whose
                    # /sources answered 200 earlier this run.
                    if self._dataset_sources_gone_for(known_good):
                        # /sources failed for the reference too. Two causes: the
                        # endpoint is gone, or that dataset was archived mid-run
                        # (operators do this while migrating). Ask the dataset
                        # API which.
                        if self._reference_says_endpoint_gone(known_good):
                            # Either the reference is still there and its
                            # /sources is not, or the dataset API path has gone
                            # too. Both mean removal.
                            self._mark_dataset_sources_gone(dataset_id, status)
                            return None
                        # The reference was archived (or the probe could not
                        # answer), so it proves nothing about the endpoint.
                        # Forget it and let the next success pick a live one;
                        # latching here would disable the route for every
                        # dataset processed afterwards.
                        logger.debug(
                            "Known-good dataset %r no longer exists; dropping it "
                            "as the re-probe reference.",
                            known_good,
                        )
                        self._known_good_dataset_id = None
                elif self._dataset_api_is_gone(dataset_id):
                    # No reference to consult -- either nothing has succeeded
                    # yet, or the previous reference was just rotated away. Fall
                    # back to asking whether the dataset API path still answers.
                    self._mark_dataset_sources_gone(dataset_id, status)
                    return None
                self.report.dataset_sources_lookup_failed += 1
                self.report.dataset_sources_not_found += 1
                if (
                    not self._dataset_sources_succeeded
                    and self.report.dataset_sources_not_found
                    >= _DATASET_SOURCES_NOT_FOUND_WARN_THRESHOLD
                ):
                    # Every dataset missing and none ever resolved is not a run of
                    # deleted datasets. Escalate once: identical infos collapse
                    # into a single entry, so the run would otherwise look clean
                    # while losing all dataset lineage.
                    if not self._dataset_sources_not_found_warned:
                        self._dataset_sources_not_found_warned = True
                        self.report.warning(
                            title="Sigma dataset sources unavailable for every dataset",
                            message=(
                                "/datasets/{id}/sources has not resolved for any "
                                "dataset this run. Sigma is retiring the dataset "
                                "API, so the endpoint may be partially removed; no "
                                "Sigma Dataset will get warehouse upstreamLineage. "
                                "Migrate datasets to Data Models."
                            ),
                            context=(
                                f"not_found={self.report.dataset_sources_not_found}, "
                                f"last_dataset_id={dataset_id}"
                            ),
                        )
                else:
                    self.report.info(
                        title="Sigma dataset sources not found for one dataset",
                        message=(
                            "/datasets/{id}/sources did not resolve while the "
                            "dataset API itself still responds, so this dataset "
                            "was most likely deleted or re-permissioned after the "
                            "listing. Its warehouse upstreamLineage will be "
                            "missing."
                        ),
                        context=f"dataset_id={dataset_id}, http_status={status}",
                    )
                return None
            if response.status_code == 429:
                self.report.dataset_sources_lookup_failed += 1
                self.report.dataset_sources_lookup_rate_limited += 1
                self.report.warning(
                    title="Sigma API rate-limited on /datasets/{id}/sources",
                    message=(
                        "Retry budget exhausted on a 429. Warehouse upstream will be "
                        "missing for this Sigma Dataset. Re-run the ingestion to recover."
                    ),
                    context=f"dataset_id={dataset_id}, http_status=429",
                )
                return None
            if response.status_code != 200:
                self.report.dataset_sources_lookup_failed += 1
                self.report.warning(
                    title="Sigma /datasets/{id}/sources lookup returned non-200",
                    message=(
                        "Unable to resolve the warehouse tables behind a Sigma "
                        "Dataset. Its warehouse upstreamLineage will be missing, "
                        "and chart columns reading through it fall back to "
                        "self-references."
                    ),
                    context=f"dataset_id={dataset_id}, http_status={response.status_code}",
                )
                return None
            entries = response.json()
            if not isinstance(entries, list):
                self.report.dataset_sources_lookup_failed += 1
                self.report.warning(
                    title="Sigma /datasets/{id}/sources returned an unexpected shape",
                    message=(
                        "Expected a bare JSON list of source entries. Warehouse "
                        "upstream resolution is skipped for this Sigma Dataset. "
                        "Individual entries are validated by the caller, which "
                        "skips a malformed one rather than losing the rest."
                    ),
                    context=f"dataset_id={dataset_id}, body_type={type(entries).__name__}",
                )
                return None
            self._dataset_sources_succeeded = True
            if self._known_good_dataset_id is None:
                self._known_good_dataset_id = dataset_id
            return entries
        except Exception as e:
            self.report.dataset_sources_lookup_failed += 1
            self.report.warning(
                title="Sigma /datasets/{id}/sources lookup failed",
                message=(
                    "Exception while fetching the sources of a Sigma Dataset; "
                    "its warehouse upstream is skipped."
                ),
                context=f"dataset_id={dataset_id}",
                exc=e,
            )
            return None

    def get_connection_path(self, inode_id: str) -> Optional[ConnectionPath]:
        """Resolve a warehouse-table inode to its connection and path components.

        ``GET /v2/connections/paths/{inodeId}`` returns
        ``{"connectionId": "<uuid>", "path": ["DB", "SCHEMA", "TABLE"]}``.

        Preferred over ``/files/{inodeId}`` for warehouse tables: it carries the
        ``connectionId`` (so the URN can be built through the connection
        registry like every other warehouse route) and gives the path already
        split into components instead of a ``Connection Root/...`` string.

        Callers cache; this always makes a live call.
        """
        logger.debug("Fetching connection path for inode '%s'.", inode_id)
        url = f"{self.config.api_url}/connections/paths/{quote(inode_id, safe='')}"
        try:
            response = self._get_api_call(url)
            if response.status_code == 429:
                self.report.connection_path_lookup_failed += 1
                self.report.connection_path_lookup_rate_limited += 1
                self.report.warning(
                    title="Sigma API rate-limited on /connections/paths lookup",
                    message=(
                        "Retry budget exhausted on a 429. Warehouse upstream will be "
                        "missing for this table. Re-run the ingestion to recover."
                    ),
                    context=f"inode_id={inode_id}, http_status=429",
                )
                return None
            if response.status_code != 200:
                self.report.connection_path_lookup_failed += 1
                self.report.warning(
                    title="Sigma /connections/paths lookup returned non-200",
                    message=(
                        "Unable to resolve a warehouse table's connection and path. "
                        "Warehouse upstream will be missing for this table."
                    ),
                    context=f"inode_id={inode_id}, http_status={response.status_code}",
                )
                return None
            body = response.json()
            if not isinstance(body, dict):
                # Explicit, like get_dataset_sources: otherwise body.get() raises
                # and is reported as a generic exception.
                self.report.connection_path_lookup_failed += 1
                self.report.warning(
                    title="Sigma /connections/paths returned an unexpected body",
                    message=(
                        "Expected a JSON object with connectionId and path; "
                        "warehouse upstream is skipped for this table."
                    ),
                    context=f"inode_id={inode_id}, body_type={type(body).__name__}",
                )
                return None
            connection_id = body.get("connectionId")
            path = body.get("path")
            if not isinstance(connection_id, str) or not connection_id:
                self.report.connection_path_lookup_failed += 1
                self.report.warning(
                    title="Sigma /connections/paths response missing connectionId",
                    message=(
                        "Cannot map the table to a warehouse platform without a "
                        "connectionId; warehouse upstream is skipped."
                    ),
                    context=f"inode_id={inode_id}, keys={sorted(body)!r}",
                )
                return None
            if (
                not isinstance(path, list)
                or not path
                or not all(isinstance(p, str) and p for p in path)
            ):
                self.report.connection_path_lookup_failed += 1
                self.report.warning(
                    title="Sigma /connections/paths returned an unexpected path",
                    message=(
                        "Expected `path` to be a list of non-empty strings; "
                        "warehouse upstream is skipped for this table."
                    ),
                    context=f"inode_id={inode_id}, path={path!r}",
                )
                return None
            return ConnectionPath(connection_id=connection_id, path=path)
        except Exception as e:
            self.report.connection_path_lookup_failed += 1
            self.report.warning(
                title="Sigma /connections/paths lookup failed",
                message="Exception while resolving a table's connection path; warehouse upstream skipped.",
                context=f"inode_id={inode_id}",
                exc=e,
            )
            return None

    def get_file_metadata(self, inode_id: str) -> Optional[Dict[str, Any]]:
        """Fetch /files/{inodeId} and return the raw JSON dict, or None on
        non-200 or exception.  Resolves a warehouse-table lineage ``inodeId``
        (UUID) to its ``urlId`` (alphanumeric slug) and file-system ``path``
        (``Connection Root/<DB>/<SCHEMA>`` for Snowflake; shape for other
        platforms is unverified — see TODO in _build_dm_warehouse_url_id_map).

        Callers are responsible for caching; this method always makes a live
        HTTP call so the instance-level cache on ``SigmaSource`` can be shared
        across multiple callers without duplicating retry/error logic here.

        Error handling mirrors ``get_data_model_by_url_id``: 429 gets a
        dedicated counter + warning; other non-200 statuses and exceptions
        emit a rate-limited structured warning so operators can distinguish
        rate-limiting from missing-scope (403/404) from server errors (5xx).
        """
        logger.debug("Fetching file metadata for inode '%s'.", inode_id)
        url = f"{self.config.api_url}/files/{quote(inode_id, safe='')}"
        try:
            response = self._get_api_call(url)
            if response.status_code == 200:
                return response.json()
            status = response.status_code
            if status == 429:
                self.report.dm_element_warehouse_table_lookup_rate_limited += 1
                self.report.warning(
                    title="Sigma API rate-limited on /files lookup",
                    message=(
                        "Retry budget exhausted on a 429 response for a /files inode lookup. "
                        "Warehouse upstream will be missing for this inode. "
                        "Re-run the ingestion to recover."
                    ),
                    context=f"inode_id={inode_id}, http_status={status}",
                )
            else:
                self.report.warning(
                    title="Sigma /files lookup returned non-200",
                    message=(
                        "Unable to resolve warehouse table metadata for an inode. "
                        "Warehouse upstream will be missing."
                    ),
                    context=f"inode_id={inode_id}, http_status={status}",
                )
            return None
        except Exception as e:
            self.report.warning(
                title="Sigma /files lookup failed",
                message="Exception while fetching file metadata for an inode; warehouse upstream skipped.",
                context=f"inode_id={inode_id}",
                exc=e,
            )
            return None

    def get_workbook_lineage(
        self, workbook_id: str
    ) -> Optional[List[WorkbookLineageTableEntry]]:
        """Fetch /v2/workbooks/{workbook_id}/lineage and return parsed type=table
        entries, or None on non-200/exception.

        Non-table entries (type=dataset/customSQL/element) are silently skipped.
        Table entries missing required fields emit a structured warning and are
        skipped; they do not cause the whole call to fail.

        Error handling: 404 is treated as a silent None (workbook deleted
        since listing). 429 and other non-200 statuses emit a structured
        warning; failures return None so the caller can increment
        chart_input_fields_warehouse_index_lookup_failed. Paginated to
        handle workbooks with large lineage graphs.
        """
        logger.debug("Fetching workbook lineage for workbook '%s'.", workbook_id)
        base_url = (
            f"{self.config.api_url}/workbooks/{quote(workbook_id, safe='')}/lineage"
        )
        all_entries: List[WorkbookLineageTableEntry] = []
        url = base_url
        try:
            while True:
                response = self._get_api_call(url)
                if response.status_code == 200:
                    data = response.json()
                    for raw in data.get("entries") or []:
                        if raw.get("type") != "table":
                            logger.debug(
                                "Workbook %s: skipping lineage entry with type %r.",
                                workbook_id,
                                raw.get("type"),
                            )
                            continue
                        try:
                            all_entries.append(
                                WorkbookLineageTableEntry.model_validate(raw)
                            )
                        except ValidationError:
                            self.report.warning(
                                title="Sigma workbook lineage type=table entry missing required fields",
                                message=(
                                    "A type=table lineage entry is missing one or more "
                                    "required fields (name, connectionId, inodeId). "
                                    "Warehouse table index entry skipped."
                                ),
                                context=f"workbook_id={workbook_id}, entry={raw}",
                            )
                    next_page = data.get("nextPage")
                    if not next_page:
                        return all_entries
                    sep = "&" if "?" in base_url else "?"
                    url = f"{base_url}{sep}page={next_page}"
                    continue
                status = response.status_code
                if status == 404:
                    # Workbook may have been deleted between listing and lineage fetch.
                    return None
                if status == 429:
                    self.report.warning(
                        title="Sigma API rate-limited on /workbooks/{id}/lineage",
                        message=(
                            "Retry budget exhausted on a 429 response for workbook "
                            "lineage lookup. Chart formula warehouse resolution will "
                            "be incomplete for this workbook. Re-run the ingestion "
                            "to recover."
                        ),
                        context=f"workbook_id={workbook_id}, http_status={status}",
                    )
                else:
                    self.report.warning(
                        title="Sigma /workbooks/{id}/lineage returned non-200",
                        message=(
                            "Unable to fetch workbook lineage for warehouse table "
                            "index. Chart formula warehouse resolution may be "
                            "incomplete."
                        ),
                        context=f"workbook_id={workbook_id}, http_status={status}",
                    )
                return None
        except Exception as e:
            self.report.warning(
                title="Sigma /workbooks/{id}/lineage lookup failed",
                message=(
                    "Exception while fetching workbook lineage; warehouse table "
                    "index skipped for this workbook."
                ),
                context=f"workbook_id={workbook_id}",
                exc=e,
            )
            return None

    def get_data_model_by_url_id(self, url_id: str) -> Optional[SigmaDataModel]:
        """Fetch a DM by its urlId (not UUID). Used to resolve personal-space
        or otherwise unlisted DMs referenced from another DM's /lineage.

        Returns None on non-200 so the caller can count and continue.
        The HTTP status code is surfaced in the report warning so operators
        can distinguish 429 (rate-limited, re-run the pipeline) from 403 /
        404 (genuinely forbidden / deleted). 429s (after the urllib3 retry
        budget has been exhausted) additionally bump a dedicated
        ``data_model_external_reference_rate_limited`` counter.
        """
        logger.debug(f"Fetching data model by url_id '{url_id}'.")
        url = f"{self.config.api_url}/dataModels/{url_id}"
        try:
            response = self._get_api_call(url)
            if response.status_code != 200:
                status = response.status_code
                if status == 429:
                    self.report.data_model_external_reference_rate_limited += 1
                    self.report.warning(
                        title="Sigma API rate-limited while fetching orphan Data Model",
                        message=(
                            "Retry budget exhausted on 429; this DM will be "
                            "reported as unresolved for the rest of the run. "
                            "Re-run the ingestion to pick up the cross-DM "
                            "edge, or investigate the Sigma API rate limit."
                        ),
                        context=f"url_id={url_id}, http_status={status}",
                    )
                else:
                    # 401 / 403 / 404 / 5xx (after retries) land here. Emit
                    # a low-severity structured entry so operators can
                    # triage without stdout tailing; the warning is
                    # rate-limited by LossyList on the report side.
                    self.report.warning(
                        title="Sigma orphan Data Model fetch returned non-200",
                        message=(
                            "Cross-DM reference could not be resolved; "
                            "treating as ``dm_unknown`` for the rest of "
                            "the run. Common causes: DM deleted, admin "
                            "scope revoked, personal space not shared with "
                            "the ingest principal."
                        ),
                        context=f"url_id={url_id}, http_status={status}",
                    )
                return None
            data = response.json()
            # By-urlId responses return ``dataModelUrlId`` and a null
            # ``urlId``; by-UUID responses use ``urlId``. Normalize.
            if "dataModelUrlId" in data and not data.get("urlId"):
                data["urlId"] = data["dataModelUrlId"]
            dm = SigmaDataModel.model_validate(data)
            # No file_meta: these DMs are not in /files.
            self._assemble_data_model(dm, file_meta=None)
            return dm
        except Exception as e:
            detail = self._log_http_error(
                message=f"Unable to fetch data model by url_id '{url_id}'. Exception: {e}",
                report_warning=False,
            )
            self.report.warning(
                title="Sigma orphan Data Model fetch raised exception",
                message=(
                    "An unexpected exception occurred while fetching or "
                    "assembling the cross-DM reference; treating as "
                    "``dm_unknown`` for the rest of the run. Common causes: "
                    "Pydantic validation failure on a malformed 200 payload, "
                    "network error inside element/column/lineage assembly."
                ),
                context=f"url_id={url_id}, {detail}, exception={type(e).__name__}: {e}",
            )
            return None

    def get_data_models(self) -> List[SigmaDataModel]:
        logger.debug("Fetching all accessible data models metadata.")
        data_model_files_metadata = self._get_files_metadata(
            file_type=Constant.DATA_MODEL
        )
        # Pagination and per-entry parse errors are handled by
        # ``_paginated_entries``; this method only has to apply the
        # workspace / DM-pattern filters and assemble each DM.
        raw_data_models = self._paginated_entries(
            f"{self.config.api_url}/dataModels",
            SigmaDataModel,
            "Unable to fetch sigma data models.",
            dedup_key=lambda dm: dm.dataModelId,
            enumerates_entities=True,
            optional_feature="ingest_data_models=False",
        )
        data_models: List[SigmaDataModel] = []
        for data_model in raw_data_models:
            try:
                file_meta = data_model_files_metadata.get(data_model.dataModelId)

                # DM-pattern filter runs before workspace lookup to
                # short-circuit three extra HTTP calls per filtered DM.
                # (get_sigma_workbooks/datasets check workspace first
                # because their payload is already complete.)
                if not self.config.data_model_pattern.allowed(data_model.name):
                    self.report.data_models.dropped(
                        f"{data_model.name} ({data_model.dataModelId})"
                    )
                    continue

                workspace = None
                # Prefer ``/files`` workspaceId (authoritative for the folder
                # tree) and fall back to the ``/dataModels`` payload so a DM
                # whose ``/files`` row is missing workspace (admin-perm /
                # legacy-tenant edge case, same shape as workbook L833-L839
                # below) but whose payload names an allowed workspace still
                # routes through the normal workspace-pattern branch instead
                # of being dropped / gated behind ``ingest_shared_entities``.
                candidate_workspace_id = (
                    file_meta.workspaceId if file_meta else None
                ) or data_model.workspaceId
                if candidate_workspace_id:
                    workspace = self.get_workspace(candidate_workspace_id)

                if workspace:
                    if self.config.workspace_pattern.allowed(workspace.name):
                        self.report.data_models.processed(
                            f"{data_model.name} ({data_model.dataModelId}) in {workspace.name}"
                        )
                        self._assemble_data_model(
                            data_model,
                            file_meta,
                            resolved_workspace_id=candidate_workspace_id,
                        )
                        data_models.append(data_model)
                    else:
                        self.report.data_models.dropped(
                            f"{data_model.name} ({data_model.dataModelId}) in {workspace.name}"
                        )
                elif self.config.ingest_shared_entities:
                    self.report.data_models_without_workspace += 1
                    self.report.data_models.processed(
                        f"{data_model.name} ({data_model.dataModelId}) (no workspace)"
                    )
                    self._assemble_data_model(
                        data_model,
                        file_meta,
                        resolved_workspace_id=candidate_workspace_id,
                    )
                    data_models.append(data_model)
                else:
                    self.report.data_models.dropped(
                        f"{data_model.name} ({data_model.dataModelId}) (no workspace, ingest_shared_entities=False)"
                    )
            except Exception as e:
                # Per-DM isolation: an unexpected exception during
                # assembly (pydantic remodel, network transient that
                # escapes the inner silent-status gate, etc.) must not
                # abort the whole DM feed. Mirrors ``get_sigma_workbooks``
                # which swallows per-workbook failures for the same
                # reason. The warning carries enough identifiers to
                # locate the offender, and the outer loop continues
                # with the next DM.
                self.report.warning(
                    title="Failed to assemble Sigma Data Model",
                    message="Skipping this DM; other DMs will still be "
                    "assembled. The DM Container, its elements, and any "
                    "lineage derived from it will be absent from this "
                    "ingestion run.",
                    context=(
                        f"dataModelId={data_model.dataModelId}, "
                        f"name={data_model.name!r}"
                    ),
                    exc=e,
                )
                self.report.data_models.dropped(
                    f"{data_model.name} ({data_model.dataModelId}) "
                    "(assembly failed -- see warning)"
                )
        return data_models

    def get_sigma_workbooks(self) -> List[Workbook]:
        logger.debug("Fetching all accessible workbooks metadata.")
        workbook_url = url = f"{self.config.api_url}/workbooks"
        workbook_files_metadata = self._get_files_metadata(file_type=Constant.WORKBOOK)
        workbooks: List[Workbook] = []
        # The row in hand when a failure escapes the loop: a ValidationError
        # identifies the FIELD, never the object.
        last_row: Dict[str, Any] = {}
        try:
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = _envelope(response.json(), entries_must_be_a_list=True)
                for workbook_dict in response_dict[Constant.ENTRIES]:
                    last_row = workbook_dict
                    workbook = Workbook.model_validate(workbook_dict)

                    # Skip workbook if workbook name filtered out by config
                    if not self.config.workbook_pattern.allowed(workbook.name):
                        self.report.workbooks.dropped(
                            f"{workbook.name} ({workbook.workbookId})"
                        )
                        continue

                    if workbook.workbookId not in workbook_files_metadata:
                        # Due to a bug in the Sigma API, it seems like the /files endpoint does not
                        # return file metadata when the user has access via admin permissions. In
                        # those cases, the user associated with the token needs to be manually added
                        # to the workspace.
                        self.report.workbooks.dropped(
                            f"{workbook.name} ({workbook.workbookId}) (missing file metadata; path: {workbook.path}; likely need to manually add user to workspace)"
                        )
                        continue

                    workbook.workspaceId = workbook_files_metadata[
                        workbook.workbookId
                    ].workspaceId

                    workbook.badge = workbook_files_metadata[workbook.workbookId].badge

                    workspace = None
                    if workbook.workspaceId:
                        workspace = self.get_workspace(workbook.workspaceId)

                    if workspace:
                        if self.config.workspace_pattern.allowed(workspace.name):
                            self.report.workbooks.processed(
                                f"{workbook.name} ({workbook.workbookId}) in {workspace.name}"
                            )
                            workbook.pages = self.get_workbook_pages(workbook)
                            workbooks.append(workbook)
                        else:
                            self.report.workbooks.dropped(
                                f"{workbook.name} ({workbook.workbookId}) in {workspace.name}"
                            )
                    elif self.config.ingest_shared_entities:
                        # If no workspace for workbook we can consider it as shared entity
                        self.report.workbooks_without_workspace += 1
                        self.report.workbooks.processed(
                            f"{workbook.name} ({workbook.workbookId}) in workspace id {workbook.workspaceId or 'unknown'}"
                        )
                        workbook.pages = self.get_workbook_pages(workbook)
                        workbooks.append(workbook)
                    else:
                        self.report.workbooks.dropped(
                            f"{workbook.name} ({workbook.workbookId}) in workspace id {workbook.workspaceId or 'unknown'}"
                        )

                if response_dict[Constant.NEXTPAGE]:
                    url = f"{workbook_url}?page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break
            return workbooks
        except Exception as e:
            detail = self._log_http_error(
                message=f"Unable to fetch sigma workbooks. Exception: {e}",
                report_warning=False,
            )
            self._record_enumeration_failure(
                what="Sigma workbooks",
                context=f"{_failed_row(e, last_row)}{detail}, exception={_exc_text(e)}",
                unparseable_row=isinstance(e, _UNREADABLE_ROW),
                malformed_response=_is_malformed_response(e),
                status=_http_status(e),
            )
            # Partial rows, not []: the run fails either way, so keeping the
            # pages already read leaves those entities fresh.
            return workbooks
