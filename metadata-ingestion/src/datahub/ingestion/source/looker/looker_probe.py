import re
from contextlib import contextmanager
from dataclasses import dataclass
from typing import (
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    TypeVar,
    Union,
)
from urllib.parse import urlparse

import looker_sdk.rtl.requests_transport as looker_requests_transport
import requests
from looker_sdk.error import SDKError
from looker_sdk.rtl.serialize import DeserializeError
from looker_sdk.sdk.api40.models import (
    Dashboard,
    DashboardBase,
    DashboardElement,
    FolderBase,
    Look,
    Query,
)

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import (
    ProbeConnectionError,
    ProbeReadFailed,
    ProbeSoftError,
)
from datahub.ingestion.source.looker.looker_config import LookerDashboardSourceConfig
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
from datahub.ingestion.source.looker.looker_probe_verdicts import (
    ATTR_DELETED,
    ATTR_FOLDER_PATH,
    ATTR_FOLDER_PATH_ALLOWED,
    ATTR_FOLDER_PERSONAL,
    ATTR_HAS_QUERY,
    ATTR_TYPE,
    DASHBOARD_KIND,
    LOOK_KIND,
)
from datahub.ingestion.source.looker.looker_source import (
    BASIC_INGEST_REQUIRED_PERMISSIONS,
    USAGE_INGEST_REQUIRED_PERMISSIONS,
    looker_folder_path,
)

_T = TypeVar("_T")

# looker_sdk raises SDKError carrying Looker's documentation link, whose path
# holds the HTTP status: .../looker/docs/r/err/4.0/404/get/dashboards/...
_DOC_URL_STATUS = re.compile(r"/r/err/[^/]+/(\d{3})(?:/|$)")
# Older SDKs and LookerAPI's own checks use "Looker Not Found (404)".
_MESSAGE_STATUS = re.compile(r"\((\d{3})\)")

# No user fields (user_id, last_updater_id, deleter_id) and no usage counts:
# what is not requested cannot leak.
_DASHBOARD_LIST_FIELDS = ["id", "title", "folder"]
# LookerAPI.folder_ancestors' default fields, as ingestion requests them.
_FOLDER_ANCESTOR_FIELDS = "id,name,parent_id"
_ANCESTORS_UNREADABLE = (
    "some folders' ancestors could not be read, so their paths are just the "
    "folder's own name -- which is also what ingestion matches "
    "folder_path_pattern against when it cannot read them"
)


@dataclass(frozen=True)
class _FolderFacts:
    # None for a personal folder: Looker names it after its user.
    path: Optional[str]
    personal: bool
    # folder_path_pattern's verdict on the real path; None with no folder.
    path_allowed: Optional[bool]


_NO_FOLDER = _FolderFacts(path=None, personal=False, path_allowed=None)

# dashboard_elements carries each element's query; user fields are not read.
_CHART_DASHBOARD_FIELDS = ["id", "deleted", "folder", "dashboard_elements"]


def _ingestion_can_read(element: DashboardElement) -> bool:
    """Whether _get_looker_dashboard_element returns an element, not None.

    It tries element.query, then element.look -- returning None when the look
    has no query, without trying result_maker -- then element.result_maker.
    """
    if element.query is not None:
        return True
    if element.look is not None:
        return element.look.query is not None
    return element.result_maker is not None


def _element_query(element: DashboardElement) -> Optional[Query]:
    """The query ingestion reads the element's explore from, same order."""
    if element.query is not None:
        return element.query
    if element.look is not None:
        return element.look.query
    if element.result_maker is not None:
        return element.result_maker.query
    return None


def _element_record(element: DashboardElement) -> Dict[str, object]:
    query = _element_query(element)
    # Model and explore only: query.filters holds filter values, which are
    # row values, and dynamic_fields holds expressions.
    return {
        "name": element.id,
        "title": element.title,
        # "" rather than None so the attribute survives --report-to: ingestion
        # emits only type == "vis", and a missing type is not "vis".
        ATTR_TYPE: element.type or "",
        ATTR_HAS_QUERY: _ingestion_can_read(element),
        "look_id": element.look_id,
        "model": query.model if query is not None else None,
        "explore": query.view if query is not None else None,
    }

# extract_independent_looks requests user_id too; it is not needed here.
_LOOK_LIST_FIELDS = ["id", "title", "query_id", "folder"]


def _look_record(look: Look, deleted: bool) -> Dict[str, object]:
    folder = look.folder
    return {
        "name": look.id,
        "title": look.title,
        ATTR_DELETED: deleted,
        ATTR_HAS_QUERY: look.query_id is not None,
        ATTR_FOLDER_PERSONAL: bool(
            folder is not None
            and (folder.is_personal or folder.is_personal_descendant)
        ),
    }


def sdk_error_status(exc: SDKError) -> Optional[int]:
    """The HTTP status behind an SDKError, or None.

    Read so that nothing else of the error has to be: SDKError's str() is
    Looker's response body plus documentation links, which is not ours to
    forward.
    """
    urls = [exc.documentation_url or ""] + [
        detail.documentation_url or "" for detail in exc.errors or []
    ]
    for url in urls:
        match = _DOC_URL_STATUS.search(url)
        if match:
            return int(match.group(1))
    match = _MESSAGE_STATUS.search(exc.message or "")
    return int(match.group(1)) if match else None


@contextmanager
def _looker_call(context: str, not_found: Optional[str] = None) -> Iterator[None]:
    """Run one Looker read, translating its failure into the probe's terms.

    Never formats the original: an SDKError renders the response body and a
    requests error the URL. `from None` keeps them out of any traceback a
    caller logs too. 403/404 degrade (ProbeSoftError), except a 404 on the
    object the caller named, which is a bad argument (`not_found`).
    """
    try:
        yield
    except SDKError as exc:
        status = sdk_error_status(exc)
        if status == 404 and not_found is not None:
            raise ValueError(not_found) from None
        if status in (403, 404):
            raise ProbeSoftError(
                f"{context} returned HTTP {status}; treating it as empty."
            ) from None
        reason = f"HTTP {status}" if status is not None else "an unrecognised error"
        raise ProbeReadFailed(f"{context} failed: Looker returned {reason}") from None
    except DeserializeError:
        raise ProbeReadFailed(
            f"{context} failed: Looker's response could not be read"
        ) from None
    except requests.exceptions.RequestException as exc:
        raise ProbeConnectionError(
            f"{context} failed: Looker could not be reached ({type(exc).__name__})"
        ) from None


def _open_looker(config: LookerDashboardSourceConfig) -> LookerAPI:
    """The connector's own wrapper: in-memory credentials
    (_DataHubLookerApiSettings), its retry adapter and transport options, and
    the me() call that proves the credentials work."""
    parsed = urlparse(config.base_url)
    if parsed.username is not None or parsed.password is not None:
        raise ValueError(
            "base_url carries a user name or password before the host; Looker "
            "authenticates with client_id and client_secret, so remove it from "
            "base_url"
        )
    host = parsed.hostname or "the configured base_url"
    try:
        return LookerAPI(config)
    except ConfigurationError:
        # LookerAPI raises this from me() for any SDKError, i.e. refused
        # credentials or a base_url that is not a Looker API.
        raise ProbeConnectionError(
            f"Looker at {host} refused the API credentials; check client_id, "
            f"client_secret and base_url"
        ) from None
    except (SDKError, DeserializeError, requests.exceptions.RequestException) as exc:
        raise ProbeConnectionError(
            f"could not reach Looker at {host} ({type(exc).__name__})"
        ) from None


class LookerMetadataProbe:
    """Metadata-only probe over the Looker API, for the `looker` source.

    Builds LookerAPI on the first command rather than in for_config, so a bad
    credential surfaces as that command's connection error. Never constructs
    LookerDashboardSource, whose __init__ opens the API and builds registries.
    """

    # Read back by run_probe_method after each command.
    warnings: List[str]

    def __init__(self, config: LookerDashboardSourceConfig) -> None:
        self._config = config
        self._looker: Optional[LookerAPI] = None
        self._folder_cache: Dict[str, _FolderFacts] = {}
        self.warnings = []

    @classmethod
    def for_config(cls, config: LookerDashboardSourceConfig) -> "LookerMetadataProbe":
        return cls(config)

    def __enter__(self) -> "LookerMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        if self._looker is None:
            return
        transport = self._looker.client.transport
        if isinstance(transport, looker_requests_transport.RequestsTransport):
            transport.session.close()

    def _api(self) -> LookerAPI:
        if self._looker is None:
            self._looker = _open_looker(self._config)
        return self._looker

    def _warn(self, message: str) -> None:
        if message not in self.warnings:
            self.warnings.append(message)

    def _degrading(self, fetch: Callable[[], _T], empty: _T) -> _T:
        """`empty` plus a warning when the read was refused (403/404), so an
        empty result says "could not look" rather than "nothing here"."""
        try:
            return fetch()
        except ProbeSoftError as exc:
            self._warn(str(exc))
            return empty

    def _fetch(self, context: str, call: Callable[[], Sequence[_T]]) -> List[_T]:
        def run() -> List[_T]:
            with _looker_call(context):
                return list(call())

        return self._degrading(run, [])

    @probe_method()
    def permissions(self) -> Dict[str, object]:
        """Looker permissions this API credential's roles grant, and the ones
        ingestion needs that are missing: `missing_for_metadata` are the
        permissions `test_connection` requires for dashboards, charts and
        ownership; `missing_for_usage` the ones usage statistics need. Names
        only, never the API user's id or email. All three are null, with a
        warning, when the credential cannot read its own roles."""
        api = self._api()

        def fetch() -> Optional[Set[str]]:
            with _looker_call("role lookup for the API user"):
                return api.get_available_permissions()

        granted = self._degrading(fetch, None)
        if granted is None:
            return {
                "granted": None,
                "missing_for_metadata": None,
                "missing_for_usage": None,
            }
        return {
            "granted": sorted(granted),
            "missing_for_metadata": sorted(BASIC_INGEST_REQUIRED_PERMISSIONS - granted),
            "missing_for_usage": sorted(USAGE_INGEST_REQUIRED_PERMISSIONS - granted),
        }

    def _ancestor_names(self, folder_id: str) -> List[str]:
        api = self._api()
        try:
            with _looker_call("folder ancestor lookup"):
                ancestors = api.client.folder_ancestors(
                    folder_id,
                    _FOLDER_ANCESTOR_FIELDS,
                    transport_options=api.transport_options,
                )
        except (ProbeSoftError, ProbeReadFailed):
            # LookerAPI.folder_ancestors, which ingestion calls, swallows every
            # SDKError and returns no ancestors. The SDK is called directly here
            # only so that degrade can be reported instead of silent.
            self._warn(_ANCESTORS_UNREADABLE)
            return []
        return [ancestor.name for ancestor in ancestors]

    def _folder_facts(self, folder: Optional[FolderBase]) -> _FolderFacts:
        """What _should_skip_personal_folder_dashboard and
        _should_skip_dashboard_by_folder_path read, for one folder."""
        if folder is None or folder.id is None:
            return _NO_FOLDER
        cached = self._folder_cache.get(folder.id)
        if cached is not None:
            return cached
        personal = bool(folder.is_personal or folder.is_personal_descendant)
        path = looker_folder_path(self._ancestor_names(folder.id), folder.name)
        facts = _FolderFacts(
            path=None if personal else path,
            personal=personal,
            path_allowed=self._config.folder_path_pattern.allowed(path),
        )
        self._folder_cache[folder.id] = facts
        return facts

    def _dashboard_record(
        self, dashboard: Union[Dashboard, DashboardBase], deleted: bool
    ) -> Optional[Dict[str, object]]:
        if dashboard.id is None:
            # get_workunits_internal skips a dashboard without an id too.
            return None
        folder = self._folder_facts(dashboard.folder)
        return {
            "name": dashboard.id,
            "title": dashboard.title,
            ATTR_DELETED: deleted,
            ATTR_FOLDER_PATH: folder.path,
            ATTR_FOLDER_PERSONAL: folder.personal,
            ATTR_FOLDER_PATH_ALLOWED: folder.path_allowed,
        }

    @probe_method(kind=DASHBOARD_KIND, row_limit_param="limit")
    def dashboards(self, limit: int = 200) -> List[Dict[str, object]]:
        """Dashboards this credential can see, by id -- the string
        dashboard_pattern matches -- including ones the recipe would drop: a
        dropped dashboard is reported, not hidden. Deleted dashboards are
        listed too (`deleted: true`); ingestion reads them only with
        include_deleted. Each record carries what ingestion also filters on:
        `folder_path` (what folder_path_pattern matches), `folder_personal`
        (what skip_personal_folders drops) and `folder_path_allowed`. A
        personal folder's path is withheld, because Looker names it after its
        user; `folder_path_allowed` is folder_path_pattern's verdict on it,
        computed from this recipe at run time. Save this with `--report-to` and
        judge it with `probe filter --kind Dashboard --from-run <report>`.
        Metadata only: no owners, users or usage counts."""
        api = self._api()
        rows: List[Dict[str, object]] = []
        # Live first, then deleted, as get_workunits_internal orders them. The
        # framework asks for limit+1; stopping there keeps folder lookups and
        # the deleted listing from being requested at all.
        live = self._fetch(
            "dashboard listing",
            lambda: api.all_dashboards(fields=_DASHBOARD_LIST_FIELDS),
        )
        self._extend_dashboards(rows, live, deleted=False, limit=limit)
        if len(rows) < limit:
            deleted = self._fetch(
                "deleted dashboard listing",
                lambda: api.search_dashboards(
                    fields=_DASHBOARD_LIST_FIELDS, deleted="true"
                ),
            )
            self._extend_dashboards(rows, deleted, deleted=True, limit=limit)
        return rows

    def _extend_dashboards(
        self,
        rows: List[Dict[str, object]],
        dashboards: Sequence[Union[Dashboard, DashboardBase]],
        deleted: bool,
        limit: int,
    ) -> None:
        for dashboard in dashboards:
            if len(rows) >= limit:
                return
            record = self._dashboard_record(dashboard, deleted=deleted)
            if record is not None:
                rows.append(record)

    @probe_method(kind=LOOK_KIND, parent_params=("dashboard",))
    def charts(self, dashboard: str) -> List[Dict[str, object]]:
        """Elements of one dashboard, by dashboard id, each by element id --
        the string chart_pattern matches. Ingestion emits these as charts of
        subtype Look, and only an element whose `type` is `vis` and
        `has_query` is true; `model` and `explore` are the explore it queries,
        which is what emit_used_explores_only keeps. Warns when ingestion
        drops the dashboard itself (deleted, personal folder, or
        folder_path_pattern), since then it reads none of these. Judge with
        `probe filter --kind Look --from-run <report>`. Metadata only: no
        query filters, fields or SQL."""
        api = self._api()

        def fetch() -> Optional[Dashboard]:
            with _looker_call(
                f"dashboard '{dashboard}'",
                not_found=(
                    f"no dashboard with id '{dashboard}'; pass an id from the "
                    f"`dashboards` listing"
                ),
            ):
                return api.dashboard(
                    dashboard_id=dashboard, fields=_CHART_DASHBOARD_FIELDS
                )

        detail = self._degrading(fetch, None)
        if detail is None:
            return []
        self._note_parent_dashboard(detail)
        return [
            _element_record(element)
            for element in detail.dashboard_elements or []
            if element.id is not None
        ]

    def _note_parent_dashboard(self, detail: Dashboard) -> None:
        """`probe filter --parent` judges a dashboard by id only; say when one
        of ingestion's other dashboard rules drops it (process_dashboard)."""
        label = f"dashboard '{detail.id}'"
        if detail.deleted and not self._config.include_deleted:
            self._warn(
                f"{label} is deleted and include_deleted is false, so ingestion "
                f"reads none of its charts"
            )
        folder = self._folder_facts(detail.folder)
        if folder.personal and self._config.skip_personal_folders:
            self._warn(
                f"{label} is in a personal folder and skip_personal_folders is "
                f"set, so ingestion reads none of its charts"
            )
        elif folder.path_allowed is False:
            where = f"folder '{folder.path}'" if folder.path else "a personal folder"
            self._warn(
                f"{label} is in {where}, which folder_path_pattern denies, so "
                f"ingestion reads none of its charts"
            )

    @probe_method(kind=LOOK_KIND, row_limit_param="limit")
    def looks(self, limit: int = 200) -> List[Dict[str, object]]:
        """Saved looks, by look id, as extract_independent_looks reads them:
        live ones, and deleted ones (`deleted: true`), which it reads only with
        include_deleted. Ingestion emits a look as a standalone chart only when
        extract_independent_looks is true, it has a query (`has_query`), and,
        under skip_personal_folders, it is not in a personal folder
        (`folder_personal`). chart_pattern does not apply to these. A look that
        is also on a dashboard ingestion reads is emitted as that dashboard's
        chart instead, which this listing cannot tell. Judge with `probe filter
        --kind Look --from-run <report>` and no --parent. Metadata only: no
        owners and no folder names."""
        if not self._config.extract_independent_looks:
            self._warn(
                "extract_independent_looks is false, so ingestion emits none of "
                "these as standalone charts; looks on dashboards are listed by "
                "`charts`"
            )
        api = self._api()
        rows: List[Dict[str, object]] = []
        seen: Set[str] = set()
        live = self._fetch(
            "look listing",
            lambda: api.all_looks(fields=_LOOK_LIST_FIELDS, soft_deleted=False),
        )
        self._extend_looks(rows, seen, live, deleted=False, limit=limit)
        if len(rows) < limit:
            deleted = self._fetch(
                "deleted look listing",
                lambda: api.search_looks(fields=_LOOK_LIST_FIELDS, deleted=True),
            )
            self._extend_looks(rows, seen, deleted, deleted=True, limit=limit)
        return rows

    @staticmethod
    def _extend_looks(
        rows: List[Dict[str, object]],
        seen: Set[str],
        looks: Sequence[Look],
        deleted: bool,
        limit: int,
    ) -> None:
        for look in looks:
            if len(rows) >= limit:
                return
            # extract_independent_looks skips a look without an id.
            if look.id is None or look.id in seen:
                continue
            seen.add(look.id)
            rows.append(_look_record(look, deleted=deleted))
