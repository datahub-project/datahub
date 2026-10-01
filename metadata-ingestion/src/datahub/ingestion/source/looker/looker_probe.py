import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import partial
from typing import (
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
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
from datahub.ingestion.agent.probe_methods import MAX_PROBE_ITEMS, probe_method
from datahub.ingestion.agent.verdicts import (
    ProbeConnectionError,
    ProbeReadFailed,
    ProbeSoftError,
)
from datahub.ingestion.source.looker.looker_config import LookerDashboardSourceConfig
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
from datahub.ingestion.source.looker.looker_probe_verdicts import (
    ATTR_DASHBOARD_DELETED,
    ATTR_DASHBOARD_FOLDER_PATH,
    ATTR_DASHBOARD_FOLDER_PATH_ALLOWED,
    ATTR_DASHBOARD_FOLDER_PERSONAL,
    ATTR_DELETED,
    ATTR_EXPLORE_COUNT,
    ATTR_FOLDER_PATH,
    ATTR_FOLDER_PATH_ALLOWED,
    ATTR_FOLDER_PERSONAL,
    ATTR_HAS_QUERY,
    ATTR_ON_KEPT_DASHBOARD,
    ATTR_TYPE,
    ATTR_USED,
    DASHBOARD_KIND,
    EXPLORE_KIND,
    LOOK_KIND,
    MODEL_KIND,
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

# The folder fields every rule here reads, named rather than left to whatever
# a listing endpoint returns for a bare `folder`: a missing personal flag would
# read as "not personal".
_FOLDER_FIELDS = "folder(id,name,parent_id,is_personal,is_personal_descendant)"
# No user fields (user_id, last_updater_id, deleter_id) and no usage counts:
# what is not requested cannot leak.
_DASHBOARD_LIST_FIELDS = ["id", "title", _FOLDER_FIELDS]
# LookerAPI.folder_ancestors' default fields, as ingestion requests them.
_FOLDER_ANCESTOR_FIELDS = "id,name,parent_id"
# Looker's roots for per-user folders, each child named after its user. A path
# under one is withheld even when the folder's personal flags are missing.
_USER_FOLDER_ROOTS = frozenset({"Users", "Embed Users"})
_ANCESTORS_UNREADABLE = (
    "some folders' ancestors could not be read, so ingestion matches "
    "folder_path_pattern against the folder's own name; that name may be a "
    "user's, so those paths are withheld and folder_path_allowed carries the "
    "verdict"
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
_CHART_DASHBOARD_FIELDS = ["id", "deleted", _FOLDER_FIELDS, "dashboard_elements"]


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


def _element_record(
    element: DashboardElement, dashboard_deleted: bool, folder: _FolderFacts
) -> Dict[str, object]:
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
        # The dashboard's own facts, for the rules that drop all its charts.
        ATTR_DASHBOARD_DELETED: dashboard_deleted,
        ATTR_DASHBOARD_FOLDER_PATH: folder.path,
        ATTR_DASHBOARD_FOLDER_PERSONAL: folder.personal,
        ATTR_DASHBOARD_FOLDER_PATH_ALLOWED: folder.path_allowed,
    }

# extract_independent_looks requests user_id too; it is not needed here.
_LOOK_LIST_FIELDS = ["id", "title", "query_id", _FOLDER_FIELDS]


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

# How many dashboard and look reads one --trace-charts run may make before it
# stops and leaves what it has not seen undetermined. Ingestion reads them
# all; a probe command stays bounded.
_TRACE_FETCH_LIMIT = MAX_PROBE_ITEMS
# The element queries carry the explores; user fields are not read.
_TRACE_DASHBOARD_FIELDS = ["id", _FOLDER_FIELDS, "dashboard_elements"]
_TRACE_INCOMPLETE = (
    "the chart trace did not read everything ingestion would (a read was "
    "refused or failed, see above, or it stopped after {limit} reads), so "
    "what it did not find in use is left undetermined (null)"
)

_ExploreRef = Tuple[str, str]


def _element_explores(element: DashboardElement) -> List[_ExploreRef]:
    """The (model, explore) pairs _get_looker_dashboard_element records with
    add_reachable_explore, with the same precedence: query, else look, else
    result_maker (its query and its filterables)."""
    queries: List[Optional[Query]] = []
    pairs: List[_ExploreRef] = []
    if element.query is not None:
        queries.append(element.query)
    elif element.look is not None:
        queries.append(element.look.query)
    elif element.result_maker is not None:
        queries.append(element.result_maker.query)
        for filterable in element.result_maker.filterables or []:
            if filterable.model is not None and filterable.view is not None:
                pairs.append((filterable.model, filterable.view))
    for query in queries:
        # A pair without a model names no explore any listing holds.
        if query is not None and query.model is not None and query.view is not None:
            pairs.append((query.model, query.view))
    return pairs


@dataclass
class _Reachability:
    """What ingestion's reachable_explores and reachable_look_registry would
    hold, as far as one bounded trace could tell."""

    explores: Set[_ExploreRef] = field(default_factory=set)
    looks: Set[str] = field(default_factory=set)
    # Every dashboard ingestion reads was read, so a look or explore not
    # found on one is not on one.
    dashboards_complete: bool = True
    # Every standalone look ingestion reads was read (or none are read).
    looks_complete: bool = True
    reads: int = 0

    def explore_used(self, model: str, explore: str) -> Optional[bool]:
        if (model, explore) in self.explores:
            return True
        return False if self.dashboards_complete and self.looks_complete else None

    def model_used(self, model: str) -> Optional[bool]:
        if any(used_model == model for used_model, _ in self.explores):
            return True
        return False if self.dashboards_complete and self.looks_complete else None

    def on_kept_dashboard(self, look_id: str) -> Optional[bool]:
        if look_id in self.looks:
            return True
        return False if self.dashboards_complete else None


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
        # LookerAPI raises this from me() for any SDKError: refused
        # credentials, a server error, or a base_url that is not a Looker API.
        raise ProbeConnectionError(
            f"Looker at {host} refused or did not answer the credential check; "
            f"check client_id, client_secret and base_url, and that the "
            f"instance is up"
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

    def _ancestor_names(self, folder_id: str) -> Optional[List[str]]:
        """The folder's ancestors' names, or None when they could not be read."""
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
            return None
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
        ancestors = self._ancestor_names(folder.id)
        path = looker_folder_path(ancestors or [], folder.name)
        # Withheld past the flags too: a path under a user root, or a nested
        # folder's bare name when its ancestors are unknown, may name a user.
        withheld = (
            personal
            or (ancestors is None and folder.parent_id is not None)
            or path.split("/", 1)[0] in _USER_FOLDER_ROOTS
        )
        facts = _FolderFacts(
            path=None if withheld else path,
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
        which is what emit_used_explores_only keeps. Each record also carries
        its dashboard's facts (`dashboard_deleted`, `dashboard_folder_*`, the
        path withheld for a personal folder), because ingestion reads none of
        a dashboard's charts when it drops the dashboard itself (deleted,
        personal folder, or folder_path_pattern); this warns when it does,
        and `probe filter` applies those rules. Judge with
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
        folder = self._folder_facts(detail.folder)
        return [
            _element_record(element, bool(detail.deleted), folder)
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
    def looks(
        self, limit: int = 200, trace_charts: bool = False
    ) -> List[Dict[str, object]]:
        """Saved looks, by look id, as extract_independent_looks reads them:
        live ones, and deleted ones (`deleted: true`), which it reads only with
        include_deleted. Ingestion emits a look as a standalone chart only when
        extract_independent_looks is true, it has a query (`has_query`: a
        query id, and a query when the look is read back, as ingestion reads
        it -- one read per look; null when that read failed), and,
        under skip_personal_folders, it is not in a personal folder
        (`folder_personal`). chart_pattern does not apply to these. A look that
        is also on a dashboard ingestion reads is emitted as that dashboard's
        chart instead; only `trace_charts` tells that: it reads every
        dashboard dashboard_pattern keeps, as ingestion does (bounded), and
        sets `on_kept_dashboard`, computed from this recipe at run time (null
        when the trace could not settle it).
        Judge with `probe filter --kind Look --from-run <report>` and no
        --parent. Metadata only: no owners and no folder names."""
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
        for row in rows:
            if row[ATTR_HAS_QUERY] is True:
                read, query = self._read_look_query(str(row["name"]), "look")
                # Unread: undetermined, not "no query".
                row[ATTR_HAS_QUERY] = (query is not None) if read else None
        if trace_charts:
            reach = self._trace(with_standalone_looks=False)
            for row in rows:
                row[ATTR_ON_KEPT_DASHBOARD] = reach.on_kept_dashboard(str(row["name"]))
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

    @probe_method(kind=MODEL_KIND)
    def models(self, trace_charts: bool = False) -> List[Dict[str, object]]:
        """LookML models, as ingestion lists them for explores
        (list_all_explores), with `explore_count`. Ingestion emits a model's
        container only when it emits one of its explores: with
        emit_used_explores_only (the default) only explores a kept chart or
        look queries; otherwise every explore. A model with no explores is
        never emitted. `trace_charts` settles the first case: it reads what
        ingestion reads to find them (see `explores`) and sets `used`,
        computed from this recipe at run time (null when the trace could not
        settle it). Metadata only."""
        api = self._api()
        rows: List[Dict[str, object]] = [
            {
                "name": model.name,
                "project": model.project_name,
                # list_all_explores skips an unnamed explore.
                ATTR_EXPLORE_COUNT: sum(
                    1 for explore in model.explores or [] if explore.name is not None
                ),
            }
            for model in self._fetch("LookML model listing", api.all_lookml_models)
            if model.name is not None
        ]
        if trace_charts:
            reach = self._trace(with_standalone_looks=True)
            for row in rows:
                row[ATTR_USED] = reach.model_used(str(row["name"]))
        return rows

    @probe_method(kind=EXPLORE_KIND, parent_params=("model",))
    def explores(
        self, model: str, trace_charts: bool = False
    ) -> List[Dict[str, object]]:
        """Explores of one LookML model, by name, from the same model listing
        ingestion reads (list_all_explores), hidden ones included as ingestion
        includes them. With emit_used_explores_only (the default) ingestion
        emits only those a kept chart or look queries; `trace_charts` finds
        them as ingestion does -- it reads every dashboard dashboard_pattern
        keeps and, with extract_independent_looks, every standalone look's
        query (bounded) -- and sets `used`, computed from this recipe at run
        time (null when the trace could not settle it). It costs one read per
        kept dashboard plus one per standalone look, at most 1000 reads.
        Without it, `charts` shows the explore each chart of one dashboard
        uses. Metadata only."""
        api = self._api()
        for lookml_model in self._fetch("LookML model listing", api.all_lookml_models):
            if lookml_model.name == model:
                rows: List[Dict[str, object]] = [
                    {"name": explore.name, "hidden": bool(explore.hidden)}
                    for explore in lookml_model.explores or []
                    if explore.name is not None
                ]
                if trace_charts:
                    reach = self._trace(with_standalone_looks=True)
                    for row in rows:
                        row[ATTR_USED] = reach.explore_used(model, str(row["name"]))
                return rows
        if self.warnings:
            # The listing was refused and the warning says so; "no such model"
            # would blame the caller for what is a permissions gap.
            return []
        raise ValueError(
            f"no LookML model named '{model}'; pass a name from the `models` listing"
        )

    def _read_look_query(
        self, look_id: str, what: str
    ) -> Tuple[bool, Optional[Query]]:
        """(read, query) for one look, as extract_independent_looks reads it.

        Like ingestion, which skips a look whose read raises anything, a
        failure here is a warning, not the command's failure: read is False
        and the caller treats the look as undetermined. Only the exception's
        type is reported. An unreachable Looker still raises.
        """
        api = self._api()
        context = f"{what} '{look_id}'"
        try:
            with _looker_call(context):
                return True, api.get_look(look_id, fields=["query"]).query
        except (ProbeSoftError, ProbeReadFailed) as exc:
            self._warn(str(exc))
        except ProbeConnectionError:
            raise
        except Exception as exc:
            self._warn(f"{context} could not be read ({type(exc).__name__})")
        return False, None

    def _trace(self, with_standalone_looks: bool) -> _Reachability:
        """Replay what get_workunits_internal records while it reads
        dashboards (and, when the recipe extracts them, standalone looks), so
        `used` and `on_kept_dashboard` follow ingestion's own reachability.

        Like `folder_path_allowed`, the result is a run-time fact: it reflects
        this recipe's dashboard_pattern, chart_pattern, skip_personal_folders,
        include_deleted and extract_independent_looks, and is stale for a
        `probe filter` against a recipe that changes them.

        A refused or failed read leaves the trace undetermined; a Looker that
        cannot be reached at all (ProbeConnectionError) aborts the listing,
        since nothing it would report could be trusted."""
        reach = _Reachability()
        api = self._api()
        for dashboard_id in self._traced_dashboard_ids(reach):
            if reach.reads >= _TRACE_FETCH_LIMIT:
                reach.dashboards_complete = False
                break
            reach.reads += 1
            detail = self._trace_read(
                reach,
                f"dashboard '{dashboard_id}' for the chart trace",
                partial(
                    api.dashboard,
                    dashboard_id=dashboard_id,
                    fields=_TRACE_DASHBOARD_FIELDS,
                ),
            )
            if detail is not None:
                self._trace_dashboard(reach, detail)
        if with_standalone_looks and self._config.extract_independent_looks:
            self._trace_standalone_looks(reach)
        if not (reach.dashboards_complete and reach.looks_complete):
            self._warn(_TRACE_INCOMPLETE.format(limit=_TRACE_FETCH_LIMIT))
        return reach

    def _trace_read(
        self,
        reach: _Reachability,
        context: str,
        call: Callable[[], _T],
        looks: bool = False,
    ) -> Optional[_T]:
        """One trace read. A refused or failed one leaves the trace
        incomplete instead of failing the listing it decorates."""
        try:
            with _looker_call(context):
                return call()
        except (ProbeSoftError, ProbeReadFailed) as exc:
            self._warn(str(exc))
            if looks:
                reach.looks_complete = False
            else:
                reach.dashboards_complete = False
            return None

    def _traced_dashboard_ids(self, reach: _Reachability) -> List[str]:
        """get_workunits_internal's dashboard ids: live, deleted ones only
        under include_deleted, then dashboard_pattern on the id."""
        api = self._api()
        listed: List[Union[Dashboard, DashboardBase]] = list(
            self._trace_read(
                reach,
                "dashboard listing for the chart trace",
                lambda: api.all_dashboards(fields="id"),
            )
            or []
        )
        if self._config.include_deleted:
            listed.extend(
                self._trace_read(
                    reach,
                    "deleted dashboard listing for the chart trace",
                    lambda: api.search_dashboards(fields="id", deleted="true"),
                )
                or []
            )
        return [
            dashboard.id
            for dashboard in listed
            if dashboard.id is not None
            and self._config.dashboard_pattern.allowed(dashboard.id)
        ]

    def _trace_dashboard(self, reach: _Reachability, detail: Dashboard) -> None:
        """What process_dashboard records for one dashboard. folder_path_pattern
        is deliberately not applied: process_dashboard checks it only after
        _get_looker_dashboard has recorded the dashboard's looks and explores,
        so a dashboard it drops still makes them reachable."""
        folder = detail.folder
        if (
            self._config.skip_personal_folders
            and folder is not None
            and (folder.is_personal or folder.is_personal_descendant)
        ):
            return
        for element in detail.dashboard_elements or []:
            if element.id is None or not self._config.chart_pattern.allowed(
                element.id
            ):
                continue
            if element.look_id is not None:
                reach.looks.add(element.look_id)
            reach.explores.update(_element_explores(element))

    def _trace_standalone_looks(self, reach: _Reachability) -> None:
        """The explores extract_independent_looks records, with its skips."""
        api = self._api()
        looks = (
            self._trace_read(
                reach,
                "look listing for the chart trace",
                lambda: api.all_looks(
                    fields=_LOOK_LIST_FIELDS,
                    soft_deleted=self._config.include_deleted,
                ),
                looks=True,
            )
            or []
        )
        for look in looks:
            look_id = look.id
            if look_id is None or look_id in reach.looks or look.query_id is None:
                continue
            folder = look.folder
            if (
                self._config.skip_personal_folders
                and folder is not None
                and (folder.is_personal or folder.is_personal_descendant)
            ):
                continue
            if reach.reads >= _TRACE_FETCH_LIMIT:
                reach.looks_complete = False
                return
            reach.reads += 1
            read, query = self._read_look_query(look_id, "look for the chart trace")
            if not read:
                reach.looks_complete = False
            elif (
                query is not None
                and query.model is not None
                and query.view is not None
            ):
                reach.explores.add((query.model, query.view))
