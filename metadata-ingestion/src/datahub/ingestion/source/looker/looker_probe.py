from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import (
    Any,
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
    LookmlModel,
    Query,
)

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.agent.probe_methods import MAX_PROBE_ITEMS, probe_method
from datahub.ingestion.agent.provider_helpers import (
    ProbeProviderBase,
    echoed,
    resolve_name,
    soft_listing,
    take,
)
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeReadFailed,
    ProbeSoftError,
)
from datahub.ingestion.source.looker.looker_config import LookerDashboardSourceConfig
from datahub.ingestion.source.looker.looker_lib_wrapper import (
    BASIC_INGEST_REQUIRED_PERMISSIONS,
    USAGE_INGEST_REQUIRED_PERMISSIONS,
    LookerAPI,
)
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
    ATTR_TRACED,
    ATTR_TYPE,
    ATTR_USED,
)
from datahub.ingestion.source.looker.looker_selection import (
    DASHBOARD_KIND,
    EXPLORE_KIND,
    LOOK_KIND,
    MODEL_KIND,
    LookFacts,
    chart_id_verdict,
    dashboard_id_verdict,
    element_has_query,
    is_personal_folder,
    looker_folder_path,
    skips_personal_folder,
    standalone_look_verdict,
)

_T = TypeVar("_T")

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
_ANCESTORS_UNDETERMINED = (
    "some folders' ancestors could not be read (Looker failed or rate-limited "
    "the read), so their folder path is undetermined and folder_path_pattern "
    "is not judged for their dashboards; a healthy ingestion run reads it"
)


@dataclass(frozen=True)
class _FolderFacts:
    # None for a personal folder: Looker names it after its user.
    path: Optional[str]
    personal: bool
    # folder_path_pattern's verdict on the real path; None when it could not
    # be decided (the record then leaves it out).
    path_allowed: Optional[bool]


# No folder, no folder_path_pattern rule: written as allowed, so a missing
# flag on a record always means undetermined.
_NO_FOLDER = _FolderFacts(path=None, personal=False, path_allowed=True)

# dashboard_elements carries each element's query; user fields are not read.
_CHART_DASHBOARD_FIELDS = ["id", "deleted", _FOLDER_FIELDS, "dashboard_elements"]


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
    element: DashboardElement,
    dashboard_deleted: bool,
    folder: _FolderFacts,
    withhold_title: bool,
) -> Dict[str, object]:
    query = _element_query(element)
    # Model and explore only: query.filters holds filter values, which are
    # row values, and dynamic_fields holds expressions.
    return {
        "name": element.id,
        "title": None if withhold_title else element.title,
        # A null type is dropped from a saved run; next to has_query, which is
        # always written, the verdict reads its absence as null.
        ATTR_TYPE: element.type,
        ATTR_HAS_QUERY: element_has_query(element),
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


def _look_record(look: Look, deleted: bool, withhold_title: bool) -> Dict[str, object]:
    return {
        "name": look.id,
        "title": None if withhold_title else look.title,
        ATTR_DELETED: deleted,
        ATTR_HAS_QUERY: look.query_id is not None,
        ATTR_FOLDER_PERSONAL: is_personal_folder(look.folder),
    }


# How many dashboard and look reads one --trace-charts run may make before it
# stops and leaves what it has not seen undetermined. Ingestion reads them
# all; a probe command stays bounded.
_TRACE_FETCH_LIMIT = MAX_PROBE_ITEMS
# The element queries carry the explores; user fields are not read.
_TRACE_DASHBOARD_FIELDS = ["id", _FOLDER_FIELDS, "dashboard_elements"]
# The folder flags let the trace skip what skip_personal_folders discards
# without spending a dashboard read on it.
_TRACE_LIST_FIELDS = ["id", "folder(id,is_personal,is_personal_descendant)"]
_TRACE_CAPPED = (
    "the chart trace stopped after {limit} reads, before reading everything "
    "ingestion would, so what it did not find in use is left undetermined "
    "(null)"
)
_TRACE_INCOMPLETE = (
    "the chart trace could not read everything ingestion would (a read was "
    "refused or failed, see above), so what it did not find in use is left "
    "undetermined (null)"
)

_ExploreRef = Tuple[str, str]


def _element_explores(element: DashboardElement) -> List[_ExploreRef]:
    """The (model, explore) pairs _get_looker_dashboard_element records with
    add_reachable_explore, with the same precedence: query, else look, else
    result_maker (its query and its filterables)."""
    pairs: List[_ExploreRef] = []
    if element.query is None and element.look is None and element.result_maker:
        for filterable in element.result_maker.filterables or []:
            if filterable.model is not None and filterable.view is not None:
                pairs.append((filterable.model, filterable.view))
    query = _element_query(element)
    # A pair without a model names no explore any listing holds.
    if query is not None and query.model is not None and query.view is not None:
        pairs.append((query.model, query.view))
    return pairs


@dataclass
class _Reachability:
    """What ingestion's reachable_explores and reachable_look_registry would
    hold, as far as one bounded trace could tell."""

    # Explores kept dashboard charts query.
    explores: Set[_ExploreRef] = field(default_factory=set)
    # Explores only standalone looks query. A look on a dashboard the walk did
    # not read is traced as standalone, but ingestion reads it as that
    # dashboard's tile, whose query can differ: these count only when the
    # dashboard walk was complete.
    look_explores: Set[_ExploreRef] = field(default_factory=set)
    looks: Set[str] = field(default_factory=set)
    # Every dashboard ingestion reads was read, so a look or explore not
    # found on one is not on one.
    dashboards_complete: bool = True
    # Every standalone look ingestion reads was read (or none are read).
    looks_complete: bool = True
    # The read bound stopped the trace (as opposed to a refused read).
    capped: bool = False
    reads: int = 0

    def _used(self, found: Callable[[Set[_ExploreRef]], bool]) -> Optional[bool]:
        if found(self.explores):
            return True
        if found(self.look_explores) and self.dashboards_complete:
            return True
        return False if self.dashboards_complete and self.looks_complete else None

    def explore_used(self, model: str, explore: str) -> Optional[bool]:
        return self._used(lambda refs: (model, explore) in refs)

    def model_used(self, model: str) -> Optional[bool]:
        return self._used(lambda refs: any(m == model for m, _ in refs))

    def on_kept_dashboard(self, look_id: str) -> Optional[bool]:
        if look_id in self.looks:
            return True
        return False if self.dashboards_complete else None


class _Refused(ProbeSoftError):
    """A 403/404 on one read. `outcome` names it without the object, so a
    caller reading many objects can count them in one warning."""

    def __init__(self, context: str, outcome: str) -> None:
        super().__init__(f"{context} {outcome}; treating it as empty.")
        self.outcome = outcome


class _Failed(ProbeReadFailed):
    """Any other failed read, with the same object-free `outcome`."""

    def __init__(self, context: str, outcome: str) -> None:
        super().__init__(f"{context} {outcome}")
        self.outcome = outcome


class _HttpOutcome:
    """What the last Looker read got back, read off the probe's own requests
    session.

    looker_sdk discards the HTTP status (its transport.Response keeps only
    ok and the body) and folds a request that got no response at all into
    the same SDKError as a 5xx, so the SDKError alone cannot tell a gateway
    page or a slow read from a Looker that is down. A response hook records
    the status of every response; a wrapper around session.request records
    the exception when there was none. LookerAPI already reaches into this
    session to mount its retry adapter.
    """

    def __init__(self) -> None:
        self.attached = False
        self.status: Optional[int] = None
        self.failure: Optional[requests.RequestException] = None

    def attach(self, looker: LookerAPI) -> None:
        transport = looker.client.transport
        if not isinstance(transport, looker_requests_transport.RequestsTransport):
            return
        session = transport.session
        session.hooks["response"].append(self._record_response)
        send = session.request

        def request(*args: Any, **kwargs: Any) -> requests.Response:
            try:
                return send(*args, **kwargs)
            except requests.RequestException as exc:
                self.failure = exc
                raise

        # An instance attribute, so only this probe's session is wrapped.
        setattr(session, "request", request)  # noqa: B010
        self.attached = True

    def _record_response(
        self, response: requests.Response, *args: Any, **kwargs: Any
    ) -> requests.Response:
        self.status = response.status_code
        return response

    def reset(self) -> None:
        self.status = None
        self.failure = None


def _open_looker(config: LookerDashboardSourceConfig) -> LookerAPI:
    """The connector's own wrapper: in-memory credentials
    (_DataHubLookerApiSettings), its retry adapter and transport options, and
    the me() call that proves the credentials work."""
    parsed = urlparse(config.base_url)
    if parsed.username is not None or parsed.password is not None:
        raise ProbeArgumentError(
            "base_url carries a user name or password before the host; Looker "
            "authenticates with client_id and client_secret, so remove it from "
            "base_url"
        )
    host = parsed.hostname or "the configured base_url"
    try:
        return LookerAPI(config)
    except ConfigurationError:
        # LookerAPI raises this from me() for any SDKError, a transport
        # failure included: refused credentials, a server error, an instance
        # that is down, or a base_url that is not a Looker API.
        raise ProbeConnectionError(
            f"Looker at {host} refused or did not answer the credential check; "
            f"check client_id, client_secret and base_url, and that the "
            f"instance is up"
        ) from None
    except (SDKError, DeserializeError) as exc:
        raise ProbeConnectionError(
            f"could not reach Looker at {host} ({type(exc).__name__})"
        ) from None


def _close_session(looker: LookerAPI) -> None:
    transport = looker.client.transport
    if isinstance(transport, looker_requests_transport.RequestsTransport):
        transport.session.close()


class LookerMetadataProbe(ProbeProviderBase):
    """Metadata-only probe over the Looker API, for the `looker` source.

    Builds LookerAPI on the first command rather than in for_config, so a bad
    credential surfaces as that command's connection error. Never constructs
    LookerDashboardSource, whose __init__ opens the API and builds registries.
    """

    def __init__(self, config: LookerDashboardSourceConfig) -> None:
        self._config = config
        self._folder_cache: Dict[str, _FolderFacts] = {}
        # Folders whose ancestors could not be read in this command; asked
        # once, so a 429 or 5xx is not repeated for every dashboard in them.
        self._folders_undetermined: Set[str] = set()
        self._http = _HttpOutcome()

    @classmethod
    def for_config(cls, config: LookerDashboardSourceConfig) -> "LookerMetadataProbe":
        return cls(config)

    def _api(self) -> LookerAPI:
        return self._open_once("looker", self._open, close=_close_session)

    def _open(self) -> LookerAPI:
        looker = _open_looker(self._config)
        self._http.attach(looker)
        return looker

    @contextmanager
    def _looker_call(
        self, context: str, not_found: Optional[str] = None
    ) -> Iterator[None]:
        """Run one Looker read, translating its failure into the probe's terms.

        Decided by what the read got back (_HttpOutcome), never by the
        error's text, which is never formatted either: an SDKError renders
        the response body. `from None` keeps it out of any traceback a caller
        logs too.
        - No response, and not a read timeout: Looker could not be reached,
          which ends the command.
        - 403/404: degrades (ProbeSoftError), except a 404 on the object the
          caller named (`not_found`), which is a bad argument.
        - Anything else -- a 5xx or gateway page, a 429, a read that timed
          out -- is one failed read (ProbeReadFailed), which a listing that
          reads many objects records and carries on from, as ingestion does.
        """
        http = self._http
        http.reset()
        try:
            yield
        except SDKError:
            status = (
                http.status if http.status is not None and http.status >= 400 else None
            )
            if status is None:
                if isinstance(http.failure, requests.exceptions.ReadTimeout):
                    raise _Failed(
                        context, "failed: Looker did not answer in time"
                    ) from None
                if http.attached:
                    raise ProbeConnectionError(
                        f"{context} failed: Looker could not be reached "
                        f"({type(http.failure).__name__ if http.failure else 'no HTTP response'})"
                    ) from None
            if status == 404 and not_found is not None:
                raise ProbeArgumentError(not_found) from None
            if status in (403, 404):
                raise _Refused(context, f"returned HTTP {status}") from None
            reason = f"HTTP {status}" if status is not None else "an unrecognised error"
            raise _Failed(context, f"failed: Looker returned {reason}") from None
        except DeserializeError:
            raise _Failed(
                context, "failed: Looker's response could not be read"
            ) from None

    def _listing(self, context: str, call: Callable[[], Sequence[_T]]) -> List[_T]:
        """One listing read; a refused one is empty with a warning, so empty
        says "could not look" rather than "nothing here"."""
        with soft_listing(self._warn), self._looker_call(context):
            return list(call())
        return []

    def _withholds_title(self, folder: Optional[FolderBase]) -> bool:
        # A title in a personal folder is user-written, and ingestion does not
        # emit the object under skip_personal_folders: list it by id only.
        return skips_personal_folder(self._config, folder)

    def _warn_counted(self, failures: "Counter[str]", what: str, then: str) -> None:
        """One warning per kind of failure, not one per object."""
        for outcome, count in sorted(failures.items()):
            self._warn(f"{count} {what} {outcome}; {then}")

    @probe_method()
    def permissions(self) -> Dict[str, object]:
        """Looker permissions this API credential's roles grant, and the ones
        ingestion needs that are missing: `missing_for_metadata` are the
        permissions `test_connection` requires for dashboards, charts and
        ownership; `missing_for_usage` the ones usage statistics need. Names
        only, never the API user's id or email. All three are null, with a
        warning, when the credential cannot read its own roles."""
        api = self._api()
        with (
            soft_listing(self._warn),
            self._looker_call("role lookup for the API user"),
        ):
            granted = api.get_available_permissions()
            return {
                "granted": sorted(granted),
                "missing_for_metadata": sorted(
                    BASIC_INGEST_REQUIRED_PERMISSIONS - granted
                ),
                "missing_for_usage": sorted(
                    USAGE_INGEST_REQUIRED_PERMISSIONS - granted
                ),
            }
        return {
            "granted": None,
            "missing_for_metadata": None,
            "missing_for_usage": None,
        }

    def _folder_facts(self, folder: Optional[FolderBase]) -> _FolderFacts:
        """What _should_skip_personal_folder_dashboard and
        _should_skip_dashboard_by_folder_path read, for one folder."""
        if folder is None or folder.id is None:
            return _NO_FOLDER
        cached = self._folder_cache.get(folder.id)
        if cached is not None:
            return cached
        personal = is_personal_folder(folder)
        undetermined = _FolderFacts(path=None, personal=personal, path_allowed=None)
        if folder.id in self._folders_undetermined:
            return undetermined
        api = self._api()
        ancestors: Optional[List[str]]
        try:
            if folder.parent_id is None:
                # A root folder has no ancestors; its path is its own name.
                ancestors = []
            else:
                with self._looker_call("folder ancestor lookup"):
                    ancestors = [
                        ancestor.name
                        for ancestor in api.client.folder_ancestors(
                            folder.id,
                            _FOLDER_ANCESTOR_FIELDS,
                            transport_options=api.transport_options,
                        )
                    ]
        except ProbeSoftError:
            # A refusal persists: LookerAPI.folder_ancestors, which ingestion
            # calls, swallows it too and matches the bare name. The SDK is
            # called directly here only so the degrade is reported.
            self._warn(_ANCESTORS_UNREADABLE)
            ancestors = None
        except ProbeReadFailed:
            # A 5xx or rate limit says nothing about what a healthy run reads:
            # undetermined, not a verdict. Remembered for this command only,
            # so the read is not repeated for every dashboard in the folder.
            self._warn(_ANCESTORS_UNDETERMINED)
            self._folders_undetermined.add(folder.id)
            return undetermined
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
            "title": None
            if self._withholds_title(dashboard.folder)
            else dashboard.title,
            ATTR_DELETED: deleted,
            ATTR_FOLDER_PATH: folder.path,
            ATTR_FOLDER_PERSONAL: folder.personal,
            ATTR_FOLDER_PATH_ALLOWED: folder.path_allowed,
        }

    @probe_method(kind=DASHBOARD_KIND, row_limit_param="limit")
    def dashboards(self, limit: int = 200) -> List[Dict[str, object]]:
        """Dashboards this credential can see, by id -- the string
        dashboard_pattern matches -- including ones the recipe would drop.
        Deleted ones are listed too (`deleted: true`); ingestion reads them
        only with include_deleted. Each record carries what ingestion also
        filters on: `folder_path` (what folder_path_pattern matches),
        `folder_personal` (what skip_personal_folders drops) and
        `folder_path_allowed`, folder_path_pattern's verdict computed at run
        time. A personal folder's path is withheld, because Looker names it
        after its user, and so is the title of a dashboard
        skip_personal_folders drops. Judge with `probe filter --kind Dashboard
        --from-run <report>`. Metadata only: no owners, users or usage
        counts."""
        return take(self._dashboard_rows(), limit)

    def _dashboard_rows(self) -> Iterator[Dict[str, object]]:
        # Live first, then deleted, as get_workunits_internal orders them.
        # Lazy, so a limit the live ones reach leaves folder lookups and the
        # deleted listing unrequested.
        api = self._api()
        listings: Sequence[
            Tuple[bool, str, Callable[[], Sequence[Union[Dashboard, DashboardBase]]]]
        ] = (
            (
                False,
                "dashboard listing",
                lambda: api.all_dashboards(fields=_DASHBOARD_LIST_FIELDS),
            ),
            (
                True,
                "deleted dashboard listing",
                lambda: api.search_dashboards(
                    fields=_DASHBOARD_LIST_FIELDS, deleted="true"
                ),
            ),
        )
        for deleted, context, call in listings:
            for dashboard in self._listing(context, call):
                record = self._dashboard_record(dashboard, deleted=deleted)
                if record is not None:
                    yield record

    @probe_method(kind=LOOK_KIND, parent_params=("dashboard",))
    def charts(self, dashboard: str) -> List[Dict[str, object]]:
        """Elements of one dashboard, by dashboard id, each by element id --
        the string chart_pattern matches. Ingestion emits these as charts of
        subtype Look, and only an element whose `type` is `vis` and
        `has_query` is true; `model` and `explore` are the explore it queries,
        which is what emit_used_explores_only keeps. Each record also carries
        its dashboard's facts (`dashboard_deleted`, `dashboard_folder_*`),
        because ingestion reads none of a dashboard's charts when it drops the
        dashboard itself; `probe filter` applies those rules. Judge with
        `probe filter --kind Look --from-run <report>`. Metadata only: no
        query filters, fields or SQL."""
        api = self._api()
        shown = echoed(dashboard)
        with (
            soft_listing(self._warn),
            self._looker_call(
                f"dashboard {shown}",
                not_found=(
                    f"no dashboard with id {shown}; pass an id from the `dashboards` "
                    f"listing"
                ),
            ),
        ):
            detail = api.dashboard(
                dashboard_id=dashboard, fields=_CHART_DASHBOARD_FIELDS
            )
            self._note_parent_dashboard(detail)
            folder = self._folder_facts(detail.folder)
            withhold = self._withholds_title(detail.folder)
            return [
                _element_record(element, bool(detail.deleted), folder, withhold)
                for element in detail.dashboard_elements or []
                if element.id is not None
            ]
        return []

    def _note_parent_dashboard(self, detail: Dashboard) -> None:
        """`probe filter --parent` judges a dashboard by id only; say when one
        of ingestion's other dashboard rules drops it (process_dashboard)."""
        label = f"dashboard {echoed(str(detail.id))}"
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
            where = (
                f"folder {echoed(folder.path)}" if folder.path else "a personal folder"
            )
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
        include_deleted. chart_pattern does not apply to these.

        Ingestion emits a look as a standalone chart only when
        extract_independent_looks is true, the look has a query (`has_query`,
        read back once per look as ingestion reads it; null when that read
        failed), and, under skip_personal_folders, it is not in a personal
        folder (`folder_personal`; such a look's title is withheld).

        A look that is also on a dashboard ingestion reads is emitted as that
        dashboard's chart instead. Only `trace_charts` tells that: it reads the
        dashboards ingestion reads (bounded) and sets `on_kept_dashboard`,
        null when the trace could not settle it.

        Judge with `probe filter --kind Look --from-run <report>` and no
        --parent. Metadata only: no owners and no folder names."""
        if not self._config.extract_independent_looks:
            self._warn(
                "extract_independent_looks is false, so ingestion emits none of "
                "these as standalone charts; looks on dashboards are listed by "
                "`charts`"
            )
        rows = take(self._look_rows(), limit)
        failures: Counter[str] = Counter()
        for row in rows:
            if row[ATTR_HAS_QUERY] is True:
                read, query = self._read_look_query(str(row["name"]), failures)
                # Unread: undetermined, not "no query".
                row[ATTR_HAS_QUERY] = (query is not None) if read else None
        self._warn_counted(
            failures, "look read(s)", "their has_query is null (undetermined)"
        )
        if trace_charts:
            reach = self._trace(with_standalone_looks=False)
            for row in rows:
                row[ATTR_TRACED] = True
                row[ATTR_ON_KEPT_DASHBOARD] = reach.on_kept_dashboard(str(row["name"]))
        return rows

    def _look_rows(self) -> Iterator[Dict[str, object]]:
        api = self._api()
        seen: Set[str] = set()
        listings: Sequence[Tuple[bool, str, Callable[[], Sequence[Look]]]] = (
            (
                False,
                "look listing",
                lambda: api.all_looks(fields=_LOOK_LIST_FIELDS, soft_deleted=False),
            ),
            (
                True,
                "deleted look listing",
                lambda: api.search_looks(fields=_LOOK_LIST_FIELDS, deleted=True),
            ),
        )
        for deleted, context, call in listings:
            for look in self._listing(context, call):
                # extract_independent_looks skips a look without an id.
                if look.id is None or look.id in seen:
                    continue
                seen.add(look.id)
                yield _look_record(
                    look,
                    deleted=deleted,
                    withhold_title=self._withholds_title(look.folder),
                )

    def _lookml_models(self) -> List[LookmlModel]:
        return self._listing("LookML model listing", self._api().all_lookml_models)

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
        rows: List[Dict[str, object]] = [
            {
                "name": model.name,
                "project": model.project_name,
                # list_all_explores skips an unnamed explore.
                ATTR_EXPLORE_COUNT: sum(
                    1 for explore in model.explores or [] if explore.name is not None
                ),
            }
            for model in self._lookml_models()
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
        query -- and sets `used`, computed from this recipe at run time (null
        when the trace could not settle it). It costs one read per kept
        dashboard plus one per standalone look, up to a bound. Without it,
        `charts` shows the explore each chart of one dashboard uses. Metadata
        only."""
        models = self._lookml_models()
        if not models and self.warnings:
            # The listing was refused and the warning says so; "no such model"
            # would blame the caller for what is a permissions gap.
            return []
        lookml_model = resolve_name(
            model,
            [m for m in models if m.name is not None],
            key=lambda m: str(m.name),
            kind="LookML model",
            list_command="probe run models",
            distinguish=lambda m: m.project_name,
        ).record
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

    def _read_look_query(
        self, look_id: str, failures: "Counter[str]"
    ) -> Tuple[bool, Optional[Query]]:
        """(read, query) for one look, as extract_independent_looks reads it.

        Ingestion skips a look whose read fails and carries on; here a refused
        or failed read is counted in `failures` (the caller warns once per
        kind) and read is False, so the caller treats the look as
        undetermined. An unreachable Looker still ends the command, and
        anything else is a defect that propagates.
        """
        api = self._api()
        try:
            with self._looker_call("look read"):
                return True, api.get_look(look_id, fields=["query"]).query
        except (_Refused, _Failed) as exc:
            failures[exc.outcome] += 1
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
        cannot be reached at all (ProbeConnectionError) ends the command,
        since nothing it would report could be trusted."""
        reach = _Reachability()
        api = self._api()
        failures: Counter[str] = Counter()
        for dashboard_id in self._traced_dashboard_ids(reach):
            if reach.reads >= _TRACE_FETCH_LIMIT:
                reach.dashboards_complete = False
                reach.capped = True
                break
            reach.reads += 1
            try:
                with self._looker_call("dashboard read for the chart trace"):
                    detail = api.dashboard(
                        dashboard_id=dashboard_id, fields=_TRACE_DASHBOARD_FIELDS
                    )
            except (_Refused, _Failed) as exc:
                failures[exc.outcome] += 1
                reach.dashboards_complete = False
                continue
            self._trace_dashboard(reach, detail)
        self._warn_counted(
            failures,
            "dashboard read(s) for the chart trace",
            "what they hold is undetermined",
        )
        # After an incomplete dashboard walk, look-only explores cannot be
        # counted (_Reachability._used), so their reads would change nothing.
        if (
            with_standalone_looks
            and self._config.extract_independent_looks
            and reach.dashboards_complete
        ):
            self._trace_standalone_looks(reach)
        if reach.capped:
            self._warn(_TRACE_CAPPED.format(limit=_TRACE_FETCH_LIMIT))
        elif not (reach.dashboards_complete and reach.looks_complete):
            self._warn(_TRACE_INCOMPLETE)
        return reach

    def _trace_listing(
        self, reach: _Reachability, context: str, call: Callable[[], Sequence[_T]]
    ) -> List[_T]:
        """One trace listing. A refused or failed one leaves the trace
        incomplete instead of failing the listing it decorates."""
        try:
            with self._looker_call(context):
                return list(call())
        except (ProbeSoftError, ProbeReadFailed) as exc:
            self._warn(str(exc))
            reach.dashboards_complete = False
            return []

    def _traced_dashboard_ids(self, reach: _Reachability) -> List[str]:
        """get_workunits_internal's dashboard ids: live, deleted ones only
        under include_deleted, then dashboard_pattern on the id."""
        api = self._api()
        listed: List[Union[Dashboard, DashboardBase]] = list(
            self._trace_listing(
                reach,
                "dashboard listing for the chart trace",
                lambda: api.all_dashboards(fields=_TRACE_LIST_FIELDS),
            )
        )
        if self._config.include_deleted:
            listed.extend(
                self._trace_listing(
                    reach,
                    "deleted dashboard listing for the chart trace",
                    lambda: api.search_dashboards(
                        fields=_TRACE_LIST_FIELDS, deleted="true"
                    ),
                )
            )
        return [
            dashboard.id
            for dashboard in listed
            if dashboard.id is not None
            and dashboard_id_verdict(self._config, dashboard.id).included
            # Ingestion reads these and then discards them; skipping them here
            # keeps them from spending the read bound. _trace_dashboard still
            # checks the read folder, for a listing that lacked the flags.
            and not skips_personal_folder(self._config, dashboard.folder)
        ]

    def _trace_dashboard(self, reach: _Reachability, detail: Dashboard) -> None:
        """What process_dashboard records for one dashboard. folder_path_pattern
        is deliberately not applied: process_dashboard checks it only after
        _get_looker_dashboard has recorded the dashboard's looks and explores,
        so a dashboard it drops still makes them reachable."""
        if skips_personal_folder(self._config, detail.folder):
            return
        for element in detail.dashboard_elements or []:
            if (
                element.id is None
                or not chart_id_verdict(self._config, element.id).included
            ):
                continue
            if element.look_id is not None:
                reach.looks.add(element.look_id)
            reach.explores.update(_element_explores(element))

    def _trace_standalone_looks(self, reach: _Reachability) -> None:
        """The explores extract_independent_looks records, with its skips."""
        api = self._api()
        try:
            with self._looker_call("look listing for the chart trace"):
                looks: Sequence[Look] = api.all_looks(
                    fields=_LOOK_LIST_FIELDS,
                    soft_deleted=self._config.include_deleted,
                )
        except (ProbeSoftError, ProbeReadFailed) as exc:
            self._warn(str(exc))
            reach.looks_complete = False
            return
        failures: Counter[str] = Counter()
        for look in looks:
            if look.id is None:
                continue
            # The verdict ingestion applies to each look; one call, so a rule
            # added to it reaches the trace too.
            if not standalone_look_verdict(
                self._config,
                LookFacts(
                    personal=is_personal_folder(look.folder),
                    on_kept_dashboard=look.id in reach.looks,
                    has_query=look.query_id is not None,
                ),
            ).included:
                continue
            if reach.reads >= _TRACE_FETCH_LIMIT:
                reach.looks_complete = False
                reach.capped = True
                break
            reach.reads += 1
            read, query = self._read_look_query(look.id, failures)
            if not read:
                reach.looks_complete = False
            elif (
                query is not None and query.model is not None and query.view is not None
            ):
                reach.look_explores.add((query.model, query.view))
        self._warn_counted(
            failures,
            "look read(s) for the chart trace",
            "the explores they query are undetermined",
        )
