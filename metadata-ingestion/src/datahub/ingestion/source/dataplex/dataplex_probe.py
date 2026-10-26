"""Probe support for the Dataplex source.

Lists what the Universal Catalog holds under the names ingestion's patterns
are matched against -- full resource names for entry groups and entries, FQNs
for fqn_pattern, bare aspect-type ids for aspect_type_pattern -- and lists
denied objects too, so `probe filter` can explain them. Metadata only: no
aspect data, descriptions or glossary content leaves this module.

GCP failures are labelled by the framework (class and HTTP status only, via
probe_error_code); GCP error text names the service account, the permission
and the resource, and is never shown.
"""

from contextlib import AbstractContextManager, contextmanager
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
)

from google.api_core import exceptions
from google.auth.exceptions import GoogleAuthError
from google.cloud import dataplex_v1, resourcemanager_v3
from google.oauth2 import service_account

from datahub.ingestion.agent.error_policy import name_foreign
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import ProbeProviderBase, echoed, take
from datahub.ingestion.agent.verdicts import ProbeArgumentError, ProbeReadFailed
from datahub.ingestion.source.common.gcp_errors import gcp_error_code
from datahub.ingestion.source.common.gcp_project_filter import iter_candidate_projects
from datahub.ingestion.source.dataplex.dataplex_config import (
    DATAPLEX_ASPECT_TYPE_KIND,
    DATAPLEX_ENTRY_FQN_KIND,
    DATAPLEX_ENTRY_GROUP_KIND,
    DATAPLEX_ENTRY_KIND,
    DATAPLEX_PROJECT_KIND,
    DataplexConfig,
)
from datahub.ingestion.source.dataplex.dataplex_context import DataplexContext
from datahub.ingestion.source.dataplex.dataplex_entries import (
    DataplexEntriesProcessor,
    DataplexEntriesReport,
)
from datahub.ingestion.source.dataplex.dataplex_export import (
    build_service_account_credentials,
)
from datahub.ingestion.source.dataplex.dataplex_ids import (
    extract_entry_type_short_name,
)
from datahub.ingestion.source.dataplex.dataplex_mappers import ENTRY_MAPPERS
from datahub.ingestion.source.dataplex.dataplex_properties import (
    aspect_type_short_name,
)
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport

T = TypeVar("T")
Credentials = Optional[service_account.Credentials]
CatalogFactory = Callable[[Credentials], dataplex_v1.CatalogServiceClient]
ProjectsFactory = Callable[[Credentials], resourcemanager_v3.ProjectsClient]

# A location whose Dataplex API is disabled or forbidden answers one of these.
# Ingestion warns and moves on to the next location (process_entries), and a
# sweep does the same -- but only a sweep: a location the caller named is a
# question about that location, and "empty with a warning" would misreport it.
_LOCATION_SOFT_ERRORS = (exceptions.NotFound, exceptions.PermissionDenied)

_EXPLICIT_PROJECTS_NOTE = (
    "project_ids is set, so these are exactly the projects ingestion reads; "
    "project_labels and project_id_pattern are not consulted and Resource "
    "Manager is not called"
)


@contextmanager
def _named_by_caller(message: str) -> Iterator[None]:
    """A NotFound on a resource the caller named is the caller's to fix
    (exit 2). Every other failure propagates for the framework to label."""
    try:
        yield
    except exceptions.NotFound:
        raise ProbeArgumentError(message) from None


def _new_catalog_client(credentials: Credentials) -> dataplex_v1.CatalogServiceClient:
    return dataplex_v1.CatalogServiceClient(credentials=credentials)


def _new_projects_client(credentials: Credentials) -> resourcemanager_v3.ProjectsClient:
    return resourcemanager_v3.ProjectsClient(credentials=credentials)


def _close(client: object) -> None:
    transport = getattr(client, "transport", None)
    if transport is not None:
        transport.close()


class DataplexMetadataProbe(ProbeProviderBase):
    """Metadata-only probe over the Dataplex Catalog and Resource Manager APIs.

    Clients are created on first use, so building the provider opens nothing
    and a command only needs the API it calls: `projects` never touches the
    Catalog API, and `entry_groups` never touches Resource Manager.
    """

    def __init__(
        self,
        config: DataplexConfig,
        credentials: Credentials,
        *,
        catalog_factory: CatalogFactory = _new_catalog_client,
        projects_factory: ProjectsFactory = _new_projects_client,
    ) -> None:
        self._config = config
        self._credentials = credentials
        self._catalog_factory = catalog_factory
        self._projects_factory = projects_factory

    @classmethod
    def for_config(cls, config: DataplexConfig) -> "DataplexMetadataProbe":
        # Credentials resolve exactly as DataplexSource.__init__ resolves them.
        # Building service-account credentials from info makes no request.
        try:
            credentials = build_service_account_credentials(config)
        except (ValueError, OSError, GoogleAuthError) as exc:
            # SECURITY: the parser's message can quote the key material or the
            # key id it choked on. The type is enough to act on.
            raise ProbeArgumentError(
                f"could not build service-account credentials from the recipe's "
                f"`credential` block ({type(exc).__name__}); check that it holds "
                f"a complete service-account key"
            ) from None
        return cls(config, credentials)

    @staticmethod
    def probe_error_code(exc: BaseException) -> Optional[str]:
        """The HTTP status a google-api-core error carries ("HTTP 403")."""
        return gcp_error_code(exc)

    def _catalog(self) -> dataplex_v1.CatalogServiceClient:
        return self._open_once(
            "catalog", lambda: self._catalog_factory(self._credentials), close=_close
        )

    def _projects(self) -> resourcemanager_v3.ProjectsClient:
        return self._open_once(
            "projects", lambda: self._projects_factory(self._credentials), close=_close
        )

    def _entries_processor(self) -> DataplexEntriesProcessor:
        # A processor, not a Source: it only holds the client and reports, so
        # building one opens nothing. Used for its fetchers, never its filters.
        return self._open_once(
            "entries-processor",
            lambda: DataplexEntriesProcessor(
                config=self._config,
                catalog_client=self._catalog(),
                report=DataplexEntriesReport(),
                source_report=DataplexReport(),
                ctx=DataplexContext(config=self._config, credentials=self._credentials),
            ),
        )

    def _sweep_locations(
        self,
        project: str,
        location: Optional[str],
        list_one: Callable[[str], Iterable[T]],
        what: str,
    ) -> Iterator[Tuple[str, T]]:
        """(location, item) across one named location, or every entries_location.

        A forbidden/missing location in a sweep is skipped with a warning, as
        ingestion skips it. If every location fails the sweep fails (exit 3):
        an unusable credential must not read as an empty catalog. Not as the
        last GCP error: a NotFound there would reach the caller's not_found
        and blame the project, when it equally means the API is disabled or
        the location holds no catalog.
        """
        if location is not None:
            for item in list_one(location):
                yield location, item
            return
        locations = list(self._config.entries_locations)
        failed: List[exceptions.GoogleAPICallError] = []
        for loc in locations:
            try:
                for item in list_one(loc):
                    yield loc, item
            except _LOCATION_SOFT_ERRORS as exc:
                failed.append(exc)
                self._warn(
                    f"{what} for project {echoed(project)} in location "
                    f"{echoed(loc)} could not be read "
                    f"{name_foreign(exc, type(self))}; skipped, as ingestion skips it"
                )
        if failed and len(failed) == len(locations):
            statuses = ", ".join(
                sorted({name_foreign(exc, type(self)) for exc in failed})
            )
            raise ProbeReadFailed(
                f"{what} for project {echoed(project)} could not be read in any "
                f"of entries_locations ({statuses}); a NotFound means the "
                f"Dataplex API is not enabled, no location there holds a "
                f"catalog, or the project id is wrong"
            )

    @probe_method(kind=DATAPLEX_PROJECT_KIND, row_limit_param="limit")
    def projects(self, limit: int = 200) -> List[Dict[str, str]]:
        """GCP projects this recipe would consider, named by project id -- the
        string project_id_pattern is matched against. With project_ids set these
        are exactly those ids and Resource Manager is not called; otherwise every
        project the credential can see (narrowed by project_labels, as ingestion
        narrows it), including ones project_id_pattern excludes."""
        if self._config.project_ids:
            self._warn(_EXPLICIT_PROJECTS_NOTE)
        # The candidates resolve_gcp_projects chooses from, without its
        # project_id_pattern filter: a denied project is reported, not hidden.
        # Lazy, so the pager stops at the limit. Resource Manager is not
        # called when project_ids is set.
        projects = None if self._config.project_ids else self._projects()
        return [
            {"name": p.id, "display_name": p.name}
            for p in take(iter_candidate_projects(self._config, projects), limit)
        ]

    @probe_method(
        kind=DATAPLEX_ENTRY_GROUP_KIND,
        row_limit_param="limit",
        parent_params=("project",),
    )
    def entry_groups(
        self, project: str, location: Optional[str] = None, limit: int = 200
    ) -> List[Dict[str, str]]:
        """Entry groups in one project, named by full resource name
        (projects/<p>/locations/<l>/entryGroups/<g>) -- the exact string
        filter_config.entry_groups.pattern is matched against, returned as the
        API returns it (the project segment may be a number). Omit --location to
        sweep every entries_locations entry as ingestion does. The sweep skips,
        with a warning, only a location answering NotFound or PermissionDenied
        (ingestion skips a location on any error), so a 503 or Unauthenticated
        in one location fails the whole sweep with exit 3. Includes system
        groups such as @bigquery and groups the pattern denies."""
        located = self._sweep_locations(
            project,
            location,
            lambda loc: self._entries_processor().list_entry_groups(project, loc),
            "entry groups",
        )
        where = f"location {echoed(location)}" if location else "any entries_locations"
        with _named_by_caller(
            f"Dataplex found no project {echoed(project)} in {where}; pass the "
            f"project id as the recipe names it"
        ):
            return [
                {
                    "name": group.name,
                    "location": loc,
                    "display_name": group.display_name,
                }
                for loc, group in take(located, limit)
            ]

    def _list_entries(self, entry_group: str) -> Iterable[dataplex_v1.Entry]:
        # Ingestion's own listing request, unfiltered: _list_entry_stubs drops
        # denied and FQN-less entries, which is ingestion's policy, and a
        # probe must report both.
        return self._entries_processor().list_entries(entry_group)

    @staticmethod
    def _entry_group_named(entry_group: str) -> AbstractContextManager[None]:
        return _named_by_caller(
            f"no entry group named {echoed(entry_group)}; list them with "
            f"`entry_groups` and pass a name exactly as it is returned"
        )

    @probe_method(
        kind=DATAPLEX_ENTRY_KIND,
        row_limit_param="limit",
        parent_params=("project", "entry_group"),
    )
    def entries(
        self, project: str, entry_group: str, limit: int = 200
    ) -> List[Dict[str, object]]:
        """Entries in one entry group, named by full resource name -- the
        string filter_config.entries.pattern is matched against. Pass
        entry_group exactly as `entry_groups` returns it, and project as the
        recipe names it (it is used to judge project_id_pattern, not to fetch).
        Each record also carries fully_qualified_name, which
        filter_config.entries.fqn_pattern judges separately (use `entry_fqns`
        and --kind EntryFqn), and whether ingestion has a mapper for its type.
        An entry with an empty FQN or an unsupported type is never ingested,
        whatever the patterns say. Metadata only: no aspect data."""
        records: List[Dict[str, object]] = []
        with self._entry_group_named(entry_group):
            for entry in take(self._list_entries(entry_group), limit):
                short = extract_entry_type_short_name(entry.entry_type)
                records.append(
                    {
                        "name": entry.name,
                        "fully_qualified_name": entry.fully_qualified_name,
                        "entry_type": short or entry.entry_type,
                        "supported": short is not None and short in ENTRY_MAPPERS,
                    }
                )
        if any(not r["fully_qualified_name"] for r in records):
            self._warn(
                "some entries have no fully_qualified_name; ingestion skips those "
                "whatever filter_config says"
            )
        if any(not r["supported"] for r in records):
            self._warn(
                "some entries have an entry_type ingestion has no mapper for "
                "(supported: false); ingestion skips those whatever filter_config "
                "says"
            )
        return records

    @probe_method(
        kind=DATAPLEX_ENTRY_FQN_KIND,
        row_limit_param="limit",
        parent_params=("project", "entry_group"),
    )
    def entry_fqns(self, project: str, entry_group: str, limit: int = 200) -> List[str]:
        """Fully-qualified names of the entries in one entry group -- the
        strings filter_config.entries.fqn_pattern is matched against
        (e.g. bigquery:<project>.<dataset>.<table>). Entries without one are
        left out; `entries` shows them. An entry is ingested only if both its
        name (--kind Entry) and its FQN (--kind EntryFqn) are included."""
        # The listing must start inside the block: the first ListEntries RPC
        # goes out when the pager is built, and a NotFound from it is a
        # mistyped --entry-group.
        with self._entry_group_named(entry_group):
            fqns = (
                entry.fully_qualified_name
                for entry in self._list_entries(entry_group)
                if entry.fully_qualified_name
            )
            return take(fqns, limit)

    @probe_method(kind=DATAPLEX_ASPECT_TYPE_KIND)
    def entry_aspect_types(self, entry: str) -> List[str]:
        """Aspect types attached to one entry, as the bare ids
        aspect_type_pattern is matched against (the default denies datahub-*,
        the aspects DataHub's sync-back writes). A denied aspect is dropped from
        custom properties only; the entry itself is still ingested. Pass the
        entry's full resource name as `entries` returns it. Names only -- aspect
        data is never returned."""
        with _named_by_caller(
            f"no entry named {echoed(entry)}; list them with `entries` and pass "
            f"a name exactly as it is returned"
        ):
            detail = self._entries_processor().get_entry(entry)
        return sorted({aspect_type_short_name(key) for key in detail.aspects})
