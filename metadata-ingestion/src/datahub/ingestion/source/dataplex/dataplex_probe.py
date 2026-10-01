"""Probe support for the Dataplex source.

Lists what the Universal Catalog holds under the names ingestion's patterns
are matched against -- full resource names for entry groups and entries, FQNs
for fqn_pattern, bare aspect-type ids for aspect_type_pattern -- and lists
denied objects too, so `probe filter` can explain them. Metadata only: no
aspect data, descriptions or glossary content leaves this module.
"""

import itertools
from contextlib import AbstractContextManager, contextmanager
from functools import cached_property
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

from google.api_core import exceptions
from google.auth.exceptions import GoogleAuthError
from google.cloud import dataplex_v1, resourcemanager_v3
from google.oauth2 import service_account

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.source.common.gcp_project_filter import (
    GcpProject,
    _search_all_projects,
    _search_projects_by_labels,
)
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
_Client = Union[dataplex_v1.CatalogServiceClient, resourcemanager_v3.ProjectsClient]

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


def _status(exc: exceptions.GoogleAPICallError) -> str:
    code = f"HTTP {exc.code}" if exc.code is not None else "no HTTP status"
    return f"{code} ({type(exc).__name__})"


@contextmanager
def _scrubbed(method: str, not_found: Optional[str] = None) -> Iterator[None]:
    """Re-raise a GCP failure as its status and RPC method only.

    SECURITY: GCP error text is server-composed and routinely names the
    calling service account, the permission and the resource; auth errors can
    carry token-endpoint responses. None of it is needed to act on the
    failure, so none of it is kept -- `from None` drops the chained original
    too, so a traceback cannot resurrect it.

    `not_found` turns a NotFound into a bad-argument error: the resource was
    named by the caller, so the fix is the name, not the connection.
    """
    try:
        yield
    except exceptions.NotFound as exc:
        if not_found is not None:
            raise ValueError(not_found) from None
        raise ProbeConnectionError(
            f"Dataplex {method} failed: {_status(exc)}"
        ) from None
    except exceptions.GoogleAPICallError as exc:
        raise ProbeConnectionError(
            f"Dataplex {method} failed: {_status(exc)}"
        ) from None
    except GoogleAuthError as exc:
        raise ProbeConnectionError(
            f"Dataplex {method} could not authenticate ({type(exc).__name__})"
        ) from None


class DataplexMetadataProbe:
    """Metadata-only probe over the Dataplex Catalog and Resource Manager APIs.

    Clients are created on first use, so building the provider opens nothing
    and a command only needs the API it calls: `projects` never touches the
    Catalog API, and `entry_groups` never touches Resource Manager.
    """

    def __init__(
        self,
        config: DataplexConfig,
        credentials: Optional[service_account.Credentials],
        *,
        catalog_client: Optional[dataplex_v1.CatalogServiceClient] = None,
        projects_client: Optional[resourcemanager_v3.ProjectsClient] = None,
    ) -> None:
        self._config = config
        self._credentials = credentials
        self._catalog_client = catalog_client
        self._projects_client = projects_client
        # Clients this provider built itself, and so must close.
        self._opened: List[_Client] = []
        self.warnings: List[str] = []

    @classmethod
    def for_config(cls, config: DataplexConfig) -> "DataplexMetadataProbe":
        # Credentials resolve exactly as DataplexSource.__init__ resolves them.
        # Building service-account credentials from info makes no request.
        try:
            credentials = build_service_account_credentials(config)
        except Exception as exc:
            # SECURITY: the parser's message can quote the key material or the
            # key id it choked on. The type is enough to act on.
            raise ValueError(
                f"could not build service-account credentials from the recipe's "
                f"`credential` block ({type(exc).__name__}); check that it holds "
                f"a complete service-account key"
            ) from None
        return cls(config, credentials)

    def __enter__(self) -> "DataplexMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        for client in self._opened:
            client.transport.close()

    @cached_property
    def _catalog(self) -> dataplex_v1.CatalogServiceClient:
        if self._catalog_client is not None:
            return self._catalog_client
        client = dataplex_v1.CatalogServiceClient(credentials=self._credentials)
        self._opened.append(client)
        return client

    @cached_property
    def _projects(self) -> resourcemanager_v3.ProjectsClient:
        if self._projects_client is not None:
            return self._projects_client
        client = resourcemanager_v3.ProjectsClient(credentials=self._credentials)
        self._opened.append(client)
        return client

    @cached_property
    def _entries_processor(self) -> DataplexEntriesProcessor:
        # A processor, not a Source: it only holds the client and reports, so
        # building one opens nothing. Used for its fetchers, never its filters.
        return DataplexEntriesProcessor(
            config=self._config,
            catalog_client=self._catalog,
            report=DataplexEntriesReport(),
            source_report=DataplexReport(),
            ctx=DataplexContext(config=self._config, credentials=self._credentials),
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
        ingestion skips it. If every location fails the last error is raised:
        an unusable credential must not read as an empty catalog.
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
                self.warnings.append(
                    f"{what} for project '{project}' in location '{loc}' could "
                    f"not be read ({_status(exc)}); skipped, as ingestion skips it"
                )
        if failed and len(failed) == len(locations):
            raise failed[-1]

    @probe_method(kind=DATAPLEX_PROJECT_KIND, row_limit_param="limit")
    def projects(self, limit: int = 200) -> List[Dict[str, str]]:
        """GCP projects this recipe would consider, named by project id -- the
        string project_id_pattern is matched against. With project_ids set these
        are exactly those ids and Resource Manager is not called; otherwise every
        project the credential can see (narrowed by project_labels, as ingestion
        narrows it), including ones project_id_pattern excludes."""
        if self._config.project_ids:
            self.warnings.append(_EXPLICIT_PROJECTS_NOTE)
            return [
                {"name": pid, "display_name": pid}
                for pid in self._config.project_ids[:limit]
            ]
        # Ingestion's own fetchers, called the way resolve_gcp_projects calls
        # them but without its is_project_allowed filter: a denied project is
        # reported, not hidden.
        labels = self._config.project_labels
        with _scrubbed("search_projects"):
            found: List[GcpProject] = (
                _search_projects_by_labels(frozenset(labels), self._projects)
                if labels
                else _search_all_projects(self._projects)
            )
        return [{"name": p.id, "display_name": p.name} for p in found[:limit]]

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
        sweep every entries_locations entry as ingestion does; a location that
        cannot be read is skipped with a warning. Includes system groups such as
        @bigquery and groups the pattern denies."""
        located = self._sweep_locations(
            project,
            location,
            lambda loc: self._entries_processor.list_entry_groups(project, loc),
            "entry groups",
        )
        where = f"location '{location}'" if location else "any entries_locations"
        with _scrubbed(
            "list_entry_groups",
            not_found=f"Dataplex found no project '{project}' in {where}; pass "
            f"the project id as the recipe names it",
        ):
            return [
                {
                    "name": group.name,
                    "location": loc,
                    "display_name": group.display_name,
                }
                for loc, group in itertools.islice(located, limit)
            ]

    def _list_entries(self, entry_group: str) -> Iterator[dataplex_v1.Entry]:
        # The call _list_entry_stubs makes, without its filter: that method
        # drops denied and FQN-less entries, which is ingestion's policy, and a
        # probe must report both.
        request = dataplex_v1.ListEntriesRequest(parent=entry_group)
        yield from self._catalog.list_entries(request=request)

    @staticmethod
    def _entries_scrubbed(entry_group: str) -> AbstractContextManager[None]:
        return _scrubbed(
            "list_entries",
            not_found=f"no entry group named '{entry_group}'; list them with "
            f"`entry_groups` and pass a name exactly as it is returned",
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
        with self._entries_scrubbed(entry_group):
            for entry in itertools.islice(self._list_entries(entry_group), limit):
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
            self.warnings.append(
                "some entries have no fully_qualified_name; ingestion skips those "
                "whatever filter_config says"
            )
        if any(not r["supported"] for r in records):
            self.warnings.append(
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
    def entry_fqns(
        self, project: str, entry_group: str, limit: int = 200
    ) -> List[str]:
        """Fully-qualified names of the entries in one entry group -- the
        strings filter_config.entries.fqn_pattern is matched against
        (e.g. bigquery:<project>.<dataset>.<table>). Entries without one are
        left out; `entries` shows them. An entry is ingested only if both its
        name (--kind Entry) and its FQN (--kind EntryFqn) are included."""
        fqns = (
            entry.fully_qualified_name
            for entry in self._list_entries(entry_group)
            if entry.fully_qualified_name
        )
        with self._entries_scrubbed(entry_group):
            return list(itertools.islice(fqns, limit))

    @probe_method(kind=DATAPLEX_ASPECT_TYPE_KIND)
    def entry_aspect_types(self, entry: str) -> List[str]:
        """Aspect types attached to one entry, as the bare ids
        aspect_type_pattern is matched against (the default denies datahub-*,
        the aspects DataHub's sync-back writes). A denied aspect is dropped from
        custom properties only; the entry itself is still ingested. Pass the
        entry's full resource name as `entries` returns it. Names only -- aspect
        data is never returned."""
        with _scrubbed(
            "get_entry",
            not_found=f"no entry named '{entry}'; list them with `entries` and "
            f"pass a name exactly as it is returned",
        ):
            # Ingestion's own get_entry(view=ALL) fetch, so the aspect keys are
            # the ones ingestion sees.
            detail = self._entries_processor._fetch_entry_detail(entry)
        return sorted({aspect_type_short_name(key) for key in detail.aspects})
