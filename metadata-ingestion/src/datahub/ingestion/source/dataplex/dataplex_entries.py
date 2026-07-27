"""Entry processing utilities for Dataplex source (Universal Catalog/Entries API)."""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from itertools import islice
from typing import (
    TYPE_CHECKING,
    Iterable,
    List,
    Tuple,
)

from google.cloud import dataplex_v1

from datahub.ingestion.api.report import Report
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_context import DataplexContext
from datahub.ingestion.source.dataplex.dataplex_mappers import (
    EntryMappingContext,
    get_entry_mapper,
)
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.perf_timer import PerfTimer

if TYPE_CHECKING:
    from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple
    from datahub.sdk.entity import Entity


logger = logging.getLogger(__name__)

# Naive upper bound on in-flight futures submitted to the thread pool at once.
# Prevents O(N) memory growth when there are thousands of entries.
# TODO: replace with proper backpressure (e.g. bounded queue / semaphore).
WORKERS_BATCH_SIZE = 200


@dataclass
class DataplexEntriesReport(Report):
    """Entry-processing observability metrics.

    Tracks high-level counters and lossy samples for filtered and processed
    entry groups / entries. This report is intentionally scoped to the entries
    processing loop and can later be folded into DataplexReport.
    """

    entry_groups_seen: int = 0
    entry_groups_filtered: int = 0
    entry_group_filtered_samples: LossyList[str] = field(default_factory=LossyList)
    entry_groups_processed: int = 0
    entry_group_processed_samples: LossyList[str] = field(default_factory=LossyList)

    entries_seen: int = 0
    entries_filtered_by_pattern: int = 0
    entry_pattern_filtered_samples: LossyList[str] = field(default_factory=LossyList)
    entries_filtered_by_missing_fqn: int = 0
    entry_missing_fqn_samples: LossyList[str] = field(default_factory=LossyList)
    entries_filtered_by_fqn_pattern: int = 0
    entry_fqn_filtered_samples: LossyList[str] = field(default_factory=LossyList)

    entries_processed: int = 0
    entries_processed_samples: LossyList[str] = field(default_factory=LossyList)
    catalog_api: dict[str, tuple[int, float]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Lock protecting all mutable fields when report methods are called from
        # parallel worker threads (Phase 1b of process_entries).
        self._lock: threading.Lock = threading.Lock()

    def report_catalog_api_call(self, api_name: str, elapsed_seconds: float) -> None:
        """Accumulate per-API call count and total latency in seconds."""
        with self._lock:
            num_calls, total_time_secs = self.catalog_api.get(api_name, (0, 0.0))
            self.catalog_api[api_name] = (
                num_calls + 1,
                total_time_secs + elapsed_seconds,
            )

    def report_entry_group(self, entry_group_name: str, filtered: bool) -> None:
        """Report one scanned entry group and its filtering outcome."""
        with self._lock:
            self.entry_groups_seen += 1
            if filtered:
                self.entry_groups_filtered += 1
                self.entry_group_filtered_samples.append(entry_group_name)
                logger.debug(
                    f"Entry group filtered out by entry_groups.pattern: {entry_group_name}"
                )
            else:
                self.entry_groups_processed += 1
                self.entry_group_processed_samples.append(entry_group_name)

    def report_entry(
        self,
        entry_name: str,
        filtered_missing_fqn: bool,
        filtered_fqn: bool,
        filtered_name: bool,
    ) -> None:
        """Report one scanned entry and its filtering outcome."""
        with self._lock:
            self.entries_seen += 1

            if filtered_name:
                self.entries_filtered_by_pattern += 1
                self.entry_pattern_filtered_samples.append(entry_name)
                logger.debug(f"Entry filtered out by entries.pattern: {entry_name}")

            if filtered_missing_fqn:
                self.entries_filtered_by_missing_fqn += 1
                self.entry_missing_fqn_samples.append(entry_name)
                logger.debug(
                    f"Entry filtered out by missing fully_qualified_name: {entry_name}"
                )

            if filtered_fqn:
                self.entries_filtered_by_fqn_pattern += 1
                self.entry_fqn_filtered_samples.append(entry_name)
                logger.debug(f"Entry filtered out by entries.fqn_pattern: {entry_name}")

            if not filtered_name and not filtered_missing_fqn and not filtered_fqn:
                self.entries_processed += 1
                self.entries_processed_samples.append(entry_name)


class DataplexEntriesProcessor:
    """Class-based Dataplex entry processing surface."""

    def __init__(
        self,
        config: DataplexConfig,
        catalog_client: dataplex_v1.CatalogServiceClient,
        report: DataplexEntriesReport,
        source_report: SourceReport,
        ctx: DataplexContext,
    ) -> None:
        self.config = config
        self.catalog_client = catalog_client
        self.report = report
        self.source_report = source_report
        self._ctx = ctx
        self._emitted_project_containers: set[str] = set()
        # Guards the check+add on _emitted_project_containers across parallel workers.
        self._container_lock: threading.Lock = threading.Lock()

    @property
    def entry_data(self) -> List["EntryDataTuple"]:
        return self._ctx.entry_data

    # ------------------------------------------------------------------
    # Parallel entry processing (three-phase)
    # ------------------------------------------------------------------

    def process_entries(
        self, project_ids: List[str], max_workers: int
    ) -> Iterable["Entity"]:
        """Process all entries across configured projects using a thread pool.

        Three-phase execution:

        * Phase 1a (sequential): iterate all project × location pairs and call
          ``list_entry_groups`` + ``list_entries`` to accumulate the minimal set
          of entry names that pass configured filters.  These listing calls are
          fast (paginated, no detail payload) so sequential execution is fine.

        * Phase 1b (parallel): submit all accumulated entry-name stubs to a
          ``ThreadPoolExecutor``.  Each worker calls ``get_entry(view=ALL)``
          (the expensive per-entry RPC) and then builds DataHub entities.
          Distributing across a flat pool avoids the bottleneck of a single
          project with thousands of entries.

        * Phase 1c (sequential): run the Spanner ``search_entries`` workaround
          for each project × location pair.  Search results contain only stub
          data (no aspects), so a separate ``get_entry(view=ALL)`` call is made
          per entry to fetch full detail including schema aspects.
        """
        # Phase 1a: accumulate entry stubs across all projects and locations.
        # Wrap each (project, location) pair individually so one failing project
        # (e.g. PermissionDenied) does not abort listing for the others.
        entry_stubs: List[Tuple[str, str]] = []  # (entry_name, location)
        for project_id in project_ids:
            for location in self.config.entries_locations:
                try:
                    for name in self._list_entry_stubs(project_id, location):
                        entry_stubs.append((name, location))
                except Exception as exc:
                    self.source_report.warning(
                        title="Dataplex entry listing failed",
                        message="Failed to list entries for project/location. Skipping.",
                        context=f"project_id={project_id}, location={location}",
                        exc=exc,
                    )

        # Phase 1b: fetch entry details in parallel and build entities.
        # Submit stubs in bounded batches to cap in-flight futures and prevent
        # O(N) memory growth for large deployments.
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            it = iter(entry_stubs)
            while batch := list(islice(it, WORKERS_BATCH_SIZE)):
                futures = {
                    executor.submit(self._fetch_and_build_entry, name, loc): (name, loc)
                    for name, loc in batch
                }
                for future in as_completed(futures):
                    entry_name, _ = futures[future]
                    try:
                        yield from future.result()
                    except Exception as exc:
                        self.source_report.warning(
                            title="Dataplex entry processing failed",
                            message="Failed to fetch or build entity for entry. Skipping.",
                            context=f"entry_name={entry_name}",
                            exc=exc,
                        )

        # Phase 1c: Spanner entries (sequential — already fully-fetched from search_entries).
        for project_id in project_ids:
            for location in self.config.entries_locations:
                yield from self._process_spanner_entries(project_id, location)

    def _list_entry_stubs(self, project_id: str, location: str) -> List[str]:
        """Return entry names that pass filters for one project/location pair.

        Only performs ``list_entry_groups`` and ``list_entries`` — does NOT
        call ``get_entry``.  Meant to be called sequentially in Phase 1a so
        that the slower ``get_entry`` calls can be parallelised in Phase 1b.
        """
        entry_names: List[str] = []
        for entry_group in self.list_entry_groups(project_id, location):
            logger.info(f"Listing entry group {entry_group.name}")
            logger.info(f"Entry group payload: {entry_group}")
            filtered = not self.should_process_entry_group(entry_group.name)
            self.report.report_entry_group(entry_group.name, filtered=filtered)
            if filtered:
                continue

            request = dataplex_v1.ListEntriesRequest(parent=entry_group.name)
            with PerfTimer() as timer:
                for entry in self.catalog_client.list_entries(request=request):
                    logger.info(
                        f"Listing entry {entry.name} from group {entry_group.name}"
                    )
                    logger.info(f"ListEntries payload: {entry}")
                    if self._report_and_should_process_entry(entry):
                        entry_names.append(entry.name)
                    else:
                        logger.debug(
                            "Skipping entry stub for filtered entry %s from group %s",
                            entry.name,
                            entry_group.name,
                        )
            self.report.report_catalog_api_call("list_entries", timer.elapsed_seconds())
        return entry_names

    def _fetch_entry_detail(self, entry_name: str) -> dataplex_v1.Entry:
        """Fetch full entry detail via ``get_entry(view=ALL)``.

        Safe to call from parallel worker threads — only reads from shared GCP
        client (thread-safe) and reports via the lock-protected report methods.
        Raises on failure; callers should catch via ``future.result()``.
        """
        request = dataplex_v1.GetEntryRequest(
            name=entry_name,
            view=dataplex_v1.EntryView.ALL,
        )
        with PerfTimer() as timer:
            detailed_entry = self.catalog_client.get_entry(request=request)
        self.report.report_catalog_api_call("get_entry", timer.elapsed_seconds())
        logger.debug(f"Detailed entry {detailed_entry}")
        return detailed_entry

    def _build_entities_for_entry(
        self, entry: dataplex_v1.Entry, location: str
    ) -> List["Entity"]:
        """Build DataHub entities from a fully-fetched entry via its type mapper.

        The per-type transformation lives in ``dataplex_mappers``; this method
        only orchestrates the two cross-entry concerns that cannot live in a pure
        mapper: the global project-container dedup and the lineage side-channel.

        Safe to call from parallel worker threads:
        - Uses ``_container_lock`` for the atomic check+add on
          ``_emitted_project_containers``.
        - Uses ``ctx.append_entry`` (thread-safe) for appends to ``ctx.entry_data``.
        - The mapper itself is pure; all shared state accessed here is either
          read-only or already lock-protected.
        """
        mapper = get_entry_mapper(entry.entry_type, self.source_report, entry=entry)
        if mapper is None:
            return []

        result = mapper.map(
            entry,
            EntryMappingContext(
                config=self.config,
                location=location,
                report=self.source_report,
            ),
        )
        if result is None:
            return []

        results: List["Entity"] = []

        # additional_entities (e.g. the owning project container) are global-dedup
        # candidates: emit each unique one exactly once across all entries.
        for extra in result.additional_entities:
            with self._container_lock:
                extra_urn = extra.urn.urn()
                if extra_urn not in self._emitted_project_containers:
                    self._emitted_project_containers.add(extra_urn)
                    results.append(extra)

        if result.main_entity is not None:
            if result.lineage_entry is not None:
                self._ctx.append_entry(result.lineage_entry)
            results.append(result.main_entity)

        return results

    def _fetch_and_build_entry(self, entry_name: str, location: str) -> List["Entity"]:
        """Fetch entry details and build entities. Called from thread pool."""
        return self._build_entities_for_entry(
            self._fetch_entry_detail(entry_name), location
        )

    def _process_spanner_entries(
        self, project_id: str, location: str
    ) -> Iterable["Entity"]:
        """Process Spanner entries via ``search_entries`` workaround (Phase 1c).

        ``search_entries`` returns stub entries without aspects, so a separate
        ``get_entry(view=ALL)`` call is made per entry to fetch full detail
        including schema aspects.
        """
        logger.info(
            f"SearchEntries spanner for project={project_id} location={location}"
        )
        request = dataplex_v1.SearchEntriesRequest(
            name=f"projects/{project_id}/locations/{location}",
            scope=f"projects/{project_id}",
            query="system=cloud_spanner",
        )
        try:
            with PerfTimer() as timer:
                for result in self.catalog_client.search_entries(request=request):
                    logger.info(f"SearchEntries result payload: {result}")
                    dataplex_entry = getattr(result, "dataplex_entry", None)
                    if dataplex_entry is None:
                        continue
                    if not self._report_and_should_process_entry(dataplex_entry):
                        logger.debug(
                            "Skipping filtered spanner entry %s from search_entries",
                            dataplex_entry.name,
                        )
                        continue
                    try:
                        detailed_entry = self._fetch_entry_detail(dataplex_entry.name)
                    except Exception as exc:
                        self.source_report.warning(
                            title="Dataplex Spanner entry fetch failed",
                            message="Failed to fetch detail for a Spanner entry. Skipping.",
                            context=f"entry_name={dataplex_entry.name}",
                            exc=exc,
                        )
                        continue
                    yield from self._build_entities_for_entry(detailed_entry, location)
            self.report.report_catalog_api_call(
                "search_entries", timer.elapsed_seconds()
            )
        except Exception as exc:
            # Surfaced (not just logged) so a permissions gap does not read as a
            # green run with zero Spanner entities.
            self.source_report.warning(
                title="Dataplex Spanner search failed",
                message="search_entries workaround failed for a project/location. Skipping.",
                context=f"project_id={project_id}, location={location}",
                exc=exc,
            )
            return

    def list_entry_groups(
        self, project_id: str, location: str
    ) -> Iterable[dataplex_v1.EntryGroup]:
        """List entry groups for a ``(project_id, location)`` pair."""
        parent = f"projects/{project_id}/locations/{location}"
        request = dataplex_v1.ListEntryGroupsRequest(parent=parent)
        with PerfTimer() as timer:
            response = self.catalog_client.list_entry_groups(request=request)
        self.report.report_catalog_api_call(
            "list_entry_groups", timer.elapsed_seconds()
        )
        return response

    def should_process_entry_group(self, entry_group_name: str) -> bool:
        """Evaluate ``filter_config.entry_groups.pattern``."""
        entry_groups_filter = self.config.filter_config.entry_groups
        return entry_groups_filter.pattern.allowed(entry_group_name)

    def should_process_entry(self, entry: dataplex_v1.Entry) -> bool:
        """Apply entry-level ``pattern`` and ``fqn_pattern`` filters."""
        if not entry.fully_qualified_name:
            return False
        entry_name = entry.name
        return self._entry_name_allowed(entry_name) and self._entry_fqn_allowed(
            entry.fully_qualified_name
        )

    def _report_and_should_process_entry(self, entry: dataplex_v1.Entry) -> bool:
        """Apply entry filters, report counters/samples, and return pass/fail."""
        entry_name = entry.name
        filtered_missing_fqn = not bool(entry.fully_qualified_name)
        if filtered_missing_fqn:
            self.report.report_entry(
                entry_name=entry_name,
                filtered_missing_fqn=True,
                filtered_fqn=False,
                filtered_name=False,
            )
            return False

        assert entry.fully_qualified_name
        filtered_name = not self._entry_name_allowed(entry_name)
        filtered_fqn = not self._entry_fqn_allowed(entry.fully_qualified_name)
        self.report.report_entry(
            entry_name=entry_name,
            filtered_missing_fqn=False,
            filtered_fqn=filtered_fqn,
            filtered_name=filtered_name,
        )
        return not filtered_name and not filtered_fqn

    def _entry_name_allowed(self, entry_name: str) -> bool:
        entries_filter = self.config.filter_config.entries
        return entries_filter.pattern.allowed(entry_name)

    def _entry_fqn_allowed(self, fully_qualified_name: str) -> bool:
        entries_filter = self.config.filter_config.entries
        return entries_filter.fqn_pattern.allowed(fully_qualified_name)
