"""Entry processing utilities for Dataplex source (Universal Catalog/Entries API)."""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import islice
from typing import (
    TYPE_CHECKING,
    Iterable,
    List,
    Optional,
    Tuple,
    cast,
)

from google.cloud import dataplex_v1

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.report import Report
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_context import DataplexContext
from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple
from datahub.ingestion.source.dataplex.dataplex_ids import (
    DATAPLEX_ENTRY_TYPE_MAPPINGS,
    DataplexEntryTypeMapping,
    build_container_key_from_fqn,
    build_parent_container_key,
    build_project_schema_key_from_fqn,
    extract_datahub_dataset_name_from_fqn,
    extract_entry_type_short_name,
    parse_fully_qualified_name,
)
from datahub.ingestion.source.dataplex.dataplex_properties import (
    extract_entry_custom_properties,
)
from datahub.ingestion.source.dataplex.dataplex_schema import (
    extract_graph_schema_from_entry_aspects,
    extract_schema_from_entry_aspects,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.perf_timer import PerfTimer

if TYPE_CHECKING:
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
    def entry_data(self) -> list:
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
                entries = list(self.catalog_client.list_entries(request=request))
            self.report.report_catalog_api_call("list_entries", timer.elapsed_seconds())

            for entry in entries:
                logger.info(f"Listing entry {entry.name} from group {entry_group.name}")
                logger.info(f"ListEntries payload: {entry}")
                if self._report_and_should_process_entry(entry):
                    entry_names.append(entry.name)
                else:
                    logger.debug(
                        "Skipping entry stub for filtered entry %s from group %s",
                        entry.name,
                        entry_group.name,
                    )
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
        """Build DataHub entities from a fully-fetched entry.

        Safe to call from parallel worker threads:
        - Uses ``_container_lock`` for the atomic check+add on
          ``_emitted_project_containers``.
        - Uses ``ctx.append_entry`` (thread-safe) for appends to ``ctx.entry_data``.
        - All other state accessed here (config, report methods) is either
          read-only or already lock-protected.
        """
        results: List["Entity"] = []

        project_container = self.build_project_container_for_entry(entry)
        if project_container is not None:
            with self._container_lock:
                project_container_urn = project_container.urn.urn()
                if project_container_urn not in self._emitted_project_containers:
                    self._emitted_project_containers.add(project_container_urn)
                    results.append(project_container)

        entity = self.build_entity_for_entry(entry)
        if entity is not None:
            self._track_entry_for_lineage(dataplex_location=location, entry=entry)
            results.append(entity)

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
                search_results = list(
                    self.catalog_client.search_entries(request=request)
                )
            self.report.report_catalog_api_call(
                "search_entries", timer.elapsed_seconds()
            )
        except Exception as exc:
            logger.warning(
                f"Spanner workaround search failed for {project_id}/{location}: {exc}"
            )
            return

        for result in search_results:
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
                logger.warning(
                    f"Failed to fetch detail for Spanner entry {dataplex_entry.name}: {exc}"
                )
                continue
            yield from self._build_entities_for_entry(detailed_entry, location)

    def _track_entry_for_lineage(
        self, dataplex_location: str, entry: dataplex_v1.Entry
    ) -> None:
        """Register a dataset entry for lineage extraction.

        Safe to call from parallel worker threads — delegates to
        ``ctx.append_entry`` which is thread-safe.
        """
        if not entry.fully_qualified_name:
            return

        short_name = extract_entry_type_short_name(entry.entry_type)
        if short_name is None:
            self.source_report.warning(
                title="Invalid Dataplex entry type format",
                message="Failed to extract short entry type from Dataplex entry_type. Skipping lineage tracking.",
                context=f"entry_type={entry.entry_type}, entry_name={entry.name}, entry_payload={entry}",
            )
            return
        mapping = DATAPLEX_ENTRY_TYPE_MAPPINGS.get(short_name)
        if mapping is None or mapping.datahub_entity_type != "Dataset":
            return

        dataset_name = extract_datahub_dataset_name_from_fqn(
            entry_type_or_short_name=short_name,
            fully_qualified_name=entry.fully_qualified_name,
        )
        if dataset_name is None:
            return

        entry_data_tuple = EntryDataTuple(
            dataplex_entry_short_name=entry.name.split("/")[-1],
            dataplex_entry_name=entry.name,
            dataplex_location=dataplex_location,
            dataplex_entry_fqn=entry.fully_qualified_name,
            dataplex_entry_type_short_name=short_name,
            datahub_platform=mapping.datahub_platform,
            datahub_dataset_name=dataset_name,
            datahub_dataset_urn=make_dataset_urn_with_platform_instance(
                platform=mapping.datahub_platform,
                name=dataset_name,
                platform_instance=None,
                env=self.config.env,
            ),
        )
        self._ctx.append_entry(entry_data_tuple)

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

    def build_entity_for_entry(self, entry: dataplex_v1.Entry) -> Optional["Entity"]:
        """Map Dataplex entry to DataHub SDK v2 Dataset/Container entity."""
        if not entry.fully_qualified_name:
            logger.debug(
                "Skipping entity build for entry %s: missing fully_qualified_name",
                entry.name,
            )
            return None

        mapping_result = self._resolve_entry_type_mapping(entry)
        if mapping_result is None:
            logger.debug(
                "Skipping entity build for entry %s: unresolved entry type mapping",
                entry.name,
            )
            return None
        short_name, mapping = mapping_result

        entry_name = entry.name
        display_name = self._extract_display_name(entry)
        description = self._extract_description(entry)
        created = self._extract_datetime(entry, "create_time")
        last_modified = self._extract_datetime(entry, "update_time")
        entry_group_id = self._extract_entry_group_id(entry.name)
        custom_properties = extract_entry_custom_properties(
            entry, entry_name, entry_group_id
        )

        if mapping.datahub_entity_type == "Dataset":
            dataset_name = extract_datahub_dataset_name_from_fqn(
                entry_type_or_short_name=short_name,
                fully_qualified_name=entry.fully_qualified_name,
            )
            if dataset_name is None:
                logger.debug(
                    "Skipping dataset entity build for entry %s: unable to extract dataset name from FQN %s",
                    entry.name,
                    entry.fully_qualified_name,
                )
                return None

            schema_metadata = None
            if self.config.include_schema:
                schema_metadata = (
                    extract_schema_from_entry_aspects(
                        entry, entry_name, mapping.datahub_platform
                    )
                    or extract_graph_schema_from_entry_aspects(  # cloud-spanner-graph entries store schema in a graph-schema aspect rather than the standard schema aspect
                        entry, entry_name, mapping.datahub_platform
                    )
                )

            parent_container_key: Optional[ContainerKey] = None
            if (
                mapping.parent_container_key_class is not None
                and mapping.parent_entry_regex is not None
            ):
                if entry.parent_entry:
                    parent_container_key = cast(
                        Optional[ContainerKey],
                        build_parent_container_key(
                            entry_type_or_short_name=short_name,
                            parent_entry=entry.parent_entry,
                        ),
                    )
                else:
                    self.source_report.warning(
                        title="Missing Dataplex parent_entry",
                        message="Dataplex mapping expects parent_entry for parent container derivation. Emitting dataset without parent container.",
                        context=(
                            f"entry_type={entry.entry_type}, "
                            f"entry_name={entry.name}, "
                            f"fully_qualified_name={entry.fully_qualified_name}"
                        ),
                    )
            else:
                parent_container_key = cast(
                    Optional[ContainerKey],
                    build_project_schema_key_from_fqn(
                        entry_type_or_short_name=short_name,
                        fully_qualified_name=entry.fully_qualified_name,
                    ),
                )
            if parent_container_key is not None:
                return Dataset(
                    platform=mapping.datahub_platform,
                    name=dataset_name,
                    env=self.config.env,
                    display_name=display_name,
                    description=description or None,
                    custom_properties=custom_properties,
                    created=created,
                    last_modified=last_modified,
                    subtype=mapping.datahub_subtype,
                    parent_container=parent_container_key,
                    schema=schema_metadata,
                )
            return Dataset(
                platform=mapping.datahub_platform,
                name=dataset_name,
                env=self.config.env,
                display_name=display_name,
                description=description or None,
                custom_properties=custom_properties,
                created=created,
                last_modified=last_modified,
                subtype=mapping.datahub_subtype,
                schema=schema_metadata,
            )

        container_key = build_container_key_from_fqn(
            entry_type_or_short_name=short_name,
            fully_qualified_name=entry.fully_qualified_name,
        )
        if container_key is None:
            logger.debug(
                "Skipping container entity build for entry %s: unable to build container key from FQN %s",
                entry.name,
                entry.fully_qualified_name,
            )
            return None

        # For containers, always derive parent linkage from Dataplex parent_entry.
        container_parent_key: Optional[ContainerKey] = self.build_entry_container_key(
            entry
        )
        if container_parent_key is None:
            project_schema_key = build_project_schema_key_from_fqn(
                entry_type_or_short_name=short_name,
                fully_qualified_name=entry.fully_qualified_name,
            )
            if project_schema_key is not None:
                container_parent_key = project_schema_key

        return Container(
            container_key=container_key,
            display_name=display_name,
            description=description or None,
            created=created,
            last_modified=last_modified,
            parent_container=container_parent_key,
            subtype=mapping.datahub_subtype,
            extra_properties=custom_properties,
        )

    def build_project_container_for_entry(
        self, entry: dataplex_v1.Entry
    ) -> Optional[Container]:
        """Build project-level container for an entry's owning project."""
        if not entry.fully_qualified_name:
            logger.debug(
                "Skipping project container build for entry %s: missing fully_qualified_name",
                entry.name,
            )
            return None

        mapping_result = self._resolve_entry_type_mapping(entry)
        if mapping_result is None:
            logger.debug(
                "Skipping project container build for entry %s: unresolved entry type mapping",
                entry.name,
            )
            return None
        short_name, mapping = mapping_result

        project_schema_key = build_project_schema_key_from_fqn(
            entry_type_or_short_name=short_name,
            fully_qualified_name=entry.fully_qualified_name,
        )
        if project_schema_key is None:
            logger.debug(
                "Skipping project container build for entry %s: unable to build project schema key from FQN %s",
                entry.name,
                entry.fully_qualified_name,
            )
            return None

        identity = parse_fully_qualified_name(short_name, entry.fully_qualified_name)
        if identity is None:
            logger.debug(
                "Skipping project container build for entry %s: unable to parse FQN %s",
                entry.name,
                entry.fully_qualified_name,
            )
            return None
        project_id = identity.get("project_id")
        if project_id is None:
            logger.debug(
                "Skipping project container build for entry %s: parsed identity missing project_id (identity=%s)",
                entry.name,
                identity,
            )
            return None

        return Container(
            container_key=project_schema_key,
            display_name=project_id,
            parent_container=None,
            subtype=DatasetContainerSubTypes.BIGQUERY_PROJECT,
            extra_properties={
                "dataplex_ingested": "true",
                "dataplex_project_id": project_id,
                "dataplex_entry_type": entry.entry_type,
                "dataplex_source_platform": mapping.datahub_platform,
            },
        )

    def build_entry_container_key(
        self, entry: dataplex_v1.Entry
    ) -> Optional[ContainerKey]:
        """Build container-relationship parent key for entry parent."""
        if not entry.parent_entry:
            logger.debug(
                "Skipping parent container key build for entry %s: missing parent_entry",
                entry.name,
            )
            return None
        short_name = extract_entry_type_short_name(entry.entry_type)
        if short_name is None:
            self.source_report.warning(
                title="Invalid Dataplex entry type format",
                message="Failed to extract short entry type from Dataplex entry_type. Skipping parent container link.",
                context=f"entry_type={entry.entry_type}, entry_name={entry.name}, entry_payload={entry}",
            )
            return None
        parent_schema_key = build_parent_container_key(
            entry_type_or_short_name=short_name,
            parent_entry=entry.parent_entry,
        )
        if parent_schema_key is None:
            logger.debug(
                "Skipping parent container key build for entry %s: unable to build parent key from parent_entry %s",
                entry.name,
                entry.parent_entry,
            )
            return None
        return parent_schema_key

    def _entry_name_allowed(self, entry_name: str) -> bool:
        entries_filter = self.config.filter_config.entries
        return entries_filter.pattern.allowed(entry_name)

    def _entry_fqn_allowed(self, fully_qualified_name: str) -> bool:
        entries_filter = self.config.filter_config.entries
        return entries_filter.fqn_pattern.allowed(fully_qualified_name)

    def _extract_entry_group_id(self, entry_name: str) -> str:
        parts = entry_name.split("/")
        try:
            entry_groups_index = parts.index("entryGroups")
            return parts[entry_groups_index + 1]
        except (ValueError, IndexError):
            return "unknown"

    def _extract_display_name(self, entry: dataplex_v1.Entry) -> str:
        if entry.entry_source:
            display_name = getattr(entry.entry_source, "display_name", None)
            if isinstance(display_name, str) and display_name.strip():
                return display_name
        return entry.name.split("/")[-1]

    def _extract_description(self, entry: dataplex_v1.Entry) -> str:
        if entry.entry_source and entry.entry_source.description:
            return entry.entry_source.description
        return ""

    def _extract_datetime(
        self,
        entry: dataplex_v1.Entry,
        field_name: str,
    ) -> Optional[datetime]:
        if not entry.entry_source:
            return None
        value = getattr(entry.entry_source, field_name, None)
        if value is None:
            return None
        return datetime.fromtimestamp(value.timestamp(), tz=timezone.utc)

    def _resolve_entry_type_mapping(
        self, entry: dataplex_v1.Entry
    ) -> Optional[tuple[str, DataplexEntryTypeMapping]]:
        short_name = extract_entry_type_short_name(entry.entry_type)
        if short_name is None:
            self.source_report.warning(
                title="Invalid Dataplex entry type format",
                message="Failed to extract short entry type from Dataplex entry_type. Skipping entry.",
                context=f"entry_type={entry.entry_type}, entry_name={entry.name}, entry_payload={entry}",
            )
            return None

        mapping = DATAPLEX_ENTRY_TYPE_MAPPINGS.get(short_name)
        if mapping is None:
            self.source_report.warning(
                title="Unsupported Dataplex entry type",
                message="Encountered Dataplex entry with unsupported entry_type. Skipping entry.",
                context=f"entry_type={entry.entry_type}, entry_name={entry.name}, entry_payload={entry}",
            )
            return None

        return short_name, mapping
