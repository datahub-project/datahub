"""Entry processing utilities for Dataplex source (Universal Catalog/Entries API)."""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import TYPE_CHECKING, Iterable, Optional, Protocol, cast

from google.cloud import dataplex_v1

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.report import Report
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple
from datahub.ingestion.source.dataplex.dataplex_ids import (
    DATAPLEX_ENTRY_TYPE_MAPPINGS,
    DataplexEntryTypeMapping,
    build_parent_schema_key,
    build_project_schema_key_from_fqn,
    build_schema_key_from_fqn,
    extract_datahub_dataset_name_from_fqn,
    extract_entry_type_short_name,
    parse_fully_qualified_name,
)
from datahub.ingestion.source.dataplex.dataplex_properties import (
    extract_entry_custom_properties,
)
from datahub.ingestion.source.dataplex.dataplex_schema import (
    extract_schema_from_entry_aspects,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.utilities.lossy_collections import LossyList

if TYPE_CHECKING:
    from datahub.sdk.entity import Entity


class DataplexProcessorReport(Protocol):
    def report_entry_group(self, entry_group_name: str, filtered: bool) -> None: ...

    def report_entry(
        self,
        entry_name: str,
        filtered_missing_fqn: bool,
        filtered_fqn: bool,
        filtered_name: bool,
    ) -> None: ...


logger = logging.getLogger(__name__)


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

    entries_seen: int = 0
    entries_filtered_by_pattern: int = 0
    entry_pattern_filtered_samples: LossyList[str] = field(default_factory=LossyList)
    entries_filtered_by_missing_fqn: int = 0
    entry_missing_fqn_samples: LossyList[str] = field(default_factory=LossyList)
    entries_filtered_by_fqn_pattern: int = 0
    entry_fqn_filtered_samples: LossyList[str] = field(default_factory=LossyList)

    entries_processed: int = 0
    entries_processed_samples: LossyList[str] = field(default_factory=LossyList)

    def report_entry_group(self, entry_group_name: str, filtered: bool) -> None:
        """Report one scanned entry group and its filtering outcome."""
        self.entry_groups_seen += 1
        if filtered:
            self.entry_groups_filtered += 1
            self.entry_group_filtered_samples.append(entry_group_name)
            logger.debug(
                f"Entry group filtered out by entry_groups.pattern: {entry_group_name}"
            )

    def report_entry(
        self,
        entry_name: str,
        filtered_missing_fqn: bool,
        filtered_fqn: bool,
        filtered_name: bool,
    ) -> None:
        """Report one scanned entry and its filtering outcome."""
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
        report: DataplexProcessorReport,
        entry_data_by_project: dict[str, set[EntryDataTuple]],
        entry_data_lock: Lock,
        source_report: SourceReport,
    ) -> None:
        self.config = config
        self.catalog_client = catalog_client
        self.report = report
        self.source_report = source_report
        self.entry_data_by_project = entry_data_by_project
        self.entry_data_lock = entry_data_lock
        self._emitted_project_containers: set[str] = set()

    def process_project(self, project_id: str) -> Iterable["Entity"]:
        """Iterate all configured locations for project-level entry processing."""
        for location in self.config.entries_locations:
            yield from self.process_location(project_id, location)

    def process_location(self, project_id: str, location: str) -> Iterable["Entity"]:
        """Process all eligible entry groups and entries for one location."""
        for entry in self.collect_entries(project_id, location):
            entry_name = entry.name
            filtered_missing_fqn = not bool(entry.fully_qualified_name)
            if filtered_missing_fqn:
                self.report.report_entry(
                    entry_name=entry_name,
                    filtered_missing_fqn=True,
                    filtered_fqn=False,
                    filtered_name=False,
                )
                continue
            assert entry.fully_qualified_name
            fqn = entry.fully_qualified_name
            filtered_name = not self._entry_name_allowed(entry_name)
            filtered_fqn = not self._entry_fqn_allowed(fqn)
            self.report.report_entry(
                entry_name=entry_name,
                filtered_missing_fqn=False,
                filtered_fqn=filtered_fqn,
                filtered_name=filtered_name,
            )
            if filtered_name or filtered_missing_fqn or filtered_fqn:
                continue

            project_container = self.build_project_container_for_entry(entry)
            if project_container is not None:
                project_container_urn = project_container.urn.urn()
                if project_container_urn not in self._emitted_project_containers:
                    self._emitted_project_containers.add(project_container_urn)
                    yield project_container

            entity = self.build_entity_for_entry(entry)
            if entity is None:
                continue
            self._track_entry_for_lineage(project_id=project_id, entry=entry)
            yield entity

    def list_entry_groups(
        self, project_id: str, location: str
    ) -> Iterable[dataplex_v1.EntryGroup]:
        """List entry groups for a ``(project_id, location)`` pair."""
        parent = f"projects/{project_id}/locations/{location}"
        request = dataplex_v1.ListEntryGroupsRequest(parent=parent)
        return self.catalog_client.list_entry_groups(request=request)

    def collect_entries(
        self,
        project_id: str,
        location: str,
    ) -> Iterable[dataplex_v1.Entry]:
        """Stream entries from entry groups and Spanner workaround."""

        for entry_group in self.list_entry_groups(project_id, location):
            logger.info(f"Listing entry group {entry_group.name}")
            logger.info(f"Entry group payload: {entry_group}")
            filtered_entry_group = not self.should_process_entry_group(entry_group.name)
            self.report.report_entry_group(
                entry_group.name, filtered=filtered_entry_group
            )
            if filtered_entry_group:
                continue

            request = dataplex_v1.ListEntriesRequest(parent=entry_group.name)
            for entry in self.catalog_client.list_entries(request=request):
                logger.info(f"Listing entry {entry.name} from group {entry_group.name}")
                logger.info(f"ListEntries payload: {entry}")
                try:
                    entry_request = dataplex_v1.GetEntryRequest(
                        name=entry.name,
                        view=dataplex_v1.EntryView.ALL,
                    )
                    yield self.catalog_client.get_entry(request=entry_request)
                except Exception as exc:
                    self.source_report.report_warning(
                        title="Dataplex entry fetch failed",
                        message="Failed to fetch Dataplex entry details. Skipping entry.",
                        context=f"entry_name={entry.name}, entry_payload={entry}",
                        exc=exc,
                    )

        yield from self.collect_spanner_entries(project_id, location)

    def collect_spanner_entries(
        self, project_id: str, location: str
    ) -> Iterable[dataplex_v1.Entry]:
        """Stream Spanner entries via search_entries workaround."""
        request = dataplex_v1.SearchEntriesRequest(
            name=f"projects/{project_id}/locations/{location}",
            scope=f"projects/{project_id}",
            query="system=cloud_spanner",
        )
        try:
            for result in self.catalog_client.search_entries(request=request):
                logger.info(
                    f"SearchEntries result for project={project_id} location={location}"
                )
                logger.debug(f"SearchEntries payload: {result}")
                dataplex_entry = getattr(result, "dataplex_entry", None)
                if dataplex_entry is not None:
                    logger.info(f"Yielding spanner entry {dataplex_entry.name}")
                    logger.debug(f"Spanner dataplex entry payload: {dataplex_entry}")
                    yield dataplex_entry
        except Exception as exc:
            logger.warning(
                f"Spanner workaround search failed for {project_id}/{location}: {exc}"
            )

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

    def build_entity_for_entry(self, entry: dataplex_v1.Entry) -> Optional["Entity"]:
        """Map Dataplex entry to DataHub SDK v2 Dataset/Container entity."""
        if not entry.fully_qualified_name:
            return None

        mapping_result = self._resolve_entry_type_mapping(entry)
        if mapping_result is None:
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
                return None

            schema_metadata = None
            if self.config.include_schema:
                schema_metadata = extract_schema_from_entry_aspects(
                    entry,
                    entry_name,
                    mapping.datahub_platform,
                )

            parent_container_key: Optional[ContainerKey] = None
            if entry.parent_entry:
                parent_container_key = cast(
                    Optional[ContainerKey],
                    build_parent_schema_key(
                        entry_type_or_short_name=short_name,
                        parent_entry=entry.parent_entry,
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

        container_key = build_schema_key_from_fqn(
            entry_type_or_short_name=short_name,
            fully_qualified_name=entry.fully_qualified_name,
        )
        if container_key is None:
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
            return None

        mapping_result = self._resolve_entry_type_mapping(entry)
        if mapping_result is None:
            return None
        short_name, mapping = mapping_result

        project_schema_key = build_project_schema_key_from_fqn(
            entry_type_or_short_name=short_name,
            fully_qualified_name=entry.fully_qualified_name,
        )
        if project_schema_key is None:
            return None

        identity = parse_fully_qualified_name(short_name, entry.fully_qualified_name)
        if identity is None:
            return None
        project_id = identity.get("project_id")
        if project_id is None:
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
            return None
        short_name = extract_entry_type_short_name(entry.entry_type)
        if short_name is None:
            self.source_report.report_warning(
                title="Invalid Dataplex entry type format",
                message="Failed to extract short entry type from Dataplex entry_type. Skipping parent container link.",
                context=f"entry_type={entry.entry_type}, entry_name={entry.name}, entry_payload={entry}",
            )
            return None
        parent_schema_key = build_parent_schema_key(
            entry_type_or_short_name=short_name,
            parent_entry=entry.parent_entry,
        )
        if parent_schema_key is None:
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

    def _track_entry_for_lineage(
        self, project_id: str, entry: dataplex_v1.Entry
    ) -> None:
        if not entry.fully_qualified_name:
            return

        short_name = extract_entry_type_short_name(entry.entry_type)
        if short_name is None:
            self.source_report.report_warning(
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

        with self.entry_data_lock:
            if project_id not in self.entry_data_by_project:
                self.entry_data_by_project[project_id] = set()
            self.entry_data_by_project[project_id].add(
                EntryDataTuple(
                    dataplex_entry_short_name=entry.name.split("/")[-1],
                    dataplex_entry_name=entry.name,
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
            )

    def _resolve_entry_type_mapping(
        self, entry: dataplex_v1.Entry
    ) -> Optional[tuple[str, DataplexEntryTypeMapping]]:
        short_name = extract_entry_type_short_name(entry.entry_type)
        if short_name is None:
            self.source_report.report_warning(
                title="Invalid Dataplex entry type format",
                message="Failed to extract short entry type from Dataplex entry_type. Skipping entry.",
                context=f"entry_type={entry.entry_type}, entry_name={entry.name}, entry_payload={entry}",
            )
            return None

        mapping = DATAPLEX_ENTRY_TYPE_MAPPINGS.get(short_name)
        if mapping is None:
            self.source_report.report_warning(
                title="Unsupported Dataplex entry type",
                message="Encountered Dataplex entry with unsupported entry_type. Skipping entry.",
                context=f"entry_type={entry.entry_type}, entry_name={entry.name}, entry_payload={entry}",
            )
            return None

        return short_name, mapping
