"""Entry processing utilities for Dataplex source (Universal Catalog/Entries API)."""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import TYPE_CHECKING, Iterable, Optional

from google.cloud import dataplex_v1

from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple
from datahub.ingestion.source.dataplex.dataplex_ids import (
    DATAPLEX_ENTRY_TYPE_MAPPINGS,
    build_parent_container_urn,
    build_parent_schema_key,
    build_schema_key_from_fqn,
    extract_entry_type_short_name,
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

logger = logging.getLogger(__name__)


@dataclass
class DataplexEntriesReport:
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

        if filtered_missing_fqn:
            self.entries_filtered_by_missing_fqn += 1
            self.entry_missing_fqn_samples.append(entry_name)

        if filtered_fqn:
            self.entries_filtered_by_fqn_pattern += 1
            self.entry_fqn_filtered_samples.append(entry_name)

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
        entry_data_by_project: dict[str, set[EntryDataTuple]],
        entry_data_lock: Lock,
    ) -> None:
        self.config = config
        self.catalog_client = catalog_client
        self.report = report
        self.entry_data_by_project = entry_data_by_project
        self.entry_data_lock = entry_data_lock

    def process_project(self, project_id: str) -> Iterable["Entity"]:
        """Iterate all configured locations for project-level entry processing."""
        entries_locations = getattr(self.config, "entries_locations", None)
        if entries_locations is None:
            entries_locations = [self.config.entries_location]

        for location in entries_locations:
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
            logger.info("Listing entry group %s", entry_group.name)
            logger.debug("Entry group payload: %s", entry_group)
            filtered_entry_group = not self.should_process_entry_group(entry_group.name)
            self.report.report_entry_group(
                entry_group.name, filtered=filtered_entry_group
            )
            if filtered_entry_group:
                continue

            request = dataplex_v1.ListEntriesRequest(parent=entry_group.name)
            for entry in self.catalog_client.list_entries(request=request):
                logger.info(
                    "Listing entry %s from group %s", entry.name, entry_group.name
                )
                logger.debug("ListEntries payload: %s", entry)
                try:
                    entry_request = dataplex_v1.GetEntryRequest(
                        name=entry.name,
                        view=dataplex_v1.EntryView.ALL,
                    )
                    yield self.catalog_client.get_entry(request=entry_request)
                except Exception as exc:
                    logger.warning(
                        "Failed to fetch entry details for %s: %s",
                        entry.name,
                        exc,
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
                    "SearchEntries result for project=%s location=%s",
                    project_id,
                    location,
                )
                logger.debug("SearchEntries payload: %s", result)
                dataplex_entry = getattr(result, "dataplex_entry", None)
                if dataplex_entry is not None:
                    logger.info("Yielding spanner entry %s", dataplex_entry.name)
                    logger.debug("Spanner dataplex entry payload: %s", dataplex_entry)
                    yield dataplex_entry
        except Exception as exc:
            logger.warning(
                "Spanner workaround search failed for %s/%s: %s",
                project_id,
                location,
                exc,
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

        short_name = extract_entry_type_short_name(entry.entry_type) or entry.entry_type
        mapping = DATAPLEX_ENTRY_TYPE_MAPPINGS.get(short_name)
        if mapping is None:
            logger.warning(
                "Unsupported Dataplex entry_type=%s for entry=%s, skipping",
                entry.entry_type,
                entry.name,
            )
            return None

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
            dataset_name = self._extract_dataset_name_from_fqn(
                entry.fully_qualified_name
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

            parent_container_urn = None
            if entry.parent_entry:
                parent_container_urn = build_parent_container_urn(
                    entry_type_or_short_name=short_name,
                    parent_entry=entry.parent_entry,
                )
            if parent_container_urn is not None:
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
                    parent_container=[parent_container_urn],
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

        return Container(
            container_key=container_key,
            display_name=display_name,
            description=description or None,
            created=created,
            last_modified=last_modified,
            subtype=mapping.datahub_subtype,
            extra_properties=custom_properties,
        )

    def build_entry_container_urn(self, entry: dataplex_v1.Entry) -> Optional[str]:
        """Build container-relationship URN for entry parent."""
        if not entry.parent_entry:
            return None
        short_name = extract_entry_type_short_name(entry.entry_type) or entry.entry_type
        parent_schema_key = build_parent_schema_key(
            entry_type_or_short_name=short_name,
            parent_entry=entry.parent_entry,
        )
        if parent_schema_key is None:
            return None
        return parent_schema_key.as_urn()

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
        if entry.entry_source and entry.entry_source.display_name:
            return entry.entry_source.display_name
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

    def _extract_dataset_name_from_fqn(
        self, fully_qualified_name: str
    ) -> Optional[str]:
        if ":" not in fully_qualified_name:
            return None
        _, dataset_name = fully_qualified_name.split(":", 1)
        return dataset_name

    def _track_entry_for_lineage(
        self, project_id: str, entry: dataplex_v1.Entry
    ) -> None:
        if not entry.fully_qualified_name:
            return

        short_name = extract_entry_type_short_name(entry.entry_type) or entry.entry_type
        mapping = DATAPLEX_ENTRY_TYPE_MAPPINGS.get(short_name)
        if mapping is None or mapping.datahub_entity_type != "Dataset":
            return

        dataset_name = self._extract_dataset_name_from_fqn(entry.fully_qualified_name)
        if dataset_name is None:
            return

        entry_id = entry.name.split("/")[-1]
        with self.entry_data_lock:
            if project_id not in self.entry_data_by_project:
                self.entry_data_by_project[project_id] = set()
            self.entry_data_by_project[project_id].add(
                EntryDataTuple(
                    entry_id=entry_id,
                    source_platform=mapping.datahub_platform,
                    dataset_id=dataset_name,
                )
            )
