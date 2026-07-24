from dataclasses import dataclass, field
from typing import Set

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class ZiplineSourceReport(StaleEntityRemovalSourceReport):
    feature_tables_scanned: int = 0
    features_scanned: int = 0
    primary_keys_scanned: int = 0
    joins_scanned: int = 0
    staging_queries_scanned: int = 0

    teams_scanned: int = 0

    filtered_teams: int = 0
    filtered_feature_tables: int = 0
    filtered_joins: int = 0
    filtered_staging_queries: int = 0

    join_sources_skipped: int = 0
    unresolved_join_part_inlets: int = 0
    unparseable_files: LossyList[str] = field(default_factory=LossyList)
    unmapped_source_namespaces: Set[str] = field(default_factory=set)

    sql_lineage_parsed: int = 0
    sql_lineage_failures: int = 0

    def report_feature_table_scanned(self) -> None:
        self.feature_tables_scanned += 1

    def report_feature_scanned(self) -> None:
        self.features_scanned += 1

    def report_primary_key_scanned(self) -> None:
        self.primary_keys_scanned += 1

    def report_join_scanned(self) -> None:
        self.joins_scanned += 1

    def report_staging_query_scanned(self) -> None:
        self.staging_queries_scanned += 1

    def report_unparseable_file(self, path: str) -> None:
        self.unparseable_files.append(path)

    def report_unmapped_namespace(self, namespace: str) -> None:
        # An unmapped namespace falls back to default_source_platform, so its URN
        # may point at a non-existent dataset and fail to stitch to the upstream
        # connector. Warn once per namespace so the gap is actionable.
        if namespace in self.unmapped_source_namespaces:
            return
        self.unmapped_source_namespaces.add(namespace)
        self.warning(
            title="Unmapped source namespace",
            message=(
                "Table namespace is not in source_platform_map; used "
                "default_source_platform. Add it to source_platform_map so URNs "
                "resolve to the correct platform and lineage stitches."
            ),
            context=namespace,
        )
