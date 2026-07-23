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

    source_datasets_emitted: int = 0
    teams_scanned: int = 0

    filtered_teams: int = 0
    filtered_feature_tables: int = 0

    join_sources_skipped: int = 0
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
        self.unmapped_source_namespaces.add(namespace)
