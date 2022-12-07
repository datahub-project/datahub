from dataclasses import dataclass, field
from typing import Dict

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.stats_collections import TopKDict


@dataclass
class UnityCatalogReport(StaleEntityRemovalSourceReport):
    scanned_metastore: int = 0
    scanned_catalog: int = 0
    scanned_schema: int = 0
    scanned_table: int = 0
    num_catalogs_to_scan: Dict[str, int] = field(default_factory=TopKDict)
    num_schemas_to_scan: Dict[str, int] = field(default_factory=TopKDict)
    num_tables_to_scan: Dict[str, int] = field(default_factory=TopKDict)
    tables_scanned: int = 0
    views_scanned: int = 0
    filtered: LossyList[str] = field(default_factory=LossyList)

    def increment_scanned_metastore(self, count: int = 1) -> None:
        self.scanned_metastore = self.scanned_metastore + count

    def increment_scanned_catalog(self, count: int = 1) -> None:
        self.scanned_catalog = self.scanned_catalog + count

    def increment_scanned_schema(self, count: int = 1) -> None:
        self.scanned_schema = self.scanned_schema + count

    def increment_scanned_table(self, count: int = 1) -> None:
        self.scanned_table = self.scanned_table + count

    def report_dropped(self, ent_name: str) -> None:
        self.filtered.append(ent_name)

    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        """
        Entity could be a view or a table
        """
        if ent_type == "table":
            self.tables_scanned += 1
        elif ent_type == "view":
            self.views_scanned += 1
        else:
            raise KeyError(f"Unknown entity {ent_type}.")
