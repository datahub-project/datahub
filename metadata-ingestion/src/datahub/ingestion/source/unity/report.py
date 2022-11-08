from dataclasses import dataclass, field
from typing import Dict

from datahub.ingestion.source.sql.sql_common import SQLSourceReport
from datahub.utilities.stats_collections import TopKDict


@dataclass
class UnityCatalogReport(SQLSourceReport):
    scanned_metastore: int = 0
    scanned_catalog: int = 0
    scanned_schema: int = 0
    scanned_table: int = 0
    num_catalogs_to_scan: Dict[str, int] = field(default_factory=TopKDict)
    num_schemas_to_scan: Dict[str, int] = field(default_factory=TopKDict)
    num_tables_to_scan: Dict[str, int] = field(default_factory=TopKDict)

    def increment_scanned_metastore(self, count: int = 1) -> None:
        self.scanned_metastore = self.scanned_metastore + count

    def increment_scanned_catalog(self, count: int = 1) -> None:
        self.scanned_catalog = self.scanned_catalog + count

    def increment_scanned_schema(self, count: int = 1) -> None:
        self.scanned_schema = self.scanned_schema + count

    def increment_scanned_table(self, count: int = 1) -> None:
        self.scanned_table = self.scanned_table + count
