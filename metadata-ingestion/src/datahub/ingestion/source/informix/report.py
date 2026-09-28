from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class InformixSourceReport(StaleEntityRemovalSourceReport):
    # Tables and views that passed every filter, so "selected but not ingested"
    # is distinguishable from "nothing matched" -- see source.get_workunits_internal.
    objects_selected: int = 0
    tables_scanned: int = 0
    views_scanned: int = 0
    filtered: LossyList[str] = field(default_factory=LossyList)
    row_counts_emitted: int = 0
    views_with_lineage: int = 0
    views_without_definition: int = 0
    view_lineage_failures: int = 0
    # A view whose sources resolved but whose column lineage did not parse, so only
    # table-level lineage was emitted -- see lineage.build_view_upstream_lineage.
    view_column_lineage_failures: int = 0
    # Foreign keys dropped because the catalog returned a different number of child
    # and parent index columns, leaving the pairing ambiguous -- see
    # source._build_table_schema.
    foreign_keys_dropped_mismatched: int = 0
    # Informix rewrites a view's projection list, so sqlglot's downstream column
    # names are remapped positionally onto the view's declared columns. A count
    # mismatch means that remap was skipped and the raw inner projection names
    # were emitted instead -- see lineage.build_view_upstream_lineage.
    view_column_remap_mismatches: int = 0

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)
