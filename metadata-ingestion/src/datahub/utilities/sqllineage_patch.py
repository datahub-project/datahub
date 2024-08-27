from typing import Optional

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Column
from sqllineage.exceptions import SQLLineageException
from sqllineage.utils.constant import EdgeType


# Patch based on sqllineage v1.3.3
def end_of_query_cleanup_patch(self, holder: SubQueryLineageHolder) -> None:  # type: ignore
    for i, tbl in enumerate(self.tables):
        holder.add_read(tbl)
    self.union_barriers.append((len(self.columns), len(self.tables)))

    for i, (col_barrier, tbl_barrier) in enumerate(self.union_barriers):
        prev_col_barrier, prev_tbl_barrier = (
            (0, 0) if i == 0 else self.union_barriers[i - 1]
        )
        col_grp = self.columns[prev_col_barrier:col_barrier]
        tbl_grp = self.tables[prev_tbl_barrier:tbl_barrier]
        tgt_tbl = None
        if holder.write:
            if len(holder.write) > 1:
                raise SQLLineageException
            tgt_tbl = list(holder.write)[0]
        elif holder.read:
            tgt_tbl = list(holder.read)[0]

        if tgt_tbl:
            for tgt_col in col_grp:
                tgt_col.parent = tgt_tbl
                for src_col in tgt_col.to_source_columns(
                    self._get_alias_mapping_from_table_group(tbl_grp, holder)
                ):
                    if holder.write:
                        holder.add_column_lineage(src_col, tgt_col)
                    else:
                        holder.add_column_lineage(None, tgt_col)


def add_column_lineage_patch(self, src: Optional[Column], tgt: Column) -> None:  # type: ignore
    if tgt and src:
        self.graph.add_edge(src, tgt, type=EdgeType.LINEAGE)
    if tgt:
        self.graph.add_edge(tgt.parent, tgt, type=EdgeType.HAS_COLUMN)
    if src and src.parent is not None:
        # starting NetworkX v2.6, None is not allowed as node, see https://github.com/networkx/networkx/pull/4892
        self.graph.add_edge(src.parent, src, type=EdgeType.HAS_COLUMN)
