"""Derive notebook table lineage from executed Spark physical plans.

Fabric exposes no notebook lineage API, and the notebook source text hides the
tables it touches when paths are supplied as run-time parameters, e.g.
``spark.read.format("delta").load(f"{source}{table}")`` or
``DeltaTable.forPath(spark, target)``. The concrete OneLake paths only exist
once the notebook runs.

The Fabric Spark monitoring API exposes the Spark History Server for each
notebook run. Its SQL execution plans record the *resolved* ``abfss://`` paths
that were actually read and written, which is exactly the lineage the source
text hides. This module turns those plan descriptions into OneLake dataset
URNs.
"""

import logging
import re
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, Optional, Set

from datahub.ingestion.source.fabric.common.urn_generator import make_onelake_urn

logger = logging.getLogger(__name__)

# Only OneLake table paths map to datasets this connector ingests. Internal
# Spark scratch paths (shuffle, lakehouse-warmup, app logs) live on the
# capacity's own storage account host and are ignored.
_ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"

# abfss://{workspaceId}@{host}/{itemId}/Tables/{schema}/{table}/...
# The workspace id is the storage container; the lakehouse/warehouse GUID is the
# first path segment. Capturing stops before the per-table structural suffix
# (`/_delta_log`, partition dirs, data files) handled by _table_segments().
_ABFSS_RE = re.compile(
    r"abfss://(?P<workspace>[0-9a-fA-F-]+)@(?P<host>[^/]+)/(?P<path>[^\s,\]\)]+)"
)

# A plan node whose Arguments expose the write target via `path=<abfss...>`.
# Delta/Parquet/datasource writes all surface the sink this way; reads surface
# their location through FileScan/Scan nodes without a `path=` key.
_WRITE_PATH_RE = re.compile(r"path=(?P<path>abfss://[^\s,\]\)]+)")

# Structural suffixes that are not part of the logical table name.
_NON_TABLE_SEGMENT_RE = re.compile(
    r"^(_delta_log$|_temporary$|part-|.*=.*|.*\.(parquet|json|csv|snappy))"
)


@dataclass(frozen=True)
class SparkTableRef:
    """A OneLake table identity extracted from a resolved abfss path.

    Mirrors the GUID-based dataset naming the connector uses
    (``{workspaceGUID}.{itemGUID}.{schema}.{table}``), so a ref maps straight
    to the dataset URN of an already-ingested table.
    """

    workspace_id: str
    item_id: str
    table: str
    schema: Optional[str]

    def to_urn(
        self,
        env: str,
        platform_instance: Optional[str],
        normalize: Callable[[str], str] = lambda name: name,
    ) -> str:
        # The schema/table names must be normalized the same way the source
        # normalizes ingested table names (see FabricOneLakeSource._norm), or a
        # ref built from a Spark path will not match the registered dataset URN
        # when convert_urns_to_lowercase is on (e.g. a plan's `dim_User` vs the
        # ingested `dim_user`), silently dropping the lineage edge.
        return make_onelake_urn(
            workspace_id=self.workspace_id,
            item_id=self.item_id,
            table_name=normalize(self.table),
            schema_name=normalize(self.schema) if self.schema else None,
            env=env,
            platform_instance=platform_instance,
        )


def _ref_from_path(workspace_id: str, path: str) -> Optional[SparkTableRef]:
    """Parse ``{itemId}/Tables/{schema}/{table}/...`` into a table ref.

    Returns None when the path is not an OneLake ``/Tables/`` path (e.g. a
    ``/Files/`` path, which this connector does not model as a dataset).
    """
    parts = path.strip("/").split("/")
    if "Tables" not in parts:
        return None
    item_id = parts[0]
    tables_idx = parts.index("Tables")
    # Drop the structural suffix (_delta_log, partition dirs, data files).
    logical = []
    for seg in parts[tables_idx + 1 :]:
        if _NON_TABLE_SEGMENT_RE.match(seg):
            break
        logical.append(seg)
    if not logical:
        return None
    # OneLake tables are at most schema/table deep; a single segment is a
    # schemaless lakehouse table.
    schema = logical[0] if len(logical) >= 2 else None
    table = logical[1] if len(logical) >= 2 else logical[0]
    return SparkTableRef(
        workspace_id=workspace_id,
        item_id=item_id,
        table=table,
        schema=schema,
    )


def _plan_outputs(plan: str) -> Set[SparkTableRef]:
    """OneLake tables written by a plan, via the ``path=<abfss...>`` sink arg."""
    outputs: Set[SparkTableRef] = set()
    for m in _WRITE_PATH_RE.finditer(plan):
        abfss = _ABFSS_RE.match(m.group("path"))
        if abfss is None or abfss.group("host") != _ONELAKE_HOST:
            continue
        ref = _ref_from_path(abfss.group("workspace"), abfss.group("path"))
        if ref is not None:
            outputs.add(ref)
    return outputs


def _plan_table_refs(plan: str) -> Set[SparkTableRef]:
    """Every OneLake table a plan references, read or written."""
    refs: Set[SparkTableRef] = set()
    for m in _ABFSS_RE.finditer(plan):
        if m.group("host") != _ONELAKE_HOST:
            continue
        ref = _ref_from_path(m.group("workspace"), m.group("path"))
        if ref is not None:
            refs.add(ref)
    return refs


def extract_spark_lineage(
    plan_descriptions: Iterable[str],
) -> Dict[SparkTableRef, Set[SparkTableRef]]:
    """Map each written OneLake table to the tables that fed it, within a run.

    ``plan_descriptions`` are one Spark session's SQL executions in chronological
    order. A notebook commonly reads a table in one execution
    (``spark.read.table(A)``) and writes a derived table in a later execution
    whose plan references an in-memory/cached relation rather than ``A``'s path,
    so grouping strictly per execution would drop the ``A -> X`` edge. Instead
    attribution is order-aware: a write is credited with the OneLake tables read
    earlier in the same run plus any scans in its own plan, and the pending-read
    set is reset after each write. This keeps independent, sequential pipelines
    precise (``A -> X`` then ``B -> Y`` do not cross-contaminate) while still
    recovering read-then-write lineage that spans executions.

    A table both read and written by the same write (a delta write reads its own
    transaction log) is treated as an output only, never its own input. Writes
    are identified by the ``path=<abfss...>`` argument that datasource write
    commands emit; every other OneLake ``/Tables/`` path is a read. Writes that
    do not expose their sink via ``path=`` (e.g. catalog-table writes) are
    therefore not detected as outputs.
    """
    lineage: Dict[SparkTableRef, Set[SparkTableRef]] = {}
    pending_reads: Set[SparkTableRef] = set()
    for plan in plan_descriptions:
        if not plan:
            continue
        outputs = _plan_outputs(plan)
        reads = _plan_table_refs(plan) - outputs
        if not outputs:
            pending_reads |= reads
            continue
        inputs = pending_reads | reads
        for output in outputs:
            lineage.setdefault(output, set()).update(inputs - {output})
        pending_reads = set()
    return lineage
