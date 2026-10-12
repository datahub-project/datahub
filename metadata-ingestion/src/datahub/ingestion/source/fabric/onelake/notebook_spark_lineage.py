"""Derive notebook table lineage from executed Spark physical plans.

Static source parsing (``notebook_lineage.py``) cannot resolve lineage when a
notebook reads and writes paths that are supplied as run-time parameters, e.g.
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
from dataclasses import dataclass, field
from typing import Iterable, Optional, Set

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

    def to_urn(self, env: str, platform_instance: Optional[str]) -> str:
        return make_onelake_urn(
            workspace_id=self.workspace_id,
            item_id=self.item_id,
            table_name=self.table,
            schema_name=self.schema,
            env=env,
            platform_instance=platform_instance,
        )


@dataclass
class SparkIO:
    """Resolved input/output table refs across a notebook run's Spark plans."""

    inputs: Set[SparkTableRef] = field(default_factory=set)
    outputs: Set[SparkTableRef] = field(default_factory=set)

    def is_empty(self) -> bool:
        return not self.inputs and not self.outputs


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


def extract_spark_io(plan_descriptions: Iterable[str]) -> SparkIO:
    """Extract read/write OneLake table refs from Spark plan descriptions.

    Writes are identified by the ``path=<abfss...>`` argument that datasource
    write commands emit; every other OneLake ``/Tables/`` path is treated as a
    read. A table that is both read and written in the same run (a delta write
    reads the target's own transaction log) is classed as an output only.

    ponytail: this is a plan-text heuristic, not a parse of the plan node tree.
    Its ceiling is write commands that do not expose the sink via ``path=``
    (e.g. catalog-table writes); the upgrade path is to walk the structured
    ``nodes``/``edges`` the SQL API returns alongside ``planDescription``.
    """
    io = SparkIO()
    for plan in plan_descriptions:
        if not plan:
            continue
        outputs: Set[SparkTableRef] = set()
        for m in _WRITE_PATH_RE.finditer(plan):
            abfss = _ABFSS_RE.match(m.group("path"))
            if abfss is None or abfss.group("host") != _ONELAKE_HOST:
                continue
            ref = _ref_from_path(abfss.group("workspace"), abfss.group("path"))
            if ref is not None:
                outputs.add(ref)

        all_refs: Set[SparkTableRef] = set()
        for m in _ABFSS_RE.finditer(plan):
            if m.group("host") != _ONELAKE_HOST:
                continue
            ref = _ref_from_path(m.group("workspace"), m.group("path"))
            if ref is not None:
                all_refs.add(ref)

        io.outputs |= outputs
        io.inputs |= all_refs - outputs

    # A target read for its own delta log is not an input.
    io.inputs -= io.outputs
    return io
