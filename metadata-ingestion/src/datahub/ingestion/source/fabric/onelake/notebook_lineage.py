"""Static table-lineage extraction from Fabric notebook source.

Fabric exposes no API that lists the tables a notebook reads or writes: the
Job Scheduler run API only reports status/timing, and Purview's tenant scan is
item-level for non-Power-BI items. So notebook -> table lineage has to be
derived from the notebook source itself (the ``fabricGitSource`` content the
connector already decodes).

This module parses that source for table reads and writes and resolves the
human table references (``lakehouse.schema.table``) against a registry of the
tables actually ingested in the run, which carry GUID-based URNs. References
that do not resolve to an ingested table are reported and dropped rather than
emitted as ghost datasets, matching the usage extractor's philosophy.

ponytail: this is a static-parse baseline. It covers the common Spark SQL and
DataFrame forms, not arbitrary dynamic code (names built at runtime, UDFs,
reflection). The exhaustive path is the Spark execution plan behind each run
(Spark History Server APIs), handled separately.
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)

# Spark is the Fabric notebook engine; use it for both spark.sql strings and
# %%sql cells. Warehouse (T-SQL) notebooks parse acceptably under this dialect
# for table-identifier extraction.
_SQL_DIALECT = "spark"

# DataFrame API writes: df.write.saveAsTable("x") / df.write.insertInto("x").
_DF_WRITE_RE = re.compile(
    r"\.(?:saveAsTable|insertInto)\(\s*[\"']([^\"']+)[\"']",
)
# DataFrame API / catalog reads: spark.read...table("x"), spark.table("x"),
# and the generic .table("x") accessor.
_DF_READ_RE = re.compile(
    r"(?:spark\.read[\w.]*\.table|spark\.table|\.table)\(\s*[\"']([^\"']+)[\"']",
)
# spark.sql("...") / spark.sql('''...''') / spark.sql(f"...") string argument.
# ponytail: a single string literal only; SQL assembled from variables or
# concatenation is not reconstructed.
_SPARK_SQL_RE = re.compile(
    r"spark\.sql\(\s*f?(\"\"\"|'''|\"|')(.*?)\1",
    re.DOTALL,
)
# Fabric cell delimiter in the exported notebook-content file.
_CELL_SPLIT_RE = re.compile(r"^#\s*(?:CELL|MARKDOWN|META)\b.*$", re.MULTILINE)
# A %%sql magic opening a SQL cell, with or without the Databricks "# MAGIC "
# line prefix used in some exports.
_SQL_MAGIC_RE = re.compile(r"^(?:#\s*MAGIC\s*)?%%sql\b", re.MULTILINE | re.IGNORECASE)
_MAGIC_PREFIX_RE = re.compile(r"^#\s*MAGIC\s?", re.MULTILINE)


@dataclass(frozen=True)
class TableRef:
    """A table reference parsed from notebook source.

    ``parts`` holds 1-3 lowercased identifiers (``table``, ``schema.table``, or
    ``item.schema.table``) with quotes/backticks stripped.
    """

    parts: Tuple[str, ...]
    raw: str

    @classmethod
    def from_name(cls, name: str) -> Optional["TableRef"]:
        cleaned = [
            segment.strip().strip('`"[]').lower()
            for segment in name.split(".")
            if segment.strip().strip('`"[]')
        ]
        # Keep at most the last three identifiers (item.schema.table); anything
        # deeper is a catalog/path prefix we cannot map to a Fabric item.
        if not cleaned:
            return None
        return cls(parts=tuple(cleaned[-3:]), raw=name)


@dataclass
class NotebookReferences:
    """Table references parsed from a notebook, split by direction."""

    reads: List[TableRef] = field(default_factory=list)
    writes: List[TableRef] = field(default_factory=list)


def _table_ref_from_exp(table: exp.Table) -> Optional[TableRef]:
    parts = [p.name for p in (table.args.get("catalog"), table.args.get("db")) if p]
    if table.this:
        parts.append(table.name)
    joined = ".".join(part for part in parts if part)
    return TableRef.from_name(joined) if joined else None


def _classify_statement(
    statement: exp.Expression,
) -> Tuple[List[TableRef], List[TableRef]]:
    """Return (reads, writes) for one parsed SQL statement."""
    reads: List[TableRef] = []
    writes: List[TableRef] = []

    write_target: Optional[exp.Table] = None
    if isinstance(
        statement, (exp.Insert, exp.Create, exp.Merge, exp.Update, exp.Delete)
    ):
        this = statement.this
        if isinstance(this, exp.Table):
            write_target = this
        elif isinstance(this, exp.Schema) and isinstance(this.this, exp.Table):
            write_target = this.this

    for table in statement.find_all(exp.Table):
        ref = _table_ref_from_exp(table)
        if ref is None:
            continue
        if write_target is not None and table is write_target:
            writes.append(ref)
        else:
            reads.append(ref)
    return reads, writes


def _extract_sql(sql: str) -> NotebookReferences:
    refs = NotebookReferences()
    try:
        statements = sqlglot.parse(sql, dialect=_SQL_DIALECT)
    except Exception as e:
        logger.debug(f"Could not parse notebook SQL: {type(e).__name__}: {e}")
        return refs
    for statement in statements:
        if not isinstance(statement, exp.Expression):
            continue
        reads, writes = _classify_statement(statement)
        refs.reads.extend(reads)
        refs.writes.extend(writes)
    return refs


def _extract_sql_magic_cells(content: str) -> str:
    """Concatenate the SQL from every %%sql cell in an exported notebook."""
    cells = _CELL_SPLIT_RE.split(content)
    sql_blocks: List[str] = []
    for cell in cells:
        if not _SQL_MAGIC_RE.search(cell):
            continue
        body = _SQL_MAGIC_RE.sub("", cell, count=1)
        body = _MAGIC_PREFIX_RE.sub("", body)
        sql_blocks.append(body)
    return "\n;\n".join(sql_blocks)


def extract_table_references(
    content: Optional[str], language: Optional[str]
) -> NotebookReferences:
    """Parse notebook source for table reads and writes.

    Handles pure-SQL notebooks, ``spark.sql(...)`` string arguments, ``%%sql``
    magic cells, and the common DataFrame read/write accessors.
    """
    refs = NotebookReferences()
    if not content:
        return refs

    if language == "sql":
        refs = _extract_sql(content)
    else:
        for match in _SPARK_SQL_RE.finditer(content):
            sub = _extract_sql(match.group(2))
            refs.reads.extend(sub.reads)
            refs.writes.extend(sub.writes)
        magic_sql = _extract_sql_magic_cells(content)
        if magic_sql:
            sub = _extract_sql(magic_sql)
            refs.reads.extend(sub.reads)
            refs.writes.extend(sub.writes)

    write_names = {match.group(1) for match in _DF_WRITE_RE.finditer(content)}
    for name in write_names:
        ref = TableRef.from_name(name)
        if ref is not None:
            refs.writes.append(ref)
    for match in _DF_READ_RE.finditer(content):
        name = match.group(1)
        if name in write_names:
            continue
        ref = TableRef.from_name(name)
        if ref is not None:
            refs.reads.append(ref)

    return refs


class NotebookTableRegistry:
    """Resolves parsed table references to ingested OneLake dataset URNs.

    Populated as tables and views are emitted. A reference resolves only when it
    maps to exactly one ingested table; bare or ambiguous names are left
    unresolved (and counted) rather than guessed.
    """

    def __init__(self) -> None:
        self._by_full: Dict[Tuple[str, str, str], str] = {}
        self._by_schema_table: Dict[Tuple[str, str], Set[str]] = {}
        self._by_table: Dict[str, Set[str]] = {}

    def register(self, item_name: str, schema: str, table: str, urn: str) -> None:
        item_l, schema_l, table_l = item_name.lower(), schema.lower(), table.lower()
        self._by_full[(item_l, schema_l, table_l)] = urn
        self._by_schema_table.setdefault((schema_l, table_l), set()).add(urn)
        self._by_table.setdefault(table_l, set()).add(urn)

    def resolve(self, ref: TableRef) -> Optional[str]:
        if len(ref.parts) == 3:
            return self._by_full.get(ref.parts)  # type: ignore[arg-type]
        if len(ref.parts) == 2:
            return self._single(self._by_schema_table.get(ref.parts))  # type: ignore[arg-type]
        if len(ref.parts) == 1:
            return self._single(self._by_table.get(ref.parts[0]))
        return None

    @staticmethod
    def _single(urns: Optional[Set[str]]) -> Optional[str]:
        # Only resolve an unqualified reference when it is unambiguous.
        if urns and len(urns) == 1:
            return next(iter(urns))
        return None


@dataclass
class ResolvedLineage:
    """Resolved notebook lineage: input/output dataset URNs plus unresolved count."""

    input_urns: List[str] = field(default_factory=list)
    output_urns: List[str] = field(default_factory=list)
    unresolved: int = 0


def resolve_lineage(
    refs: NotebookReferences, registry: NotebookTableRegistry
) -> ResolvedLineage:
    """Resolve parsed references against the registry, de-duplicating URNs."""
    result = ResolvedLineage()
    inputs: Set[str] = set()
    outputs: Set[str] = set()
    for ref in refs.reads:
        urn = registry.resolve(ref)
        if urn is None:
            result.unresolved += 1
        else:
            inputs.add(urn)
    for ref in refs.writes:
        urn = registry.resolve(ref)
        if urn is None:
            result.unresolved += 1
        else:
            outputs.add(urn)
    # A table both read and written (e.g. MERGE) is an input and an output.
    result.input_urns = sorted(inputs)
    result.output_urns = sorted(outputs)
    return result
