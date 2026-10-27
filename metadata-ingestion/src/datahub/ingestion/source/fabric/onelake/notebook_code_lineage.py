"""Narrow Spark-plan lineage using the notebook source.

A Spark plan credits every table read earlier in the run to the next write.
Notebooks often load many tables up front and then write a result that uses
only some of them, so the plan reports sources that do not feed that output.

The source text names those tables even when the path is built at run time
(``f"{source}orders"``), and it shows which dataframe each write actually uses.
Commented-out cells are not real reads or writes. This module recovers that
per-target source set, and the column renames that dataframe applies:
``col(src).alias(dst)``, a ``{source: target}`` mapping dict, ``alias.*``,
and ``df["column"]``. The caller keeps an edge only when the executed plans
also saw the same source and target, so the resolved OneLake path still comes
from the run.
"""

from __future__ import annotations

import ast
import copy
import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple, Union

from datahub.ingestion.source.fabric.onelake.notebook_spark_lineage import (
    SparkTableRef,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CodeTable:
    """A table named by the notebook source, before a run resolves its path."""

    table: str
    schema: Optional[str] = None


# downstream column -> {(upstream table, upstream column)}
ColumnLineage = Dict[str, Set[Tuple[CodeTable, str]]]


@dataclass
class NotebookCodeAnalysis:
    """Table edges and column renames recovered from one notebook."""

    tables: Dict[CodeTable, Set[CodeTable]]
    columns: Dict[CodeTable, ColumnLineage]


def analyze_notebook_code(
    source: str, *, case_sensitive_columns: bool = False
) -> Optional[NotebookCodeAnalysis]:
    """Recover per-write table lineage and column renames from notebook source.

    Returns None when ``source`` is not parseable Python, so the caller can
    keep the Spark-plan lineage unchanged. Empty mappings mean the code parsed
    and contains no writes. Column names match without regard to case unless
    ``case_sensitive_columns`` is set, and the dataframe's own spelling is kept.
    """
    try:
        tree = ast.parse(_strip_magics(source))
    except SyntaxError:
        logger.debug("Notebook source is not parseable Python; skipping code lineage")
        return None

    extractor = _Extractor(case_sensitive_columns=case_sensitive_columns)
    extractor.walk(tree.body)
    columns: Dict[CodeTable, ColumnLineage] = {}
    for target, mapping in extractor.column_lineage.items():
        kept = {
            column: {(ref.table, ref.column) for ref in refs}
            for column, refs in mapping.items()
            if refs
        }
        if kept:
            columns[target] = kept
    return NotebookCodeAnalysis(tables=extractor.lineage, columns=columns)


def extract_notebook_code_lineage(
    source: str,
) -> Optional[Dict[CodeTable, Set[CodeTable]]]:
    """Map each table the notebook writes to the tables that write uses.

    Returns None when ``source`` is not parseable Python, so the caller can
    keep the Spark-plan lineage unchanged. An empty dict means the code parsed
    and contains no writes.
    """
    analysis = analyze_notebook_code(source)
    if analysis is None:
        return None
    return analysis.tables


def reconcile_spark_lineage(
    code_lineage: Dict[CodeTable, Set[CodeTable]],
    execution: Dict[SparkTableRef, Set[SparkTableRef]],
    normalize: Callable[[str], str],
) -> Dict[SparkTableRef, Set[SparkTableRef]]:
    """Keep code edges whose source and target both appear in the executed plans.

    Targets the current code does not write are dropped, and so are sources the
    code does not use for that target. A table name is resolved to the
    ``SparkTableRef`` the plans recorded, which carries the workspace and
    lakehouse the source text never spells out.
    """
    reads: List[SparkTableRef] = []
    writes: List[SparkTableRef] = []
    for output, inputs in execution.items():
        writes.append(output)
        reads.extend(inputs)
    # An intermediate table written by this notebook can be a later job's input.
    sources_pool = reads + writes

    reconciled: Dict[SparkTableRef, Set[SparkTableRef]] = {}
    for target, sources in code_lineage.items():
        outputs = _resolve(target, writes, normalize)
        if not outputs:
            continue
        # A write whose dataframe lineage was not visible keeps the plan's own
        # inputs. An empty source set must not wipe a real edge.
        if not sources:
            for output in outputs:
                reconciled.setdefault(output, set()).update(execution.get(output, ()))
            continue
        resolved_sources: List[SparkTableRef] = []
        for source in sources:
            resolved_sources.extend(_resolve(source, sources_pool, normalize))
        for output in outputs:
            inputs = reconciled.setdefault(output, set())
            inputs.update(ref for ref in resolved_sources if ref != output)
    return reconciled


def reconcile_column_lineage(
    columns: Dict[CodeTable, ColumnLineage],
    execution: Dict[SparkTableRef, Set[SparkTableRef]],
    normalize: Callable[[str], str],
) -> Dict[SparkTableRef, Dict[str, Set[Tuple[SparkTableRef, str]]]]:
    """Attach code column renames to the Spark refs the plans resolved.

    A rename is kept only when both tables appear in that write's executed
    inputs. The column names stay as the notebook wrote them.
    """
    reconciled: Dict[SparkTableRef, Dict[str, Set[Tuple[SparkTableRef, str]]]] = {}
    for target, mapping in columns.items():
        outputs = _resolve(target, execution, normalize)
        for output in outputs:
            inputs = execution.get(output, set())
            colmap = reconciled.setdefault(output, {})
            for downstream, upstreams in mapping.items():
                resolved: Set[Tuple[SparkTableRef, str]] = set()
                for code_table, column in upstreams:
                    for ref in _resolve(code_table, inputs, normalize):
                        if ref != output and column:
                            resolved.add((ref, column))
                if resolved:
                    colmap.setdefault(downstream, set()).update(resolved)
    return reconciled


def _resolve(
    code: CodeTable,
    pool: Iterable[SparkTableRef],
    normalize: Callable[[str], str],
) -> List[SparkTableRef]:
    table_hits = [ref for ref in pool if normalize(ref.table) == normalize(code.table)]
    if code.schema is not None:
        schema_hits = [
            ref
            for ref in table_hits
            if ref.schema is not None
            and normalize(ref.schema) == normalize(code.schema)
        ]
        if schema_hits:
            table_hits = schema_hits
    unique: List[SparkTableRef] = []
    seen: Set[SparkTableRef] = set()
    for ref in table_hits:
        if ref not in seen:
            seen.add(ref)
            unique.append(ref)
    return unique


def _strip_magics(source: str) -> str:
    """Drop notebook magics so ``ast.parse`` can read the rest of the cell."""
    kept: List[str] = []
    for line in source.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("%") or stripped.startswith("!"):
            kept.append("")
        else:
            kept.append(line)
    return "\n".join(kept)


def _code_table(text: str) -> Optional[CodeTable]:
    parts = [part for part in text.strip().strip("`").split("/") if part]
    if not parts:
        return None
    if "Tables" in parts:
        after = parts[parts.index("Tables") + 1 :]
        if len(after) >= 2:
            return CodeTable(table=after[-1], schema=after[-2])
        if len(after) == 1:
            return CodeTable(table=after[0])
        return None
    if len(parts) == 1 and "/" not in text:
        dotted = [part for part in parts[0].split(".") if part]
        if len(dotted) >= 2:
            return CodeTable(table=dotted[-1], schema=dotted[-2])
        if dotted:
            return CodeTable(table=dotted[0])
    return CodeTable(table=parts[-1])


def _eval_tail(expr: ast.AST, env: Dict[str, str]) -> Optional[str]:
    """Known suffix of a string expression.

    An unknown interpolation resets the suffix. ``f"{runtime}/Tables/silver/orders"``
    therefore still ends in the table name.
    """
    if isinstance(expr, ast.Constant):
        return expr.value if isinstance(expr.value, str) else None
    if isinstance(expr, ast.Name):
        return env.get(expr.id)
    if isinstance(expr, ast.JoinedStr):
        acc = ""
        for part in expr.values:
            if isinstance(part, ast.Constant) and isinstance(part.value, str):
                acc += part.value
            elif isinstance(part, ast.FormattedValue):
                inner = _eval_tail(part.value, env)
                if inner is None:
                    acc = ""
                else:
                    acc += inner
            else:
                acc = ""
        return acc
    if isinstance(expr, ast.BinOp) and isinstance(expr.op, ast.Add):
        right = _eval_tail(expr.right, env)
        if right is None:
            return None
        left = _eval_tail(expr.left, env)
        return right if left is None else left + right
    return None


def _code_table_from_expr(expr: ast.expr, env: Dict[str, str]) -> Optional[CodeTable]:
    tail = _eval_tail(expr, env)
    if tail is None or not tail.strip():
        return None
    return _code_table(tail)


def _chain_has(expr: ast.AST, attr: str) -> bool:
    seen: Set[int] = set()
    current: Optional[ast.AST] = expr
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, ast.Attribute):
            if current.attr == attr:
                return True
            current = current.value
        elif isinstance(current, ast.Call):
            current = current.func
        else:
            return False
    return False


def _root_name(expr: Optional[ast.AST]) -> Optional[str]:
    current = expr
    while isinstance(current, (ast.Attribute, ast.Subscript, ast.Call)):
        if isinstance(current, (ast.Attribute, ast.Subscript)):
            current = current.value
        elif isinstance(current.func, ast.Attribute):
            current = current.func.value
        else:
            return None
    if isinstance(current, ast.Name):
        return current.id
    return None


def _write_receiver(expr: ast.AST) -> Optional[ast.expr]:
    """Dataframe expression under a ``.write`` attribute."""
    current: ast.AST = expr
    while isinstance(current, (ast.Attribute, ast.Call)):
        if isinstance(current, ast.Attribute):
            if current.attr == "write":
                return current.value
            current = current.value
        else:
            current = current.func
    return None


def _read_table(call: ast.Call, env: Dict[str, str]) -> Optional[CodeTable]:
    func = call.func
    if not isinstance(func, ast.Attribute) or func.attr not in {
        "load",
        "table",
        "csv",
        "parquet",
        "json",
        "orc",
    }:
        return None
    if not _chain_has(func.value, "read"):
        return None
    if not call.args:
        return None
    return _code_table_from_expr(call.args[0], env)


def _same_table(left: CodeTable, right: CodeTable) -> bool:
    if left.table != right.table:
        return False
    if left.schema is None or right.schema is None:
        return True
    return left.schema == right.schema


def _rooted_in_frame(expr: ast.AST, frames: Dict[str, Set[CodeTable]]) -> bool:
    root = _root_name(expr)
    return root is not None and root in frames


def _references_frame(expr: ast.AST, frames: Dict[str, Set[CodeTable]]) -> bool:
    if isinstance(expr, ast.Name):
        return expr.id in frames
    return any(_references_frame(child, frames) for child in ast.iter_child_nodes(expr))


def _tail_has_tables(expr: ast.expr, env: Dict[str, str]) -> bool:
    tail = _eval_tail(expr, env)
    return tail is not None and "Tables" in tail


@dataclass(frozen=True)
class _ColRef:
    table: CodeTable
    column: str


@dataclass
class _Projection:
    """Columns of one dataframe expression.

    ``passthrough`` is a table read whose columns flow through under their own
    names. ``explicit`` is the columns a select or withColumn introduced.
    ``projected`` means a select replaced the schema, so only ``explicit``
    remains. ``aliases`` are the dataframes a join can still name.
    """

    reads: Set[CodeTable] = field(default_factory=set)
    passthrough: Optional[CodeTable] = None
    explicit: Dict[str, Set[_ColRef]] = field(default_factory=dict)
    dropped: Set[str] = field(default_factory=set)
    projected: bool = False
    alias: Optional[str] = None
    aliases: Dict[str, _Projection] = field(default_factory=dict)


# Dataframe methods that keep or reshape columns. Anything else (count, show)
# is not a dataframe we can follow.
_DF_METHODS = {
    "alias",
    "select",
    "withColumn",
    "withColumnRenamed",
    "drop",
    "filter",
    "where",
    "distinct",
    "dropDuplicates",
    "orderBy",
    "sort",
    "limit",
    "persist",
    "cache",
    "hint",
    "repartition",
    "coalesce",
    "dropna",
    "fillna",
    "join",
    "union",
    "unionAll",
    "unionByName",
}
_COLUMN_FUNCS = {"col", "column", "upper", "lower", "trim", "ltrim", "rtrim"}
_LITERAL_FUNCS = {"lit", "current_timestamp", "current_date", "now", "expr"}


def _known_column(
    names: Iterable[str], column: str, *, case_sensitive: bool
) -> Optional[str]:
    """Return the stored spelling of ``column``.

    Spark resolves column names without regard to case unless
    ``spark.sql.caseSensitive`` is on. The stored spelling is the one the
    dataframe already uses.
    """
    if case_sensitive:
        return column if column in names else None
    folded = column.casefold()
    for name in names:
        if name.casefold() == folded:
            return name
    return None


def _visible_columns(
    proj: _Projection, *, case_sensitive: bool
) -> List[Tuple[str, Set["_ColRef"]]]:
    return [
        (column, refs)
        for column, refs in proj.explicit.items()
        if _known_column(proj.dropped, column, case_sensitive=case_sensitive) is None
    ]


def _clone(proj: _Projection) -> _Projection:
    return _Projection(
        reads=set(proj.reads),
        passthrough=proj.passthrough,
        explicit={column: set(refs) for column, refs in proj.explicit.items()},
        dropped=set(proj.dropped),
        projected=proj.projected,
        alias=proj.alias,
        aliases=dict(proj.aliases),
    )


class _Extractor:
    def __init__(self, *, case_sensitive_columns: bool = False) -> None:
        self._case_sensitive_columns = case_sensitive_columns
        self.lineage: Dict[CodeTable, Set[CodeTable]] = {}
        self.column_lineage: Dict[CodeTable, Dict[str, Set[_ColRef]]] = {}
        self._frames: Dict[str, Set[CodeTable]] = {}
        self._produced: Dict[str, Set[CodeTable]] = {}
        self._strings: Dict[str, str] = {}
        self._dicts: Dict[str, Dict[str, str]] = {}
        self._lists: Dict[str, List[str]] = {}
        self._col_lists: Dict[str, List[ast.expr]] = {}
        self._bools: Dict[str, bool] = {}
        self._projections: Dict[str, _Projection] = {}
        self._exprs: Dict[str, ast.expr] = {}
        self._fns: Dict[str, Union[ast.FunctionDef, ast.AsyncFunctionDef]] = {}
        self._executing: Set[str] = set()
        self._return_projection: Optional[_Projection] = None
        # function name -> (dataframe param, path param, dataframe index, path index)
        self._writers: Dict[str, Tuple[str, str, int, int]] = {}

    def walk(self, stmts: List[ast.stmt]) -> None:
        for stmt in stmts:
            self._stmt(stmt)

    def _stmt(self, stmt: ast.stmt) -> None:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            self._fns[stmt.name] = stmt
            writer = _writer_params(stmt)
            if writer is not None:
                self._writers[stmt.name] = writer
            return
        if isinstance(stmt, ast.ClassDef):
            return
        if isinstance(stmt, ast.Assign):
            self._writes_in(stmt.value)
            for target in stmt.targets:
                if isinstance(target, ast.Name):
                    self._bind(target.id, stmt.value)
            return
        if (
            isinstance(stmt, ast.AnnAssign)
            and isinstance(stmt.target, ast.Name)
            and stmt.value is not None
        ):
            self._writes_in(stmt.value)
            self._bind(stmt.target.id, stmt.value)
            return
        if isinstance(stmt, ast.Expr):
            self._writes_in(stmt.value)
            return
        if isinstance(stmt, ast.If):
            cond = self._eval_bool(stmt.test)
            if cond is True:
                self.walk(stmt.body)
            elif cond is False:
                self.walk(stmt.orelse)
            else:
                self.walk(stmt.body)
                self.walk(stmt.orelse)
            return
        if isinstance(stmt, ast.Return) and stmt.value is not None:
            self._writes_in(stmt.value)
            self._return_projection = self._projection_of(stmt.value)
            return
        if isinstance(stmt, (ast.For, ast.AsyncFor, ast.While)):
            self.walk(stmt.body)
            self.walk(stmt.orelse)
            return
        if isinstance(stmt, (ast.With, ast.AsyncWith)):
            self.walk(stmt.body)
            return
        if isinstance(stmt, ast.Try):
            self.walk(stmt.body)
            for handler in stmt.handlers:
                self.walk(handler.body)
            self.walk(stmt.orelse)
            self.walk(stmt.finalbody)

    def _bind(self, name: str, value: ast.expr) -> None:
        reads, produced = self._collect(value)
        proj = self._projection_of(value)
        if proj is not None:
            reads |= proj.reads
        tail = _eval_tail(value, self._strings)
        if tail is not None:
            self._strings[name] = tail
        else:
            self._strings.pop(name, None)
        mapping = _eval_dict(value)
        if mapping is None and isinstance(value, ast.Name):
            mapping = self._dicts.get(value.id)
        if mapping is not None:
            self._dicts[name] = mapping
        else:
            self._dicts.pop(name, None)
        values = _eval_str_list(value)
        if values is None and isinstance(value, ast.Name):
            values = self._lists.get(value.id)
        if values is not None:
            self._lists[name] = values
        else:
            self._lists.pop(name, None)
        column_exprs = self._expand_column_exprs(value)
        if column_exprs is not None:
            self._col_lists[name] = column_exprs
        elif isinstance(value, ast.Name) and value.id in self._col_lists:
            self._col_lists[name] = self._col_lists[value.id]
        else:
            self._col_lists.pop(name, None)
        flag = self._eval_bool(value)
        if flag is not None and not isinstance(value, ast.Name):
            self._bools[name] = flag
        elif isinstance(value, ast.Name) and value.id in self._bools:
            self._bools[name] = self._bools[value.id]
        else:
            self._bools.pop(name, None)
        if reads or produced or _rooted_in_frame(value, self._frames):
            self._frames[name] = reads
            self._produced[name] = produced
        else:
            self._frames.pop(name, None)
            self._produced.pop(name, None)
        if proj is not None:
            self._projections[name] = proj
            self._exprs.pop(name, None)
        else:
            self._projections.pop(name, None)
            self._exprs[name] = value

    def _writes_in(self, expr: ast.AST) -> None:
        if isinstance(expr, ast.Call):
            self._record_save(expr)
            self._record_writer_call(expr)
        for child in ast.iter_child_nodes(expr):
            self._writes_in(child)

    def _record_save(self, call: ast.Call) -> None:
        func = call.func
        if not isinstance(func, ast.Attribute) or func.attr not in {
            "save",
            "saveAsTable",
            "insertInto",
        }:
            return
        if not _chain_has(func.value, "write"):
            return
        if not call.args:
            return
        target = _code_table_from_expr(call.args[0], self._strings)
        if target is None:
            return
        df_expr = _write_receiver(func.value)
        if df_expr is None:
            return
        self._add_edge(target, df_expr)

    def _record_writer_call(self, call: ast.Call) -> None:
        if not isinstance(call.func, ast.Name):
            return
        df_expr, path_expr = self._writer_args(call)
        if df_expr is None or path_expr is None:
            return
        target = _code_table_from_expr(path_expr, self._strings)
        if target is None:
            return
        self._add_edge(target, df_expr)

    def _writer_args(
        self, call: ast.Call
    ) -> Tuple[Optional[ast.expr], Optional[ast.expr]]:
        assert isinstance(call.func, ast.Name)
        writer = self._writers.get(call.func.id)
        if writer is not None:
            df_name, path_name, df_index, path_index = writer
            return (
                _argument(call, df_name, df_index),
                _argument(call, path_name, path_index),
            )
        df_expr: Optional[ast.expr] = None
        path_expr: Optional[ast.expr] = None
        candidates = list(call.args) + [kw.value for kw in call.keywords]
        for arg in candidates:
            if path_expr is None and _tail_has_tables(arg, self._strings):
                path_expr = arg
            elif df_expr is None and _references_frame(arg, self._frames):
                df_expr = arg
        if df_expr is None or path_expr is None:
            return None, None
        return df_expr, path_expr

    def _add_edge(self, target: CodeTable, df_expr: ast.expr) -> None:
        reads, produced = self._collect(df_expr)
        proj = self._projection_of(df_expr)
        if proj is not None:
            reads |= proj.reads
        sources = {
            source for source in reads | produced if not _same_table(source, target)
        }
        self.lineage.setdefault(target, set()).update(sources)
        root = _root_name(df_expr)
        if root is not None:
            self._produced.setdefault(root, set()).add(target)
        if proj is not None:
            self._record_columns(target, proj)

    def _record_columns(self, target: CodeTable, proj: _Projection) -> None:
        bucket = self.column_lineage.setdefault(target, {})
        for column, refs in proj.explicit.items():
            if self._is_dropped(proj, column):
                continue
            kept = {ref for ref in refs if not _same_table(ref.table, target)}
            if kept:
                bucket.setdefault(column, set()).update(kept)

    def _projection_of(self, expr: ast.AST) -> Optional[_Projection]:
        if isinstance(expr, ast.Name):
            return self._projections.get(expr.id)
        if not isinstance(expr, ast.Call):
            return None
        called = self._call_function(expr)
        if called is not None:
            return called
        read = _read_table(expr, self._strings)
        if read is not None:
            return _Projection(reads={read}, passthrough=read)
        func = expr.func
        if not isinstance(func, ast.Attribute) or func.attr not in _DF_METHODS:
            return None
        base = self._projection_of(func.value)
        if base is None:
            return None
        return self._apply_method(base, func.attr, expr)

    def _apply_method(
        self, base: _Projection, attr: str, call: ast.Call
    ) -> Optional[_Projection]:
        if attr in {"union", "unionAll", "unionByName"}:
            return self._union(base, call, by_name=attr == "unionByName")
        handler = {
            "alias": self._alias,
            "select": self._select,
            "withColumn": self._with_column,
            "withColumnRenamed": self._rename_column,
            "drop": self._drop,
            "join": self._join,
        }.get(attr)
        if handler is None:
            return _clone(base)
        return handler(base, call)

    def _alias(self, base: _Projection, call: ast.Call) -> _Projection:
        name = _eval_tail(call.args[0], self._strings) if call.args else None
        if not name:
            return base
        cloned = _clone(base)
        cloned.alias = name
        return cloned

    def _select(self, base: _Projection, call: ast.Call) -> _Projection:
        explicit: Dict[str, Set[_ColRef]] = {}
        for arg in call.args:
            for column, refs in self._eval_columns(arg, base):
                explicit.setdefault(column, set()).update(refs)
        reads = set(base.reads)
        for refs in explicit.values():
            reads.update(ref.table for ref in refs)
        return _Projection(reads=reads, explicit=explicit, projected=True)

    def _with_column(self, base: _Projection, call: ast.Call) -> _Projection:
        if len(call.args) < 2:
            return base
        name = _eval_tail(call.args[0], self._strings)
        if not name:
            return base
        cloned = _clone(base)
        existing = self._known_column(cloned.explicit, name)
        if existing is not None and existing != name:
            cloned.explicit.pop(existing, None)
        cloned.explicit[name] = self._origins_in(call.args[1], base)
        self._undrop(cloned, name)
        return cloned

    def _rename_column(self, base: _Projection, call: ast.Call) -> _Projection:
        if len(call.args) < 2:
            return base
        old = _eval_tail(call.args[0], self._strings)
        new = _eval_tail(call.args[1], self._strings)
        if not old or not new:
            return base
        cloned = _clone(base)
        origins = self._lookup(base, old)
        existing = self._known_column(cloned.explicit, old)
        if cloned.projected:
            cloned.explicit.pop(existing if existing is not None else old, None)
        else:
            cloned.dropped.add(existing if existing is not None else old)
        replaced = self._known_column(cloned.explicit, new)
        if replaced is not None and replaced != new:
            cloned.explicit.pop(replaced, None)
        cloned.explicit[new] = origins
        self._undrop(cloned, new)
        return cloned

    def _drop(self, base: _Projection, call: ast.Call) -> _Projection:
        cloned = _clone(base)
        for arg in call.args:
            name = _eval_tail(arg, self._strings)
            if not name:
                continue
            existing = self._known_column(cloned.explicit, name)
            stored = existing if existing is not None else name
            if cloned.projected:
                cloned.explicit.pop(stored, None)
            else:
                cloned.dropped.add(stored)
        return cloned

    def _join(self, base: _Projection, call: ast.Call) -> _Projection:
        if not call.args:
            return base
        right = self._projection_of(call.args[0])
        if right is None:
            return base
        return _joined(base, right)

    def _union(
        self, base: _Projection, call: ast.Call, *, by_name: bool
    ) -> _Projection:
        """Stack two dataframes.

        ``union`` and ``unionAll`` pair columns by position and keep the left
        names. ``unionByName`` pairs columns that share a name.
        """
        if not call.args:
            return base
        right = self._projection_of(call.args[0])
        if right is None or not base.projected or not right.projected:
            return _Projection(reads=base.reads | (right.reads if right else set()))
        case_sensitive = self._case_sensitive_columns
        left_columns = _visible_columns(base, case_sensitive=case_sensitive)
        right_columns = _visible_columns(right, case_sensitive=case_sensitive)
        if by_name:
            explicit = {column: set(refs) for column, refs in left_columns}
            for column, refs in right_columns:
                key = _known_column(explicit, column, case_sensitive=case_sensitive)
                if key is None:
                    explicit[column] = set(refs)
                else:
                    explicit[key].update(refs)
        else:
            explicit = {}
            for index, (column, refs) in enumerate(left_columns):
                merged = set(refs)
                if index < len(right_columns):
                    merged.update(right_columns[index][1])
                explicit[column] = merged
        return _Projection(
            reads=base.reads | right.reads, explicit=explicit, projected=True
        )

    def _call_function(self, call: ast.Call) -> Optional[_Projection]:
        if not isinstance(call.func, ast.Name):
            return None
        fn = self._fns.get(call.func.id)
        if fn is None or call.func.id in self._executing or not _returns_value(fn):
            return None
        bindings = self._capture_bindings(fn, call)
        saved = self._snapshot()
        self._executing.add(call.func.id)
        try:
            for name, captured in bindings:
                self._install(name, captured)
            self._return_projection = None
            self.walk(fn.body)
            return self._return_projection
        finally:
            self._executing.discard(call.func.id)
            self._restore(saved)

    def _snapshot(self) -> Tuple:
        return (
            dict(self._frames),
            dict(self._produced),
            dict(self._strings),
            dict(self._dicts),
            dict(self._lists),
            dict(self._col_lists),
            dict(self._bools),
            dict(self._projections),
            dict(self._exprs),
            self._return_projection,
        )

    def _restore(self, snap: Tuple) -> None:
        (
            self._frames,
            self._produced,
            self._strings,
            self._dicts,
            self._lists,
            self._col_lists,
            self._bools,
            self._projections,
            self._exprs,
            self._return_projection,
        ) = snap

    def _capture_bindings(
        self, fn: Union[ast.FunctionDef, ast.AsyncFunctionDef], call: ast.Call
    ) -> List[Tuple[str, Tuple[str, object]]]:
        params = [arg.arg for arg in [*fn.args.posonlyargs, *fn.args.args]]
        defaults: List[Optional[ast.expr]] = [None] * (
            len(params) - len(fn.args.defaults)
        ) + list(fn.args.defaults)
        bound: Dict[str, ast.expr] = {}
        for keyword in call.keywords:
            if keyword.arg:
                bound[keyword.arg] = keyword.value
        for index, arg in enumerate(call.args):
            if index < len(params):
                bound[params[index]] = arg
        for name, default in zip(params, defaults, strict=False):
            if name not in bound and default is not None:
                bound[name] = default
        return [(name, self._capture(expr)) for name, expr in bound.items()]

    def _capture(self, expr: ast.expr) -> Tuple[str, object]:
        if isinstance(expr, ast.Name) and expr.id in self._projections:
            return ("proj", self._projections[expr.id])
        if isinstance(expr, ast.Name) and expr.id in self._dicts:
            return ("dict", self._dicts[expr.id])
        mapping = _eval_dict(expr)
        if mapping is not None:
            return ("dict", mapping)
        if isinstance(expr, ast.Name) and expr.id in self._lists:
            return ("list", self._lists[expr.id])
        values = _eval_str_list(expr)
        if values is not None:
            return ("list", values)
        flag = self._eval_bool(expr)
        if flag is not None:
            return ("bool", flag)
        text = _eval_tail(expr, self._strings)
        if text is not None:
            return ("str", text)
        return ("expr", expr)

    def _install(self, name: str, captured: Tuple[str, object]) -> None:
        kind, value = captured
        if kind == "str" and isinstance(value, str):
            self._strings[name] = value
        elif kind == "bool" and isinstance(value, bool):
            self._bools[name] = value
        elif kind == "dict" and isinstance(value, dict):
            self._dicts[name] = value
        elif kind == "list" and isinstance(value, list):
            self._lists[name] = value
        elif kind == "proj" and isinstance(value, _Projection):
            self._projections[name] = value
            self._frames[name] = set(value.reads)

    def _eval_bool(self, expr: ast.AST) -> Optional[bool]:
        if isinstance(expr, ast.Constant) and isinstance(expr.value, bool):
            return expr.value
        if isinstance(expr, ast.Name) and expr.id in self._bools:
            return self._bools[expr.id]
        return None

    def _eval_columns(
        self, expr: ast.expr, current: _Projection
    ) -> List[Tuple[str, Set[_ColRef]]]:
        if isinstance(expr, ast.Starred):
            return self._eval_expanded(expr.value, current)
        alias = self._alias_name(expr)
        if (
            alias
            and isinstance(expr, ast.Call)
            and isinstance(expr.func, ast.Attribute)
        ):
            origins = self._origins_in(expr.func.value, current)
            return [(alias, origins)]
        named = _named_column(expr, self._strings)
        if named is not None:
            return self._resolve_named(named, current)
        if isinstance(expr, ast.Subscript):
            ref = self._subscript(expr)
            if ref is not None:
                return [ref]
        if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
            return self._resolve_named(expr.value, current)
        return []

    def _eval_expanded(
        self, expr: ast.expr, current: _Projection
    ) -> List[Tuple[str, Set[_ColRef]]]:
        if (
            isinstance(expr, (ast.ListComp, ast.GeneratorExp))
            and len(expr.generators) == 1
        ):
            bindings = self._comprehension_bindings(expr.generators[0])
            if bindings is None:
                return []
            out: List[Tuple[str, Set[_ColRef]]] = []
            saved = dict(self._strings)
            try:
                for binding in bindings:
                    self._strings.update(binding)
                    out.extend(self._eval_columns(expr.elt, current))
            finally:
                self._strings = saved
            return out
        if isinstance(expr, ast.IfExp):
            cond = self._eval_bool(expr.test)
            chosen = expr.body if cond else expr.orelse if cond is not None else None
            if chosen is None:
                return []
            return self._eval_expanded(chosen, current)
        if isinstance(expr, (ast.List, ast.Tuple)):
            out = []
            for elt in expr.elts:
                out.extend(self._eval_columns(elt, current))
            return out
        stored = self._expand_column_exprs(expr)
        if stored:
            out = []
            for elt in stored:
                out.extend(self._eval_columns(elt, current))
            return out
        return self._eval_columns(expr, current)

    def _expand_column_exprs(self, expr: ast.expr) -> Optional[List[ast.expr]]:
        """Column expressions behind ``select(*saved_list)``.

        Notebooks often build that list as comprehensions added together
        (``[col(f"p.{c}").alias(f"person_{c}") for c in person_cols] + ...``)
        and only apply it later.
        """
        if isinstance(expr, ast.BinOp) and isinstance(expr.op, ast.Add):
            left = self._expand_column_exprs(expr.left)
            right = self._expand_column_exprs(expr.right)
            if left is None or right is None:
                return None
            return left + right
        if isinstance(expr, ast.Name):
            return self._col_lists.get(expr.id)
        if (
            isinstance(expr, (ast.ListComp, ast.GeneratorExp))
            and len(expr.generators) == 1
            and not expr.generators[0].ifs
        ):
            bindings = self._comprehension_bindings(expr.generators[0])
            if bindings is None:
                return None
            return [_substitute_names(expr.elt, binding) for binding in bindings]
        return None

    def _comprehension_bindings(
        self, gen: ast.comprehension
    ) -> Optional[List[Dict[str, str]]]:
        if gen.ifs:
            return None
        mapping = self._dict_iter(gen.iter)
        if (
            mapping is not None
            and isinstance(gen.target, ast.Tuple)
            and len(gen.target.elts) == 2
            and all(isinstance(elt, ast.Name) for elt in gen.target.elts)
        ):
            src, dst = gen.target.elts
            assert isinstance(src, ast.Name) and isinstance(dst, ast.Name)
            return [{src.id: key, dst.id: value} for key, value in mapping.items()]
        if isinstance(gen.target, ast.Name) and isinstance(gen.iter, ast.Name):
            values = self._lists.get(gen.iter.id)
            if values is not None:
                return [{gen.target.id: value} for value in values]
        return None

    def _dict_iter(self, expr: ast.expr) -> Optional[Dict[str, str]]:
        if (
            isinstance(expr, ast.Call)
            and isinstance(expr.func, ast.Attribute)
            and expr.func.attr == "items"
            and isinstance(expr.func.value, ast.Name)
        ):
            return self._dicts.get(expr.func.value.id)
        return None

    def _alias_name(self, expr: ast.expr) -> Optional[str]:
        if (
            isinstance(expr, ast.Call)
            and isinstance(expr.func, ast.Attribute)
            and expr.func.attr == "alias"
            and expr.args
        ):
            return _eval_tail(expr.args[0], self._strings)
        return None

    def _resolve_named(
        self, name: str, current: _Projection
    ) -> List[Tuple[str, Set[_ColRef]]]:
        if name == "*" or name.endswith(".*"):
            qual = None if name == "*" else name[: -len(".*")]
            proj = self._qualified(current, qual) if qual is not None else current
            if proj is None or not proj.projected:
                return []
            return [
                (column, set(refs))
                for column, refs in proj.explicit.items()
                if not self._is_dropped(proj, column)
            ]
        if "." in name:
            qual, column = name.split(".", 1)
            proj = self._qualified(current, qual)
            if proj is None:
                return []
            return [(self._canonical(proj, column), self._lookup(proj, column))]
        return [(self._canonical(current, name), self._lookup(current, name))]

    def _qualified(self, current: _Projection, qual: str) -> Optional[_Projection]:
        if current.alias == qual:
            return current
        return current.aliases.get(qual) or self._projections.get(qual)

    def _known_column(self, names: Iterable[str], column: str) -> Optional[str]:
        return _known_column(names, column, case_sensitive=self._case_sensitive_columns)

    def _canonical(self, proj: _Projection, column: str) -> str:
        return self._known_column(proj.explicit, column) or column

    def _is_dropped(self, proj: _Projection, column: str) -> bool:
        return self._known_column(proj.dropped, column) is not None

    def _undrop(self, proj: _Projection, column: str) -> None:
        dropped = self._known_column(proj.dropped, column)
        if dropped is not None:
            proj.dropped.discard(dropped)

    def _lookup(self, proj: _Projection, column: str) -> Set[_ColRef]:
        explicit = self._known_column(proj.explicit, column)
        if self._is_dropped(proj, column) and explicit is None:
            return set()
        if proj.projected or explicit is not None:
            if explicit is not None:
                return set(proj.explicit[explicit])
            if proj.projected:
                return set()
        if proj.passthrough is not None:
            return {_ColRef(proj.passthrough, column)}
        return set()

    def _subscript(self, expr: ast.Subscript) -> Optional[Tuple[str, Set[_ColRef]]]:
        if not isinstance(expr.value, ast.Name):
            return None
        proj = self._projections.get(expr.value.id)
        if proj is None:
            return None
        column = _eval_tail(expr.slice, self._strings)
        if not column:
            return None
        return (self._canonical(proj, column), self._lookup(proj, column))

    def _origins_in(
        self,
        expr: ast.AST,
        current: _Projection,
        seen: Optional[Set[int]] = None,
        names: Optional[Set[str]] = None,
    ) -> Set[_ColRef]:
        if seen is None:
            seen = set()
        if names is None:
            names = set()
        if id(expr) in seen:
            return set()
        seen.add(id(expr))
        if isinstance(expr, ast.Name):
            if expr.id in names or expr.id in self._projections:
                return set()
            stored = self._exprs.get(expr.id)
            if stored is None:
                return set()
            names.add(expr.id)
            return self._origins_in(stored, current, seen, names)
        if isinstance(expr, ast.Subscript):
            ref = self._subscript(expr)
            return set(ref[1]) if ref is not None else set()
        if isinstance(expr, ast.Call) and _call_name(expr) in _LITERAL_FUNCS:
            return set()
        if (
            isinstance(expr, ast.Call)
            and _call_name(expr) in _COLUMN_FUNCS
            and expr.args
        ):
            named = _eval_tail(expr.args[0], self._strings)
            if named and not named.endswith(".*"):
                found: Set[_ColRef] = set()
                for _column, refs in self._resolve_named(named, current):
                    found.update(refs)
                return found
        found = set()
        for child in ast.iter_child_nodes(expr):
            found.update(self._origins_in(child, current, seen, names))
        return found

    def _collect(self, expr: ast.AST) -> Tuple[Set[CodeTable], Set[CodeTable]]:
        reads: Set[CodeTable] = set()
        produced: Set[CodeTable] = set()
        if isinstance(expr, ast.Name):
            reads.update(self._frames.get(expr.id, ()))
            produced.update(self._produced.get(expr.id, ()))
            return reads, produced
        if isinstance(expr, ast.Call):
            read = _read_table(expr, self._strings)
            if read is not None:
                reads.add(read)
        for child in ast.iter_child_nodes(expr):
            child_reads, child_produced = self._collect(child)
            reads.update(child_reads)
            produced.update(child_produced)
        return reads, produced


def _joined(left: _Projection, right: _Projection) -> _Projection:
    aliases = dict(left.aliases)
    aliases.update(right.aliases)
    if left.alias:
        aliases[left.alias] = left
    if right.alias:
        aliases[right.alias] = right
    return _Projection(reads=left.reads | right.reads, aliases=aliases)


def _returns_value(fn: Union[ast.FunctionDef, ast.AsyncFunctionDef]) -> bool:
    return any(
        isinstance(node, ast.Return) and node.value is not None for node in ast.walk(fn)
    )


def _call_name(expr: ast.Call) -> Optional[str]:
    func = expr.func
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return None


def _named_column(expr: ast.expr, env: Dict[str, str]) -> Optional[str]:
    """Column named by ``col("name")`` or ``F.col("name")``, before any alias."""
    if not isinstance(expr, ast.Call) or _call_name(expr) not in {"col", "column"}:
        return None
    if not expr.args:
        return None
    return _eval_tail(expr.args[0], env)


def _eval_dict(expr: ast.expr) -> Optional[Dict[str, str]]:
    if not isinstance(expr, ast.Dict):
        return None
    mapping: Dict[str, str] = {}
    for key, value in zip(expr.keys, expr.values, strict=False):
        if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
            return None
        if not isinstance(value, ast.Constant) or not isinstance(value.value, str):
            return None
        mapping[key.value] = value.value
    return mapping


def _substitute_names(expr: ast.expr, binding: Dict[str, str]) -> ast.expr:
    """Replace comprehension variables with the string they stand for."""

    class _Sub(ast.NodeTransformer):
        def visit_Name(self, node: ast.Name) -> ast.AST:
            if isinstance(node.ctx, ast.Load) and node.id in binding:
                return ast.copy_location(ast.Constant(value=binding[node.id]), node)
            return node

    return _Sub().visit(copy.deepcopy(expr))


def _eval_str_list(expr: ast.expr) -> Optional[List[str]]:
    if not isinstance(expr, (ast.List, ast.Tuple)):
        return None
    values: List[str] = []
    for elt in expr.elts:
        if not isinstance(elt, ast.Constant) or not isinstance(elt.value, str):
            return None
        values.append(elt.value)
    return values


def _writer_params(
    fn: ast.FunctionDef | ast.AsyncFunctionDef,
) -> Optional[Tuple[str, str, int, int]]:
    """Return dataframe and path parameters when the function saves one of them."""
    params = [arg.arg for arg in [*fn.args.posonlyargs, *fn.args.args]]
    for node in ast.walk(fn):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        func = node.func
        if not isinstance(func, ast.Attribute) or func.attr != "save":
            continue
        if not _chain_has(func.value, "write"):
            continue
        path = node.args[0]
        df_expr = _write_receiver(func.value)
        root = _root_name(df_expr)
        if not isinstance(path, ast.Name) or path.id not in params:
            continue
        if root is None or root not in params:
            continue
        return root, path.id, params.index(root), params.index(path.id)
    return None


def _argument(call: ast.Call, name: str, index: int) -> Optional[ast.expr]:
    for keyword in call.keywords:
        if keyword.arg == name:
            return keyword.value
    if index < len(call.args):
        return call.args[index]
    return None
