"""Narrow Spark-plan lineage using the notebook source.

A Spark plan credits every table read earlier in the run to the next write.
Notebooks often load many tables up front and then write a result that uses
only some of them, so the plan reports sources that do not feed that output.

The source text names those tables even when the path is built at run time
(``f"{source}orders"``), and it shows which dataframe each write actually uses.
Commented-out cells are not real reads or writes. This module recovers that
per-target source set. The caller keeps an edge only when the executed plans
also saw the same source and target, so the resolved OneLake path still comes
from the run.
"""

import ast
import logging
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

from datahub.ingestion.source.fabric.onelake.notebook_spark_lineage import (
    SparkTableRef,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CodeTable:
    """A table named by the notebook source, before a run resolves its path."""

    table: str
    schema: Optional[str] = None


def extract_notebook_code_lineage(
    source: str,
) -> Optional[Dict[CodeTable, Set[CodeTable]]]:
    """Map each table the notebook writes to the tables that write uses.

    Returns None when ``source`` is not parseable Python, so the caller can
    keep the Spark-plan lineage unchanged. An empty dict means the code parsed
    and contains no writes.
    """
    try:
        tree = ast.parse(_strip_magics(source))
    except SyntaxError:
        logger.debug("Notebook source is not parseable Python; skipping code lineage")
        return None

    extractor = _Extractor()
    extractor.walk(tree.body)
    return extractor.lineage


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


class _Extractor:
    def __init__(self) -> None:
        self.lineage: Dict[CodeTable, Set[CodeTable]] = {}
        self._frames: Dict[str, Set[CodeTable]] = {}
        self._produced: Dict[str, Set[CodeTable]] = {}
        self._strings: Dict[str, str] = {}
        # function name -> (dataframe param, path param, dataframe index, path index)
        self._writers: Dict[str, Tuple[str, str, int, int]] = {}

    def walk(self, stmts: List[ast.stmt]) -> None:
        for stmt in stmts:
            self._stmt(stmt)

    def _stmt(self, stmt: ast.stmt) -> None:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
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
            self.walk(stmt.body)
            self.walk(stmt.orelse)
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
        tail = _eval_tail(value, self._strings)
        if tail is not None:
            self._strings[name] = tail
        else:
            self._strings.pop(name, None)
        if reads or produced or _rooted_in_frame(value, self._frames):
            self._frames[name] = reads
            self._produced[name] = produced
        else:
            self._frames.pop(name, None)
            self._produced.pop(name, None)

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
        sources = {
            source for source in reads | produced if not _same_table(source, target)
        }
        self.lineage.setdefault(target, set()).update(sources)
        root = _root_name(df_expr)
        if root is not None:
            self._produced.setdefault(root, set()).add(target)

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
