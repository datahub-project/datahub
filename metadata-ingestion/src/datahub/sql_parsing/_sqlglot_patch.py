import dataclasses
import difflib
import logging

import patchy.api
import sqlglot
import sqlglot.expressions
import sqlglot.lineage
import sqlglot.optimizer.scope
import sqlglot.optimizer.unnest_subqueries

from datahub.utilities.is_pytest import is_pytest_running
from datahub.utilities.unified_diff import apply_diff

# This injects a few patches into sqlglot to add features and mitigate
# some bugs and performance issues.
# The diffs in this file should match the diffs declared in our fork.
# https://github.com/tobymao/sqlglot/compare/main...hsheth2:sqlglot:main
# For a diff-formatted view, see:
# https://github.com/tobymao/sqlglot/compare/main...hsheth2:sqlglot:main.diff

_DEBUG_PATCHER = is_pytest_running() or True
logger = logging.getLogger(__name__)

_apply_diff_subprocess = patchy.api._apply_patch


def _new_apply_patch(source: str, patch_text: str, forwards: bool, name: str) -> str:
    assert forwards, "Only forward patches are supported"

    result = apply_diff(source, patch_text)

    # TODO: When in testing mode, still run the subprocess and check that the
    # results line up.
    if _DEBUG_PATCHER:
        result_subprocess = _apply_diff_subprocess(source, patch_text, forwards, name)
        if result_subprocess != result:
            logger.info("Results from subprocess and _apply_diff do not match")
            logger.debug(f"Subprocess result:\n{result_subprocess}")
            logger.debug(f"Our result:\n{result}")
            diff = difflib.unified_diff(
                result_subprocess.splitlines(), result.splitlines()
            )
            logger.debug("Diff:\n" + "\n".join(diff))
            raise ValueError("Results from subprocess and _apply_diff do not match")

    return result


patchy.api._apply_patch = _new_apply_patch


def _patch_deepcopy() -> None:
    patchy.patch(
        sqlglot.expressions.Expression.__deepcopy__,
        """\
@@ -1,4 +1,7 @@ def meta(self) -> t.Dict[str, t.Any]:
 def __deepcopy__(self, memo):
+    import datahub.utilities.cooperative_timeout
+    datahub.utilities.cooperative_timeout.cooperate()
+
     root = self.__class__()
     stack = [(self, root)]
""",
    )


def _patch_scope_traverse() -> None:
    # Circular scope dependencies can happen in somewhat specific circumstances
    # due to our usage of sqlglot.
    # See https://github.com/tobymao/sqlglot/pull/4244
    patchy.patch(
        sqlglot.optimizer.scope.Scope.traverse,
        """\
@@ -5,9 +5,16 @@ def traverse(self):
         Scope: scope instances in depth-first-search post-order
     \"""
     stack = [self]
+    seen_scopes = set()
     result = []
     while stack:
         scope = stack.pop()
+
+        # Scopes aren't hashable, so we use id(scope) instead.
+        if id(scope) in seen_scopes:
+            raise OptimizeError(f"Scope {scope} has a circular scope dependency")
+        seen_scopes.add(id(scope))
+
         result.append(scope)
         stack.extend(
             itertools.chain(
""",
    )


def _patch_unnest_subqueries() -> None:
    patchy.patch(
        sqlglot.optimizer.unnest_subqueries.decorrelate,
        """\
@@ -261,16 +261,19 @@ def remove_aggs(node):
         if key in group_by:
             key.replace(nested)
         elif isinstance(predicate, exp.EQ):
-            parent_predicate = _replace(
-                parent_predicate,
-                f"({parent_predicate} AND ARRAY_CONTAINS({nested}, {column}))",
-            )
+            if parent_predicate:
+                parent_predicate = _replace(
+                    parent_predicate,
+                    f"({parent_predicate} AND ARRAY_CONTAINS({nested}, {column}))",
+                )
         else:
             key.replace(exp.to_identifier("_x"))
-            parent_predicate = _replace(
-                parent_predicate,
-                f"({parent_predicate} AND ARRAY_ANY({nested}, _x -> {predicate}))",
-            )
+
+            if parent_predicate:
+                parent_predicate = _replace(
+                    parent_predicate,
+                    f"({parent_predicate} AND ARRAY_ANY({nested}, _x -> {predicate}))",
+                )
""",
    )


def _patch_lineage() -> None:
    # Add the "subfield" attribute to sqlglot.lineage.Node.
    # With dataclasses, the easiest way to do this is with inheritance.
    # Unfortunately, mypy won't pick up on the new field, so we need to
    # use type ignores everywhere we use subfield.
    @dataclasses.dataclass(frozen=True)
    class Node(sqlglot.lineage.Node):
        subfield: str = ""

    sqlglot.lineage.Node = Node  # type: ignore

    patchy.patch(
        sqlglot.lineage.lineage,
        """\
@@ -12,7 +12,8 @@ def lineage(
     \"""

     expression = maybe_parse(sql, dialect=dialect)
-    column = normalize_identifiers.normalize_identifiers(column, dialect=dialect).name
+    # column = normalize_identifiers.normalize_identifiers(column, dialect=dialect).name
+    assert isinstance(column, str)

     if sources:
         expression = exp.expand(
""",
    )

    patchy.patch(
        sqlglot.lineage.to_node,
        """\
@@ -235,11 +237,12 @@ def to_node(
             )

     # Find all columns that went into creating this one to list their lineage nodes.
-    source_columns = set(find_all_in_scope(select, exp.Column))
+    source_columns = list(find_all_in_scope(select, exp.Column))

-    # If the source is a UDTF find columns used in the UTDF to generate the table
+    # If the source is a UDTF find columns used in the UDTF to generate the table
+    source = scope.expression
     if isinstance(source, exp.UDTF):
-        source_columns |= set(source.find_all(exp.Column))
+        source_columns += list(source.find_all(exp.Column))
         derived_tables = [
             source.expression.parent
             for source in scope.sources.values()
@@ -254,6 +257,7 @@ def to_node(
         if dt.comments and dt.comments[0].startswith("source: ")
     }

+    c: exp.Column
     for c in source_columns:
         table = c.table
         source = scope.sources.get(table)
@@ -281,8 +285,21 @@ def to_node(
             # it means this column's lineage is unknown. This can happen if the definition of a source used in a query
             # is not passed into the `sources` map.
             source = source or exp.Placeholder()
+
+            subfields = []
+            field: exp.Expression = c
+            while isinstance(field.parent, exp.Dot):
+                field = field.parent
+                subfields.append(field.name)
+            subfield = ".".join(subfields)
+
             node.downstream.append(
-                Node(name=c.sql(comments=False), source=source, expression=source)
+                Node(
+                    name=c.sql(comments=False),
+                    source=source,
+                    expression=source,
+                    subfield=subfield,
+                )
             )

     return node
""",
    )


_patch_deepcopy()
_patch_scope_traverse()
_patch_unnest_subqueries()
_patch_lineage()

SQLGLOT_PATCHED = True
