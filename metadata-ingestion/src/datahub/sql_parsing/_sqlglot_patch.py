import patchy.api
import sqlglot
import sqlglot.expressions
import sqlglot.optimizer.scope

# This injects a few patches into sqlglot to add features and mitigate
# some bugs and performance issues.

assert sqlglot is not None
SQLGLOT_PATCHED = True


"""
def _new_apply_patch(source: str, patch_text: str, forwards: bool, name: str) -> str:
    assert forwards

    # TODO: Implement the patch
    raise NotImplementedError


patchy.api._apply_patch = _new_apply_patch
"""

deepcopy_memory_patch = """\
@@ -1,4 +1,7 @@ def meta(self) -> t.Dict[str, t.Any]:
 def __deepcopy__(self, memo):
+    import datahub.utilities.cooperative_timeout
+    datahub.utilities.cooperative_timeout.cooperate()
+
     root = self.__class__()
     stack = [(self, root)]
"""
patchy.patch(sqlglot.expressions.Expression.__deepcopy__, deepcopy_memory_patch)

cyclic_scope_patch = """\
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
"""
patchy.patch(sqlglot.optimizer.scope.Scope.traverse, cyclic_scope_patch)
