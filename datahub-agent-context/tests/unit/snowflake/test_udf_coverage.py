"""Tests verifying UDF coverage and code validity.

These tests catch several classes of regressions:
1. A new tool is added to mcp_tools but no UDF is generated for it.
2. A UDF generator emits Python code that is syntactically invalid.
3. Tool call arguments are passed with incorrect coercions for their Python types.

All checks run purely from generated strings — no Snowflake connection needed.
"""

import ast
import inspect
import re
from typing import get_args, get_origin

import datahub_agent_context.mcp_tools as mcp_tools_module
from datahub_agent_context.snowflake.generate_udfs import generate_all_udfs

# Tools intentionally excluded from Snowflake UDFs with the reason why.
# Add an entry here when a tool is deliberately not exposed as a UDF.
_TOOLS_WITHOUT_UDFS: dict[str, str] = {
    "save_document": (
        "Requires multi-step document hierarchy setup and user-context that "
        "doesn't map cleanly to a single stateless Snowflake UDF."
    ),
}

# Map mcp_tools function name → expected UDF name (uppercase by convention).
# Only needed when the names diverge; most tools follow the default mapping.
_TOOL_TO_UDF_NAME: dict[str, str] = {
    "search": "SEARCH_DATAHUB",
}


def _expected_udf_name(tool_name: str) -> str:
    """Return the expected Snowflake UDF name for a given mcp_tools function."""
    return _TOOL_TO_UDF_NAME.get(tool_name, tool_name.upper())


def _extract_python_body(udf_sql: str) -> str:
    """Extract the Python code from between the $$ delimiters in a UDF SQL statement."""
    match = re.search(r"\$\$(.*?)\$\$", udf_sql, re.DOTALL)
    assert match, f"No $$ delimiters found in UDF SQL:\n{udf_sql[:200]}"
    return match.group(1)


def _build_assignment_map(tree: ast.AST) -> dict[str, list[str]]:
    """Walk the AST and return a map of variable_name → list of all RHS expressions.

    Collects every simple `name = expr` assignment for a variable (not augmented,
    not tuple unpacking). A variable may be assigned multiple times (e.g. once to
    None, then inside a try block, then in an except fallback), so we keep all
    values to allow checking if *any* assignment uses the expected coercion.
    """
    assignments: dict[str, list[str]] = {}
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
        ):
            name = node.targets[0].id
            assignments.setdefault(name, []).append(ast.unparse(node.value))
    return assignments


def _resolve_expr(expr: str, assignments: dict[str, list[str]]) -> list[str]:
    """If expr is a bare variable name, return all RHS expressions it was assigned.

    This lets us follow one level of indirection (e.g. `entity_urn_list` →
    all values it was ever assigned, including the json.loads(...) one).
    Falls back to [expr] if the variable wasn't found.
    """
    return assignments.get(expr, [expr])


def _unwrap_optional(annotation: type) -> tuple[bool, type]:
    """Return (is_optional, inner_type) for an annotation."""
    args = get_args(annotation)
    origin = get_origin(annotation)
    if origin is not None and type(None) in args:
        non_none = [a for a in args if a is not type(None)]
        inner = non_none[0] if len(non_none) == 1 else annotation
        return True, inner
    return False, annotation


def _classify_python_type(annotation: type) -> str:
    """Classify a Python type annotation into one of our coercion categories.

    Categories:
      "str"    – plain str, Literal[...], or Optional[str/Literal]  → passed as-is
      "int"    – int or Optional[int]  → coerced with int(x)
      "bool"   – bool  → coerced with bool(x) (Snowflake passes 0/1 as NUMBER)
      "list"   – List[...] or Optional[List[...]]  → JSON-decoded with json.loads(x)
      "dict"   – Dict[...] or Optional[Dict[...]]  → JSON-decoded with json.loads(x)
    """
    if annotation is inspect.Parameter.empty:
        return "str"

    _, inner = _unwrap_optional(annotation)
    origin = get_origin(inner)

    if inner is bool:
        return "bool"
    if inner is int:
        return "int"
    if inner is str:
        return "str"
    if origin is list or inner is list:
        return "list"
    if origin is dict or inner is dict:
        return "dict"
    # Literal[...], enums, and other string-like types
    s = str(inner)
    if "List[" in s:
        return "list"
    if "Dict[" in s:
        return "dict"
    return "str"


class TestUdfCoverage:
    """Every tool in mcp_tools.__all__ must have a UDF or be explicitly excluded."""

    def test_all_tools_have_udfs_or_are_excluded(self) -> None:
        """Fail if a new tool is added to mcp_tools without a corresponding UDF.

        When this test fails it means either:
        a) A new tool needs a UDF — add a UDF generator and wire it into generate_all_udfs().
        b) The tool is intentionally excluded — add it to _TOOLS_WITHOUT_UDFS with a reason.
        """
        all_tools = set(mcp_tools_module.__all__)
        excluded = set(_TOOLS_WITHOUT_UDFS)
        tools_needing_udfs = all_tools - excluded

        udfs = generate_all_udfs(include_mutations=True)
        udf_names = set(udfs.keys())

        missing_udfs = {
            tool
            for tool in tools_needing_udfs
            if _expected_udf_name(tool) not in udf_names
        }

        assert not missing_udfs, (
            "The following mcp_tools functions have no Snowflake UDF:\n"
            + "\n".join(
                f"  - {t} (expected UDF: {_expected_udf_name(t)})"
                for t in sorted(missing_udfs)
            )
            + "\n\nEither add a UDF generator and register it in generate_all_udfs(), "
            "or add the tool to _TOOLS_WITHOUT_UDFS in this file with an explanation."
        )

    def test_no_unexpected_udfs(self) -> None:
        """Fail if a UDF exists that doesn't correspond to any known mcp_tool.

        This keeps the UDF list in sync with actual tools and catches orphaned UDFs.
        """
        all_tools = set(mcp_tools_module.__all__)
        all_expected_udf_names = {_expected_udf_name(t) for t in all_tools}

        udfs = generate_all_udfs(include_mutations=True)
        unexpected = set(udfs.keys()) - all_expected_udf_names

        assert not unexpected, (
            "The following UDFs don't correspond to any mcp_tools function:\n"
            + "\n".join(f"  - {u}" for u in sorted(unexpected))
            + "\n\nEither add the function to mcp_tools/__init__.py or remove the UDF generator."
        )


class TestUdfPythonSyntax:
    """Each UDF's embedded Python body must be syntactically valid."""

    def test_all_udf_python_bodies_are_valid(self) -> None:
        """Compile-check the Python code inside every generated UDF.

        This catches typos, missing colons, mismatched parens, etc. that would
        cause the UDF to fail at CREATE time in Snowflake.
        """
        udfs = generate_all_udfs(include_mutations=True)
        errors: list[str] = []

        for udf_name, udf_sql in udfs.items():
            python_body = _extract_python_body(udf_sql)
            try:
                ast.parse(python_body)
            except SyntaxError as e:
                errors.append(f"{udf_name}: {e}")

        assert not errors, (
            "The following UDFs contain invalid Python syntax:\n"
            + "\n".join(f"  - {err}" for err in errors)
        )

    def test_all_udf_python_bodies_import_correct_tool(self) -> None:
        """Each UDF must import the mcp_tools function it claims to call.

        Verifies the import statement is present so the function will be
        resolvable at Snowflake UDF runtime.
        """
        udfs = generate_all_udfs(include_mutations=True)
        all_tools = set(mcp_tools_module.__all__)
        excluded = set(_TOOLS_WITHOUT_UDFS)
        tools_with_udfs = all_tools - excluded

        missing_imports: list[str] = []

        for tool_name in sorted(tools_with_udfs):
            udf_name = _expected_udf_name(tool_name)
            if udf_name not in udfs:
                continue  # already caught by coverage test

            python_body = _extract_python_body(udfs[udf_name])
            expected_import = f"from datahub_agent_context.mcp_tools import {tool_name}"
            if expected_import not in python_body:
                missing_imports.append(f"{udf_name}: missing '{expected_import}'")

        assert not missing_imports, (
            "The following UDFs are missing their mcp_tools import:\n"
            + "\n".join(f"  - {m}" for m in missing_imports)
        )


class TestUdfParameterCoercions:
    """Each UDF must handle type coercions correctly for its tool's parameter types.

    Snowflake UDFs receive parameters as Snowflake types (STRING, NUMBER). Before
    passing them to the Python mcp_tools functions the UDF body must convert them:
      - int params         → int(x)       (NUMBER may arrive as float)
      - bool params        → bool(x)      (Snowflake sends NUMBER 0/1)
      - List/Dict params   → json.loads(x) (caller sends JSON-encoded string)
      - str/Literal params → passed as-is

    Some UDFs intentionally simplify the tool's interface:
      _UDF_INTERFACE_SIMPLIFICATIONS maps UDF_NAME → set of param names that are
      handled differently (hardcoded, restructured, etc.) and should be skipped.
    """

    # UDF_NAME → param names that are intentionally handled differently from the
    # default coercion pattern (hardcoded values, restructured inputs, etc.)
    _UDF_INTERFACE_SIMPLIFICATIONS: dict[str, set[str]] = {
        # GET_ENTITIES takes List[str] but the UDF accepts a single STRING and
        # wraps it: get_entities([entity_urn]). The list-wrapping contract is
        # verified separately by test_get_entities_udf_wraps_single_urn_in_list.
        "GET_ENTITIES": {"urns"},
        # SEARCH_DATAHUB hardcodes num_results=10; it is not a Snowflake parameter.
        "SEARCH_DATAHUB": {"num_results", "sort_by", "sort_order", "offset"},
        # LIST_SCHEMA_FIELDS passes limit directly from NUMBER (Snowflake already
        # delivers it as a Python int; no explicit int() cast is needed here).
        "LIST_SCHEMA_FIELDS": {"limit", "offset"},
    }

    def _tools_with_udfs(self) -> set[str]:
        return set(mcp_tools_module.__all__) - set(_TOOLS_WITHOUT_UDFS)

    def _params_needing_coercion(
        self, tool_name: str, category: str, udf_name: str
    ) -> list[str]:
        """Return param names of the given category that aren't intentionally simplified."""
        tool_fn = getattr(mcp_tools_module, tool_name)
        sig = inspect.signature(tool_fn)
        skipped = self._UDF_INTERFACE_SIMPLIFICATIONS.get(udf_name, set())
        return [
            name
            for name, param in sig.parameters.items()
            if param.annotation is not inspect.Parameter.empty
            and _classify_python_type(param.annotation) == category
            and name not in skipped
        ]

    def test_list_and_dict_params_use_json_loads(self) -> None:
        """UDFs whose tool takes List/Dict params must call json.loads in the body.

        List/Dict parameters arrive from Snowflake as JSON strings (e.g. '["a","b"]').
        The UDF body must deserialize them with json.loads before passing to the tool.
        """
        udfs = generate_all_udfs(include_mutations=True)
        errors: list[str] = []

        for tool_name in sorted(self._tools_with_udfs()):
            udf_name = _expected_udf_name(tool_name)
            if udf_name not in udfs:
                continue

            params = self._params_needing_coercion(tool_name, "list", udf_name)
            params += self._params_needing_coercion(tool_name, "dict", udf_name)
            if not params:
                continue

            python_body = _extract_python_body(udfs[udf_name])
            if "json.loads" not in python_body:
                errors.append(
                    f"{udf_name}: tool '{tool_name}' has List/Dict params "
                    f"{params} but UDF body contains no json.loads() call"
                )

        assert not errors, (
            "The following UDFs are missing json.loads for List/Dict parameters:\n"
            + "\n".join(f"  - {e}" for e in errors)
        )

    def test_int_params_use_int_coercion(self) -> None:
        """UDFs whose tool takes int params must coerce them with int() in the body.

        Snowflake NUMBER values may arrive as floats; int() ensures correct typing.
        """
        udfs = generate_all_udfs(include_mutations=True)
        errors: list[str] = []

        for tool_name in sorted(self._tools_with_udfs()):
            udf_name = _expected_udf_name(tool_name)
            if udf_name not in udfs:
                continue

            params = self._params_needing_coercion(tool_name, "int", udf_name)
            if not params:
                continue

            python_body = _extract_python_body(udfs[udf_name])
            if "int(" not in python_body:
                errors.append(
                    f"{udf_name}: tool '{tool_name}' has int params "
                    f"{params} but UDF body contains no int() coercion"
                )

        assert not errors, (
            "The following UDFs are missing int() coercions for int parameters:\n"
            + "\n".join(f"  - {e}" for e in errors)
        )

    def test_bool_params_use_bool_coercion(self) -> None:
        """UDFs whose tool takes bool params must coerce them with bool() in the body.

        Snowflake passes booleans as NUMBER (1/0); bool() converts them correctly.
        """
        udfs = generate_all_udfs(include_mutations=True)
        errors: list[str] = []

        for tool_name in sorted(self._tools_with_udfs()):
            udf_name = _expected_udf_name(tool_name)
            if udf_name not in udfs:
                continue

            params = self._params_needing_coercion(tool_name, "bool", udf_name)
            if not params:
                continue

            python_body = _extract_python_body(udfs[udf_name])
            if "bool(" not in python_body:
                errors.append(
                    f"{udf_name}: tool '{tool_name}' has bool params "
                    f"{params} but UDF body contains no bool() coercion"
                )

        assert not errors, (
            "The following UDFs are missing bool() coercions for bool parameters:\n"
            + "\n".join(f"  - {e}" for e in errors)
        )

    def test_each_argument_has_correct_coercion(self) -> None:
        """Per-argument check: each kwarg at the tool call site is correctly coerced.

        This is the fine-grained version of the body-level checks above. It walks the
        AST of every UDF, finds the call to the wrapped mcp_tools function, and for
        each keyword argument resolves one level of variable indirection before checking
        that the expression matches the expected coercion for the parameter's Python type:

          int   → expression must contain int(...)
          bool  → expression must contain bool(...)
          list  → resolved expression must contain json.loads(...)
          dict  → resolved expression must contain json.loads(...)
          str   → no coercion requirement (passed as-is or with None-guard)

        If a parameter type changes (e.g. int → str, or list added), the corresponding
        UDF kwarg won't have the right coercion and this test will catch it.
        """
        udfs = generate_all_udfs(include_mutations=True)
        errors: list[str] = []

        for tool_name in sorted(self._tools_with_udfs()):
            udf_name = _expected_udf_name(tool_name)
            if udf_name not in udfs:
                continue

            tool_fn = getattr(mcp_tools_module, tool_name)
            sig = inspect.signature(tool_fn)
            skipped = self._UDF_INTERFACE_SIMPLIFICATIONS.get(udf_name, set())

            python_body = _extract_python_body(udfs[udf_name])
            tree = ast.parse(python_body)
            assignments = _build_assignment_map(tree)

            # Find the call node for this tool function
            call_node = None
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == tool_name
                    and node.keywords  # has kwargs → it's the real tool call
                ):
                    call_node = node
                    break

            if call_node is None:
                # Tool called without kwargs (e.g. get_entities([urn])) — skip
                continue

            kwargs = {kw.arg: ast.unparse(kw.value) for kw in call_node.keywords}

            for param_name, param in sig.parameters.items():
                if param_name in skipped:
                    continue
                if param.annotation is inspect.Parameter.empty:
                    continue
                if param_name not in kwargs:
                    # Optional param not forwarded — fine
                    continue

                category = _classify_python_type(param.annotation)
                raw_expr = kwargs[param_name]
                # Follow one level of variable indirection; a var may be assigned
                # multiple times (try/except blocks), so check all its values.
                all_exprs = [raw_expr] + _resolve_expr(raw_expr, assignments)

                if category == "int" and not any("int(" in e for e in all_exprs):
                    errors.append(
                        f"{udf_name}: param '{param_name}' (int) passed as `{raw_expr}` "
                        f"— expected int() coercion"
                    )
                elif category == "bool" and not any("bool(" in e for e in all_exprs):
                    errors.append(
                        f"{udf_name}: param '{param_name}' (bool) passed as `{raw_expr}` "
                        f"— expected bool() coercion (Snowflake sends NUMBER 0/1)"
                    )
                elif category in ("list", "dict") and not any(
                    "json.loads" in e for e in all_exprs
                ):
                    errors.append(
                        f"{udf_name}: param '{param_name}' ({category}) passed as "
                        f"`{raw_expr}` (resolved: {_resolve_expr(raw_expr, assignments)}) "
                        f"— expected json.loads() coercion"
                    )

        assert not errors, (
            "The following UDF arguments have incorrect type coercions.\n"
            "If this is an intentional interface simplification, add the param name\n"
            "to _UDF_INTERFACE_SIMPLIFICATIONS for this UDF.\n\n"
            + "\n".join(f"  - {e}" for e in errors)
        )

    def test_get_entities_udf_wraps_single_urn_in_list(self) -> None:
        """GET_ENTITIES exposes one STRING param but the tool takes List[str].

        The UDF bridges this by calling get_entities([entity_urn]) — wrapping the
        single string in a list literal. This test verifies that contract so a change
        to get_entities(urns: str) (removing the List) would be caught here.
        """
        import datahub_agent_context.mcp_tools as m

        # The tool must still accept List[str] for the wrapping to be correct.
        sig = inspect.signature(m.get_entities)
        urns_param = sig.parameters["urns"]
        assert _classify_python_type(urns_param.annotation) == "list", (
            "get_entities 'urns' is no longer List[str]. "
            "Update the GET_ENTITIES UDF to match the new signature, then update "
            "this test and _UDF_INTERFACE_SIMPLIFICATIONS accordingly."
        )

        # The UDF body must call get_entities with a list literal wrapping the string param.
        udfs = generate_all_udfs(include_mutations=True)
        python_body = _extract_python_body(udfs["GET_ENTITIES"])
        tree = ast.parse(python_body)

        # Find get_entities([...]) — positional call with a list as first arg
        found_list_wrap = False
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "get_entities"
                and node.args
                and isinstance(node.args[0], ast.List)
            ):
                found_list_wrap = True
                break

        assert found_list_wrap, (
            "GET_ENTITIES UDF no longer calls get_entities([entity_urn]). "
            "If the tool signature changed, update the UDF body and this test."
        )
