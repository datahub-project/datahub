"""
Jinja2-based SQL resolver for LookML template expressions.

Replaces the python-liquid transformer pipeline from the old looker/ source.
Two-step process:
  1. Regex preprocessing — converts LookML-specific syntax to valid Jinja2
     (${TABLE}, @{constant}, ${view.field}, if-comments)
  2. Jinja2 rendering — resolves liquid variables, custom Looker tags
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List

import jinja2
import jinja2.ext
import jinja2.nodes
import jinja2.parser

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Jinja2 extensions for Looker-specific liquid tags
# ---------------------------------------------------------------------------


class _ConditionExtension(jinja2.ext.Extension):
    """Handle {% condition %} / {% endcondition %} Looker tags by stripping them."""

    tags = {"condition", "endcondition"}

    def parse(self, parser: jinja2.parser.Parser) -> jinja2.nodes.Output:
        lineno = next(parser.stream).lineno
        # Skip all tokens until the block end
        while parser.stream.current.test("block_end") is False:
            next(parser.stream)
        return jinja2.nodes.Output([], lineno=lineno)


class _IncrementConditionExtension(jinja2.ext.Extension):
    """Handle {% incrementcondition %} Looker tags by stripping them."""

    tags = {"incrementcondition", "endincrementcondition"}

    def parse(self, parser: jinja2.parser.Parser) -> jinja2.nodes.Output:
        lineno = next(parser.stream).lineno
        while parser.stream.current.test("block_end") is False:
            next(parser.stream)
        return jinja2.nodes.Output([], lineno=lineno)


def _make_jinja2_env() -> jinja2.Environment:
    env = jinja2.Environment(
        undefined=jinja2.Undefined,  # silently ignore unknown vars (like liquid)
        extensions=[
            "jinja2.ext.do",
            _ConditionExtension,
            _IncrementConditionExtension,
        ],
    )
    # Liquid → Jinja2 filter name aliases
    env.filters["upcase"] = lambda s: str(s).upper()
    env.filters["downcase"] = lambda s: str(s).lower()
    env.filters["size"] = len
    env.filters["strip"] = lambda s: str(s).strip()
    env.filters["lstrip"] = lambda s: str(s).lstrip()
    env.filters["rstrip"] = lambda s: str(s).rstrip()
    env.filters["prepend"] = lambda s, prefix: f"{prefix}{s}"
    env.filters["append"] = lambda s, suffix: f"{s}{suffix}"
    env.filters["remove"] = lambda s, sub: str(s).replace(sub, "")
    env.filters["truncate"] = lambda s, n: str(s)[:n]
    env.filters["date"] = lambda s, fmt="": s  # no-op for SQL context
    return env


_JINJA2_ENV = _make_jinja2_env()


# ---------------------------------------------------------------------------
# Pre-processing — LookML-specific syntax → valid Jinja2
# ---------------------------------------------------------------------------

# ${TABLE} → __TABLE__ (placeholder; not a real Jinja2 variable)
_TABLE_PATTERN = re.compile(r"\$\{TABLE\}", re.IGNORECASE)

# ${view_name.field_name} → {{view_name__field_name}} (Jinja2 safe variable)
_VIEW_FIELD_PATTERN = re.compile(r"\$\{(\w+)\.(\w+)\}")

# @{constant_name} → constant value (resolved via constants dict)
_CONSTANT_PATTERN = re.compile(r"@\{(\w+)\}")

# -- if prod -- ... -- endif -- style conditional comments
_IF_PROD_PATTERN = re.compile(
    r"--\s*if\s+prod\s*--(.+?)--\s*end\s*if\s*--",
    re.IGNORECASE | re.DOTALL,
)
_IF_DEV_PATTERN = re.compile(
    r"--\s*if\s+dev\s*--(.+?)--\s*end\s*if\s*--",
    re.IGNORECASE | re.DOTALL,
)

# {% unless %} → {% if not %}
_UNLESS_PATTERN = re.compile(r"\{%-?\s*unless\s+(.+?)\s*-?%\}")
_ENDUNLESS_PATTERN = re.compile(r"\{%-?\s*endunless\s*-?%\}")

# {% assign var = value %} → {% set var = value %}
_ASSIGN_PATTERN = re.compile(r"\{%-?\s*assign\s+(.+?)\s*-?%\}")


def _preprocess(sql: str, constants: Dict[str, str], environment: str) -> str:
    """Convert LookML-specific syntax to valid Jinja2."""

    # Resolve @{constant} references
    def _resolve_constant(m: re.Match) -> str:
        name = m.group(1)
        return constants.get(name, f"@{{{name}}}")

    sql = _CONSTANT_PATTERN.sub(_resolve_constant, sql)

    # ${view.field} → preserved as a readable placeholder (not a real Jinja2 expr)
    sql = _VIEW_FIELD_PATTERN.sub(r"${{\1.\2}}", sql)

    # ${TABLE} → ${TABLE} preserved (already handled above if needed)
    # Keep as-is; it gets passed through Jinja2 unchanged because it uses $ prefix
    # which Jinja2 doesn't interpret as a template expression.

    # Environment-based if-comment blocks
    if environment == "prod":
        sql = _IF_PROD_PATTERN.sub(r"\1", sql)
        sql = _IF_DEV_PATTERN.sub("", sql)
    else:
        sql = _IF_DEV_PATTERN.sub(r"\1", sql)
        sql = _IF_PROD_PATTERN.sub("", sql)

    # {% unless %} → {% if not %}
    sql = _UNLESS_PATTERN.sub(r"{%- if not \1 -%}", sql)
    sql = _ENDUNLESS_PATTERN.sub("{%- endif -%}", sql)

    # {% assign %} → {% set %}
    sql = _ASSIGN_PATTERN.sub(r"{%- set \1 -%}", sql)

    return sql


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------


@dataclass
class LookMLSQLResolver:
    """
    Resolves LookML SQL expressions containing Liquid template syntax.

    Usage:
        resolver = LookMLSQLResolver(
            template_variables={"user_attribute": "value"},
            constants={"my_schema": "prod_schema"},
            environment="prod",
        )
        resolved_sql = resolver.resolve("SELECT * FROM ${TABLE} -- if prod -- WHERE 1=1 -- end if --")
    """

    template_variables: Dict[str, Any] = field(default_factory=dict)
    constants: Dict[str, str] = field(default_factory=dict)
    environment: str = "prod"

    def resolve(self, sql: str) -> str:
        """
        Resolve LookML template expressions in a SQL string.

        Returns the original sql unchanged if resolution fails, so callers
        always get usable SQL even when templates contain unknown variables.
        """
        if not sql:
            return sql
        # Quick exit: no template syntax present (check all patterns)
        if (
            "{%" not in sql
            and "{{" not in sql
            and "@{" not in sql
            and "-- if " not in sql.lower()
        ):
            return sql
        try:
            preprocessed = _preprocess(sql, self.constants, self.environment)
            template = _JINJA2_ENV.from_string(preprocessed)
            return template.render(self.template_variables)
        except Exception as e:
            logger.warning(
                f"LookMLSQLResolver failed to render template, "
                f"returning unresolved SQL which may produce incorrect lineage: {e}"
            )
            return sql

    def resolve_dict_values(self, d: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
        """Resolve template expressions in specific keys of a dict, returning a copy."""
        result = dict(d)
        for key in keys:
            if key in result and isinstance(result[key], str):
                result[key] = self.resolve(result[key])
        return result
