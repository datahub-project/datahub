"""Shared utility functions for assertion evaluators."""

import base64
import re
from typing import Any, Dict, Optional, Tuple

from datahub.metadata.urns import (
    AssertionUrn,
    DataHubMetricCubeUrn,
    DatasetUrn,
    MonitorUrn,
)
from jinja2 import Environment, StrictUndefined, UndefinedError

from datahub_executor.common.exceptions import InvalidParametersException
from datahub_executor.common.types import (
    Assertion,
    AssertionType,
)

__all__: Tuple[str, ...] = (
    "ASSERTION_TYPES_REQUIRING_TRAINING",
    "is_training_required",
    "is_field_metric_assertion",
    "encode_monitor_urn",
    "default_volume_assertion_urn",
    "default_volume_monitor_urn",
    "make_monitor_metric_cube_urn",
    "apply_runtime_parameters",
    "render_sql_template",
)

ASSERTION_TYPES_REQUIRING_TRAINING: Tuple[AssertionType, ...] = (
    AssertionType.VOLUME,
    AssertionType.FRESHNESS,
    AssertionType.FIELD,
    AssertionType.SQL,
)


def is_training_required(
    assertion: Assertion,
) -> bool:
    # Currently, we'll assume that all smart assertions
    # require some training.
    return (
        assertion.type in ASSERTION_TYPES_REQUIRING_TRAINING and assertion.is_inferred
    )


def is_field_metric_assertion(
    assertion: Assertion,
) -> bool:
    # Currently, we'll assume that all smart assertions
    # require some training.
    return (
        assertion.field_assertion is not None
        and assertion.field_assertion.field_metric_assertion is not None
    )


def encode_monitor_urn(urn: str) -> str:
    """Encodes a URN using Base64 URL encoding in a reversible way."""
    encoded_bytes = base64.urlsafe_b64encode(urn.encode("utf-8"))
    return encoded_bytes.decode("utf-8")


# TODO: Determine whether we should move the following methods.


def default_volume_assertion_urn(dataset_urn: str) -> str:
    assert DatasetUrn.from_string(dataset_urn)
    return AssertionUrn(f"{encode_monitor_urn(dataset_urn)}-__system__volume").urn()


def default_volume_monitor_urn(dataset_urn: str) -> str:
    assert DatasetUrn.from_string(dataset_urn)
    return MonitorUrn(dataset_urn, "__system__volume").urn()


# Shared helper for evaluators to compute the Metric Cube URN
def make_monitor_metric_cube_urn(monitor_urn: str) -> str:
    """
    Build the DataHub Metric Cube URN for a given monitor URN.
    """
    assert MonitorUrn.from_string(monitor_urn)
    return DataHubMetricCubeUrn(encode_monitor_urn(monitor_urn)).urn()


def _create_jinja_env() -> Environment:
    """
    Create a Jinja2 Environment configured for SQL template substitution.

    A fresh Environment is created per call to avoid potential race conditions
    during concurrent template compilation. This is intentional for thread safety.

    WARNING - SQL Injection Prevention:
        This function creates an environment for string substitution, NOT
        parameterized queries. Values are inserted directly into SQL without
        any escaping. Callers MUST provide pre-quoted and properly escaped
        values for their SQL dialect.

        UNSAFE (vulnerable to SQL injection):
            {"city": "Chicago"}              # Missing quotes
            {"name": "O'Brien"}              # Unescaped quote
            {"id": "1; DROP TABLE users"}   # SQL injection

        SAFE (properly quoted):
            {"city": "'Chicago'"}            # String literal with quotes
            {"name": "'O''Brien'"}           # Escaped single quote (SQL standard)
            {"id": "1"}                      # Numeric literal (no quotes)
            {"ids": "(SELECT id FROM users WHERE active = true)"}  # SQL subquery

        For user-provided input, use your database driver's parameterized
        queries instead of this template mechanism.

    Security Note:
        This Environment is used exclusively for SQL string substitution, NOT for
        HTML rendering. There is no HTML context where XSS could occur. Auto-escaping
        is intentionally disabled (autoescape=False) because HTML escaping would
        corrupt SQL syntax. StrictUndefined ensures missing variables raise
        errors rather than silently rendering as empty strings.
    """
    # autoescape=False is safe here: this is SQL templating, not HTML.
    # XSS is not applicable in a SQL-only context.
    return Environment(
        variable_start_string="${",
        variable_end_string="}",
        autoescape=False,  # Safe: SQL context only, no HTML rendering
        undefined=StrictUndefined,
    )


def _extract_variable_name(error_msg: str) -> Optional[str]:
    """Extract variable name from Jinja2 UndefinedError message.

    Args:
        error_msg: The Jinja2 error message

    Returns:
        The variable name, or None if not found

    Example:
        >>> _extract_variable_name("'start' is undefined")
        'start'
    """
    # Jinja2 format: "'variable_name' is undefined"
    match = re.match(r"'([^']+)' is undefined", error_msg)
    return match.group(1) if match else None


def apply_runtime_parameters(
    text: str, runtime_parameters: Optional[Dict[str, Any]]
) -> str:
    """
    Render `${var}` placeholders using the Jinja2 templating engine.

    Supports templates that contain `${name}` placeholders by configuring
    Jinja2 to use `${` and `}` as variable delimiters.

    WARNING - SQL Injection Prevention:
        This function performs string substitution, NOT parameterized queries.
        Values are inserted directly into SQL without escaping. Callers MUST
        provide pre-quoted and properly escaped values for their SQL dialect.

        Auto-escaping is intentionally disabled because this is for SQL
        template substitution, not HTML rendering. HTML escaping would corrupt
        SQL syntax.

        Examples by SQL data type:

        String literals (MUST be quoted):
            UNSAFE: {"city": "Chicago"}
            SAFE:   {"city": "'Chicago'"}

        String with quotes (MUST be escaped):
            UNSAFE: {"name": "O'Brien"}
            SAFE:   {"name": "'O''Brien'"}  # SQL standard: double the quote

        Numeric literals (no quotes):
            SAFE: {"count": "42"}
            SAFE: {"price": "19.99"}

        SQL keywords and expressions:
            SAFE: {"order": "ASC"}
            SAFE: {"condition": "status = 'active'"}

        SQL subqueries:
            SAFE: {"ids": "(SELECT id FROM users WHERE active = true)"}

        For user-provided input, use your database driver's parameterized
        queries instead of this template mechanism to prevent SQL injection.

    Args:
        text: The template string containing `${var}` placeholders.
        runtime_parameters: Dictionary mapping variable names to their values.
            Values should be pre-quoted and escaped as appropriate for SQL.

    Returns:
        The rendered string with all placeholders substituted.

    Raises:
        InvalidParametersException: If any placeholder in the text is not
            provided in runtime_parameters. The exception includes diagnostic
            information: the template, provided parameters, and missing variable.

    Example:
        >>> apply_runtime_parameters(
        ...     "SELECT * FROM T WHERE dt >= ${start}",
        ...     {"start": "'2024-10-01'"}
        ... )
        "SELECT * FROM T WHERE dt >= '2024-10-01'"
    """
    # Create a fresh Jinja2 Environment per call for thread safety
    env = _create_jinja_env()

    # If no runtime parameters provided, use empty dict (Jinja2 will raise UndefinedError
    # if any variables are referenced, which we'll catch and convert)
    if runtime_parameters is None:
        runtime_parameters = {}

    try:
        template = env.from_string(text)
        return template.render(runtime_parameters)
    except UndefinedError as e:
        # Jinja2's UndefinedError with StrictUndefined provides a clear message
        # like "'start' is undefined", which we use directly.
        raise InvalidParametersException(
            message=f"Missing required SQL runtime parameter: {e}",
            parameters={
                "error": str(e),
                "template": text,
                "provided_parameters": list(runtime_parameters.keys())
                if runtime_parameters
                else [],
                "missing_variable": _extract_variable_name(str(e)),
            },
        ) from e


def render_sql_template(
    text: str,
    runtime_parameters: Optional[Dict[str, Any]],
    *,
    require_nonempty: bool = False,
) -> str:
    """
    Render a SQL fragment with parameter substitution and cleanup.

    Applies runtime parameter substitution via `apply_runtime_parameters`,
    then strips leading/trailing whitespace and removes trailing semicolons.
    This is useful for preparing SQL subqueries or fragments that will be
    embedded in larger queries.

    WARNING - SQL Injection Prevention:
        This function performs string substitution using `apply_runtime_parameters`,
        NOT parameterized queries. See `apply_runtime_parameters` documentation
        for detailed security guidance and examples of safe vs unsafe usage.

        Callers MUST provide pre-quoted and properly escaped values for their
        SQL dialect to prevent SQL injection vulnerabilities.

    Args:
        text: The SQL template string containing `${var}` placeholders.
        runtime_parameters: Dictionary mapping variable names to their values.
            Values should be pre-quoted and escaped as appropriate for SQL.
        require_nonempty: If True, raises an exception when the rendered and
            cleaned result is empty. Defaults to False.

    Returns:
        The rendered and cleaned SQL string.

    Raises:
        InvalidParametersException: If any placeholder in the text is not
            provided in runtime_parameters, or if require_nonempty is True
            and the result is empty after cleaning. The exception includes
            diagnostic information to aid debugging.

    Example:
        >>> render_sql_template(
        ...     "SELECT id FROM users WHERE active = ${active};",
        ...     {"active": "true"},
        ...     require_nonempty=True
        ... )
        "SELECT id FROM users WHERE active = true"
    """
    rendered = apply_runtime_parameters(text, runtime_parameters)
    cleaned = rendered.strip().rstrip(";")
    if require_nonempty and not cleaned:
        raise InvalidParametersException(
            message="Invalid SQL provided after templating; result must not be empty",
            parameters={
                "original_template": text,
                "rendered_result": rendered,
                "cleaned_result": cleaned,
                "provided_parameters": list(runtime_parameters.keys())
                if runtime_parameters
                else [],
            },
        )
    return cleaned
