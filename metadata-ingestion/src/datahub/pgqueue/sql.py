"""Helpers for safely quoting PostgreSQL identifiers used in pgQueue paths."""

from __future__ import annotations

import re

_IDENTIFIER_UNQUOTED = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_pg_identifier(name: str, field_name: str) -> str:
    """Restrict schema/table identifiers to unquoted-safe tokens."""
    if not _IDENTIFIER_UNQUOTED.fullmatch(name):
        raise ValueError(
            f"{field_name} must match {_IDENTIFIER_UNQUOTED.pattern} (got {name!r})"
        )
    return name


def quote_ident(ident: str) -> str:
    """Quote a PostgreSQL identifier (duplicate internal double-quotes)."""
    validate_pg_identifier(ident, "identifier")
    return '"' + ident.replace('"', '""') + '"'


def qualified_table(schema: str, table_prefix: str, suffix: str) -> str:
    """Return `"schema"."prefix_suffix"` for DDL-derived table names."""
    validate_pg_identifier(schema, "schema")
    validate_pg_identifier(table_prefix, "table_prefix")
    validate_pg_identifier(suffix, "suffix")
    return f"{quote_ident(schema)}.{quote_ident(f'{table_prefix}_{suffix}')}"
