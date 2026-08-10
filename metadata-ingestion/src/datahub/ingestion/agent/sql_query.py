from dataclasses import dataclass, field
from typing import Dict, List, Protocol, runtime_checkable

from datahub.ingestion.agent.sql_gate import check_query_scope


@dataclass
class SqlRows:
    """One catalog result set: the column names, and rows as positional values.

    Rows are positional rather than a list of dicts so a wide result does not
    repeat every column name on every row -- probe output is read by an agent
    with a finite context window.
    """

    columns: List[str]
    rows: List[List[object]]


@runtime_checkable
class SqlQueryProvider(Protocol):
    """A probe provider that can answer a catalog query.

    Implementations must NOT expose execute_sql as an @probe_method: that would
    put a raw-SQL parameter on `probe run`, reaching the engine without passing
    check_query_scope. The only supported route to execute_sql is
    execute_scoped_sql below.
    """

    # The platform name to resolve a sqlglot dialect from -- the connector's
    # own name for itself, not SQLAlchemy's (see SqlAlchemyMetadataProbe).
    sql_dialect: str

    def execute_sql(self, query: str, limit: int) -> SqlRows:
        """Run an already-scope-checked query, reading at most `limit` rows."""
        ...


@dataclass
class SqlQueryResult:
    source_type: str
    sql: str
    dialect: str
    columns: List[str]
    rows: List[List[object]]
    truncated: bool
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        return {
            "source_type": self.source_type,
            "sql": self.sql,
            "dialect": self.dialect,
            "columns": self.columns,
            "rows": self.rows,
            "row_count": len(self.rows),
            "truncated": self.truncated,
            "warnings": self.warnings,
        }


_JSON_SAFE_TYPES = (str, int, float, bool)


def _json_safe(value: object) -> object:
    # Catalog reads return dates, decimals, UUIDs and (on some drivers) bytes.
    # Coercing here keeps every caller free of a custom JSON encoder.
    if value is None or isinstance(value, _JSON_SAFE_TYPES):
        return value
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def execute_scoped_sql(
    provider: object, source_type: str, sql: str, limit: int
) -> SqlQueryResult:
    """Scope-check `sql`, then run it against an already-open provider.

    The check runs before the provider is touched, so a refused query never
    reaches the connector, let alone the database.
    """
    if not isinstance(provider, SqlQueryProvider):
        raise ValueError(
            f"source '{source_type}' does not support SQL probing; "
            f"use `probe methods` to see what it does offer"
        )

    dialect = provider.sql_dialect
    check_query_scope(sql, platform=dialect)

    # One row beyond the limit, so truncation is observed rather than inferred
    # from a full page.
    fetched = provider.execute_sql(sql, limit + 1)
    rows = [[_json_safe(value) for value in row] for row in fetched.rows[:limit]]

    provider_warnings = getattr(provider, "warnings", None)
    return SqlQueryResult(
        source_type=source_type,
        sql=sql,
        dialect=dialect,
        columns=list(fetched.columns),
        rows=rows,
        truncated=len(fetched.rows) > limit,
        warnings=list(provider_warnings) if provider_warnings else [],
    )


def run_probe_sql(
    source_type: str, config_dict: Dict[str, object], sql: str, limit: int
) -> SqlQueryResult:
    from datahub.ingestion.agent.probe_methods import config_class_for

    config_cls = config_class_for(source_type)
    if config_cls is None:
        raise ValueError(f"source '{source_type}' has no probe configuration")
    config = config_cls.model_validate(config_dict)

    build = getattr(config, "build_probe_provider", None)
    if build is None:
        raise ValueError(f"source '{source_type}' does not support SQL probing")

    with build() as provider:
        return execute_scoped_sql(provider, source_type, sql, limit)
