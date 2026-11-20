import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ArgumentError

from datahub.ingestion.source.sql_queries.config import SqlTableQuerySourceConfig
from datahub.ingestion.source.sql_queries.readers.mapped import (
    MappedQueryLogReader,
    coerce_text,
)
from datahub.ingestion.source.sql_queries.report import SqlQueriesSourceReport

logger = logging.getLogger(__name__)

CONTEXT_SQL_PREVIEW_CHARS = 200


@dataclass(frozen=True)
class Row:
    number: int
    values: Dict[str, Any]


class SqlTableQueryLogReader(MappedQueryLogReader[Row]):
    """Reads query records from a relational database via SQLAlchemy.

    Rows with no SQL in the mapped query column are skipped, mirroring the file
    and Elasticsearch readers.
    """

    def __init__(
        self, config: SqlTableQuerySourceConfig, report: SqlQueriesSourceReport
    ) -> None:
        super().__init__(config, report)
        self.sql_config = config
        self._column_for_path: Dict[str, str] = {}

    def _create_engine(self) -> Engine:
        connection = self.sql_config.connection
        try:
            return create_engine(connection.get_sql_alchemy_url(), **connection.options)
        except ArgumentError:
            # SQLAlchemy echoes the whole URL, password included, when it can't
            # parse it, so don't let that message reach the logs or the report.
            raise ValueError(
                "Could not parse the sql_table connection URL. Check "
                "connection.sqlalchemy_uri, or the scheme and host_port fields."
            ) from None

    def _read_records(self) -> Iterable[Row]:
        engine = self._create_engine()
        try:
            with engine.connect() as conn:
                # stream_results + yield_per keep a server-side cursor so a large
                # audit table isn't buffered into memory at once.
                result = conn.execution_options(stream_results=True).execute(
                    text(self.sql_config.query)
                )
                self._resolve_columns(list(result.keys()))
                rows = result.mappings().yield_per(self.sql_config.batch_size)
                for number, mapping in enumerate(rows, start=1):
                    yield Row(number=number, values=dict(mapping))
        finally:
            engine.dispose()

    def _resolve_columns(self, columns: List[str]) -> None:
        """Map each configured column name to the column the query returned.

        Databases differ in how they case unquoted identifiers (Snowflake and
        Oracle return them upper-case, SQLAlchemy may lower-case them), so a
        case-insensitive match is accepted when it is unambiguous. Anything else
        fails the run, since a missing column would otherwise silently leave that
        field empty for every record.
        """
        mappings = self.sql_config.field_mappings
        mapped = {"query": mappings.query, **mappings.mapped_optional_fields()}
        by_lower: Dict[str, List[str]] = {}
        for column in columns:
            by_lower.setdefault(column.lower(), []).append(column)

        self._column_for_path = {}
        for name, path in mapped.items():
            if path in columns:
                self._column_for_path[path] = path
                continue
            matches = by_lower.get(path.lower(), [])
            if len(matches) != 1:
                raise ValueError(
                    f"field_mappings.{name} is '{path}', but the query returns no "
                    f"single column of that name. Columns returned: "
                    f"{', '.join(columns)}"
                )
            self._column_for_path[path] = matches[0]

    def _lookup(self, record: Row, path: str) -> Any:
        return record.values.get(self._column_for_path.get(path, path))

    def _context_of(self, record: Row) -> Optional[str]:
        sql = coerce_text(self._lookup(record, self.sql_config.field_mappings.query))
        preview = str(sql)[:CONTEXT_SQL_PREVIEW_CHARS] if sql is not None else ""
        return f"row {record.number}: {preview}"
