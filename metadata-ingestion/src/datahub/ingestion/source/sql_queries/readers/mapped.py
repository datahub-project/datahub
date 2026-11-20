import logging
from abc import abstractmethod
from collections import Counter
from typing import Any, Dict, Generic, Iterable, Optional

from datahub.ingestion.source.sql_queries.config import MappedQuerySourceConfig
from datahub.ingestion.source.sql_queries.query_entry import QueryEntry
from datahub.ingestion.source.sql_queries.readers.base import (
    QueryLogReader,
    RecordT,
    guard_read_errors,
    tolerant_query_entries,
)
from datahub.ingestion.source.sql_queries.report import SqlQueriesSourceReport

logger = logging.getLogger(__name__)


def _first_if_list(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    return value


def lookup_path(record: Any, path: str) -> Any:
    """The value at a dotted path in a nested record, or None if it is absent.

    A top-level key that itself contains the dots is matched first, since log
    shippers often write flattened keys like "db.statement". A path through an
    array takes the array's first element.
    """
    if isinstance(record, dict) and path in record:
        return _first_if_list(record[path])
    current = record
    for part in path.split("."):
        current = _first_if_list(current)
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return _first_if_list(current)


def coerce_text(value: Any) -> Any:
    """Decode bytes to text, leaving other types untouched.

    SQLAlchemy returns bytes for BLOB/VARBINARY columns, the usual type for
    MySQL/MariaDB general_log and audit SQL text. str() on bytes yields a
    "b'...'" repr rather than the statement, and makes an empty b"" look
    non-empty.
    """
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    return value


def _as_optional_str(value: Any) -> Optional[str]:
    return None if value is None else str(value)


class MappedQueryLogReader(QueryLogReader, Generic[RecordT]):
    """Base for backends whose records are mapped onto query fields by config.

    Subclasses read raw records and look up a configured path in one. This class
    turns records into QueryEntry, skips records with no SQL, reports read
    errors, and warns when a mapped field is missing from every record, which
    almost always means the mapping names the wrong field.
    """

    def __init__(
        self, config: MappedQuerySourceConfig, report: SqlQueriesSourceReport
    ) -> None:
        self.config = config
        self.report = report
        self._records_with_query = 0
        self._missing_fields: Counter = Counter()

    @abstractmethod
    def _read_records(self) -> Iterable[RecordT]:
        """Yield raw records. Implement as a generator, so connection errors
        surface on the first read, where they are reported."""
        raise NotImplementedError

    @abstractmethod
    def _lookup(self, record: RecordT, path: str) -> Any:
        raise NotImplementedError

    @abstractmethod
    def _context_of(self, record: RecordT) -> Optional[str]:
        """Identifies a record in warnings, so a bad one can be found."""
        raise NotImplementedError

    def get_query_entries(self) -> Iterable[QueryEntry]:
        logger.info(f"Reading query records from {self.config.label}")
        self._records_with_query = 0
        self._missing_fields.clear()
        records = guard_read_errors(self._read_records(), self._report_read_error)
        yield from tolerant_query_entries(
            records, self._to_entry, self.report, self._context_of
        )
        self._report_unmapped_fields()

    def _report_read_error(self, e: Exception) -> None:
        self.report.failure(
            title="Query source read error",
            message="Failed to read query records from the query_source backend. "
            "Check the connection settings and credentials.",
            context=self.config.label,
            exc=e,
        )

    def _to_entry(self, record: RecordT) -> Optional[QueryEntry]:
        mappings = self.config.field_mappings
        raw_sql = coerce_text(self._lookup(record, mappings.query))
        if raw_sql is None or not str(raw_sql).strip():
            return None
        self._records_with_query += 1

        values: Dict[str, Any] = {}
        for name, path in mappings.mapped_optional_fields().items():
            value = coerce_text(self._lookup(record, path))
            if value is None:
                self._missing_fields[name] += 1
            values[name] = value

        sql = str(raw_sql)
        if self.config.unescape_sql:
            # Audit logs often store SQL with escaped quotes; undo so it parses.
            sql = sql.replace("\\'", "'").replace('\\"', '"')

        # Blank actors are dropped so a system row with no user doesn't become a
        # domain-only "@<email_domain>" corp user.
        user = values.get("user")
        user_str = str(user).strip() if user is not None else ""
        if user_str and self.config.email_domain and "@" not in user_str:
            user_str = f"{user_str}@{self.config.email_domain}"

        # Records carry no table-name lineage hints, so no QueryEntryContext is
        # needed.
        return QueryEntry.model_validate(
            {
                "query": sql,
                "timestamp": values.get("timestamp"),
                "user": user_str or None,
                "session_id": _as_optional_str(values.get("session_id")),
                "default_db": _as_optional_str(values.get("default_db")),
                "default_schema": _as_optional_str(values.get("default_schema")),
            }
        )

    def _report_unmapped_fields(self) -> None:
        if not self._records_with_query:
            return
        mapped = self.config.field_mappings.mapped_optional_fields()
        for name, missing in self._missing_fields.items():
            if missing == self._records_with_query:
                self.report.warning(
                    title="Mapped field never found",
                    message=f"field_mappings.{name} is set to '{mapped[name]}', but "
                    "no record with SQL had a value there, so it is empty for "
                    "every query. Check the field name and its case.",
                    context=self.config.label,
                )
