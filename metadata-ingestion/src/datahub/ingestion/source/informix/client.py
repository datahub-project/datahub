import logging
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Protocol

import jpype  # type: ignore[import-untyped]
from jdk4py import JAVA_HOME

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.constants import (
    DRIVER_CLASS,
    SQL_COLUMNS,
    SQL_FK,
    SQL_PK,
    SQL_TABLES,
    SQL_VIEW_DEF,
    TABTYPE_VIEW,
)
from datahub.ingestion.source.informix.driver import resolve_driver_jars
from datahub.ingestion.source.informix.mapping import build_jdbc_url
from datahub.ingestion.source.informix.models import (
    ExtendedType,
    InformixColumn,
    InformixForeignKey,
    InformixTable,
)

logger = logging.getLogger(__name__)


@dataclass
class _FkBag:
    name: str
    parent_table: str
    parent_owner: str
    child_columns: List[str] = field(default_factory=list)
    parent_columns: List[str] = field(default_factory=list)


class InformixClientProtocol(Protocol):
    def get_tables(self) -> List[InformixTable]: ...

    def get_columns(self, table: InformixTable) -> List[InformixColumn]: ...

    def get_foreign_keys(self, table: InformixTable) -> List[InformixForeignKey]: ...

    def get_view_definition(self, table: InformixTable) -> Optional[str]: ...

    def close(self) -> None: ...


def _opt_str(value: object) -> Optional[str]:
    return str(value).strip() if value is not None else None


def _parse_extended_type(row: List[object]) -> Optional[ExtendedType]:
    # The sysxtdtypes outer joins yield SQL NULLs for a column with no extended
    # type (extended_id = 0), and for the source of anything but a DISTINCT
    # declared over another extended type.
    name = _opt_str(row[4])
    if name is None:
        return None
    return ExtendedType(name=name, mode=_opt_str(row[5]), source_name=_opt_str(row[6]))


def _safe_close(closeable: object) -> None:
    try:
        closeable.close()  # type: ignore[attr-defined]
    except Exception as e:
        logger.warning("Error closing JDBC resource: %s", e)


def sanitize_informix_error(
    error: BaseException, config: InformixSourceConfig, stage: str
) -> str:
    """Build a failure message that never includes the JDBC URL or password.

    ``build_jdbc_url`` embeds the password in cleartext and JVM SQLExceptions can
    echo the full URL, so ``str(error)`` is never safe to surface.
    """
    detail = ""
    try:
        detail = f" (SQLSTATE={error.getSQLState()}, code={error.getErrorCode()})"  # type: ignore[attr-defined]
    except Exception:
        detail = f" ({type(error).__name__})"
    return (
        f"Informix {stage} failed for server '{config.server}' at "
        f"{config.host_port}, database '{config.database}'.{detail}"
    )


class InformixClient:
    """JDBC client for Informix system-catalog reads via JPype."""

    def __init__(self, config: InformixSourceConfig) -> None:
        jars = resolve_driver_jars(config)

        os.environ.setdefault("JAVA_HOME", str(JAVA_HOME))

        if not jpype.isJVMStarted():
            jpype.startJVM(classpath=jars)
        else:
            for jar in jars:
                jpype.addClassPath(jar)

        driver_manager = jpype.JClass("java.sql.DriverManager")
        jpype.JClass(DRIVER_CLASS)  # force-load the Informix driver
        try:
            self._conn = driver_manager.getConnection(build_jdbc_url(config))
        except Exception as e:
            # Never log str(e) or the URL itself -- both can carry the cleartext
            # password. The exception type plus SQLSTATE is the most that is safe.
            message = sanitize_informix_error(e, config, "connection")
            logger.debug("%s", message)
            raise ConfigurationError(message) from None

    def _query(self, sql: str, params: List[str]) -> List[List[object]]:
        stmt = self._conn.prepareStatement(sql)
        try:
            for i, p in enumerate(params, start=1):
                stmt.setString(i, p)
            rs = stmt.executeQuery()
            try:
                meta = rs.getMetaData()
                n = meta.getColumnCount()
                rows: List[List[object]] = []
                while rs.next():
                    rows.append([rs.getObject(i) for i in range(1, n + 1)])
                return rows
            finally:
                _safe_close(rs)
        finally:
            _safe_close(stmt)

    @staticmethod
    def _parse_table_row(row: List[object]) -> InformixTable:
        # nrows is -1 or 0 when Informix hasn't computed a row estimate yet.
        # systables.nrows is catalogued as FLOAT, so the JDBC driver can
        # return values like "2.0"; go through float() before int().
        nrows = int(float(str(row[3]))) if row[3] is not None else 0
        return InformixTable(
            name=str(row[0]).strip(),
            owner=str(row[1]).strip(),
            is_view=str(row[2]).strip() == TABTYPE_VIEW,
            nrows=nrows if nrows > 0 else None,
        )

    def get_tables(self) -> List[InformixTable]:
        tables: List[InformixTable] = []
        for r in self._query(SQL_TABLES, []):
            try:
                tables.append(self._parse_table_row(r))
            except Exception as e:
                # One corrupt systables row must not abort the whole catalog scan.
                logger.warning("Skipping malformed systables row %r: %s", r, e)
        return tables

    def get_columns(self, table: InformixTable) -> List[InformixColumn]:
        pk_rows = self._query(SQL_PK, [table.name, table.owner])
        pk_names = {str(r[0]).strip() for r in pk_rows}
        rows = self._query(SQL_COLUMNS, [table.name, table.owner])
        return [
            InformixColumn(
                name=str(r[0]).strip(),
                coltype=int(str(r[1])),
                length=int(str(r[2])),
                colno=int(str(r[3])),
                is_pk=str(r[0]).strip() in pk_names,
                extended=_parse_extended_type(r),
            )
            for r in rows
        ]

    def get_foreign_keys(self, table: InformixTable) -> List[InformixForeignKey]:
        rows = self._query(SQL_FK, [table.name, table.owner])
        # Accumulate into mutable bags first; InformixForeignKey rejects unequal
        # child/parent lengths at construction, and the ABS(partN) join can yield
        # a cross-product that leaves the lists misaligned.
        pending: Dict[str, _FkBag] = {}
        for r in rows:
            fkname = str(r[0]).strip()
            child_col = str(r[1]).strip()
            parent_table = str(r[2]).strip()
            parent_owner = str(r[3]).strip()
            parent_col = str(r[4]).strip()
            if fkname not in pending:
                pending[fkname] = _FkBag(
                    name=fkname,
                    child_columns=[],
                    parent_table=parent_table,
                    parent_owner=parent_owner,
                    parent_columns=[],
                )
            bag = pending[fkname]
            # Dedup-by-first-seen pairs single-column FKs exactly; composite keys
            # remain best-effort (see source warning).
            if child_col not in bag.child_columns:
                bag.child_columns.append(child_col)
            if parent_col not in bag.parent_columns:
                bag.parent_columns.append(parent_col)

        result: List[InformixForeignKey] = []
        for bag in pending.values():
            if len(bag.child_columns) != len(bag.parent_columns):
                logger.warning(
                    "Skipping foreign key %s on %s.%s: mismatched column counts "
                    "child=%s parent=%s",
                    bag.name,
                    table.owner,
                    table.name,
                    len(bag.child_columns),
                    len(bag.parent_columns),
                )
                continue
            result.append(
                InformixForeignKey(
                    name=bag.name,
                    child_columns=list(bag.child_columns),
                    parent_table=bag.parent_table,
                    parent_owner=bag.parent_owner,
                    parent_columns=list(bag.parent_columns),
                )
            )
        return result

    def get_view_definition(self, table: InformixTable) -> Optional[str]:
        rows = self._query(SQL_VIEW_DEF, [table.name, table.owner])
        chunks = [str(r[0]) for r in rows if r[0] is not None]
        return "".join(chunks) if chunks else None

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception as e:
            logger.warning("Error closing Informix connection: %s", e)
