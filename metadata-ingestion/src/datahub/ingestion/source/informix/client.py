import logging
import os
from typing import Dict, List, Optional, Protocol

from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.constants import (
    SQL_COLUMNS,
    SQL_FK,
    SQL_PK,
    SQL_TABLES,
    SQL_VIEW_DEF,
)
from datahub.ingestion.source.informix.driver import resolve_driver_jars
from datahub.ingestion.source.informix.mapping import build_jdbc_url
from datahub.ingestion.source.informix.models import (
    InformixColumn,
    InformixForeignKey,
    InformixTable,
)

logger = logging.getLogger(__name__)

_DRIVER_CLASS = "com.informix.jdbc.IfxDriver"


class InformixClientProtocol(Protocol):
    def get_tables(self) -> List[InformixTable]: ...

    def get_columns(self, table: InformixTable) -> List[InformixColumn]: ...

    def get_foreign_keys(self, table: InformixTable) -> List[InformixForeignKey]: ...

    def get_view_definition(self, table: InformixTable) -> Optional[str]: ...

    def close(self) -> None: ...


def _safe_close(closeable: object) -> None:
    try:
        closeable.close()  # type: ignore[attr-defined]
    except Exception as e:
        logger.debug("Error closing JDBC resource: %s", e)


class InformixClient:
    def __init__(self, config: InformixSourceConfig) -> None:
        jars = resolve_driver_jars(config)

        from jdk4py import JAVA_HOME

        os.environ.setdefault("JAVA_HOME", str(JAVA_HOME))
        import jpype  # type: ignore[import-untyped]

        if not jpype.isJVMStarted():
            jpype.startJVM(classpath=jars)
        else:
            for jar in jars:
                jpype.addClassPath(jar)

        driver_manager = jpype.JClass("java.sql.DriverManager")
        jpype.JClass(_DRIVER_CLASS)  # force-load the Informix driver
        try:
            self._conn = driver_manager.getConnection(build_jdbc_url(config))
        except Exception as e:
            # build_jdbc_url embeds the password in cleartext and the JVM's
            # SQLException can echo the full URL back, so re-raise sanitized and
            # drop __cause__. Keep the SQLSTATE / error code (neither contains the
            # URL) so operators can distinguish auth vs host vs missing-database.
            detail = ""
            try:
                detail = f" (SQLSTATE={e.getSQLState()}, code={e.getErrorCode()})"  # type: ignore[attr-defined]
            except Exception:
                pass
            raise RuntimeError(
                f"Failed to connect to Informix server '{config.server}' at "
                f"{config.host_port}, database '{config.database}'.{detail}"
            ) from None

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

    def get_tables(self) -> List[InformixTable]:
        tables: List[InformixTable] = []
        stmt = self._conn.createStatement()
        try:
            rs = stmt.executeQuery(SQL_TABLES)
            try:
                while rs.next():
                    # nrows is -1 or 0 when Informix hasn't computed a row estimate yet.
                    # systables.nrows is catalogued as FLOAT, so the JDBC driver can
                    # return values like "2.0"; go through float() before int().
                    raw_nrows = rs.getObject(4)
                    parsed_nrows = (
                        int(float(str(raw_nrows))) if raw_nrows is not None else 0
                    )
                    tables.append(
                        InformixTable(
                            name=str(rs.getString(1)).strip(),
                            owner=str(rs.getString(2)).strip(),
                            is_view=str(rs.getString(3)).strip() == "V",
                            nrows=parsed_nrows if parsed_nrows > 0 else None,
                        )
                    )
            finally:
                _safe_close(rs)
        finally:
            _safe_close(stmt)
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
            )
            for r in rows
        ]

    def get_foreign_keys(self, table: InformixTable) -> List[InformixForeignKey]:
        rows = self._query(SQL_FK, [table.name, table.owner])
        fks: Dict[str, InformixForeignKey] = {}
        for r in rows:
            fkname = str(r[0]).strip()
            child_col = str(r[1]).strip()
            parent_table = str(r[2]).strip()
            parent_owner = str(r[3]).strip()
            parent_col = str(r[4]).strip()
            if fkname not in fks:
                fks[fkname] = InformixForeignKey(
                    name=fkname,
                    child_columns=[],
                    parent_table=parent_table,
                    parent_owner=parent_owner,
                    parent_columns=[],
                )
            fk = fks[fkname]
            # The ABS(partN) IN(...) join (see SQL_FK) yields the cross product of
            # child/parent index columns for composite keys, not pairwise-ordered
            # rows, so dedup-by-first-seen only pairs single-column FKs exactly.
            if child_col not in fk.child_columns:
                fk.child_columns.append(child_col)
            if parent_col not in fk.parent_columns:
                fk.parent_columns.append(parent_col)
        return list(fks.values())

    def get_view_definition(self, table: InformixTable) -> Optional[str]:
        rows = self._query(SQL_VIEW_DEF, [table.name, table.owner])
        chunks = [str(r[0]) for r in rows if r[0] is not None]
        return "".join(chunks) if chunks else None

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception as e:
            logger.debug("Error closing Informix connection: %s", e)
