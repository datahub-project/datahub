import logging
import os
from typing import List, Protocol

from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.constants import SQL_COLUMNS, SQL_PK, SQL_TABLES
from datahub.ingestion.source.informix.driver import resolve_driver_jars
from datahub.ingestion.source.informix.mapping import build_jdbc_url
from datahub.ingestion.source.informix.models import InformixColumn, InformixTable

logger = logging.getLogger(__name__)

_DRIVER_CLASS = "com.informix.jdbc.IfxDriver"


class InformixClientProtocol(Protocol):
    def get_tables(self) -> List[InformixTable]: ...

    def get_columns(self, table: InformixTable) -> List[InformixColumn]: ...

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
        import jpype

        if not jpype.isJVMStarted():
            jpype.startJVM(classpath=jars)
        else:
            for jar in jars:
                jpype.addClassPath(jar)

        driver_manager = jpype.JClass("java.sql.DriverManager")
        jpype.JClass(_DRIVER_CLASS)  # force-load the Informix driver
        try:
            self._conn = driver_manager.getConnection(build_jdbc_url(config))
        except Exception:
            # build_jdbc_url embeds the password in cleartext; the JVM's SQLException
            # can echo the full URL back, so re-raise sanitized and drop __cause__.
            raise RuntimeError(
                f"Failed to connect to Informix server '{config.server}' at "
                f"{config.host_port}, database '{config.database}'"
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
                    tables.append(
                        InformixTable(
                            name=str(rs.getString(1)).strip(),
                            owner=str(rs.getString(2)).strip(),
                            is_view=str(rs.getString(3)).strip() == "V",
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

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception as e:
            logger.debug("Error closing Informix connection: %s", e)
