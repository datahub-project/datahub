import logging
import os
from typing import List

from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.constants import SQL_COLUMNS, SQL_PK, SQL_TABLES
from datahub.ingestion.source.informix.driver import resolve_driver_jars
from datahub.ingestion.source.informix.mapping import build_jdbc_url
from datahub.ingestion.source.informix.models import InformixColumn, InformixTable

logger = logging.getLogger(__name__)

_DRIVER_CLASS = "com.informix.jdbc.IfxDriver"


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
        self._conn = driver_manager.getConnection(build_jdbc_url(config))

    def _query(self, sql: str, params: List[str]) -> List[List[object]]:
        stmt = self._conn.prepareStatement(sql)
        for i, p in enumerate(params, start=1):
            stmt.setString(i, p)
        rs = stmt.executeQuery()
        meta = rs.getMetaData()
        n = meta.getColumnCount()
        rows: List[List[object]] = []
        while rs.next():
            rows.append([rs.getObject(i) for i in range(1, n + 1)])
        rs.close()
        stmt.close()
        return rows

    def get_tables(self) -> List[InformixTable]:
        tables: List[InformixTable] = []
        stmt = self._conn.createStatement()
        rs = stmt.executeQuery(SQL_TABLES)
        while rs.next():
            tables.append(
                InformixTable(
                    name=str(rs.getString(1)).strip(),
                    owner=str(rs.getString(2)).strip(),
                    is_view=str(rs.getString(3)).strip() == "V",
                )
            )
        rs.close()
        stmt.close()
        return tables

    def get_columns(self, table: InformixTable) -> List[InformixColumn]:
        pk_rows = self._query(SQL_PK, [table.name, table.owner])
        pk_names = {str(r[0]).strip() for r in pk_rows}
        rows = self._query(SQL_COLUMNS, [table.name, table.owner])
        return [
            InformixColumn(
                name=str(r[0]).strip(),
                coltype=int(r[1]),
                length=int(r[2]),
                colno=int(r[3]),
                is_pk=str(r[0]).strip() in pk_names,
            )
            for r in rows
        ]

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception as e:
            logger.debug("Error closing Informix connection: %s", e)
