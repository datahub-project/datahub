import logging
from collections import defaultdict
from typing import Any, Dict, List, Union

import sqlalchemy as sa
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.reflection import Inspector

from datahub.ingestion.source.common.data_reader import DataReader
from datahub.utilities.perf_timer import PerfTimer

logger: logging.Logger = logging.getLogger(__name__)


class SqlAlchemyTableDataReader(DataReader):
    @staticmethod
    def create(inspector: Inspector) -> "SqlAlchemyTableDataReader":
        return SqlAlchemyTableDataReader(conn=inspector.bind)

    def __init__(
        self,
        conn: Union[Engine, Connection],
    ) -> None:
        self.connection = conn.engine.connect()

    def _table(self, table_id: List[str]) -> sa.Table:
        return sa.Table(
            table_id[-1],
            sa.MetaData(),
            schema=table_id[-2] if len(table_id) > 1 else None,
        )

    def get_sample_data_for_table(
        self, table_id: List[str], sample_size: int, **kwargs: Any
    ) -> Dict[str, list]:
        """
        For sqlalchemy, table_id should be in form (schema_name, table_name)
        """

        logger.debug(f"Collecting sample values for table {'.'.join(table_id)}")

        with PerfTimer() as timer:
            column_values: Dict[str, list] = defaultdict(list)
            table = self._table(table_id)

            query: Any

            # limit doesn't compile properly for oracle so we will append rownum to query string later
            if self.connection.dialect.name.lower() == "oracle":
                raw_query = sa.select([sa.text("*")]).select_from(table)

                query = str(
                    raw_query.compile(
                        self.connection, compile_kwargs={"literal_binds": True}
                    )
                )
                query += "\nAND ROWNUM <= %d" % sample_size
            else:
                query = sa.select([sa.text("*")]).select_from(table).limit(sample_size)
            query_results = self.connection.execute(query)

            # Not ideal - creates a parallel structure in column_values. Can we use pandas here ?
            for row in query_results.fetchall():
                for col, col_value in row._mapping.items():
                    column_values[col].append(col_value)
            time_taken = timer.elapsed_seconds()
            logger.debug(
                f"Finished collecting sample values for table {'.'.join(table_id)};"
                f"took {time_taken:.3f} seconds"
            )
        return column_values

    def close(self) -> None:
        self.connection.close()
