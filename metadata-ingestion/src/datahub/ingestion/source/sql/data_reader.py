import logging
from abc import abstractmethod
from collections import defaultdict
from typing import Any, Dict, List, Optional, Union

import sqlalchemy as sa
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.reflection import Inspector

from datahub.ingestion.api.closeable import Closeable
from datahub.utilities.perf_timer import PerfTimer

logger: logging.Logger = logging.getLogger(__name__)


class DataReader(Closeable):
    def get_sample_data_for_column(
        self, table_id: List[str], column_name: str, sample_size: int
    ) -> list:
        raise NotImplementedError()

    @abstractmethod
    def get_sample_data_for_table(
        self,
        table_id: List[str],
        sample_size: int,
        *,
        sample_size_percent: Optional[float] = None,
        filter: Optional[str] = None,
    ) -> Dict[str, list]:
        """
        Fetches table values , approx sample_size rows

        Args:
            table_id (List[str]): Table name identifier. One of
                        - [<db_name>, <schema_name>, <table_name>] or
                        - [<schema_name>, <table_name>] or
                        - [<table_name>]
            sample_size (int): sample size

        Keyword Args:
            sample_size_percent(float, between 0 and 1): For bigquery-like data platforms that provide only
                    percentage based sampling methods. If present, actual sample_size
                    may be ignored.

            filter (string): For bigquery-like data platforms that need mandatory filter on partition
                    column for some cases


        Returns:
            Dict[str, list]: dictionary of (column name -> list of column values)
        """

        # Ideally we do not want null values in sample data for a column.
        # However that would require separate query per column and
        # that would be expensive, hence not done. To compensate for possibility
        # of some null values in collected sample, its usually recommended to
        # fetch extra (20% more) rows than configured sample_size.

        pass


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
