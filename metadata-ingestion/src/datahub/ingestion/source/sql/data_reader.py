import logging
from abc import abstractmethod
from collections import defaultdict
from typing import Any, Dict, List, Union

import sqlalchemy as sa
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.engine.row import LegacyRow

from datahub.ingestion.api.closeable import Closeable

logger: logging.Logger = logging.getLogger(__name__)


class DataReader(Closeable):
    @abstractmethod
    def get_sample_data_for_column(
        self, table_id: List[str], column_name: str, sample_size: int = 100
    ) -> list:
        pass

    @abstractmethod
    def get_sample_data_for_table(
        self, table_id: List[str], sample_size: int = 100
    ) -> Dict[str, list]:
        pass


class SqlAlchemyTableDataReader(DataReader):
    @staticmethod
    def create(inspector: Inspector) -> "SqlAlchemyTableDataReader":
        return SqlAlchemyTableDataReader(conn=inspector.bind)

    def __init__(
        self,
        conn: Union[Engine, Connection],
    ) -> None:
        # TODO: How can this use a connection pool instead ?
        self.engine = conn.engine.connect()

    def _table(self, table_id: List[str]) -> sa.Table:
        return sa.Table(
            table_id[-1],
            sa.MetaData(),
            schema=table_id[-2] if len(table_id) > 1 else None,
        )

    def get_sample_data_for_column(
        self, table_id: List[str], column_name: str, sample_size: int = 100
    ) -> list:
        """
        Fetches non-null column values, upto <sample_size> count
        Args:
            table_id: Table name identifier. One of
                        - [<db_name>, <schema_name>, <table_name>] or
                        - [<schema_name>, <table_name>] or
                        - [<table_name>]
            column: Column name
        Returns:
            list of column values
        """

        table = self._table(table_id)
        query: Any
        ignore_null_condition = sa.column(column_name).is_(None)
        # limit doesn't compile properly for oracle so we will append rownum to query string later
        if self.engine.dialect.name.lower() == "oracle":
            raw_query = (
                sa.select([sa.column(column_name)])
                .select_from(table)
                .where(sa.not_(ignore_null_condition))
            )

            query = str(
                raw_query.compile(self.engine, compile_kwargs={"literal_binds": True})
            )
            query += "\nAND ROWNUM <= %d" % sample_size
        else:
            query = (
                sa.select([sa.column(column_name)])
                .select_from(table)
                .where(sa.not_(ignore_null_condition))
                .limit(sample_size)
            )
        query_results = self.engine.execute(query)

        return [x[column_name] for x in query_results.fetchall()]

    def get_sample_data_for_table(
        self, table_id: List[str], sample_size: int = 100
    ) -> Dict[str, list]:
        """
        Fetches table values, upto <sample_size>*1.2 count
        Args:
            table_id: Table name identifier. One of
                        - [<db_name>, <schema_name>, <table_name>] or
                        - [<schema_name>, <table_name>] or
                        - [<table_name>]
        Returns:
            dictionary of (column name -> list of column values)
        """
        column_values: Dict[str, list] = defaultdict(list)
        table = self._table(table_id)

        # Ideally we do not want null values in sample data for a column.
        # However that would require separate query per column and
        # that would be expensiv. To compensate for possibility
        # of some null values in collected sample, we fetch extra (20% more)
        # rows than configured sample_size.
        sample_size = int(sample_size * 1.2)

        query: Any

        # limit doesn't compile properly for oracle so we will append rownum to query string later
        if self.engine.dialect.name.lower() == "oracle":
            raw_query = sa.select([sa.text("*")]).select_from(table)

            query = str(
                raw_query.compile(self.engine, compile_kwargs={"literal_binds": True})
            )
            query += "\nAND ROWNUM <= %d" % sample_size
        else:
            query = sa.select([sa.text("*")]).select_from(table).limit(sample_size)
        query_results = self.engine.execute(query)

        # Not ideal - creates a parallel structure in column_values. Can we use pandas here ?
        for row in query_results.fetchall():
            if isinstance(row, LegacyRow):
                for col, col_value in row.items():
                    column_values[col].append(col_value)

        return column_values

    def close(self) -> None:
        self.engine.close()
