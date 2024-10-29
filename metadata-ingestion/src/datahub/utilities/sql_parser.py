import logging
import multiprocessing
import traceback
from multiprocessing import Process, Queue
from typing import Any, List, Optional, Tuple

from datahub.utilities.sql_lineage_parser_impl import SqlLineageSQLParserImpl
from datahub.utilities.sql_parser_base import SQLParser

logger = logging.getLogger(__name__)


def sql_lineage_parser_impl_func_wrapper(
    queue: Optional[multiprocessing.Queue], sql_query: str, use_raw_names: bool = False
) -> Optional[Tuple[List[str], List[str], Any]]:
    """
    The wrapper function that computes the tables and columns using the SqlLineageSQLParserImpl
    and puts the results on the shared IPC queue. This is used to isolate SqlLineageSQLParserImpl
    functionality in a separate process, and hence protect our sources from memory leaks originating in
    the sqllineage module.
    :param queue: The shared IPC queue on to which the results will be put.
    :param sql_query: The SQL query to extract the tables & columns from.
    :param use_raw_names: Parameter used to ignore sqllineage's default lowercasing.
    :return: None.
    """
    exception_details: Optional[Tuple[BaseException, str]] = None
    tables: List[str] = []
    columns: List[str] = []
    try:
        parser = SqlLineageSQLParserImpl(sql_query, use_raw_names)
        tables = parser.get_tables()
        columns = parser.get_columns()
    except BaseException as e:
        exc_msg = traceback.format_exc()
        exception_details = (e, exc_msg)
        logger.debug(exc_msg)

    if queue is not None:
        queue.put((tables, columns, exception_details))
        return None
    else:
        return (tables, columns, exception_details)


class SqlLineageSQLParser(SQLParser):
    def __init__(
        self,
        sql_query: str,
        use_external_process: bool = False,
        use_raw_names: bool = False,
    ) -> None:
        super().__init__(sql_query, use_external_process)
        if use_external_process:
            self.tables, self.columns = self._get_tables_columns_process_wrapped(
                sql_query, use_raw_names
            )
        else:
            return_tuple = sql_lineage_parser_impl_func_wrapper(
                None, sql_query, use_raw_names
            )
            if return_tuple is not None:
                (
                    self.tables,
                    self.columns,
                    some_exception,
                ) = return_tuple

    @staticmethod
    def _get_tables_columns_process_wrapped(
        sql_query: str, use_raw_names: bool = False
    ) -> Tuple[List[str], List[str]]:
        # Invoke sql_lineage_parser_impl_func_wrapper in a separate process to avoid
        # memory leaks from sqllineage module used by SqlLineageSQLParserImpl. This will help
        # shield our sources like lookml & redash, that need to parse a large number of SQL statements,
        # from causing significant memory leaks in the datahub cli during ingestion.
        queue: multiprocessing.Queue = Queue()
        process: multiprocessing.Process = Process(
            target=sql_lineage_parser_impl_func_wrapper,
            args=(queue, sql_query, use_raw_names),
        )
        process.start()
        tables, columns, exception_details = queue.get(block=True)
        if exception_details is not None:
            raise exception_details[0](f"Sub-process exception: {exception_details[1]}")
        return tables, columns

    def get_tables(self) -> List[str]:
        return self.tables

    def get_columns(self) -> List[str]:
        return self.columns


DefaultSQLParser = SqlLineageSQLParser
