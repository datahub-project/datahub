import logging
from typing import Any, Callable, Dict, List

import pandas as pd

from datahub.ingestion.source.common.data_reader import DataReader
from datahub.ingestion.source.snowflake.snowflake_connection import SnowflakeConnection
from datahub.utilities.perf_timer import PerfTimer

logger = logging.Logger(__name__)


class SnowflakeDataReader(DataReader):
    @staticmethod
    def create(
        conn: SnowflakeConnection, col_name_preprocessor: Callable[[str], str]
    ) -> "SnowflakeDataReader":
        return SnowflakeDataReader(conn, col_name_preprocessor)

    def __init__(
        self, conn: SnowflakeConnection, col_name_preprocessor: Callable[[str], str]
    ) -> None:
        # The lifecycle of this connection is managed externally
        self.conn = conn
        self.col_name_preprocessor = col_name_preprocessor

    def get_sample_data_for_table(
        self, table_id: List[str], sample_size: int, **kwargs: Any
    ) -> Dict[str, list]:
        """
        For snowflake, table_id should be in form (db_name, schema_name, table_name)
        """

        assert len(table_id) == 3
        db_name = table_id[0]
        schema_name = table_id[1]
        table_name = table_id[2]

        logger.debug(
            f"Collecting sample values for table {db_name}.{schema_name}.{table_name}"
        )
        with PerfTimer() as timer, self.conn.native_connection().cursor() as cursor:
            sql = f'select * from "{db_name}"."{schema_name}"."{table_name}" sample ({sample_size} rows);'
            cursor.execute(sql)
            dat = cursor.fetchall()
            # Fetch the result set from the cursor and deliver it as the Pandas DataFrame.
            df = pd.DataFrame(dat, columns=[col.name for col in cursor.description])
            df.columns = [self.col_name_preprocessor(col) for col in df.columns]
            time_taken = timer.elapsed_seconds()
            logger.debug(
                f"Finished collecting sample values for table {db_name}.{schema_name}.{table_name};"
                f"{df.shape[0]} rows; took {time_taken:.3f} seconds"
            )
            return df.to_dict(orient="list")

    def close(self) -> None:
        pass
