import logging
from typing import Any, Dict, List

import redshift_connector

from datahub.ingestion.source.common.data_reader import DataReader
from datahub.utilities.perf_timer import PerfTimer

logger = logging.Logger(__name__)


class RedshiftDataReader(DataReader):
    @staticmethod
    def create(conn: redshift_connector.Connection) -> "RedshiftDataReader":
        return RedshiftDataReader(conn)

    def __init__(self, conn: redshift_connector.Connection) -> None:
        # The lifecycle of this connection is managed externally
        self.conn = conn

    def get_sample_data_for_table(
        self, table_id: List[str], sample_size: int, **kwargs: Any
    ) -> Dict[str, list]:
        """
        For redshift, table_id should be in form (db_name, schema_name, table_name)
        """
        assert len(table_id) == 3
        db_name = table_id[0]
        schema_name = table_id[1]
        table_name = table_id[2]

        logger.debug(
            f"Collecting sample values for table {db_name}.{schema_name}.{table_name}"
        )
        with PerfTimer() as timer, self.conn.cursor() as cursor:
            sql = f"select * from {db_name}.{schema_name}.{table_name} limit {sample_size};"
            cursor.execute(sql)
            df = cursor.fetch_dataframe()
            # Fetch the result set from the cursor and deliver it as the Pandas DataFrame.
            time_taken = timer.elapsed_seconds()
            logger.debug(
                f"Finished collecting sample values for table {db_name}.{schema_name}.{table_name};"
                f"{df.shape[0]} rows; took {time_taken:.3f} seconds"
            )
            return df.to_dict(orient="list")

    def close(self) -> None:
        pass
