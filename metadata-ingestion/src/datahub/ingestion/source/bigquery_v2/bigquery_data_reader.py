import logging
from collections import defaultdict
from typing import Dict, List, Optional

from google.cloud import bigquery

from datahub.ingestion.source.common.data_reader import DataReader
from datahub.utilities.perf_timer import PerfTimer

logger = logging.Logger(__name__)


class BigQueryDataReader(DataReader):
    @staticmethod
    def create(
        client: bigquery.Client,
    ) -> "BigQueryDataReader":
        return BigQueryDataReader(client)

    def __init__(
        self,
        client: bigquery.Client,
    ) -> None:
        self.client = client

    def get_sample_data_for_table(
        self,
        table_id: List[str],
        sample_size: int,
        *,
        sample_size_percent: Optional[float] = None,
        filter: Optional[str] = None,
    ) -> Dict[str, list]:
        """
        table_id should be in the form [project, dataset, schema]
        """

        assert len(table_id) == 3
        project = table_id[0]
        dataset = table_id[1]
        table_name = table_id[2]

        column_values: Dict[str, list] = defaultdict(list)
        if sample_size_percent is None:
            return column_values
        # Ideally we always know the actual row count.
        # The alternative to perform limit query scans entire BQ table
        # and is never a recommended option due to cost factor, unless
        # additional filter clause (e.g. where condition on partition) is available.

        logger.debug(
            f"Collecting sample values for table {project}.{dataset}.{table_name}"
        )
        with PerfTimer() as timer:
            sample_pc = sample_size_percent * 100
            # TODO: handle for sharded+compulsory partitioned tables
            sql = (
                f"SELECT * FROM `{project}.{dataset}.{table_name}` "
                + f"TABLESAMPLE SYSTEM ({sample_pc:.8f} percent)"
            )
            # Ref: https://cloud.google.com/bigquery/docs/samples/bigquery-query-results-dataframe
            df = self.client.query_and_wait(sql).to_dataframe()
            time_taken = timer.elapsed_seconds()
            logger.debug(
                f"Finished collecting sample values for table {project}.{dataset}.{table_name};"
                f"{df.shape[0]} rows; took {time_taken:.3f} seconds"
            )

        return df.to_dict(orient="list")

    def close(self) -> None:
        self.client.close()
