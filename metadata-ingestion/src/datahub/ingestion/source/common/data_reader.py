from abc import abstractmethod
from typing import Dict, List, Optional

from datahub.ingestion.api.closeable import Closeable


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
