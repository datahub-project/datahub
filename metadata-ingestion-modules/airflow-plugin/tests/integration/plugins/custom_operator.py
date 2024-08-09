import logging
from typing import List

from airflow.models.baseoperator import BaseOperator

from datahub_airflow_plugin.entities import Dataset

logger = logging.getLogger(__name__)


class CustomOperator(BaseOperator):
    def __init__(self, name, **kwargs):
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        """
        Other code....
        """
        logger.info("executing other code here")

        input_tables = ["mydb.schema.tableA", "mydb.schema.tableB"]
        output_tables = ["mydb.schema.tableD"]

        inlets, outlets = self._get_sf_lineage(input_tables, output_tables)

        context["ti"].task.inlets = inlets
        context["ti"].task.outlets = outlets

    @staticmethod
    def _get_sf_lineage(input_tables, output_tables):
        """
        Get lineage tables from Snowflake.
        """
        inlets: List[Dataset] = []
        outlets: List[Dataset] = []

        for table in input_tables:
            inlets.append(Dataset(platform="snowflake", name=table))

        for table in output_tables:
            outlets.append(Dataset(platform="snowflake", name=table))

        return inlets, outlets
