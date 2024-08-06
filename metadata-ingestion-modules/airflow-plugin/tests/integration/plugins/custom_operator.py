from airflow.models.baseoperator import BaseOperator
from datahub_airflow_plugin.entities import Dataset

from typing import Any, List
import logging

logger = logging.getLogger(__name__)


class CustomOperator(BaseOperator):
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context) -> None:
        """
        Other code....
        """
        logger.info("executing other code here")

    def get_openlineage_facets_on_complete(self, task_instance):
        """
        Implement _on_complete because execute method does preprocessing on internals.

        This means we won't have to normalize self.source_object and self.source_objects,
        destination bucket and so on.
        """
        from airflow.providers.openlineage.extractors import OperatorLineage

        """
        Other code....
        """
        input_tables = ["mydb.schema.tableA", "mydb.schema.tableB"]
        output_tables = ["mydb.schema.tableD"]

        inlets, outlets = self._get_sf_lineage(input_tables, output_tables)

        return OperatorLineage(
            inputs=inlets,
            outputs=outlets,
        )

    @staticmethod
    def _get_sf_lineage(input_tables: List[str], output_tables: List[str]):
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
