import logging
from datetime import datetime, timedelta
from typing import Any, List, Tuple

from airflow import DAG
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
    def _get_sf_lineage(
        input_tables: List[str], output_tables: List[str]
    ) -> Tuple[List[Any], List[Any]]:
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


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email": ["jdoe@example.com"],
    "email_on_failure": False,
    "execution_timeout": timedelta(minutes=5),
}


with DAG(
    "custom_operator_dag",
    default_args=default_args,
    description="An example dag with custom operator",
    schedule_interval=None,
    tags=["example_tag"],
    catchup=False,
    default_view="tree",
) as dag:
    custom_task = CustomOperator(
        task_id="custom_task_id",
        name="custom_name",
        dag=dag,
    )
