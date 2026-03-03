from datetime import datetime
from typing import Any, List, Sequence, Tuple, Union

from airflow import DAG
from airflow.models.baseoperator import BaseOperator

from datahub_airflow_plugin._config import get_enable_multi_statement
from datahub_airflow_plugin._sql_parsing_common import parse_sql_with_datahub
from datahub_airflow_plugin.entities import Urn

ATHENA_COST_TABLE = "costs"
ATHENA_PROCESSED_TABLE = "processed_costs"


class CustomOperator(BaseOperator):
    template_fields: Sequence[str] = ("database", "schema")

    def __init__(self, database: str, schema: str, query: str, **kwargs: Any):
        super().__init__(**kwargs)
        self.platform = "athena"
        self.database = database
        self.schema = schema
        self.query = query
        self.env = "PROD"

    def execute(self, context):
        # do something
        inlets, outlets = self._get_lineage(context)
        # inlets/outlets are lists of either datahub_airflow_plugin.entities.Dataset or datahub_airflow_plugin.entities.Urn
        context["ti"].task.inlets = inlets
        context["ti"].task.outlets = outlets

    def _get_lineage(self, context: Any) -> Tuple[List, List]:
        """Extract lineage information from SQL query."""

        inlets = []
        outlets = []

        try:
            # Get database and schema
            default_database = self.database
            default_schema = self.schema

            enable_multi_statement = get_enable_multi_statement()
            sql: Union[str, List[str]] = self.query

            sql_parsing_result = parse_sql_with_datahub(
                sql=sql,
                default_database=default_database,
                platform=self.platform,
                env=self.env,
                default_schema=default_schema,
                graph=None,  # We don't have access to graph here
                enable_multi_statement=enable_multi_statement,
            )

            # Convert inputs and outputs to Dataset objects
            if not sql_parsing_result.debug_info.table_error:
                # Convert source tables to inlets
                for table in sql_parsing_result.in_tables:
                    inlets.append(Urn(table))

                # Convert target tables to outlets
                for table in sql_parsing_result.out_tables:
                    outlets.append(Urn(table))

            else:
                # Log parsing error
                self.log.warning(
                    f"SQL parsing error: {sql_parsing_result.debug_info.error}"
                )

        except Exception as e:
            self.log.exception(f"Error extracting lineage: {str(e)}")

        return inlets, outlets


with DAG(
    "custom_operator_sql_parsing",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    query = """
        CREATE OR REPLACE TABLE my_output_table AS
        SELECT
            id,
            month,
            total_cost,
            area,
            total_cost / area as cost_per_area
        FROM my_input_table
        """

    transform_cost_table = CustomOperator(
        task_id="transform_cost_table",
        database="athena_db",
        schema="default",
        query=query,
    )
