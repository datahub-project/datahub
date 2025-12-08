"""
Patch for BigQueryInsertJobOperator to use DataHub's SQL parser.

BigQueryInsertJobOperator in Airflow 3.x doesn't use the standard SQLParser approach
because it stores SQL in a configuration dictionary. This patch modifies
get_openlineage_facets_on_complete() to use DataHub's SQL parser, enabling
column-level lineage extraction.
"""

import logging
from typing import TYPE_CHECKING, Any, Optional

from airflow.templates import SandboxedEnvironment

import datahub.emitter.mce_builder as builder
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result
from datahub_airflow_plugin._constants import DATAHUB_SQL_PARSING_RESULT_KEY

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.providers.openlineage.extractors import OperatorLineage

logger = logging.getLogger(__name__)


def _render_bigquery_sql_templates(
    sql: str, operator: Any, task_instance: "TaskInstance"
) -> str:
    """
    Render Jinja templates in BigQuery SQL if they exist.

    Args:
        sql: SQL string that may contain templates
        operator: BigQuery operator instance
        task_instance: Airflow task instance for context

    Returns:
        Rendered SQL string
    """
    if "{{" not in str(sql) and "{%" not in str(sql):
        return sql

    try:
        # Get template context from task_instance
        context: Any = {}
        if hasattr(task_instance, "get_template_context"):
            context = task_instance.get_template_context()
        elif (
            hasattr(task_instance, "task")
            and task_instance.task is not None
            and hasattr(task_instance.task, "get_template_context")
        ):
            context = task_instance.task.get_template_context()

        # Try to render using the operator's render_template method
        if hasattr(operator, "render_template") and context:
            return operator.render_template(sql, context)

        # Fallback: try to render using Jinja2 directly
        jinja_env = SandboxedEnvironment()
        template = jinja_env.from_string(str(sql))
        return template.render(**context)  # type: ignore[misc]

    except Exception as e:
        logger.warning(
            "Failed to render BigQuery SQL templates, using original SQL: %s", e
        )
        return sql


def _enhance_bigquery_lineage_with_sql_parsing(
    operator: Any,
    operator_lineage: "OperatorLineage",
    rendered_sql: str,
) -> None:
    """
    Enhance BigQuery OpenLineage result with DataHub SQL parsing for column-level lineage.

    Args:
        operator: BigQuery operator instance
        operator_lineage: OpenLineage result to enhance
        rendered_sql: Rendered SQL string to parse
    """
    try:
        # Import here to avoid circular dependency
        from datahub_airflow_plugin.datahub_listener import get_airflow_plugin_listener

        platform = "bigquery"
        default_database = (
            operator.project_id if hasattr(operator, "project_id") else None
        )

        logger.debug(
            "Running DataHub SQL parser for BigQuery (platform=%s, default_db=%s): %s",
            platform,
            default_database,
            rendered_sql[:200],
        )

        listener = get_airflow_plugin_listener()
        graph = listener.graph if listener else None

        # Use DataHub's SQL parser with rendered SQL
        sql_parsing_result = create_lineage_sql_parsed_result(
            query=rendered_sql,
            graph=graph,
            platform=platform,
            platform_instance=None,
            env=builder.DEFAULT_ENV,
            default_db=default_database,
            default_schema=None,
        )

        logger.debug(
            "DataHub SQL parsing result: in_tables=%d, out_tables=%d, column_lineage=%d",
            len(sql_parsing_result.in_tables),
            len(sql_parsing_result.out_tables),
            len(sql_parsing_result.column_lineage or []),
        )

        # Check if there's a destinationTable in configuration
        destination_table = operator.configuration.get("query", {}).get(
            "destinationTable"
        )
        if destination_table:
            project_id = destination_table.get("projectId")
            dataset_id = destination_table.get("datasetId")
            table_id = destination_table.get("tableId")

            if project_id and dataset_id and table_id:
                destination_table_urn = builder.make_dataset_urn(
                    platform="bigquery",
                    name=f"{project_id}.{dataset_id}.{table_id}",
                    env=builder.DEFAULT_ENV,
                )
                # Add to output tables if not already present
                if destination_table_urn not in sql_parsing_result.out_tables:
                    sql_parsing_result.out_tables.append(destination_table_urn)
                    logger.debug(
                        "Added destination table to outputs: %s", destination_table_urn
                    )

        # Store the SQL parsing result in run_facets for DataHub listener
        if sql_parsing_result:
            operator_lineage.run_facets[DATAHUB_SQL_PARSING_RESULT_KEY] = (
                sql_parsing_result
            )
            logger.debug(
                "Added DataHub SQL parsing result with %d column lineages",
                len(sql_parsing_result.column_lineage or []),
            )

    except Exception as e:
        logger.warning(
            "Error running DataHub SQL parser for BigQuery: %s", e, exc_info=True
        )


def patch_bigquery_insert_job_operator() -> None:
    """
    Patch BigQueryInsertJobOperator to use DataHub's SQL parser for lineage extraction.

    This enhances the existing OpenLineage support with DataHub's SQL parser,
    which provides better column-level lineage.
    """
    try:
        from airflow.providers.google.cloud.operators.bigquery import (
            BigQueryInsertJobOperator,
        )

        # Check if the methods exist (only in Airflow 3.x)
        if not hasattr(BigQueryInsertJobOperator, "get_openlineage_facets_on_complete"):
            logger.debug(
                "BigQueryInsertJobOperator.get_openlineage_facets_on_complete not found - "
                "likely Airflow 2.x, skipping patch"
            )
            return

        # Check if already patched
        if hasattr(BigQueryInsertJobOperator, "_datahub_openlineage_patched"):
            logger.debug("BigQueryInsertJobOperator already patched for OpenLineage")
            return

        # Store original method
        original_get_openlineage_facets_on_complete = (
            BigQueryInsertJobOperator.get_openlineage_facets_on_complete
        )

        def get_openlineage_facets_on_complete(
            self: Any, task_instance: "TaskInstance"
        ) -> Optional["OperatorLineage"]:
            """
            Enhanced version that uses DataHub's SQL parser for better lineage.

            This method:
            1. Calls the original OpenLineage implementation
            2. Enhances it with DataHub SQL parsing result for column lineage
            """
            try:
                # Extract SQL from configuration
                sql = self.configuration.get("query", {}).get("query")
                if not sql:
                    logger.debug(
                        "No query found in BigQueryInsertJobOperator configuration"
                    )
                    return original_get_openlineage_facets_on_complete(
                        self, task_instance
                    )

                # Render Jinja templates in SQL if they exist
                rendered_sql = _render_bigquery_sql_templates(sql, self, task_instance)

                logger.debug(
                    "DataHub patched BigQuery get_openlineage_facets_on_complete called for query: %s...",
                    rendered_sql[:100],
                )

                # Get the original OpenLineage result
                operator_lineage = original_get_openlineage_facets_on_complete(
                    self, task_instance
                )

                if not operator_lineage:
                    logger.debug(
                        "Original OpenLineage returned None for BigQueryInsertJobOperator"
                    )
                    return operator_lineage

                logger.debug(
                    "Original BigQuery OpenLineage result: inputs=%d, outputs=%d",
                    len(operator_lineage.inputs),
                    len(operator_lineage.outputs),
                )

                # Enhance with DataHub SQL parsing for column-level lineage
                _enhance_bigquery_lineage_with_sql_parsing(
                    self, operator_lineage, rendered_sql
                )

                return operator_lineage

            except Exception as e:
                logger.warning(
                    "Error in patched BigQueryInsertJobOperator.get_openlineage_facets_on_complete: %s",
                    e,
                    exc_info=True,
                )
                # Fall back to original method
                return original_get_openlineage_facets_on_complete(self, task_instance)

        # Apply the patch
        BigQueryInsertJobOperator.get_openlineage_facets_on_complete = (  # type: ignore[assignment,method-assign]
            get_openlineage_facets_on_complete  # type: ignore[assignment]
        )
        BigQueryInsertJobOperator._datahub_openlineage_patched = True  # type: ignore[attr-defined]

        logger.debug(
            "Patched BigQueryInsertJobOperator.get_openlineage_facets_on_complete to use DataHub SQL parser"
        )

    except ImportError as e:
        logger.debug(
            "Could not patch BigQueryInsertJobOperator for OpenLineage (provider not installed or Airflow < 3.0): %s",
            e,
        )
