"""
Patch for BigQueryInsertJobOperator to use DataHub's SQL parser.

BigQueryInsertJobOperator in Airflow 3.x doesn't use the standard SQLParser approach
because it stores SQL in a configuration dictionary. This patch modifies
get_openlineage_facets_on_complete() to use DataHub's SQL parser, enabling
column-level lineage extraction.
"""

import logging
from typing import TYPE_CHECKING, Any, Optional

import datahub.emitter.mce_builder as builder

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.providers.openlineage.extractors import OperatorLineage

logger = logging.getLogger(__name__)


def _should_patch_bigquery_operator(operator_class: Any) -> bool:
    """Check if BigQuery operator should be patched."""
    if not hasattr(operator_class, "get_openlineage_facets_on_complete"):
        logger.debug(
            "BigQueryInsertJobOperator.get_openlineage_facets_on_complete not found - "
            "likely Airflow 2.x, skipping patch"
        )
        return False
    if hasattr(operator_class, "_datahub_openlineage_patched"):
        logger.debug("BigQueryInsertJobOperator already patched for OpenLineage")
        return False
    return True


def _render_bigquery_sql_templates(
    sql: str, operator: Any, task_instance: "TaskInstance"
) -> str:
    """
    Render Jinja templates in BigQuery SQL if they exist.

    Returns the rendered SQL, or original SQL if rendering fails.
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
            rendered_sql = operator.render_template(sql, context)
        else:
            # Fallback: try to render using Jinja2 directly
            from airflow.templates import SandboxedEnvironment

            jinja_env = SandboxedEnvironment()
            template = jinja_env.from_string(str(sql))
            rendered_sql = template.render(**context)  # type: ignore[misc]

        logger.debug(
            "Rendered BigQuery SQL templates: %s -> %s",
            str(sql)[:100],
            str(rendered_sql)[:100],
        )
        return rendered_sql
    except Exception as e:
        logger.warning(
            "Failed to render BigQuery SQL templates, using original SQL: %s",
            e,
        )
        return sql


def _enhance_bigquery_lineage_with_sql_parsing(
    operator_lineage: "OperatorLineage",
    rendered_sql: str,
    operator: Any,
) -> None:
    """
    Enhance OperatorLineage with DataHub SQL parsing results.

    Modifies operator_lineage in place by adding SQL parsing result to run_facets.
    """
    try:
        from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result
        from datahub_airflow_plugin._constants import DATAHUB_SQL_PARSING_RESULT_KEY
        from datahub_airflow_plugin.datahub_listener import get_airflow_plugin_listener

        platform = "bigquery"
        default_database = (
            operator.project_id if hasattr(operator, "project_id") else None
        )

        logger.debug(
            f"Running DataHub SQL parser for BigQuery (platform={platform}, "
            f"default_db={default_database}): {rendered_sql}"
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
            f"DataHub SQL parsing result: in_tables={len(sql_parsing_result.in_tables)}, "
            f"out_tables={len(sql_parsing_result.out_tables)}, "
            f"column_lineage={len(sql_parsing_result.column_lineage or [])}"
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
                        f"Added destination table to outputs: {destination_table_urn}"
                    )

        # Store the SQL parsing result in run_facets for DataHub listener
        if sql_parsing_result:
            operator_lineage.run_facets[DATAHUB_SQL_PARSING_RESULT_KEY] = (
                sql_parsing_result
            )
            logger.debug(
                f"Added DataHub SQL parsing result with "
                f"{len(sql_parsing_result.column_lineage or [])} column lineages"
            )

    except Exception as e:
        logger.warning(
            f"Error running DataHub SQL parser for BigQuery: {e}",
            exc_info=True,
        )


def _create_bigquery_openlineage_wrapper(
    original_method: Any,
) -> Any:
    """Create the wrapper function for BigQuery OpenLineage extraction."""

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
                return original_method(self, task_instance)

            # Render Jinja templates in SQL if they exist
            rendered_sql = _render_bigquery_sql_templates(sql, self, task_instance)

            logger.debug(
                f"DataHub patched BigQuery get_openlineage_facets_on_complete called for query: {rendered_sql[:100]}..."
            )

            # Get the original OpenLineage result
            operator_lineage = original_method(self, task_instance)

            # If original returns None (no job_id found in test environment),
            # create a new OperatorLineage so we can still add SQL parsing result
            if not operator_lineage:
                logger.debug(
                    "Original OpenLineage returned None for BigQueryInsertJobOperator, "
                    "creating new OperatorLineage for SQL parsing"
                )
                from airflow.providers.openlineage.extractors import OperatorLineage

                operator_lineage = OperatorLineage(  # type: ignore[misc]
                    inputs=[],
                    outputs=[],
                    job_facets={},
                    run_facets={},
                )

            logger.debug(
                f"Original BigQuery OpenLineage result: inputs={len(operator_lineage.inputs)}, outputs={len(operator_lineage.outputs)}"
            )

            # Enhance with DataHub SQL parsing
            _enhance_bigquery_lineage_with_sql_parsing(
                operator_lineage, rendered_sql, self
            )

            return operator_lineage

        except Exception as e:
            logger.warning(
                f"Error in patched BigQueryInsertJobOperator.get_openlineage_facets_on_complete: {e}",
                exc_info=True,
            )
            # Fall back to original method
            return original_method(self, task_instance)

    return get_openlineage_facets_on_complete


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

        # Check if operator should be patched
        if not _should_patch_bigquery_operator(BigQueryInsertJobOperator):
            return

        # Store original method and create wrapper
        original_method = BigQueryInsertJobOperator.get_openlineage_facets_on_complete
        wrapper = _create_bigquery_openlineage_wrapper(original_method)

        # Apply the patch
        BigQueryInsertJobOperator.get_openlineage_facets_on_complete = (  # type: ignore[assignment,method-assign]
            wrapper  # type: ignore[assignment]
        )
        BigQueryInsertJobOperator._datahub_openlineage_patched = True  # type: ignore[attr-defined]

        logger.debug(
            "Patched BigQueryInsertJobOperator.get_openlineage_facets_on_complete to use DataHub SQL parser"
        )

    except ImportError as e:
        logger.debug(
            f"Could not patch BigQueryInsertJobOperator for OpenLineage (provider not installed or Airflow < 3.0): {e}"
        )
