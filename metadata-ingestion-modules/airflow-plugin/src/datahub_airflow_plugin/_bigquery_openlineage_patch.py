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
from datahub_airflow_plugin.datahub_listener import get_airflow_plugin_listener

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.providers.openlineage.extractors import OperatorLineage

logger = logging.getLogger(__name__)


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

        # Check if the method exists (only in Airflow 3.x)
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
                from datahub.sql_parsing.sqlglot_lineage import (
                    create_lineage_sql_parsed_result,
                )

                # Extract SQL from configuration
                sql = self.configuration.get("query", {}).get("query")
                if not sql:
                    logger.debug(
                        "No query found in BigQueryInsertJobOperator configuration"
                    )
                    return original_get_openlineage_facets_on_complete(
                        self, task_instance
                    )

                logger.info(
                    f"DataHub patched BigQuery get_openlineage_facets_on_complete called for query: {sql[:100]}..."
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

                logger.info(
                    f"Original BigQuery OpenLineage result: inputs={len(operator_lineage.inputs)}, outputs={len(operator_lineage.outputs)}"
                )

                # Now enhance with DataHub SQL parsing
                try:
                    # Get platform and database info
                    platform = "bigquery"
                    default_database = (
                        self.project_id if hasattr(self, "project_id") else None
                    )

                    logger.info(
                        f"Running DataHub SQL parser for BigQuery (platform={platform}, "
                        f"default_db={default_database}): {sql}"
                    )

                    listener = get_airflow_plugin_listener()
                    graph = listener.graph if listener else None

                    # Use DataHub's SQL parser
                    sql_parsing_result = create_lineage_sql_parsed_result(
                        query=sql,
                        graph=graph,
                        platform=platform,
                        platform_instance=None,
                        env=builder.DEFAULT_ENV,
                        default_db=default_database,
                        default_schema=None,
                    )

                    logger.info(
                        f"DataHub SQL parsing result: in_tables={len(sql_parsing_result.in_tables)}, "
                        f"out_tables={len(sql_parsing_result.out_tables)}, "
                        f"column_lineage={len(sql_parsing_result.column_lineage or [])}"
                    )

                    # Check if there's a destinationTable in configuration
                    # If so, add it to the outputs in the SQL parsing result
                    destination_table = self.configuration.get("query", {}).get(
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
                            if (
                                destination_table_urn
                                not in sql_parsing_result.out_tables
                            ):
                                sql_parsing_result.out_tables.append(
                                    destination_table_urn
                                )
                                logger.info(
                                    f"Added destination table to outputs: {destination_table_urn}"
                                )

                    # Store the SQL parsing result in run_facets for DataHub listener
                    if sql_parsing_result:
                        operator_lineage.run_facets["datahub_sql_parsing_result"] = (
                            sql_parsing_result
                        )
                        logger.info(
                            f"Added DataHub SQL parsing result with "
                            f"{len(sql_parsing_result.column_lineage or [])} column lineages"
                        )

                except Exception as e:
                    logger.warning(
                        f"Error running DataHub SQL parser for BigQuery: {e}",
                        exc_info=True,
                    )

                return operator_lineage

            except Exception as e:
                logger.warning(
                    f"Error in patched BigQueryInsertJobOperator.get_openlineage_facets_on_complete: {e}",
                    exc_info=True,
                )
                # Fall back to original method
                return original_get_openlineage_facets_on_complete(self, task_instance)

        # Apply the patch
        BigQueryInsertJobOperator.get_openlineage_facets_on_complete = (  # type: ignore[assignment,method-assign]
            get_openlineage_facets_on_complete  # type: ignore[assignment]
        )
        BigQueryInsertJobOperator._datahub_openlineage_patched = True  # type: ignore[attr-defined]

        logger.info(
            "Patched BigQueryInsertJobOperator.get_openlineage_facets_on_complete to use DataHub SQL parser"
        )

    except ImportError as e:
        logger.debug(
            f"Could not patch BigQueryInsertJobOperator for OpenLineage (provider not installed or Airflow < 3.0): {e}"
        )
