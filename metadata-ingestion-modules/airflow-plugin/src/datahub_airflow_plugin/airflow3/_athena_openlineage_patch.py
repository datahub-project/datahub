"""
Patch for AthenaOperator to use DataHub's SQL parser.

AthenaOperator in Airflow 3.x uses SQLParser with dialect="generic", which doesn't provide
column-level lineage. This patch modifies get_openlineage_facets_on_complete() to use
DataHub's SQL parser instead, enabling column-level lineage extraction.
"""

import logging
from typing import TYPE_CHECKING, Any, Optional

import datahub.emitter.mce_builder as builder
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result
from datahub_airflow_plugin._constants import DATAHUB_SQL_PARSING_RESULT_KEY

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.providers.openlineage.extractors import OperatorLineage

logger = logging.getLogger(__name__)


def patch_athena_operator() -> None:
    """
    Patch AthenaOperator to use DataHub's SQL parser for lineage extraction.

    This enhances the existing OpenLineage support with DataHub's SQL parser,
    which provides better column-level lineage.
    """
    try:
        from airflow.providers.amazon.aws.operators.athena import AthenaOperator

        # Check if the method exists (only in Airflow 3.x)
        if not hasattr(AthenaOperator, "get_openlineage_facets_on_complete"):
            logger.debug(
                "AthenaOperator.get_openlineage_facets_on_complete not found - "
                "likely Airflow 2.x, skipping patch"
            )
            return

        # Check if already patched
        if hasattr(AthenaOperator, "_datahub_openlineage_patched"):
            logger.debug("AthenaOperator already patched for OpenLineage")
            return

        # Store original method
        original_get_openlineage_facets_on_complete = (
            AthenaOperator.get_openlineage_facets_on_complete
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
                logger.debug(
                    f"DataHub patched Athena get_openlineage_facets_on_complete called for query: {self.query[:100]}"
                )

                # Get the original OpenLineage result
                operator_lineage = original_get_openlineage_facets_on_complete(
                    self, task_instance
                )

                if not operator_lineage:
                    logger.debug(
                        "Original OpenLineage returned None for Athena operator"
                    )
                    return operator_lineage

                logger.debug(
                    f"Original Athena OpenLineage result: inputs={len(operator_lineage.inputs)}, outputs={len(operator_lineage.outputs)}"
                )

                # Check if SQL parsing result is already in run_facets (from SQLParser patch)
                # If not, add it manually since Athena might not use SQLParser or patch might not work
                if DATAHUB_SQL_PARSING_RESULT_KEY not in operator_lineage.run_facets:
                    # SQLParser patch didn't add it - add it manually
                    try:
                        platform = "athena"
                        default_database = (
                            self.database if hasattr(self, "database") else None
                        )

                        # Get the SQL query - templates are already rendered by Airflow during task execution
                        rendered_query = self.query

                        logger.debug(
                            f"Running DataHub SQL parser for Athena (platform={platform}, "
                            f"default_db={default_database}): {rendered_query[:200] if rendered_query else 'None'}"
                        )

                        # Use DataHub's SQL parser
                        sql_parsing_result = create_lineage_sql_parsed_result(
                            query=rendered_query,
                            platform=platform,
                            platform_instance=None,
                            env=builder.DEFAULT_ENV,
                            default_db=default_database,
                            default_schema=None,
                        )

                        # Store the SQL parsing result in run_facets for DataHub listener
                        if sql_parsing_result:
                            operator_lineage.run_facets[
                                DATAHUB_SQL_PARSING_RESULT_KEY
                            ] = sql_parsing_result
                            logger.debug(
                                f"Added DataHub SQL parsing result with "
                                f"{len(sql_parsing_result.column_lineage or [])} column lineages"
                            )
                    except Exception as e:
                        logger.warning(
                            f"Error running DataHub SQL parser for Athena: {e}",
                            exc_info=True,
                        )
                else:
                    logger.debug(
                        f"DataHub SQL parsing result already present in run_facets "
                        f"(added by SQLParser patch) with "
                        f"{len(operator_lineage.run_facets[DATAHUB_SQL_PARSING_RESULT_KEY].column_lineage or [])} column lineages"
                    )

                return operator_lineage

            except Exception as e:
                logger.warning(
                    f"Error in patched AthenaOperator.get_openlineage_facets_on_complete: {e}",
                    exc_info=True,
                )
                # Fall back to original method
                return original_get_openlineage_facets_on_complete(self, task_instance)

        # Apply the patch (mypy doesn't like dynamic method assignment, but it's necessary for patching)
        AthenaOperator.get_openlineage_facets_on_complete = (  # type: ignore[assignment,method-assign]
            get_openlineage_facets_on_complete  # type: ignore[assignment]
        )
        AthenaOperator._datahub_openlineage_patched = True  # type: ignore[attr-defined]

        logger.debug(
            "Patched AthenaOperator.get_openlineage_facets_on_complete to use DataHub SQL parser"
        )

    except ImportError as e:
        logger.debug(
            f"Could not patch AthenaOperator for OpenLineage (provider not installed or Airflow < 3.0): {e}"
        )
