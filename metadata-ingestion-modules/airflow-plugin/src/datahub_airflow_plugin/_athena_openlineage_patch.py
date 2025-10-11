"""
Patch for AthenaOperator to use DataHub's SQL parser.

AthenaOperator in Airflow 3.x uses SQLParser with dialect="generic", which doesn't provide
column-level lineage. This patch modifies get_openlineage_facets_on_complete() to use
DataHub's SQL parser instead, enabling column-level lineage extraction.
"""

import logging
from typing import TYPE_CHECKING, Any, Optional

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
                from datahub.sql_parsing.sqlglot_lineage import (
                    create_lineage_sql_parsed_result,
                )

                logger.info(
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

                logger.info(
                    f"Original Athena OpenLineage result: inputs={len(operator_lineage.inputs)}, outputs={len(operator_lineage.outputs)}"
                )

                # Now enhance with DataHub SQL parsing
                try:
                    # Get platform and database info
                    platform = "athena"
                    default_database = (
                        self.database if hasattr(self, "database") else None
                    )

                    logger.info(
                        f"Running DataHub SQL parser for Athena (platform={platform}, "
                        f"default_db={default_database}): {self.query}"
                    )

                    # Use DataHub's SQL parser
                    sql_parsing_result = create_lineage_sql_parsed_result(
                        query=self.query,
                        platform=platform,
                        platform_instance=None,
                        env="PROD",
                        default_db=default_database,
                        default_schema=None,
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
                        f"Error running DataHub SQL parser for Athena: {e}",
                        exc_info=True,
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

        logger.info(
            "Patched AthenaOperator.get_openlineage_facets_on_complete to use DataHub SQL parser"
        )

    except ImportError as e:
        logger.debug(
            f"Could not patch AthenaOperator for OpenLineage (provider not installed or Airflow < 3.0): {e}"
        )
