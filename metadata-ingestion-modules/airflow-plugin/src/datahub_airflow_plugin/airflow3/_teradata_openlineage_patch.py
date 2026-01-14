"""
Patch for TeradataOperator to use DataHub's SQL parser.

TeradataOperator in Airflow 2.x provider mode uses DefaultExtractor which returns empty
OperatorLineage. This patch modifies get_openlineage_facets_on_complete() to use
DataHub's SQL parser, enabling lineage extraction.
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


def _should_patch_teradata_operator(operator_class: Any) -> bool:
    """Check if Teradata operator should be patched."""
    if not hasattr(operator_class, "get_openlineage_facets_on_complete"):
        openlineage_methods = [
            m
            for m in dir(operator_class)
            if "openlineage" in m.lower() or "facet" in m.lower()
        ]
        logger.warning(
            f"TeradataOperator.get_openlineage_facets_on_complete not found - "
            f"skipping patch. Available OpenLineage-related methods: {openlineage_methods}"
        )
        return False
    if hasattr(operator_class, "_datahub_openlineage_patched"):
        logger.debug("TeradataOperator already patched for OpenLineage")
        return False
    return True


def _render_teradata_sql_templates(
    sql: str, operator: Any, task_instance: "TaskInstance"
) -> str:
    """
    Render Jinja templates in Teradata SQL if they exist.

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
            rendered_query = operator.render_template(sql, context)
        else:
            # Fallback: try to render using Jinja2 directly
            from airflow.templates import SandboxedEnvironment

            jinja_env = SandboxedEnvironment()
            template = jinja_env.from_string(str(sql))
            rendered_query = template.render(**context)  # type: ignore[misc]

        logger.debug(f"Rendered Teradata SQL templates: {rendered_query[:200]}")
        return rendered_query
    except Exception as e:
        logger.warning(f"Failed to render Teradata SQL templates, using original: {e}")
        return sql


def _enhance_teradata_lineage_with_sql_parsing(
    operator_lineage: "OperatorLineage",
    rendered_sql: str,
    operator: Any,
) -> "OperatorLineage":
    """
    Enhance OperatorLineage with DataHub SQL parsing results.

    Modifies operator_lineage in place by adding SQL parsing result to run_facets.
    """
    # Check if SQL parsing result is already in run_facets (from SQLParser patch)
    if DATAHUB_SQL_PARSING_RESULT_KEY not in operator_lineage.run_facets:
        # SQLParser patch didn't add it - add it manually
        try:
            platform = "teradata"
            # Teradata uses database.table naming, no separate schema
            # Get database/schema from operator if available
            default_database = operator.schema if hasattr(operator, "schema") else None

            logger.debug(
                f"Running DataHub SQL parser for Teradata (platform={platform}, "
                f"default_db={default_database}): {rendered_sql[:200] if rendered_sql else 'None'}"
            )

            # Use DataHub's SQL parser
            sql_parsing_result = create_lineage_sql_parsed_result(
                query=rendered_sql,
                platform=platform,
                platform_instance=None,
                env=builder.DEFAULT_ENV,
                default_db=default_database,
                default_schema=None,
            )

            # Store the SQL parsing result in run_facets for DataHub listener
            if sql_parsing_result:
                operator_lineage.run_facets[DATAHUB_SQL_PARSING_RESULT_KEY] = (
                    sql_parsing_result
                )
                logger.debug(
                    f"Added DataHub SQL parsing result for Teradata with "
                    f"{len(sql_parsing_result.in_tables)} input tables, "
                    f"{len(sql_parsing_result.out_tables)} output tables, "
                    f"{len(sql_parsing_result.column_lineage or [])} column lineages"
                )
        except Exception as e:
            logger.warning(
                f"Error running DataHub SQL parser for Teradata: {e}",
                exc_info=True,
            )
    else:
        logger.debug(
            f"DataHub SQL parsing result already present in run_facets "
            f"(added by SQLParser patch) with "
            f"{len(operator_lineage.run_facets[DATAHUB_SQL_PARSING_RESULT_KEY].column_lineage or [])} column lineages"
        )

    return operator_lineage


def _create_teradata_openlineage_wrapper(
    original_get_openlineage_facets_on_complete: Any,
) -> Any:
    """Create wrapper function for Teradata operator's OpenLineage method."""
    # Import OperatorLineage at wrapper creation time to check availability
    # This avoids runtime import errors that would cause the patch to return None
    # Try multiple import paths for compatibility with different Airflow versions
    # Airflow 3.x: from airflow.providers.openlineage.extractors
    # Airflow 2.x provider: from airflow.providers.openlineage.extractors.base
    OperatorLineageClass: Any = None
    import_error = None
    try:
        # Try Airflow 3.x import path first
        from airflow.providers.openlineage.extractors import (
            OperatorLineage as OperatorLineageClass,
        )
    except (ImportError, ModuleNotFoundError) as e:
        import_error = e
        try:
            # Fallback for Airflow 2.x provider mode compatibility
            from airflow.providers.openlineage.extractors.base import (
                OperatorLineage as OperatorLineageClass,
            )

            import_error = None  # Success, clear the error
        except (ImportError, ModuleNotFoundError) as e2:
            # Both imports failed - log the more specific error
            import_error = e2 if "Operator" not in str(e) else e

    if OperatorLineageClass is None or import_error is not None:
        # Log warning but don't fail - this is expected in some environments
        error_msg = str(import_error) if import_error else "Unknown import error"
        logger.warning(
            f"Could not import OperatorLineage for Teradata patch: {error_msg}. "
            "This may be due to OpenLineage provider compatibility issues. "
            "Patch will not be applied."
        )
        # Return original function if import fails
        return original_get_openlineage_facets_on_complete

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
            # Get the SQL query from operator
            sql = self.sql
            if not sql:
                logger.debug("No SQL query found in TeradataOperator")
                return original_get_openlineage_facets_on_complete(self, task_instance)

            # Handle list of SQL statements (TeradataOperator supports both str and list)
            if isinstance(sql, list):
                # Join multiple statements with semicolon
                sql = ";\n".join(str(s) for s in sql)
            else:
                sql = str(sql)

            logger.debug(
                f"DataHub patched Teradata get_openlineage_facets_on_complete called for query: {sql[:100]}"
            )

            # Get the original OpenLineage result
            operator_lineage = original_get_openlineage_facets_on_complete(
                self, task_instance
            )

            # If original returns None (DefaultExtractor returns None),
            # create a new OperatorLineage so we can still add SQL parsing result
            if not operator_lineage:
                logger.debug(
                    "Original OpenLineage returned None for TeradataOperator, "
                    "creating new OperatorLineage for SQL parsing"
                )
                # OperatorLineageClass is already imported at wrapper creation time
                operator_lineage = OperatorLineageClass(  # type: ignore[misc]
                    inputs=[],
                    outputs=[],
                    job_facets={},
                    run_facets={},
                )

            logger.debug(
                f"Original Teradata OpenLineage result: inputs={len(operator_lineage.inputs)}, outputs={len(operator_lineage.outputs)}"
            )

            # Render SQL templates if needed
            rendered_sql = _render_teradata_sql_templates(sql, self, task_instance)

            # Enhance with SQL parsing
            operator_lineage = _enhance_teradata_lineage_with_sql_parsing(
                operator_lineage, rendered_sql, self
            )

            return operator_lineage

        except Exception as e:
            logger.warning(
                f"Error in patched TeradataOperator.get_openlineage_facets_on_complete: {e}",
                exc_info=True,
            )
            # Fall back to original method
            return original_get_openlineage_facets_on_complete(self, task_instance)

    return get_openlineage_facets_on_complete


def patch_teradata_operator() -> None:
    """
    Patch TeradataOperator to use DataHub's SQL parser for lineage extraction.

    This enhances the existing OpenLineage support with DataHub's SQL parser,
    which provides column-level lineage extraction for Teradata operators.
    """
    try:
        logger.debug("Attempting to patch TeradataOperator for OpenLineage")
        from airflow.providers.teradata.operators.teradata import TeradataOperator

        logger.debug(f"Successfully imported TeradataOperator: {TeradataOperator}")

        if not _should_patch_teradata_operator(TeradataOperator):
            logger.warning(
                "TeradataOperator patch check failed - patch will not be applied"
            )
            return
        logger.debug("TeradataOperator patch check passed - proceeding with patch")

        # Store original method
        original_get_openlineage_facets_on_complete = (
            TeradataOperator.get_openlineage_facets_on_complete
        )

        # Create wrapper function
        wrapper = _create_teradata_openlineage_wrapper(
            original_get_openlineage_facets_on_complete
        )

        # Check if wrapper creation failed (import error)
        # If wrapper is the same as original, the import failed and we shouldn't apply the patch
        if wrapper is original_get_openlineage_facets_on_complete:
            logger.debug(
                "TeradataOperator patch not applied - OperatorLineage import failed. "
                "Falling back to original OpenLineage behavior."
            )
            return

        # Apply the patch (mypy doesn't like dynamic method assignment, but it's necessary for patching)
        TeradataOperator.get_openlineage_facets_on_complete = (  # type: ignore[assignment,method-assign]
            wrapper  # type: ignore[assignment]
        )
        TeradataOperator._datahub_openlineage_patched = True  # type: ignore[attr-defined]

        logger.debug(
            "Successfully patched TeradataOperator.get_openlineage_facets_on_complete to use DataHub SQL parser"
        )

    except ImportError as e:
        logger.debug(
            f"Could not patch TeradataOperator for OpenLineage (provider not installed): {e}"
        )
