"""
Patch for Airflow 3.0+ SQLParser to use DataHub's SQL parser.

In Airflow 3.0+, SQL operators call SQLParser.generate_openlineage_metadata_from_sql()
directly rather than using extractors. This module patches that method to use DataHub's
SQL parser, which provides better column-level lineage support.
"""

import logging
from types import TracebackType
from typing import TYPE_CHECKING, Any, Callable, Optional

# Airflow 3.x specific imports (wrapped in try/except for version compatibility)
try:
    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.providers.openlineage.sqlparser import DatabaseInfo
    from openlineage.client.event_v2 import Dataset as OpenLineageDataset
    from openlineage.client.facet import SqlJobFacet

    AIRFLOW3_IMPORTS_AVAILABLE = True
except ImportError:
    # Not available on Airflow < 3.0
    # Set to None for runtime checks, type checker will see these as None
    OperatorLineage = None  # type: ignore[assignment,misc]
    DatabaseInfo = None  # type: ignore[assignment,misc]
    OpenLineageDataset = None  # type: ignore[assignment,misc]
    SqlJobFacet = None  # type: ignore[assignment,misc]
    AIRFLOW3_IMPORTS_AVAILABLE = False

# DataHub imports (always available)
import datahub.emitter.mce_builder as builder
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result
from datahub_airflow_plugin._constants import DATAHUB_SQL_PARSING_RESULT_KEY
from datahub_airflow_plugin._datahub_ol_adapter import OL_SCHEME_TWEAKS

if TYPE_CHECKING:
    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.providers.openlineage.sqlparser import DatabaseInfo
    from openlineage.client.event_v2 import Dataset as OpenLineageDataset
    from openlineage.client.facet import SqlJobFacet

logger = logging.getLogger(__name__)

# Store the original SQLParser method for fallback
_original_sql_parser_method: Optional[Callable[..., Any]] = None


def _datahub_generate_openlineage_metadata_from_sql(
    self: Any,
    sql: Any,
    hook: Any,
    database_info: dict,
    database: Optional[str] = None,
    sqlalchemy_engine: Optional[Any] = None,
    use_connection: bool = True,
) -> Optional["OperatorLineage"]:
    """
    Override SQLParser.generate_openlineage_metadata_from_sql to use DataHub's SQL parser.

    This is necessary because in Airflow 3.0+, SQL operators call SQLParser directly
    rather than using extractors. We intercept this call and use DataHub's SQL parser
    to generate lineage with column-level lineage support.

    When OpenLineage plugin is enabled (disable_openlineage_plugin=False), we call both
    parsers: OpenLineage gets its own parsing results, while DataHub's enhanced parsing
    is stored in a custom facet for the DataHub listener to extract.
    """
    try:
        # Import here to avoid circular dependency (datahub_listener -> _airflow_compat -> this module)
        from datahub_airflow_plugin._config import get_lineage_config
        from datahub_airflow_plugin.datahub_listener import get_airflow_plugin_listener

        # Check if OpenLineage plugin is enabled
        try:
            config = get_lineage_config()
            openlineage_enabled = not config.disable_openlineage_plugin
        except Exception as e:
            logger.warning(
                f"Could not load config to check disable_openlineage_plugin: {e}"
            )
            openlineage_enabled = False

        # If OpenLineage is enabled, call the original parser first to get its results
        ol_result = None
        if openlineage_enabled and _original_sql_parser_method is not None:
            try:
                logger.debug(
                    "OpenLineage plugin enabled - calling original parser for OpenLineage"
                )
                ol_result = _original_sql_parser_method(
                    self,
                    sql,
                    hook,
                    database_info,
                    database,
                    sqlalchemy_engine,
                    use_connection,
                )
                logger.debug(f"OpenLineage parser result: {ol_result}")
            except Exception as e:
                logger.warning(
                    f"Error calling original OpenLineage parser, will use only DataHub parser: {e}",
                    exc_info=True,
                )

        # Handle missing database_info by creating a minimal one from connection
        if database_info is None:
            # Get basic properties from hook's connection
            conn = getattr(hook, "get_connection", lambda: None)()
            scheme = getattr(conn, "conn_type", None) if conn else None
            db_name = getattr(conn, "schema", None) if conn else None

            database_info = DatabaseInfo(
                scheme=scheme,
                authority=None,
                database=db_name,
                information_schema_columns=[],
                information_schema_table_name="",
                use_flat_cross_db_query=False,
                is_information_schema_cross_db=False,
                is_uppercase_names=False,
                normalize_name_method=lambda x: x.lower(),
            )
            logger.debug(
                f"Created minimal DatabaseInfo from connection: scheme={scheme}, database={db_name}"
            )

        # Get platform from dialect or from database_info scheme
        # If dialect is "generic", prefer database_info.scheme (connection type)
        platform = self.dialect or "sql"
        if platform == "generic" and database_info:
            # Use the actual connection type instead of "generic"
            platform = getattr(database_info, "scheme", platform) or platform
            if platform == "generic":
                raise ValueError(
                    "Could not determine platform from generic dialect or database_info"
                )

        platform = OL_SCHEME_TWEAKS.get(platform, platform)

        # Get default database and schema
        # database_info is a DatabaseInfo object (dataclass/namedtuple), not a dict
        default_database = database or getattr(database_info, "database", None)
        default_schema = self.default_schema

        # Handle list of SQL statements
        if isinstance(sql, list):
            logger.debug("Got list of SQL statements. Using first one for parsing.")
            sql = sql[0] if sql else ""

        # Check if SQL still contains templates (should be rendered by operator)
        if "{{" in str(sql):
            logger.warning(
                f"SQL still contains Jinja templates - lineage extraction may fail. "
                f"SQL: {sql[:200]}... "
                f"This usually means templates weren't rendered before SQL parsing."
            )

        # Run DataHub's SQL parser
        listener = get_airflow_plugin_listener()
        graph = listener.graph if listener else None

        logger.debug(
            "Running DataHub SQL parser %s (platform=%s, default db=%s, schema=%s): %s",
            "with graph client" if graph else "in offline mode",
            platform,
            default_database,
            default_schema,
            sql,
        )

        sql_parsing_result = create_lineage_sql_parsed_result(
            query=sql,
            graph=graph,
            platform=platform,
            platform_instance=None,
            env=builder.DEFAULT_ENV,
            default_db=default_database,
            default_schema=default_schema,
        )

        logger.debug(f"DataHub SQL parser result: {sql_parsing_result}")

        # Store the sql_parsing_result in run_facets for later retrieval by the DataHub listener
        # If OpenLineage plugin is enabled and we got a result from the original parser,
        # use OpenLineage's result but add DataHub's parsing to the facets
        if ol_result is not None:
            logger.debug(
                "Using OpenLineage parser result for OperatorLineage, "
                "adding DataHub parsing to run_facets"
            )
            # Add DataHub's SQL parsing result to the existing run_facets
            # OperatorLineage is frozen (uses @define), so we need to create a new dict
            updated_run_facets = dict(ol_result.run_facets or {})
            updated_run_facets[DATAHUB_SQL_PARSING_RESULT_KEY] = sql_parsing_result

            # Create new OperatorLineage with OpenLineage's inputs/outputs but DataHub's facet
            operator_lineage = OperatorLineage(  # type: ignore[misc]
                inputs=ol_result.inputs,
                outputs=ol_result.outputs,
                job_facets=ol_result.job_facets,
                run_facets=updated_run_facets,
            )
            return operator_lineage

        # OpenLineage is disabled or original parser failed - use DataHub's parsing for everything
        logger.debug(
            "OpenLineage plugin disabled or parser unavailable - "
            "using DataHub parser result for OperatorLineage"
        )

        # Convert DataHub URNs to OpenLineage Dataset objects
        def _urn_to_ol_dataset(urn: str) -> "OpenLineageDataset":
            """Convert DataHub URN to OpenLineage Dataset format."""
            # Parse URN to extract database, schema, table
            # URN format: urn:li:dataset:(urn:li:dataPlatform:{platform},{database}.{schema}.{table},{env})
            try:
                parts = urn.split(",")
                if len(parts) >= 2:
                    # Extract table path from URN
                    table_path = parts[1]  # e.g., "database.schema.table"

                    # Create OL namespace and name
                    # For now, use platform as namespace and full path as name
                    namespace = f"{platform}://{default_database or 'default'}"
                    name = table_path

                    return OpenLineageDataset(namespace=namespace, name=name)
            except Exception as e:
                logger.debug(f"Error converting URN {urn} to OL Dataset: {e}")

            # Fallback: use URN as name
            return OpenLineageDataset(namespace=f"{platform}://default", name=urn)

        inputs = [_urn_to_ol_dataset(urn) for urn in sql_parsing_result.in_tables]
        outputs = [_urn_to_ol_dataset(urn) for urn in sql_parsing_result.out_tables]

        run_facets = {DATAHUB_SQL_PARSING_RESULT_KEY: sql_parsing_result}

        # Create OperatorLineage with DataHub's results
        operator_lineage = OperatorLineage(  # type: ignore[misc]
            inputs=inputs,
            outputs=outputs,
            job_facets={"sql": SqlJobFacet(query=sql)},
            run_facets=run_facets,
        )
        return operator_lineage

    except Exception as e:
        logger.warning(
            f"Error in DataHub SQL parser, falling back to default OpenLineage parser: {e}",
            exc_info=True,
        )
        # Fall back to original implementation
        if _original_sql_parser_method is None:
            raise RuntimeError(
                "Original SQLParser method not stored. patch_sqlparser() may not have been called."
            ) from None
        return _original_sql_parser_method(
            self, sql, hook, database_info, database, sqlalchemy_engine, use_connection
        )


class SQLParserPatch:
    """
    Context manager for patching Airflow's SQLParser with DataHub's SQL parser.

    This class encapsulates the patching logic and manages the global state properly.
    It can be used as a context manager for automatic cleanup, or with explicit
    patch/unpatch methods for manual control.

    Usage:
        # As a context manager (recommended for testing)
        with SQLParserPatch():
            # Code runs with patched SQLParser
            pass
        # Automatically unpatched on exit

        # Or with explicit control
        patcher = SQLParserPatch()
        patcher.patch()
        try:
            # ... plugin lifetime ...
        finally:
            patcher.unpatch()

    The patch stores the original SQLParser method and replaces it with DataHub's
    enhanced implementation that provides column-level lineage support.
    """

    def patch(self) -> "SQLParserPatch":
        """
        Apply the SQLParser patch.

        Stores the original SQLParser.generate_openlineage_metadata_from_sql method
        and replaces it with DataHub's enhanced implementation.

        Returns:
            self for method chaining
        """
        global _original_sql_parser_method

        try:
            from airflow.providers.openlineage.sqlparser import SQLParser

            # Store original method for fallback (only if not already patched)
            if _original_sql_parser_method is None:
                _original_sql_parser_method = (
                    SQLParser.generate_openlineage_metadata_from_sql
                )

            SQLParser.generate_openlineage_metadata_from_sql = (  # type: ignore[assignment,method-assign]
                _datahub_generate_openlineage_metadata_from_sql  # type: ignore[assignment,method-assign]
            )
            logger.debug(
                "Patched SQLParser.generate_openlineage_metadata_from_sql with DataHub SQL parser"
            )

        except ImportError:
            # SQLParser not available (Airflow < 3.0 or openlineage provider not installed)
            logger.debug(
                "SQLParser not available, skipping patch (likely Airflow < 3.0)"
            )

        return self

    def unpatch(self) -> "SQLParserPatch":
        """
        Remove the SQLParser patch and restore the original method.

        This is primarily useful for testing to ensure clean state between tests.
        In production, the patch typically stays active for the process lifetime.

        Returns:
            self for method chaining
        """
        global _original_sql_parser_method

        if _original_sql_parser_method is None:
            logger.debug("SQLParser not patched, nothing to unpatch")
            return self

        try:
            from airflow.providers.openlineage.sqlparser import SQLParser

            # Restore original method
            SQLParser.generate_openlineage_metadata_from_sql = (  # type: ignore[method-assign]
                _original_sql_parser_method
            )
            logger.debug("Unpatched SQLParser, restored original method")

        except ImportError:
            logger.debug("SQLParser not available, nothing to unpatch")
        finally:
            # Clear the stored reference to allow re-patching
            _original_sql_parser_method = None

        return self

    def __enter__(self) -> "SQLParserPatch":
        """Context manager entry: apply the patch."""
        return self.patch()

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Context manager exit: remove the patch."""
        self.unpatch()


# Global patcher instance for backward compatibility
_global_patcher = SQLParserPatch()


def patch_sqlparser() -> None:
    """
    Patch SQLParser.generate_openlineage_metadata_from_sql to use DataHub's SQL parser.

    This is a convenience function that wraps SQLParserPatch.patch() for backward
    compatibility with existing code.

    This should be called early in the plugin initialization, before any SQL operators are used.

    When both DataHub and OpenLineage plugins are enabled (disable_openlineage_plugin=False),
    the patch calls BOTH parsers:
    - OpenLineage's original parser provides inputs/outputs for OpenLineage plugin
    - DataHub's enhanced parser (with column-level lineage) is stored in run_facets
      for DataHub listener to extract

    When only DataHub is enabled (disable_openlineage_plugin=True), only DataHub's
    parser runs and provides both the OperatorLineage structure and the enhanced parsing.
    """
    _global_patcher.patch()


def unpatch_sqlparser() -> None:
    """
    Remove the SQLParser patch and restore the original method.

    This is a convenience function that wraps SQLParserPatch.unpatch() for consistency.

    This is primarily useful for testing to ensure clean state between tests.
    In production, the patch typically stays active for the process lifetime.
    """
    _global_patcher.unpatch()
