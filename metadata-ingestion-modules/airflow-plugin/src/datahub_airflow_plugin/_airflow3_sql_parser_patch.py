"""
Patch for Airflow 3.0+ SQLParser to use DataHub's SQL parser.

In Airflow 3.0+, SQL operators call SQLParser.generate_openlineage_metadata_from_sql()
directly rather than using extractors. This module patches that method to use DataHub's
SQL parser, which provides better column-level lineage support.
"""

import logging
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from airflow.providers.openlineage.extractors import OperatorLineage

logger = logging.getLogger(__name__)


def _datahub_generate_openlineage_metadata_from_sql(
    self,
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
    """
    try:
        from airflow.providers.openlineage.extractors import OperatorLineage
        from openlineage.client.facet import SqlJobFacet

        import datahub.emitter.mce_builder as builder
        from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result
        from datahub_airflow_plugin._datahub_ol_adapter import OL_SCHEME_TWEAKS

        # Get platform from dialect
        platform = self.dialect or "sql"
        platform = OL_SCHEME_TWEAKS.get(platform, platform)

        # Get default database and schema
        # database_info is a DatabaseInfo object (dataclass/namedtuple), not a dict
        default_database = database or getattr(database_info, "database", None)
        default_schema = self.default_schema

        # Handle list of SQL statements
        if isinstance(sql, list):
            logger.debug("Got list of SQL statements. Using first one for parsing.")
            sql = sql[0] if sql else ""

        # Run DataHub's SQL parser
        from datahub_airflow_plugin.datahub_listener import get_airflow_plugin_listener

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

        # Convert DataHub URNs to OpenLineage Dataset objects
        # We extract table components from URNs and convert them to OL format
        def _urn_to_ol_dataset(urn: str):
            """Convert DataHub URN to OpenLineage Dataset format."""
            from openlineage.client.event_v2 import Dataset as OpenLineageDataset

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

        # Store the sql_parsing_result in run_facets for later retrieval by the listener
        # We use a custom facet key that the listener can check for
        # Note: We cannot add attributes directly to OperatorLineage as it uses @define (frozen)
        DATAHUB_SQL_PARSING_RESULT_KEY = "datahub_sql_parsing_result"

        run_facets = {DATAHUB_SQL_PARSING_RESULT_KEY: sql_parsing_result}

        # Create OperatorLineage with DataHub's results
        operator_lineage = OperatorLineage(
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
        # Import here to avoid circular dependency
        from airflow.providers.openlineage.sqlparser import SQLParser

        # Get the original method
        original_method = object.__getattribute__(
            SQLParser, "_original_generate_openlineage_metadata_from_sql"
        )
        return original_method(
            self, sql, hook, database_info, database, sqlalchemy_engine, use_connection
        )


def patch_sqlparser() -> None:
    """
    Patch SQLParser.generate_openlineage_metadata_from_sql to use DataHub's SQL parser.

    This should be called early in the plugin initialization, before any SQL operators are used.
    """
    try:
        from airflow.providers.openlineage.sqlparser import SQLParser

        # Store original method for fallback
        if not hasattr(SQLParser, "_original_generate_openlineage_metadata_from_sql"):
            SQLParser._original_generate_openlineage_metadata_from_sql = (
                SQLParser.generate_openlineage_metadata_from_sql
            )

        SQLParser.generate_openlineage_metadata_from_sql = (
            _datahub_generate_openlineage_metadata_from_sql
        )
        logger.info(
            "Patched SQLParser.generate_openlineage_metadata_from_sql with DataHub SQL parser"
        )

    except ImportError:
        # SQLParser not available (Airflow < 3.0 or openlineage provider not installed)
        logger.debug("SQLParser not available, skipping patch (likely Airflow < 3.0)")
