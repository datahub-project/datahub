import contextlib
import logging
import sys
import unittest.mock
from typing import TYPE_CHECKING, Any, Dict, Optional

from openlineage.client.facet import (
    ExtractionError,
    ExtractionErrorRunFacet,
    SqlJobFacet,
)

import datahub.emitter.mce_builder as builder
from datahub.ingestion.source.sql.sqlalchemy_uri_mapper import (
    get_platform_from_sqlalchemy_uri,
)
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_sql_parsed_result,
)
from datahub_airflow_plugin._constants import SQL_PARSING_RESULT_KEY
from datahub_airflow_plugin._datahub_ol_adapter import OL_SCHEME_TWEAKS
from datahub_airflow_plugin.airflow2._openlineage_compat import (
    USE_OPENLINEAGE_PROVIDER,
    BaseExtractor,
    OLExtractorManager,
    OperatorLineage,
    SnowflakeExtractor,
    SqlExtractor,
    TaskMetadata,
    get_operator_class,
    try_import_from_string,
)
from datahub_airflow_plugin.airflow2._shims import Operator

if TYPE_CHECKING:
    from airflow.models import DagRun, TaskInstance

    from datahub.ingestion.graph.client import DataHubGraph

    # For type checking, define a union type that covers both versions
    if sys.version_info >= (3, 10):
        from typing import TypeAlias
    else:
        from typing_extensions import TypeAlias

    # Define proper type aliases for the union type
    # Note: BaseExtractor, OLExtractorManager, etc. are already imported above at runtime
    from typing import Union

    ExtractResult: TypeAlias = Union[
        Any, Any
    ]  # Will be TaskMetadata or OperatorLineage at runtime

logger = logging.getLogger(__name__)
_DATAHUB_GRAPH_CONTEXT_KEY = "datahub_graph"

# Runtime type alias for the return type of extract() methods
if not TYPE_CHECKING:
    if USE_OPENLINEAGE_PROVIDER:
        ExtractResult = OperatorLineage
    else:
        ExtractResult = TaskMetadata


class ExtractorManager(OLExtractorManager):
    # TODO: On Airflow 2.7, the OLExtractorManager is part of the built-in Airflow API.
    # When available, we should use that instead. The same goe for most of the OL
    # extractors.

    def __init__(
        self,
        patch_sql_parser: bool = True,
        patch_snowflake_schema: bool = True,
        extract_athena_operator: bool = True,
        extract_bigquery_insert_job_operator: bool = True,
        extract_teradata_operator: bool = True,
    ):
        super().__init__()

        # Store patch/extractor configuration
        self._patch_sql_parser = patch_sql_parser
        self._patch_snowflake_schema = patch_snowflake_schema
        self._extract_athena_operator = extract_athena_operator
        self._extract_bigquery_insert_job_operator = (
            extract_bigquery_insert_job_operator
        )
        self._extract_teradata_operator = extract_teradata_operator

        # Legacy OpenLineage has task_to_extractor attribute, OpenLineage Provider doesn't
        # Register custom extractors only for Legacy OpenLineage (Provider has its own)
        if not USE_OPENLINEAGE_PROVIDER:
            _sql_operator_overrides = [
                # The OL BigQuery extractor has some complex logic to fetch detect
                # the BigQuery job_id and fetch lineage from there. However, it can't
                # generate CLL, so we disable it and use our own extractor instead.
                "BigQueryOperator",
                "BigQueryExecuteQueryOperator",
                # Athena also does something similar.
                "AWSAthenaOperator",
                # Additional types that OL doesn't support. This is only necessary because
                # on older versions of Airflow, these operators don't inherit from SQLExecuteQueryOperator.
                "SqliteOperator",
            ]
            for operator in _sql_operator_overrides:
                self.task_to_extractor.extractors[operator] = GenericSqlExtractor  # type: ignore[attr-defined]

            # Register custom extractors based on configuration
            if self._extract_athena_operator:
                self.task_to_extractor.extractors["AthenaOperator"] = (  # type: ignore[attr-defined]
                    AthenaOperatorExtractor
                )

            if self._extract_bigquery_insert_job_operator:
                self.task_to_extractor.extractors["BigQueryInsertJobOperator"] = (  # type: ignore[attr-defined]
                    BigQueryInsertJobOperatorExtractor
                )

            if self._extract_teradata_operator:
                self.task_to_extractor.extractors["TeradataOperator"] = (
                    TeradataOperatorExtractor
                )

        self._graph: Optional["DataHubGraph"] = None

    @contextlib.contextmanager
    def _patch_extractors(self):
        with contextlib.ExitStack() as stack:
            # Patch the SqlExtractor.extract() method if configured and available
            if self._patch_sql_parser and SqlExtractor is not None:
                stack.enter_context(
                    unittest.mock.patch.object(
                        SqlExtractor,
                        "extract",
                        _sql_extractor_extract,
                    )
                )

            # Patch the SnowflakeExtractor.default_schema property if configured and available
            if self._patch_snowflake_schema and SnowflakeExtractor is not None:
                stack.enter_context(
                    unittest.mock.patch.object(
                        SnowflakeExtractor,
                        "default_schema",
                        property(_snowflake_default_schema),
                    )
                )

            yield

    def extract_metadata(  # type: ignore[override]
        self,
        dagrun: "DagRun",
        task: "Operator",
        complete: bool = False,
        task_instance: Optional["TaskInstance"] = None,
        task_uuid: Optional[str] = None,
        graph: Optional["DataHubGraph"] = None,
    ) -> ExtractResult:
        self._graph = graph
        with self._patch_extractors():
            if USE_OPENLINEAGE_PROVIDER:
                # OpenLineage Provider: Does not have task_uuid parameter
                # In Airflow 3.x, the 'complete' parameter type changed from bool to TaskInstanceState
                return super().extract_metadata(dagrun, task, complete, task_instance)  # type: ignore[call-arg,arg-type]
            else:
                # Legacy OpenLineage: Has task_uuid parameter
                return super().extract_metadata(  # type: ignore[call-arg,arg-type]
                    dagrun,
                    task,
                    complete,  # type: ignore[arg-type]
                    task_instance,
                    task_uuid,
                )

    def _get_extractor(self, task: "Operator") -> Optional[BaseExtractor]:
        # For Legacy OpenLineage: Register GenericSqlExtractor as fallback for
        # any operator that inherits from SQLExecuteQueryOperator.
        # For OpenLineage Provider: Rely on SQLParser patch approach instead.
        if not USE_OPENLINEAGE_PROVIDER:
            clazz = get_operator_class(task)  # type: ignore[arg-type]
            SQLExecuteQueryOperator = try_import_from_string(
                "airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator"
            )
            if SQLExecuteQueryOperator and issubclass(clazz, SQLExecuteQueryOperator):
                # Legacy OpenLineage: Register GenericSqlExtractor in task_to_extractor.extractors
                self.task_to_extractor.extractors.setdefault(  # type: ignore[attr-defined]
                    clazz.__name__, GenericSqlExtractor
                )

        extractor = super()._get_extractor(task)

        # For OpenLineage Provider: If no extractor was found, check if this is a SQL operator
        # that should use GenericSqlExtractor (e.g., SqliteOperator which provider doesn't support)
        if (
            USE_OPENLINEAGE_PROVIDER
            and extractor is None
            and GenericSqlExtractor is not None
        ):
            clazz = get_operator_class(task)  # type: ignore[arg-type]
            # Check if this is SqliteOperator (provider doesn't have an extractor for it)
            if clazz.__name__ == "SqliteOperator":
                # Create a GenericSqlExtractor instance for this operator
                extractor = GenericSqlExtractor(task)  # type: ignore[call-arg]

        if extractor and not USE_OPENLINEAGE_PROVIDER:
            # set_context only exists in Legacy OpenLineage
            extractor.set_context(_DATAHUB_GRAPH_CONTEXT_KEY, self._graph)  # type: ignore[attr-defined]
        return extractor


if SqlExtractor is not None:

    class GenericSqlExtractor(SqlExtractor):  # type: ignore
        # Note that the extract() method is patched elsewhere.

        @property
        def default_schema(self):
            return super().default_schema

        def _get_scheme(self) -> Optional[str]:
            # Best effort conversion to DataHub platform names.

            with contextlib.suppress(Exception):
                if self.hook:
                    if hasattr(self.hook, "get_uri"):
                        uri = self.hook.get_uri()
                        return get_platform_from_sqlalchemy_uri(uri)

            return self.conn.conn_type or super().dialect

        def _get_database(self) -> Optional[str]:
            if self.conn:
                # For BigQuery, the "database" is the project name.
                if hasattr(self.conn, "project_id"):
                    return self.conn.project_id

                return self.conn.schema
            return None

else:
    # SqlExtractor is not available (OpenLineage Provider package)
    GenericSqlExtractor = None  # type: ignore


def _sql_extractor_extract(self: "SqlExtractor") -> Optional[ExtractResult]:
    # Why not override the OL sql_parse method directly, instead of overriding
    # extract()? A few reasons:
    #
    # 1. We would want to pass the default_db and graph instance into our sql parser
    #    method. The OL code doesn't pass the default_db (despite having it available),
    #    and it's not clear how to get the graph instance into that method.
    # 2. OL has some janky logic to fetch table schemas as part of the sql extractor.
    #    We don't want that behavior and this lets us disable it.
    # 3. Our SqlParsingResult already has DataHub urns, whereas using SqlMeta would
    #    require us to convert those urns to OL uris, just for them to get converted
    #    back to urns later on in our processing.

    task_name = f"{self.operator.dag_id}.{self.operator.task_id}"
    sql = self.operator.sql

    default_database = getattr(self.operator, "database", None)
    if not default_database:
        default_database = self.database
    default_schema = self.default_schema

    # TODO: Add better handling for sql being a list of statements.
    if isinstance(sql, list):
        logger.info(f"Got list of SQL statements for {task_name}. Using first one.")
        sql = sql[0]

    # Run the SQL parser.
    scheme = self.scheme
    platform = OL_SCHEME_TWEAKS.get(scheme, scheme)

    return _parse_sql_into_task_metadata(
        self,
        sql,
        platform=platform,
        default_database=default_database,
        default_schema=default_schema,
    )


def _normalize_sql(sql: str) -> str:
    """Normalize SQL for logging (strip extra whitespace)"""
    if SqlExtractor is not None and hasattr(SqlExtractor, "_normalize_sql"):
        return SqlExtractor._normalize_sql(sql)
    # Fallback normalization
    return " ".join(sql.split())


def _create_lineage_metadata(
    task_name: str,
    run_facets: Dict[str, Any],
    job_facets: Dict[str, Any],
) -> Optional[ExtractResult]:
    """Create TaskMetadata (Legacy OpenLineage) or OperatorLineage (OpenLineage Provider)"""
    if USE_OPENLINEAGE_PROVIDER:
        # OpenLineage Provider: Return OperatorLineage (no name field)
        return OperatorLineage(  # type: ignore
            inputs=[],
            outputs=[],
            run_facets=run_facets,
            job_facets=job_facets,
        )
    else:
        # Legacy OpenLineage: Return TaskMetadata (with name field)
        return TaskMetadata(  # type: ignore
            name=task_name,
            inputs=[],
            outputs=[],
            run_facets=run_facets,
            job_facets=job_facets,
        )


def _parse_sql_into_task_metadata(
    self: "BaseExtractor",
    sql: str,
    platform: str,
    default_database: Optional[str],
    default_schema: Optional[str],
) -> Optional[ExtractResult]:
    task_name = f"{self.operator.dag_id}.{self.operator.task_id}"

    run_facets = {}
    job_facets = {"sql": SqlJobFacet(query=_normalize_sql(sql))}

    # Get graph from context (Legacy OpenLineage only)
    graph = None
    if hasattr(self, "context"):
        graph = self.context.get(_DATAHUB_GRAPH_CONTEXT_KEY, None)  # type: ignore[attr-defined]

    self.log.debug(
        "Running the SQL parser %s (platform=%s, default db=%s, schema=%s): %s",
        "with graph client" if graph else "in offline mode",
        platform,
        default_database,
        default_schema,
        sql,
    )
    sql_parsing_result: SqlParsingResult = create_lineage_sql_parsed_result(
        query=sql,
        graph=graph,
        platform=platform,
        platform_instance=None,
        env=builder.DEFAULT_ENV,
        default_db=default_database,
        default_schema=default_schema,
    )
    self.log.debug(f"Got sql lineage {sql_parsing_result}")

    if sql_parsing_result.debug_info.error:
        error = sql_parsing_result.debug_info.error
        run_facets["extractionError"] = ExtractionErrorRunFacet(
            totalTasks=1,
            failedTasks=1,
            errors=[
                ExtractionError(
                    errorMessage=str(error),
                    stackTrace=None,
                    task="datahub_sql_parser",
                    taskNumber=None,
                )
            ],
        )

    # Save sql_parsing_result to the facets dict. It is removed from the
    # facet dict in the extractor's processing logic.
    run_facets[SQL_PARSING_RESULT_KEY] = sql_parsing_result  # type: ignore

    return _create_lineage_metadata(task_name, run_facets, job_facets)


class BigQueryInsertJobOperatorExtractor(BaseExtractor):
    def extract(self) -> Optional[ExtractResult]:
        from airflow.providers.google.cloud.operators.bigquery import (
            BigQueryInsertJobOperator,  # type: ignore
        )

        operator: "BigQueryInsertJobOperator" = self.operator
        sql = operator.configuration.get("query", {}).get("query")
        if not sql:
            self.log.warning("No query found in BigQueryInsertJobOperator")
            return None

        destination_table = operator.configuration.get("query", {}).get(
            "destinationTable"
        )
        destination_table_urn = None
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

        task_metadata = _parse_sql_into_task_metadata(
            self,
            sql,
            platform="bigquery",
            default_database=operator.project_id,
            default_schema=None,
        )

        if destination_table_urn and task_metadata:
            sql_parsing_result = task_metadata.run_facets.get(SQL_PARSING_RESULT_KEY)
            if sql_parsing_result and isinstance(sql_parsing_result, SqlParsingResult):
                sql_parsing_result.out_tables.append(destination_table_urn)

        return task_metadata


class AthenaOperatorExtractor(BaseExtractor):
    def extract(self) -> Optional[ExtractResult]:
        from airflow.providers.amazon.aws.operators.athena import (
            AthenaOperator,  # type: ignore
        )

        operator: "AthenaOperator" = self.operator
        sql = operator.query
        if not sql:
            self.log.warning("No query found in AthenaOperator")
            return None

        return _parse_sql_into_task_metadata(
            self,
            sql,
            platform="athena",
            default_database=None,
            default_schema=self.operator.database,
        )


def _snowflake_default_schema(self: "SnowflakeExtractor") -> Optional[str]:
    if hasattr(self.operator, "schema") and self.operator.schema is not None:
        return self.operator.schema
    return (
        self.conn.extra_dejson.get("extra__snowflake__schema", "")
        or self.conn.extra_dejson.get("schema", "")
        or self.conn.schema
    )
    # TODO: Should we try a fallback of:
    # execute_query_on_hook(self.hook, "SELECT current_schema();")[0][0]

    # execute_query_on_hook(self.hook, "SELECT current_schema();")


class TeradataOperatorExtractor(BaseExtractor):
    """Extractor for Teradata SQL operations.

    Extracts lineage from TeradataOperator tasks by parsing the SQL queries
    and understanding Teradata's two-tier database.table naming convention.
    """

    def extract(self) -> Optional[ExtractResult]:
        from airflow.providers.teradata.operators.teradata import TeradataOperator

        operator: "TeradataOperator" = self.operator
        sql = operator.sql
        if not sql:
            self.log.warning("No query found in TeradataOperator")
            return None

        return _parse_sql_into_task_metadata(
            self,
            sql,
            platform="teradata",
            default_database=None,
            default_schema=None,
        )
