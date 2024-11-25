import contextlib
import logging
import unittest.mock
from typing import TYPE_CHECKING, Optional

import datahub.emitter.mce_builder as builder
from datahub.ingestion.source.sql.sqlalchemy_uri_mapper import (
    get_platform_from_sqlalchemy_uri,
)
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_sql_parsed_result,
)
from openlineage.airflow.extractors import BaseExtractor
from openlineage.airflow.extractors import ExtractorManager as OLExtractorManager
from openlineage.airflow.extractors import TaskMetadata
from openlineage.airflow.extractors.snowflake_extractor import SnowflakeExtractor
from openlineage.airflow.extractors.sql_extractor import SqlExtractor
from openlineage.airflow.utils import get_operator_class, try_import_from_string
from openlineage.client.facet import (
    ExtractionError,
    ExtractionErrorRunFacet,
    SqlJobFacet,
)

from datahub_airflow_plugin._airflow_shims import Operator
from datahub_airflow_plugin._datahub_ol_adapter import OL_SCHEME_TWEAKS

if TYPE_CHECKING:
    from airflow.models import DagRun, TaskInstance
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)
_DATAHUB_GRAPH_CONTEXT_KEY = "datahub_graph"
SQL_PARSING_RESULT_KEY = "datahub_sql"


class ExtractorManager(OLExtractorManager):
    # TODO: On Airflow 2.7, the OLExtractorManager is part of the built-in Airflow API.
    # When available, we should use that instead. The same goe for most of the OL
    # extractors.

    def __init__(self):
        super().__init__()

        _sql_operator_overrides = [
            # The OL BigQuery extractor has some complex logic to fetch detect
            # the BigQuery job_id and fetch lineage from there. However, it can't
            # generate CLL, so we disable it and use our own extractor instead.
            "BigQueryOperator",
            "BigQueryExecuteQueryOperator",
            # Athena also does something similar.
            "AthenaOperator",
            "AWSAthenaOperator",
            # Additional types that OL doesn't support. This is only necessary because
            # on older versions of Airflow, these operators don't inherit from SQLExecuteQueryOperator.
            "SqliteOperator",
        ]
        for operator in _sql_operator_overrides:
            self.task_to_extractor.extractors[operator] = GenericSqlExtractor

        self.task_to_extractor.extractors[
            "BigQueryInsertJobOperator"
        ] = BigQueryInsertJobOperatorExtractor

        self._graph: Optional["DataHubGraph"] = None

    @contextlib.contextmanager
    def _patch_extractors(self):
        with contextlib.ExitStack() as stack:
            # Patch the SqlExtractor.extract() method.
            stack.enter_context(
                unittest.mock.patch.object(
                    SqlExtractor,
                    "extract",
                    _sql_extractor_extract,
                )
            )

            # Patch the SnowflakeExtractor.default_schema property.
            stack.enter_context(
                unittest.mock.patch.object(
                    SnowflakeExtractor,
                    "default_schema",
                    property(_snowflake_default_schema),
                )
            )

            # TODO: Override the BigQuery extractor to use the DataHub SQL parser.
            # self.extractor_manager.add_extractor()

            # TODO: Override the Athena extractor to use the DataHub SQL parser.

            yield

    def extract_metadata(
        self,
        dagrun: "DagRun",
        task: "Operator",
        complete: bool = False,
        task_instance: Optional["TaskInstance"] = None,
        task_uuid: Optional[str] = None,
        graph: Optional["DataHubGraph"] = None,
    ) -> TaskMetadata:
        self._graph = graph
        with self._patch_extractors():
            return super().extract_metadata(
                dagrun, task, complete, task_instance, task_uuid
            )

    def _get_extractor(self, task: "Operator") -> Optional[BaseExtractor]:
        # By adding this, we can use the generic extractor as a fallback for
        # any operator that inherits from SQLExecuteQueryOperator.
        clazz = get_operator_class(task)
        SQLExecuteQueryOperator = try_import_from_string(
            "airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator"
        )
        if SQLExecuteQueryOperator and issubclass(clazz, SQLExecuteQueryOperator):
            self.task_to_extractor.extractors.setdefault(
                clazz.__name__, GenericSqlExtractor
            )

        extractor = super()._get_extractor(task)
        if extractor:
            extractor.set_context(_DATAHUB_GRAPH_CONTEXT_KEY, self._graph)
        return extractor


class GenericSqlExtractor(SqlExtractor):
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


def _sql_extractor_extract(self: "SqlExtractor") -> TaskMetadata:
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


def _parse_sql_into_task_metadata(
    self: "BaseExtractor",
    sql: str,
    platform: str,
    default_database: Optional[str],
    default_schema: Optional[str],
) -> TaskMetadata:
    task_name = f"{self.operator.dag_id}.{self.operator.task_id}"

    run_facets = {}
    job_facets = {"sql": SqlJobFacet(query=SqlExtractor._normalize_sql(sql))}

    # Prepare to run the SQL parser.
    graph = self.context.get(_DATAHUB_GRAPH_CONTEXT_KEY, None)

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

    return TaskMetadata(
        name=task_name,
        inputs=[],
        outputs=[],
        run_facets=run_facets,
        job_facets=job_facets,
    )


class BigQueryInsertJobOperatorExtractor(BaseExtractor):
    def extract(self) -> Optional[TaskMetadata]:
        from airflow.providers.google.cloud.operators.bigquery import (
            BigQueryInsertJobOperator,  # type: ignore
        )

        operator: "BigQueryInsertJobOperator" = self.operator
        sql = operator.configuration.get("query", {}).get("query")
        if not sql:
            self.log.warning("No query found in BigQueryInsertJobOperator")
            return None

        return _parse_sql_into_task_metadata(
            self,
            sql,
            platform="bigquery",
            default_database=operator.project_id,
            default_schema=None,
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
