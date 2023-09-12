import contextlib
import logging
import unittest.mock
from typing import TYPE_CHECKING, Optional

import datahub.emitter.mce_builder as builder
from datahub.utilities.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_sql_parsed_result,
)
from openlineage.airflow.extractors import BaseExtractor
from openlineage.airflow.extractors import ExtractorManager as OLExtractorManager
from openlineage.airflow.extractors import TaskMetadata
from openlineage.airflow.extractors.snowflake_extractor import SnowflakeExtractor
from openlineage.airflow.extractors.sql_extractor import SqlExtractor
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

        self._graph: Optional["DataHubGraph"] = None

    @contextlib.contextmanager
    def _patch_extractors(self):
        with contextlib.ExitStack() as stack:
            # Patch the SqlExtractor.extract() method.
            # TODO: Make this work for Airflow 2.7+.
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
                    property(snowflake_default_schema),
                )
            )

            # TODO: Override the BigQuery extractor to use the DataHub SQL parser.
            # self.extractor_manager.add_extractor()

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
        extractor = super()._get_extractor(task)
        if extractor:
            extractor.set_context(_DATAHUB_GRAPH_CONTEXT_KEY, self._graph)
        return extractor


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

    run_facets = {}
    job_facets = {"sql": SqlJobFacet(query=self._normalize_sql(sql))}

    # Prepare to run the SQL parser.
    graph = self.context.get(_DATAHUB_GRAPH_CONTEXT_KEY, None)

    database = getattr(self.operator, "database", None)
    if not database:
        database = self._get_database()

    # TODO: Add better handling for sql being a list of statements.
    if isinstance(sql, list):
        logger.info(f"Got list of SQL statements for {task_name}. Using first one.")
        sql = sql[0]

    # Run the SQL parser.
    self.log.debug(
        "Running the SQL parser (%s): %s",
        "with graph client" if graph else "in offline mode",
        sql,
    )
    scheme = self.scheme
    platform = OL_SCHEME_TWEAKS.get(scheme, scheme)
    sql_parsing_result: SqlParsingResult = create_lineage_sql_parsed_result(
        query=sql,
        graph=graph,
        platform=platform,
        platform_instance=None,
        env=builder.DEFAULT_ENV,
        database=database,
        schema=self.default_schema,
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


def snowflake_default_schema(self: "SnowflakeExtractor") -> Optional[str]:
    if hasattr(self.operator, "schema") and self.operator.schema is not None:
        return self.operator.schema
    return (
        self.conn.extra_dejson.get("extra__snowflake__schema", "")
        or self.conn.extra_dejson.get("schema", "")
        or self.conn.schema
    )
    # TODO: Should we try a fallback of:
    # execute_query_on_hook(self.hook, "SELECT current_schema();")[0][0]
