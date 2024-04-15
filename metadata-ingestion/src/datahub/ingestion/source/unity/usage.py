import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, Generic, Iterable, List, Optional, Set, TypeVar

import pyspark
from databricks.sdk.service.sql import QueryStatementType
from sqllineage.runner import LineageRunner

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source_helpers import auto_empty_dataset_usage_statistics
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import (
    OPERATION_STATEMENT_TYPES,
    Query,
    TableReference,
)
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.ingestion.source.usage.usage_common import UsageAggregator
from datahub.metadata.schema_classes import OperationClass

logger = logging.getLogger(__name__)


TableMap = Dict[str, List[TableReference]]
T = TypeVar("T")


@dataclass  # Dataclass over NamedTuple to support generic type annotations
class GenericTableInfo(Generic[T]):
    source_tables: List[T]
    target_tables: List[T]


StringTableInfo = GenericTableInfo[str]
QueryTableInfo = GenericTableInfo[TableReference]


@dataclass(eq=False)
class UnityCatalogUsageExtractor:
    config: UnityCatalogSourceConfig
    report: UnityCatalogReport
    proxy: UnityCatalogApiProxy
    table_urn_builder: Callable[[TableReference], str]
    user_urn_builder: Callable[[str], str]

    def __post_init__(self):
        self.usage_aggregator = UsageAggregator[TableReference](self.config)
        self._spark_sql_parser: Optional[Any] = None

    @property
    def spark_sql_parser(self):
        """Lazily initializes the Spark SQL parser."""
        if self._spark_sql_parser is None:
            spark_context = pyspark.SparkContext.getOrCreate()
            spark_session = pyspark.sql.SparkSession(spark_context)
            self._spark_sql_parser = (
                spark_session._jsparkSession.sessionState().sqlParser()
            )
        return self._spark_sql_parser

    def get_usage_workunits(
        self, table_refs: Set[TableReference]
    ) -> Iterable[MetadataWorkUnit]:
        try:
            yield from self._get_workunits_internal(table_refs)
        except Exception as e:
            logger.error("Error processing usage", exc_info=True)
            self.report.report_warning("usage-extraction", str(e))

    def _get_workunits_internal(
        self, table_refs: Set[TableReference]
    ) -> Iterable[MetadataWorkUnit]:
        table_map = defaultdict(list)
        for ref in table_refs:
            table_map[ref.table].append(ref)
            table_map[f"{ref.schema}.{ref.table}"].append(ref)
            table_map[ref.qualified_table_name].append(ref)

        for query in self._get_queries():
            self.report.num_queries += 1
            table_info = self._parse_query(query, table_map)
            if table_info is not None:
                if self.config.include_operational_stats:
                    yield from self._generate_operation_workunit(query, table_info)
                for source_table in table_info.source_tables:
                    self.usage_aggregator.aggregate_event(
                        resource=source_table,
                        start_time=query.start_time,
                        query=query.query_text,
                        user=query.user_name,
                        fields=[],
                    )

        if not self.report.num_queries:
            logger.warning("No queries found in the given time range.")
            self.report.report_warning(
                "usage",
                f"No queries found: "
                f"are you missing the CAN_MANAGE permission on SQL warehouse {self.proxy.warehouse_id}?",
            )
            return

        yield from auto_empty_dataset_usage_statistics(
            self.usage_aggregator.generate_workunits(
                resource_urn_builder=self.table_urn_builder,
                user_urn_builder=self.user_urn_builder,
            ),
            dataset_urns={self.table_urn_builder(ref) for ref in table_refs},
            config=self.config,
        )

    def _generate_operation_workunit(
        self, query: Query, table_info: QueryTableInfo
    ) -> Iterable[MetadataWorkUnit]:
        if (
            not query.statement_type
            or query.statement_type not in OPERATION_STATEMENT_TYPES
        ):
            return None

        # Not sure about behavior when there are multiple target tables. This is a best attempt.
        for target_table in table_info.target_tables:
            operation_aspect = OperationClass(
                timestampMillis=int(time.time() * 1000),
                lastUpdatedTimestamp=int(query.end_time.timestamp() * 1000),
                actor=self.user_urn_builder(query.user_name)
                if query.user_name
                else None,
                operationType=OPERATION_STATEMENT_TYPES[query.statement_type],
                affectedDatasets=[
                    self.table_urn_builder(table) for table in table_info.source_tables
                ],
            )
            self.report.num_operational_stats_workunits_emitted += 1
            yield MetadataChangeProposalWrapper(
                entityUrn=self.table_urn_builder(target_table), aspect=operation_aspect
            ).as_workunit()

    def _get_queries(self) -> Iterable[Query]:
        try:
            yield from self.proxy.query_history(
                self.config.start_time, self.config.end_time
            )
        except Exception as e:
            logger.warning("Error getting queries", exc_info=True)
            self.report.report_warning("get-queries", str(e))

    def _parse_query(
        self, query: Query, table_map: TableMap
    ) -> Optional[QueryTableInfo]:
        table_info = self._parse_query_via_lineage_runner(query.query_text)
        if table_info is None and query.statement_type == QueryStatementType.SELECT:
            table_info = self._parse_query_via_spark_sql_plan(query.query_text)

        if table_info is None:
            self.report.num_queries_dropped_parse_failure += 1
            return None
        else:
            return QueryTableInfo(
                source_tables=self._resolve_tables(table_info.source_tables, table_map),
                target_tables=self._resolve_tables(table_info.target_tables, table_map),
            )

    def _parse_query_via_lineage_runner(self, query: str) -> Optional[StringTableInfo]:
        try:
            runner = LineageRunner(query)
            return GenericTableInfo(
                source_tables=[
                    self._parse_sqllineage_table(table)
                    for table in runner.source_tables
                ],
                target_tables=[
                    self._parse_sqllineage_table(table)
                    for table in runner.target_tables
                ],
            )
        except Exception as e:
            logger.info(f"Could not parse query via lineage runner, {query}: {e!r}")
            return None

    @staticmethod
    def _parse_sqllineage_table(sqllineage_table: object) -> str:
        full_table_name = str(sqllineage_table)
        default_schema = "<default>."
        if full_table_name.startswith(default_schema):
            return full_table_name[len(default_schema) :]
        else:
            return full_table_name

    def _parse_query_via_spark_sql_plan(self, query: str) -> Optional[StringTableInfo]:
        """Parse query source tables via Spark SQL plan. This is a fallback option."""
        # Would be more effective if we upgrade pyspark
        # Does not work with CTEs or non-SELECT statements
        try:
            plan = json.loads(self.spark_sql_parser.parsePlan(query).toJSON())
            tables = [self._parse_plan_item(item) for item in plan]
            self.report.num_queries_parsed_by_spark_plan += 1
            return GenericTableInfo(
                source_tables=[t for t in tables if t], target_tables=[]
            )
        except Exception as e:
            logger.info(f"Could not parse query via spark plan, {query}: {e!r}")
            return None

    @staticmethod
    def _parse_plan_item(item: dict) -> Optional[str]:
        if item["class"] == "org.apache.spark.sql.catalyst.analysis.UnresolvedRelation":
            return ".".join(item["multipartIdentifier"].strip("[]").split(", "))
        return None

    def _resolve_tables(
        self, tables: List[str], table_map: TableMap
    ) -> List[TableReference]:
        """Resolve tables to TableReferences, filtering out unrecognized or unresolvable table names."""

        missing_table = False
        duplicate_table = False
        output = []
        for table in tables:
            table = str(table)
            if table not in table_map:
                logger.debug(f"Dropping query with unrecognized table: {table}")
                missing_table = True
            else:
                refs = table_map[table]
                if len(refs) == 1:
                    output.append(refs[0])
                else:
                    logger.warning(
                        f"Could not resolve table ref for {table}: {len(refs)} duplicates."
                    )
                    duplicate_table = True

        if missing_table:
            self.report.num_queries_missing_table += 1
        if duplicate_table:
            self.report.num_queries_duplicate_table += 1

        return output
