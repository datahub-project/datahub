import contextlib
import dataclasses
import enum
import functools
import json
import logging
import os
import pathlib
import tempfile
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional, Set, Union, cast

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.configuration.time_window_config import get_time_bucket
from datahub.emitter.mce_builder import get_sys_time, make_ts_millis
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.sql_parsing_builder import compute_upstream_fields
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.report import Report
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig, UsageAggregator
from datahub.metadata.urns import (
    CorpGroupUrn,
    CorpUserUrn,
    DataPlatformUrn,
    DatasetUrn,
    QueryUrn,
    SchemaFieldUrn,
    Urn,
)
from datahub.sql_parsing.fingerprint_utils import generate_hash
from datahub.sql_parsing.schema_resolver import (
    SchemaResolver,
    SchemaResolverInterface,
    _SchemaResolverWithExtras,
)
from datahub.sql_parsing.sql_parsing_common import QueryType, QueryTypeProps
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
    SqlParsingResult,
    _sqlglot_lineage_cached,
    infer_output_schema,
    sqlglot_lineage,
)
from datahub.sql_parsing.sqlglot_utils import (
    _parse_statement,
    get_query_fingerprint,
    try_format_query,
)
from datahub.sql_parsing.tool_meta_extractor import (
    ToolMetaExtractor,
    ToolMetaExtractorReport,
)
from datahub.utilities.cooperative_timeout import CooperativeTimeoutError
from datahub.utilities.file_backed_collections import (
    ConnectionWrapper,
    FileBackedDict,
    FileBackedList,
)
from datahub.utilities.groupby import groupby_unsorted
from datahub.utilities.lossy_collections import LossyDict, LossyList
from datahub.utilities.ordered_set import OrderedSet
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)
QueryId = str
UrnStr = str


class QueryLogSetting(enum.Enum):
    DISABLED = "DISABLED"
    STORE_ALL = "STORE_ALL"
    STORE_FAILED = "STORE_FAILED"


_DEFAULT_USER_URN = CorpUserUrn("_ingestion")
_MISSING_SESSION_ID = "__MISSING_SESSION_ID"
_DEFAULT_QUERY_LOG_SETTING = QueryLogSetting[
    os.getenv("DATAHUB_SQL_AGG_QUERY_LOG") or QueryLogSetting.DISABLED.name
]
MAX_UPSTREAM_TABLES_COUNT = 300
MAX_FINEGRAINEDLINEAGE_COUNT = 2000


@dataclasses.dataclass
class LoggedQuery:
    query: str
    session_id: Optional[str]
    timestamp: Optional[datetime]
    user: Optional[UrnStr]
    default_db: Optional[str]
    default_schema: Optional[str]


@dataclasses.dataclass
class ObservedQuery:
    query: str
    session_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    user: Optional[Union[CorpUserUrn, CorpGroupUrn]] = None
    default_db: Optional[str] = None
    default_schema: Optional[str] = None
    query_hash: Optional[str] = None
    usage_multiplier: int = 1

    # Use this to store addtitional key-value information about query for debugging
    extra_info: Optional[dict] = None


@dataclasses.dataclass
class ViewDefinition:
    view_definition: str
    default_db: Optional[str] = None
    default_schema: Optional[str] = None


@dataclasses.dataclass
class QueryMetadata:
    query_id: QueryId

    # raw_query_string: str
    formatted_query_string: str

    session_id: str  # will be _MISSING_SESSION_ID if not present
    query_type: QueryType
    lineage_type: str  # from models.DatasetLineageTypeClass
    latest_timestamp: Optional[datetime]
    actor: Optional[Union[CorpUserUrn, CorpGroupUrn]]

    upstreams: List[UrnStr]  # this is direct upstreams, which may be temp tables
    column_lineage: List[ColumnLineageInfo]
    column_usage: Dict[UrnStr, Set[UrnStr]]  # TODO: Change to an OrderedSet
    confidence_score: float

    used_temp_tables: bool = True

    origin: Optional[Urn] = None

    def make_created_audit_stamp(self) -> models.AuditStampClass:
        return models.AuditStampClass(
            time=make_ts_millis(self.latest_timestamp) or 0,
            actor=(self.actor or _DEFAULT_USER_URN).urn(),
        )

    def make_last_modified_audit_stamp(self) -> models.AuditStampClass:
        return models.AuditStampClass(
            time=make_ts_millis(self.latest_timestamp or datetime.now(tz=timezone.utc))
            or 0,
            actor=(self.actor or _DEFAULT_USER_URN).urn(),
        )

    def get_subjects(
        self,
        downstream_urn: Optional[str],
        include_fields: bool,
    ) -> List[UrnStr]:
        query_subject_urns = OrderedSet[UrnStr]()
        for upstream in self.upstreams:
            query_subject_urns.add(upstream)
            if include_fields:
                for column in sorted(self.column_usage.get(upstream, [])):
                    query_subject_urns.add(
                        builder.make_schema_field_urn(upstream, column)
                    )
        if downstream_urn:
            query_subject_urns.add(downstream_urn)
            if include_fields:
                for column_lineage in self.column_lineage:
                    query_subject_urns.add(
                        builder.make_schema_field_urn(
                            downstream_urn, column_lineage.downstream.column
                        )
                    )
        return list(query_subject_urns)

    def make_query_properties(self) -> models.QueryPropertiesClass:
        return models.QueryPropertiesClass(
            statement=models.QueryStatementClass(
                value=self.formatted_query_string,
                language=models.QueryLanguageClass.SQL,
            ),
            source=models.QuerySourceClass.SYSTEM,
            created=self.make_created_audit_stamp(),
            lastModified=self.make_last_modified_audit_stamp(),
        )


def make_query_subjects(urns: List[UrnStr]) -> models.QuerySubjectsClass:
    return models.QuerySubjectsClass(
        subjects=[models.QuerySubjectClass(entity=urn) for urn in urns]
    )


@dataclasses.dataclass
class KnownQueryLineageInfo:
    query_text: str

    downstream: UrnStr
    upstreams: List[UrnStr]
    column_lineage: Optional[List[ColumnLineageInfo]] = None
    column_usage: Optional[Dict[UrnStr, Set[UrnStr]]] = None

    timestamp: Optional[datetime] = None
    session_id: Optional[str] = None
    query_type: QueryType = QueryType.UNKNOWN
    query_id: Optional[str] = None


@dataclasses.dataclass
class KnownLineageMapping:
    upstream_urn: UrnStr
    downstream_urn: UrnStr
    lineage_type: str = models.DatasetLineageTypeClass.COPY


@dataclasses.dataclass
class TableRename:
    original_urn: UrnStr
    new_urn: UrnStr
    query: Optional[str] = None
    session_id: str = _MISSING_SESSION_ID
    timestamp: Optional[datetime] = None


@dataclasses.dataclass
class TableSwap:
    urn1: UrnStr
    urn2: UrnStr
    query: Optional[str] = None
    session_id: str = _MISSING_SESSION_ID
    timestamp: Optional[datetime] = None

    def id(self) -> str:
        # TableSwap(A,B) is same as TableSwap(B,A)
        return str(hash(frozenset([self.urn1, self.urn2])))


@dataclasses.dataclass
class PreparsedQuery:
    # If not provided, we will generate one using the fingerprint generator.
    query_id: Optional[QueryId]

    query_text: str

    upstreams: List[UrnStr]
    downstream: Optional[UrnStr] = None
    column_lineage: Optional[List[ColumnLineageInfo]] = None
    column_usage: Optional[Dict[UrnStr, Set[UrnStr]]] = None
    inferred_schema: Optional[List[models.SchemaFieldClass]] = None
    confidence_score: float = 1.0

    query_count: int = 1
    user: Optional[Union[CorpUserUrn, CorpGroupUrn]] = None
    timestamp: Optional[datetime] = None
    session_id: str = _MISSING_SESSION_ID
    query_type: QueryType = QueryType.UNKNOWN
    query_type_props: QueryTypeProps = dataclasses.field(
        default_factory=lambda: QueryTypeProps()
    )
    # Use this to store addtitional key-value information about query for debugging
    extra_info: Optional[dict] = None
    origin: Optional[Urn] = None


@dataclasses.dataclass
class SqlAggregatorReport(Report):
    _aggregator: "SqlParsingAggregator"
    query_log_path: Optional[str] = None

    # Observed queries.
    num_observed_queries: int = 0
    num_observed_queries_failed: int = 0
    num_observed_queries_column_timeout: int = 0
    num_observed_queries_column_failed: int = 0
    observed_query_parse_failures: LossyList[str] = dataclasses.field(
        default_factory=LossyList
    )

    # Views.
    num_view_definitions: int = 0
    num_views_failed: int = 0
    num_views_column_timeout: int = 0
    num_views_column_failed: int = 0
    views_parse_failures: LossyDict[UrnStr, str] = dataclasses.field(
        default_factory=LossyDict
    )

    # SQL parsing (over all invocations).
    num_sql_parsed: int = 0
    sql_parsing_timer: PerfTimer = dataclasses.field(default_factory=PerfTimer)
    sql_fingerprinting_timer: PerfTimer = dataclasses.field(default_factory=PerfTimer)
    sql_formatting_timer: PerfTimer = dataclasses.field(default_factory=PerfTimer)
    sql_parsing_cache_stats: Optional[dict] = dataclasses.field(default=None)
    parse_statement_cache_stats: Optional[dict] = dataclasses.field(default=None)
    format_query_cache_stats: Optional[dict] = dataclasses.field(default=None)

    # Other lineage loading metrics.
    num_known_query_lineage: int = 0
    num_preparsed_queries: int = 0
    num_known_mapping_lineage: int = 0
    num_table_renames: int = 0
    num_table_swaps: int = 0

    # Temp tables.
    num_temp_sessions: Optional[int] = None
    num_inferred_temp_schemas: Optional[int] = None
    num_queries_with_temp_tables_in_session: int = 0
    queries_with_temp_upstreams: LossyDict[QueryId, LossyList] = dataclasses.field(
        default_factory=LossyDict
    )
    queries_with_non_authoritative_session: LossyList[QueryId] = dataclasses.field(
        default_factory=LossyList
    )
    make_schema_resolver_timer: PerfTimer = dataclasses.field(default_factory=PerfTimer)

    # Lineage-related.
    schema_resolver_count: Optional[int] = None
    num_unique_query_fingerprints: Optional[int] = None
    num_urns_with_lineage: Optional[int] = None
    num_lineage_skipped_due_to_filters: int = 0
    num_table_lineage_trimmed_due_to_large_size: int = 0
    num_column_lineage_trimmed_due_to_large_size: int = 0

    # Queries.
    num_queries_entities_generated: int = 0
    num_queries_used_in_lineage: Optional[int] = None
    num_queries_skipped_due_to_filters: int = 0

    # Usage-related.
    usage_skipped_missing_timestamp: int = 0
    num_query_usage_stats_generated: int = 0
    num_query_usage_stats_outside_window: int = 0

    # Operation-related.
    num_operations_generated: int = 0
    num_operations_skipped_due_to_filters: int = 0

    # Tool Metadata Extraction
    tool_meta_report: Optional[ToolMetaExtractorReport] = None

    def compute_stats(self) -> None:
        if self._aggregator._closed:
            return
        self.schema_resolver_count = self._aggregator._schema_resolver.schema_count()
        self.num_unique_query_fingerprints = len(self._aggregator._query_map)

        self.num_urns_with_lineage = len(self._aggregator._lineage_map)
        self.num_temp_sessions = len(self._aggregator._temp_lineage_map)
        self.num_inferred_temp_schemas = len(self._aggregator._inferred_temp_schemas)

        self.sql_parsing_cache_stats = _sqlglot_lineage_cached.cache_info()._asdict()
        self.parse_statement_cache_stats = _parse_statement.cache_info()._asdict()
        self.format_query_cache_stats = try_format_query.cache_info()._asdict()

        return super().compute_stats()


class SqlParsingAggregator(Closeable):
    def __init__(
        self,
        *,
        platform: str,
        platform_instance: Optional[str] = None,
        env: str = builder.DEFAULT_ENV,
        schema_resolver: Optional[SchemaResolver] = None,
        graph: Optional[DataHubGraph] = None,
        eager_graph_load: bool = True,
        generate_lineage: bool = True,
        generate_queries: bool = True,
        generate_query_subject_fields: bool = True,
        generate_usage_statistics: bool = False,
        generate_query_usage_statistics: bool = False,
        generate_operations: bool = False,
        usage_config: Optional[BaseUsageConfig] = None,
        is_temp_table: Optional[Callable[[str], bool]] = None,
        is_allowed_table: Optional[Callable[[str], bool]] = None,
        format_queries: bool = True,
        query_log: QueryLogSetting = _DEFAULT_QUERY_LOG_SETTING,
    ) -> None:
        self.platform = DataPlatformUrn(platform)
        self.platform_instance = platform_instance
        self.env = env

        self.generate_lineage = generate_lineage
        self.generate_queries = generate_queries
        self.generate_query_subject_fields = generate_query_subject_fields
        self.generate_usage_statistics = generate_usage_statistics
        self.generate_query_usage_statistics = generate_query_usage_statistics
        self.generate_operations = generate_operations
        if self.generate_queries and not (
            self.generate_lineage or self.generate_query_usage_statistics
        ):
            logger.warning(
                "Queries will not be generated, as neither lineage nor query usage statistics are enabled"
            )

        self.usage_config = usage_config
        if (
            self.generate_usage_statistics or self.generate_query_usage_statistics
        ) and self.usage_config is None:
            raise ValueError("Usage statistics generation requires a usage config")

        self.report = SqlAggregatorReport(_aggregator=self)

        # can be used by BQ where we have a "temp_table_dataset_prefix"
        self._is_temp_table = is_temp_table
        self._is_allowed_table = is_allowed_table

        self.format_queries = format_queries
        self.query_log = query_log

        # The exit stack helps ensure that we close all the resources we open.
        self._exit_stack = contextlib.ExitStack()
        self._closed: bool = False

        # Set up the schema resolver.
        self._schema_resolver: SchemaResolver
        if schema_resolver is not None:
            # If explicitly provided, use it.
            assert self.platform.platform_name == schema_resolver.platform
            assert self.platform_instance == schema_resolver.platform_instance
            assert self.env == schema_resolver.env
            self._schema_resolver = schema_resolver
        elif graph is not None and eager_graph_load and self._need_schemas:
            # Bulk load schemas using the graph client.
            self._schema_resolver = graph.initialize_schema_resolver_from_datahub(
                platform=self.platform.urn(),
                platform_instance=self.platform_instance,
                env=self.env,
            )
        else:
            # Otherwise, use a lazy-loading schema resolver.
            self._schema_resolver = self._exit_stack.enter_context(
                SchemaResolver(
                    platform=self.platform.platform_name,
                    platform_instance=self.platform_instance,
                    env=self.env,
                    graph=graph,
                )
            )
        # Schema resolver for special case (_MISSING_SESSION_ID)
        # This is particularly useful for via temp table lineage if session id is not available.
        self._missing_session_schema_resolver = _SchemaResolverWithExtras(
            base_resolver=self._schema_resolver, extra_schemas={}
        )

        # Initialize internal data structures.
        # This leans pretty heavily on the our query fingerprinting capabilities.
        # In particular, it must be true that if two queries have the same fingerprint,
        # they must generate the same lineage.

        self._shared_connection: Optional[ConnectionWrapper] = None
        if self.query_log != QueryLogSetting.DISABLED:
            # Initialize and log a file to store the queries.
            query_log_path = pathlib.Path(tempfile.mkdtemp()) / "query_log.db"
            self.report.query_log_path = str(query_log_path)

            # By providing a filename explicitly here, we also ensure that the file
            # is not automatically deleted on exit.
            self._shared_connection = self._exit_stack.enter_context(
                ConnectionWrapper(filename=query_log_path)
            )

        # Stores the logged queries.
        self._logged_queries = FileBackedList[LoggedQuery](
            shared_connection=self._shared_connection, tablename="stored_queries"
        )
        self._exit_stack.push(self._logged_queries)

        # Map of query_id -> QueryMetadata
        self._query_map = FileBackedDict[QueryMetadata](
            shared_connection=self._shared_connection, tablename="query_map"
        )
        self._exit_stack.push(self._query_map)

        # Map of downstream urn -> { query ids }
        self._lineage_map = FileBackedDict[OrderedSet[QueryId]](
            shared_connection=self._shared_connection, tablename="lineage_map"
        )
        self._exit_stack.push(self._lineage_map)

        # Map of view urn -> view definition
        self._view_definitions = FileBackedDict[ViewDefinition](
            shared_connection=self._shared_connection, tablename="view_definitions"
        )
        self._exit_stack.push(self._view_definitions)

        # Map of session ID -> {temp table name -> query id}
        # Needs to use the query_map to find the info about the query.
        self._temp_lineage_map = FileBackedDict[Dict[UrnStr, OrderedSet[QueryId]]](
            shared_connection=self._shared_connection, tablename="temp_lineage_map"
        )
        self._exit_stack.push(self._temp_lineage_map)

        # Map of query ID -> schema fields, only for query IDs that generate temp tables.
        self._inferred_temp_schemas = FileBackedDict[List[models.SchemaFieldClass]](
            shared_connection=self._shared_connection,
            tablename="inferred_temp_schemas",
        )
        self._exit_stack.push(self._inferred_temp_schemas)

        # Map of table renames, from original UrnStr to new UrnStr.
        self._table_renames = FileBackedDict[UrnStr](
            shared_connection=self._shared_connection, tablename="table_renames"
        )
        self._exit_stack.push(self._table_renames)

        # Map of table swaps, from unique swap id to TableSwap
        self._table_swaps = FileBackedDict[TableSwap](
            shared_connection=self._shared_connection, tablename="table_swaps"
        )
        self._exit_stack.push(self._table_swaps)

        # Usage aggregator. This will only be initialized if usage statistics are enabled.
        # TODO: Replace with FileBackedDict.
        # TODO: The BaseUsageConfig class is much too broad for our purposes, and has a number of
        # configs that won't be respected here. Using it is misleading.
        self._usage_aggregator: Optional[UsageAggregator[UrnStr]] = None
        if self.generate_usage_statistics:
            assert self.usage_config is not None
            self._usage_aggregator = UsageAggregator(config=self.usage_config)

        # Query usage aggregator.
        # Map of query ID -> { bucket -> count }
        self._query_usage_counts: Optional[FileBackedDict[Dict[datetime, int]]] = None
        if self.generate_query_usage_statistics:
            self._query_usage_counts = FileBackedDict[Dict[datetime, int]](
                shared_connection=self._shared_connection,
                tablename="query_usage_counts",
            )
            self._exit_stack.push(self._query_usage_counts)

        # Tool Extractor
        self._tool_meta_extractor = ToolMetaExtractor.create(graph)
        self.report.tool_meta_report = self._tool_meta_extractor.report

    def close(self) -> None:
        # Compute stats once before closing connections
        self.report.compute_stats()
        self._closed = True
        self._exit_stack.close()

    @property
    def _need_schemas(self) -> bool:
        # Unless the aggregator is totally disabled, we will need schema information.
        return (
            self.generate_lineage
            or self.generate_usage_statistics
            or self.generate_queries
            or self.generate_operations
            or self.generate_query_usage_statistics
        )

    def register_schema(
        self, urn: Union[str, DatasetUrn], schema: models.SchemaMetadataClass
    ) -> None:
        # If lineage or usage is enabled, adds the schema to the schema resolver
        # by putting the condition in here, we can avoid all the conditional
        # logic that we previously needed in each source

        if self._need_schemas:
            self._schema_resolver.add_schema_metadata(str(urn), schema)

    def register_schemas_from_stream(
        self, stream: Iterable[MetadataWorkUnit]
    ) -> Iterable[MetadataWorkUnit]:
        for wu in stream:
            schema_metadata = wu.get_aspect_of_type(models.SchemaMetadataClass)
            if schema_metadata:
                self.register_schema(wu.get_urn(), schema_metadata)

            yield wu

    def _maybe_format_query(self, query: str) -> str:
        if self.format_queries:
            with self.report.sql_formatting_timer:
                return try_format_query(query, self.platform.platform_name)
        return query

    @functools.lru_cache(maxsize=128)
    def _name_from_urn(self, urn: UrnStr) -> Optional[str]:
        urn_obj = DatasetUrn.from_string(urn)
        if urn_obj.platform != self.platform.urn():
            # If this is external (e.g. s3), we don't know the name.
            return None

        name = urn_obj.name
        if (
            platform_instance := self._schema_resolver.platform_instance
        ) and name.startswith(platform_instance):
            # Remove the platform instance from the name.
            name = name[len(platform_instance) + 1 :]
        return name

    def is_temp_table(self, urn: UrnStr) -> bool:
        if self._is_temp_table is None:
            return False
        name = self._name_from_urn(urn)
        if name is None:
            # External tables are not temp tables.
            return False
        return self._is_temp_table(name)

    def is_allowed_table(self, urn: UrnStr, allow_external: bool = True) -> bool:
        if self.is_temp_table(urn):
            return False
        if self._is_allowed_table is None:
            return True
        name = self._name_from_urn(urn)
        if name is None:
            # Treat external tables specially.
            return allow_external
        return self._is_allowed_table(name)

    def add(
        self,
        item: Union[
            KnownQueryLineageInfo,
            KnownLineageMapping,
            PreparsedQuery,
            ObservedQuery,
            TableRename,
            TableSwap,
        ],
    ) -> None:
        if isinstance(item, KnownQueryLineageInfo):
            self.add_known_query_lineage(item)
        elif isinstance(item, KnownLineageMapping):
            self.add_known_lineage_mapping(item.upstream_urn, item.downstream_urn)
        elif isinstance(item, PreparsedQuery):
            self.add_preparsed_query(item)
        elif isinstance(item, ObservedQuery):
            self.add_observed_query(item)
        elif isinstance(item, TableRename):
            self.add_table_rename(item)
        elif isinstance(item, TableSwap):
            self.add_table_swap(item)
        else:
            raise ValueError(f"Cannot add unknown item type: {type(item)}")

    def add_known_query_lineage(
        self, known_query_lineage: KnownQueryLineageInfo, merge_lineage: bool = False
    ) -> None:
        """Add a query and it's precomputed lineage to the aggregator.

        This is useful for cases where we have lineage information that was
        computed outside of the SQL parsing aggregator, e.g. from a data
        warehouse's system tables.

        This will also generate an operation aspect for the query if there is
        a timestamp and the query type field is set to a mutation type.

        Args:
            known_query_lineage: The known query lineage information.
            merge_lineage: Whether to merge the lineage with any existing lineage
                for the query ID.
        """

        self.report.num_known_query_lineage += 1

        # Generate a fingerprint for the query.
        query_fingerprint = known_query_lineage.query_id
        if not query_fingerprint:
            with self.report.sql_fingerprinting_timer:
                query_fingerprint = get_query_fingerprint(
                    known_query_lineage.query_text,
                    platform=self.platform.platform_name,
                )
        formatted_query = self._maybe_format_query(known_query_lineage.query_text)

        # Register the query.
        self._add_to_query_map(
            QueryMetadata(
                query_id=query_fingerprint,
                formatted_query_string=formatted_query,
                session_id=known_query_lineage.session_id or _MISSING_SESSION_ID,
                query_type=known_query_lineage.query_type,
                lineage_type=models.DatasetLineageTypeClass.TRANSFORMED,
                latest_timestamp=known_query_lineage.timestamp,
                actor=None,
                upstreams=known_query_lineage.upstreams,
                column_lineage=known_query_lineage.column_lineage or [],
                column_usage=known_query_lineage.column_usage or {},
                confidence_score=1.0,
            ),
            merge_lineage=merge_lineage,
        )

        # Register the lineage.
        self._lineage_map.for_mutation(
            known_query_lineage.downstream, OrderedSet()
        ).add(query_fingerprint)

    def add_known_lineage_mapping(
        self,
        upstream_urn: UrnStr,
        downstream_urn: UrnStr,
        lineage_type: str = models.DatasetLineageTypeClass.COPY,
    ) -> None:
        """Add a known lineage mapping to the aggregator.

        By mapping, we mean that the downstream is effectively a copy or
        alias of the upstream. This is useful for things like external tables
        (e.g. Redshift Spectrum, Redshift UNLOADs, Snowflake external tables).

        Because this method takes in urns, it does not require that the urns
        are part of the platform that the aggregator is configured for.

        The known lineage mapping does not contribute to usage statistics or operations.

        Args:
            upstream_urn: The upstream dataset URN.
            downstream_urn: The downstream dataset URN.
        """
        logger.debug(
            f"Adding lineage to the map, downstream: {downstream_urn}, upstream: {upstream_urn}"
        )
        self.report.num_known_mapping_lineage += 1

        # We generate a fake "query" object to hold the lineage.
        query_id = self._known_lineage_query_id()

        # Generate CLL if schema of downstream is known
        column_lineage: List[ColumnLineageInfo] = (
            self._generate_identity_column_lineage(
                upstream_urn=upstream_urn, downstream_urn=downstream_urn
            )
        )

        # Register the query.
        self._add_to_query_map(
            QueryMetadata(
                query_id=query_id,
                formatted_query_string="-skip-",
                session_id=_MISSING_SESSION_ID,
                query_type=QueryType.UNKNOWN,
                lineage_type=lineage_type,
                latest_timestamp=None,
                actor=None,
                upstreams=[upstream_urn],
                column_lineage=column_lineage,
                column_usage={},
                confidence_score=1.0,
            )
        )

        # Register the lineage.
        self._lineage_map.for_mutation(downstream_urn, OrderedSet()).add(query_id)

    def _generate_identity_column_lineage(
        self, *, upstream_urn: UrnStr, downstream_urn: UrnStr
    ) -> List[ColumnLineageInfo]:
        column_lineage: List[ColumnLineageInfo] = []
        if self._schema_resolver.has_urn(downstream_urn):
            schema = self._schema_resolver._resolve_schema_info(downstream_urn)
            if schema:
                column_lineage = [
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=downstream_urn, column=field_path
                        ),
                        upstreams=[ColumnRef(table=upstream_urn, column=field_path)],
                    )
                    for field_path in schema
                ]

        return column_lineage

    def add_view_definition(
        self,
        view_urn: Union[DatasetUrn, UrnStr],
        view_definition: str,
        default_db: Optional[str] = None,
        default_schema: Optional[str] = None,
    ) -> None:
        """Add a view definition to the aggregator.

        View definitions always contribute to lineage, but do not count towards
        usage statistics or operations.

        The actual processing of view definitions is deferred until output time,
        since all schemas will be registered at that point.
        """

        self.report.num_view_definitions += 1

        self._view_definitions[str(view_urn)] = ViewDefinition(
            view_definition=view_definition,
            default_db=default_db,
            default_schema=default_schema,
        )

    def add_observed_query(
        self,
        observed: ObservedQuery,
        is_known_temp_table: bool = False,
        require_out_table_schema: bool = False,
    ) -> None:
        """Add an observed query to the aggregator.

        This will always generate usage. If it's a mutation query, it will also generate
        lineage and an operation. If it's a temp table, the lineage gets logged in a separate
        map, which will get used in subsequent queries with the same session ID.

        This assumes that queries come in order of increasing timestamps.
        """
        self.report.num_observed_queries += 1

        # All queries with no session ID are assumed to be part of the same session.
        session_id = observed.session_id or _MISSING_SESSION_ID

        with self.report.make_schema_resolver_timer:
            # Load in the temp tables for this session.
            schema_resolver: SchemaResolverInterface = (
                self._make_schema_resolver_for_session(session_id)
            )
            session_has_temp_tables = schema_resolver.includes_temp_tables()

        # Run the SQL parser.
        parsed = self._run_sql_parser(
            observed.query,
            default_db=observed.default_db,
            default_schema=observed.default_schema,
            schema_resolver=schema_resolver,
            session_id=session_id,
            timestamp=observed.timestamp,
            user=observed.user,
        )
        if parsed.debug_info.error:
            self.report.observed_query_parse_failures.append(
                f"{parsed.debug_info.error} on query: {observed.query[:100]}"
            )
        if parsed.debug_info.table_error:
            self.report.num_observed_queries_failed += 1
            return  # we can't do anything with this query
        elif parsed.debug_info.column_error:
            self.report.num_observed_queries_column_failed += 1
            if isinstance(parsed.debug_info.column_error, CooperativeTimeoutError):
                self.report.num_observed_queries_column_timeout += 1

        query_fingerprint = observed.query_hash or parsed.query_fingerprint
        self.add_preparsed_query(
            PreparsedQuery(
                query_id=query_fingerprint,
                query_text=observed.query,
                query_count=observed.usage_multiplier,
                timestamp=observed.timestamp,
                user=observed.user,
                session_id=session_id,
                query_type=parsed.query_type,
                query_type_props=parsed.query_type_props,
                upstreams=parsed.in_tables,
                downstream=parsed.out_tables[0] if parsed.out_tables else None,
                column_lineage=parsed.column_lineage,
                # TODO: We need a full list of columns referenced, not just the out tables.
                column_usage=compute_upstream_fields(parsed),
                inferred_schema=infer_output_schema(parsed),
                confidence_score=parsed.debug_info.confidence,
                extra_info=observed.extra_info,
            ),
            is_known_temp_table=is_known_temp_table,
            require_out_table_schema=require_out_table_schema,
            session_has_temp_tables=session_has_temp_tables,
            _is_internal=True,
        )

    def add_preparsed_query(
        self,
        parsed: PreparsedQuery,
        is_known_temp_table: bool = False,
        require_out_table_schema: bool = False,
        session_has_temp_tables: bool = True,
        _is_internal: bool = False,
    ) -> None:
        # Adding tool specific metadata extraction here allows it
        # to work for both ObservedQuery and PreparsedQuery as
        # add_preparsed_query it used within add_observed_query.
        self._tool_meta_extractor.extract_bi_metadata(parsed)

        if not _is_internal:
            self.report.num_preparsed_queries += 1

        if parsed.timestamp:
            # Sanity check - some of our usage subroutines require the timestamp to be in UTC.
            # Ideally we'd actually reject missing tzinfo too, but we can tighten that later.
            assert parsed.timestamp.tzinfo in {None, timezone.utc}

        query_fingerprint = parsed.query_id
        if not query_fingerprint:
            query_fingerprint = get_query_fingerprint(
                parsed.query_text,
                platform=self.platform.platform_name,
            )

        # Format the query.
        formatted_query = self._maybe_format_query(parsed.query_text)

        # Register the query's usage.
        if not self._usage_aggregator:
            pass  # usage is not enabled
        elif parsed.timestamp is None:
            self.report.usage_skipped_missing_timestamp += 1
        else:
            upstream_fields = parsed.column_usage or {}
            for upstream_urn in parsed.upstreams:
                # If the upstream table is a temp table or otherwise denied by filters, don't log usage for it.
                if not self.is_allowed_table(upstream_urn, allow_external=False) or (
                    require_out_table_schema
                    and not self._schema_resolver.has_urn(upstream_urn)
                ):
                    continue

                self._usage_aggregator.aggregate_event(
                    resource=upstream_urn,
                    start_time=parsed.timestamp,
                    query=formatted_query,
                    user=parsed.user.urn() if parsed.user else None,
                    fields=sorted(upstream_fields.get(upstream_urn, [])),
                    count=parsed.query_count,
                )

        if self._query_usage_counts is not None and parsed.timestamp is not None:
            assert self.usage_config is not None
            bucket = get_time_bucket(
                parsed.timestamp, self.usage_config.bucket_duration
            )
            counts = self._query_usage_counts.for_mutation(query_fingerprint, {})
            counts[bucket] = counts.get(bucket, 0) + parsed.query_count

        # Register the query.
        self._add_to_query_map(
            QueryMetadata(
                query_id=query_fingerprint,
                formatted_query_string=formatted_query,
                session_id=parsed.session_id,
                query_type=parsed.query_type,
                lineage_type=models.DatasetLineageTypeClass.TRANSFORMED,
                latest_timestamp=parsed.timestamp,
                actor=parsed.user,
                upstreams=parsed.upstreams,
                column_lineage=parsed.column_lineage or [],
                column_usage=parsed.column_usage or {},
                confidence_score=parsed.confidence_score,
                used_temp_tables=session_has_temp_tables,
                origin=parsed.origin,
            )
        )

        if not parsed.downstream:
            return
        out_table = parsed.downstream

        # Register the query's lineage.
        if (
            is_known_temp_table
            or (
                parsed.query_type.is_create()
                and parsed.query_type_props.get("temporary")
            )
            or self.is_temp_table(out_table)
            or (
                require_out_table_schema
                and not self._schema_resolver.has_urn(out_table)
            )
        ):
            # Infer the schema of the output table and track it for later.
            if parsed.inferred_schema is not None:
                self._inferred_temp_schemas[query_fingerprint] = parsed.inferred_schema

            # Also track the lineage for the temp table, for merging purposes later.
            self._temp_lineage_map.for_mutation(parsed.session_id, {}).setdefault(
                out_table, OrderedSet()
            ).add(query_fingerprint)

            # Also update schema resolver for missing session id
            if parsed.session_id == _MISSING_SESSION_ID and parsed.inferred_schema:
                self._missing_session_schema_resolver.add_temp_tables(
                    {out_table: parsed.inferred_schema}
                )

        else:
            # Non-temp tables immediately generate lineage.
            self._lineage_map.for_mutation(out_table, OrderedSet()).add(
                query_fingerprint
            )

    def add_table_rename(
        self,
        table_rename: TableRename,
    ) -> None:
        """Add a table rename to the aggregator.

        This will make all observed queries that reference the original urn
        will instead generate lineage for the new urn.
        """

        self.report.num_table_renames += 1

        # This will not work if the table is renamed multiple times.
        self._table_renames[table_rename.original_urn] = table_rename.new_urn

        original_table = self._name_from_urn(table_rename.original_urn)
        new_table = self._name_from_urn(table_rename.new_urn)

        self.add_preparsed_query(
            PreparsedQuery(
                query_id=None,
                query_text=table_rename.query
                or f"--Datahub generated query text--\n"
                f"alter table {original_table} rename to {new_table}",
                upstreams=[table_rename.original_urn],
                downstream=table_rename.new_urn,
                column_lineage=self._generate_identity_column_lineage(
                    downstream_urn=table_rename.new_urn,
                    upstream_urn=table_rename.original_urn,
                ),
                session_id=table_rename.session_id,
                timestamp=table_rename.timestamp,
            )
        )

    def add_table_swap(self, table_swap: TableSwap) -> None:
        """Add a table swap to the aggregator.

        Args:
            table_swap.urn1, table_swap.urn2: The dataset URNs to swap.
        """

        if table_swap.id() in self._table_swaps:
            # We have already processed this table swap once
            return

        self.report.num_table_swaps += 1
        self._table_swaps[table_swap.id()] = table_swap
        table1 = self._name_from_urn(table_swap.urn1)
        table2 = self._name_from_urn(table_swap.urn2)

        # NOTE: Both queries are different on purpose. Currently, we can not
        # store (A->B) and (B->A) lineage against same query.

        # NOTE: we do not store upstreams for temp table on purpose, as that would
        # otherwise overwrite original upstream query of temp table because
        # currently a temporay table can have only one upstream query.

        if not self.is_temp_table(table_swap.urn2):
            self.add_preparsed_query(
                PreparsedQuery(
                    query_id=None,
                    query_text=f"--Datahub generated query text--"
                    f"\nalter table {table1} swap with {table2}",
                    upstreams=[table_swap.urn1],
                    downstream=table_swap.urn2,
                    column_lineage=self._generate_identity_column_lineage(
                        upstream_urn=table_swap.urn1, downstream_urn=table_swap.urn2
                    ),
                    session_id=table_swap.session_id,
                    timestamp=table_swap.timestamp,
                )
            )

        if not self.is_temp_table(table_swap.urn1):
            self.add_preparsed_query(
                PreparsedQuery(
                    query_id=None,
                    query_text=f"--Datahub generated query text--\n"
                    f"alter table {table2} swap with {table1}",
                    upstreams=[table_swap.urn2],
                    downstream=table_swap.urn1,
                    column_lineage=self._generate_identity_column_lineage(
                        upstream_urn=table_swap.urn2, downstream_urn=table_swap.urn1
                    ),
                    session_id=table_swap.session_id,
                    timestamp=table_swap.timestamp,
                )
            )

    def _make_schema_resolver_for_session(
        self, session_id: str
    ) -> SchemaResolverInterface:
        schema_resolver: SchemaResolverInterface = self._schema_resolver
        if session_id == _MISSING_SESSION_ID:
            schema_resolver = self._missing_session_schema_resolver
        elif session_id in self._temp_lineage_map:
            temp_table_schemas: Dict[str, Optional[List[models.SchemaFieldClass]]] = {}
            for temp_table_urn, query_ids in self._temp_lineage_map[session_id].items():
                for query_id in query_ids:
                    temp_table_schemas[temp_table_urn] = (
                        self._inferred_temp_schemas.get(query_id)
                    )
                    if temp_table_schemas:
                        break

            if temp_table_schemas:
                schema_resolver = self._schema_resolver.with_temp_tables(
                    temp_table_schemas
                )
                self.report.num_queries_with_temp_tables_in_session += 1

        return schema_resolver

    def _process_view_definition(
        self, view_urn: UrnStr, view_definition: ViewDefinition
    ) -> None:
        # Note that in some cases, the view definition will be a SELECT statement
        # instead of a CREATE VIEW ... AS SELECT statement. In those cases, we can't
        # trust the parsed query type or downstream urn.

        # Run the SQL parser.
        parsed = self._run_sql_parser(
            view_definition.view_definition,
            default_db=view_definition.default_db,
            default_schema=view_definition.default_schema,
            schema_resolver=self._schema_resolver,
        )
        if parsed.debug_info.error:
            self.report.views_parse_failures[view_urn] = (
                f"{parsed.debug_info.error} on query: {view_definition.view_definition[:100]}"
            )
        if parsed.debug_info.table_error:
            self.report.num_views_failed += 1
            return  # we can't do anything with this query
        elif isinstance(parsed.debug_info.column_error, CooperativeTimeoutError):
            self.report.num_views_column_timeout += 1
        elif parsed.debug_info.column_error:
            self.report.num_views_column_failed += 1

        query_fingerprint = self._view_query_id(view_urn)
        formatted_view_definition = self._maybe_format_query(
            view_definition.view_definition
        )

        # Register the query.
        self._add_to_query_map(
            QueryMetadata(
                query_id=query_fingerprint,
                formatted_query_string=formatted_view_definition,
                session_id=_MISSING_SESSION_ID,
                query_type=QueryType.CREATE_VIEW,
                lineage_type=models.DatasetLineageTypeClass.VIEW,
                latest_timestamp=None,
                actor=None,
                upstreams=parsed.in_tables,
                column_lineage=parsed.column_lineage or [],
                column_usage=compute_upstream_fields(parsed),
                confidence_score=parsed.debug_info.confidence,
            )
        )

        # Register the query's lineage.
        self._lineage_map.for_mutation(view_urn, OrderedSet()).add(query_fingerprint)

    def _run_sql_parser(
        self,
        query: str,
        default_db: Optional[str],
        default_schema: Optional[str],
        schema_resolver: SchemaResolverInterface,
        session_id: str = _MISSING_SESSION_ID,
        timestamp: Optional[datetime] = None,
        user: Optional[Union[CorpUserUrn, CorpGroupUrn]] = None,
    ) -> SqlParsingResult:
        with self.report.sql_parsing_timer:
            parsed = sqlglot_lineage(
                query,
                schema_resolver=schema_resolver,
                default_db=default_db,
                default_schema=default_schema,
            )
        self.report.num_sql_parsed += 1

        # Conditionally log the query.
        if self.query_log == QueryLogSetting.STORE_ALL or (
            self.query_log == QueryLogSetting.STORE_FAILED and parsed.debug_info.error
        ):
            query_log_entry = LoggedQuery(
                query=query,
                session_id=session_id if session_id != _MISSING_SESSION_ID else None,
                timestamp=timestamp,
                user=user.urn() if user else None,
                default_db=default_db,
                default_schema=default_schema,
            )
            self._logged_queries.append(query_log_entry)

        # Also add some extra logging.
        if parsed.debug_info.error:
            logger.debug(
                f"Error parsing query {query}: {parsed.debug_info.error}",
                exc_info=parsed.debug_info.error,
            )

        return parsed

    def _add_to_query_map(
        self, new: QueryMetadata, merge_lineage: bool = False
    ) -> None:
        query_fingerprint = new.query_id

        if query_fingerprint in self._query_map:
            current = self._query_map.for_mutation(query_fingerprint)

            # This assumes that queries come in order of increasing timestamps,
            # so the current query is more authoritative than the previous one.
            current.formatted_query_string = new.formatted_query_string
            current.latest_timestamp = new.latest_timestamp or current.latest_timestamp
            current.actor = new.actor or current.actor

            if current.used_temp_tables and not new.used_temp_tables:
                # If we see the same query again, but in a different session,
                # it's possible that we didn't capture the temp tables in the newer session,
                # but did in the older one. If that happens, we treat the older session's
                # lineage as more authoritative. This isn't technically correct, but it's
                # better than using the newer session's lineage, which is likely incorrect.
                self.report.queries_with_non_authoritative_session.append(
                    query_fingerprint
                )
                return
            current.session_id = new.session_id

            if not merge_lineage:
                # An invariant of the fingerprinting is that if two queries have the
                # same fingerprint, they must also have the same lineage. We overwrite
                # here just in case more schemas got registered in the interim.
                current.upstreams = new.upstreams
                current.column_lineage = new.column_lineage
                current.column_usage = new.column_usage
                current.confidence_score = new.confidence_score
            else:
                # In the case of known query lineage, we might get things one at a time.
                # TODO: We don't yet support merging CLL for a single query.
                current.upstreams = list(
                    OrderedSet(current.upstreams) | OrderedSet(new.upstreams)
                )
                current.confidence_score = min(
                    current.confidence_score, new.confidence_score
                )
        else:
            self._query_map[query_fingerprint] = new

    def gen_metadata(self) -> Iterable[MetadataChangeProposalWrapper]:
        queries_generated: Set[QueryId] = set()

        yield from self._gen_lineage_mcps(queries_generated)
        self.report.num_queries_used_in_lineage = len(queries_generated)
        yield from self._gen_usage_statistics_mcps()
        yield from self._gen_operation_mcps(queries_generated)
        yield from self._gen_remaining_queries(queries_generated)

    def _gen_lineage_mcps(
        self, queries_generated: Set[QueryId]
    ) -> Iterable[MetadataChangeProposalWrapper]:
        if not self.generate_lineage:
            return

        # Process all views and inject them into the lineage map.
        # The parsing of view definitions is deferred until this point
        # to ensure the availability of all schema metadata.
        for view_urn, view_definition in self._view_definitions.items():
            self._process_view_definition(view_urn, view_definition)
        self._view_definitions.clear()

        # Generate lineage and queries.
        for downstream_urn in sorted(self._lineage_map):
            yield from self._gen_lineage_for_downstream(
                downstream_urn, queries_generated=queries_generated
            )

    @classmethod
    def _query_type_precedence(cls, query_type: str) -> int:
        query_precedence = [
            models.DatasetLineageTypeClass.COPY,
            models.DatasetLineageTypeClass.VIEW,
            models.DatasetLineageTypeClass.TRANSFORMED,
        ]

        # Lower value = higher precedence.
        idx = query_precedence.index(query_type)
        if idx == -1:
            return len(query_precedence)
        return idx

    def _gen_lineage_for_downstream(
        self, downstream_urn: str, queries_generated: Set[QueryId]
    ) -> Iterable[MetadataChangeProposalWrapper]:
        if not self.is_allowed_table(downstream_urn):
            self.report.num_lineage_skipped_due_to_filters += 1
            return

        query_ids = self._lineage_map[downstream_urn]
        queries: List[QueryMetadata] = [
            self._resolve_query_with_temp_tables(self._query_map[query_id])
            for query_id in query_ids
        ]

        # Sort the queries by highest precedence first, then by latest timestamp.
        # In case of ties, prefer queries with a known query type.
        # Tricky: by converting the timestamp to a number, we also can ignore the
        # differences between naive and aware datetimes.
        queries = sorted(
            # Sorted is a stable sort, so in the case of total ties, we want
            # to prefer the most recently added query.
            reversed(queries),
            key=lambda query: (
                self._query_type_precedence(query.lineage_type),
                -(make_ts_millis(query.latest_timestamp) or 0),
                query.query_type == QueryType.UNKNOWN,
            ),
        )

        queries_map: Dict[QueryId, QueryMetadata] = {
            query.query_id: query for query in queries
        }

        # mapping of upstream urn -> query id that produced it
        upstreams: Dict[UrnStr, QueryId] = {}
        # mapping of downstream column -> { upstream column -> query id that produced it }
        cll: Dict[str, Dict[SchemaFieldUrn, QueryId]] = defaultdict(dict)

        for query in queries:
            # Using setdefault to respect the precedence of queries.

            for upstream in query.upstreams:
                upstreams.setdefault(upstream, query.query_id)

            for lineage_info in query.column_lineage:
                for upstream_ref in lineage_info.upstreams:
                    cll[lineage_info.downstream.column].setdefault(
                        SchemaFieldUrn(upstream_ref.table, upstream_ref.column),
                        query.query_id,
                    )

        # Finally, we can build our lineage edge.
        required_queries = OrderedSet[QueryId]()
        upstream_aspect = models.UpstreamLineageClass(upstreams=[])
        for upstream_urn, query_id in upstreams.items():
            required_queries.add(query_id)

            upstream_aspect.upstreams.append(
                models.UpstreamClass(
                    dataset=upstream_urn,
                    type=queries_map[query_id].lineage_type,
                    query=(
                        self._query_urn(query_id)
                        if self.can_generate_query(query_id)
                        else None
                    ),
                    created=query.make_created_audit_stamp(),
                    auditStamp=models.AuditStampClass(
                        time=get_sys_time(),
                        actor=_DEFAULT_USER_URN.urn(),
                    ),
                )
            )
        upstream_aspect.fineGrainedLineages = []
        for downstream_column, all_upstream_columns in cll.items():
            # Group by query ID.
            for query_id, upstream_columns_for_query in groupby_unsorted(
                all_upstream_columns.items(),
                key=lambda x: x[1],
            ):
                upstream_columns = [x[0] for x in upstream_columns_for_query]
                required_queries.add(query_id)

                upstream_aspect.fineGrainedLineages.append(
                    models.FineGrainedLineageClass(
                        upstreamType=models.FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=[
                            upstream_column.urn()
                            for upstream_column in upstream_columns
                        ],
                        downstreamType=models.FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[
                            SchemaFieldUrn(downstream_urn, downstream_column).urn()
                        ],
                        query=(
                            self._query_urn(query_id)
                            if self.can_generate_query(query_id)
                            else None
                        ),
                        confidenceScore=queries_map[query_id].confidence_score,
                    )
                )

        if len(upstream_aspect.upstreams) > MAX_UPSTREAM_TABLES_COUNT:
            logger.warning(
                f"Too many upstream tables for {downstream_urn}: {len(upstream_aspect.upstreams)}"
                f"Keeping only {MAX_UPSTREAM_TABLES_COUNT} table level upstreams/"
            )
            upstream_aspect.upstreams = upstream_aspect.upstreams[
                :MAX_UPSTREAM_TABLES_COUNT
            ]
            self.report.num_table_lineage_trimmed_due_to_large_size += 1
        if len(upstream_aspect.fineGrainedLineages) > MAX_FINEGRAINEDLINEAGE_COUNT:
            logger.warning(
                f"Too many upstream columns for {downstream_urn}: {len(upstream_aspect.fineGrainedLineages)}"
                f"Keeping only {MAX_FINEGRAINEDLINEAGE_COUNT} column level upstreams/"
            )
            upstream_aspect.fineGrainedLineages = upstream_aspect.fineGrainedLineages[
                :MAX_FINEGRAINEDLINEAGE_COUNT
            ]
            self.report.num_column_lineage_trimmed_due_to_large_size += 1

        upstream_aspect.fineGrainedLineages = (
            upstream_aspect.fineGrainedLineages or None
        )

        if not upstream_aspect.upstreams and not upstream_aspect.fineGrainedLineages:
            return

        yield MetadataChangeProposalWrapper(
            entityUrn=downstream_urn,
            aspect=upstream_aspect,
        )

        for query_id in required_queries:
            # Avoid generating the same query twice.
            if query_id in queries_generated:
                continue
            queries_generated.add(query_id)

            query = queries_map[query_id]
            yield from self._gen_query(query, downstream_urn)

    @classmethod
    def _query_urn(cls, query_id: QueryId) -> str:
        return QueryUrn(query_id).urn()

    @classmethod
    def _composite_query_id(cls, composed_of_queries: List[QueryId]) -> str:
        combined = json.dumps(composed_of_queries)
        return f"composite_{generate_hash(combined)}"

    @classmethod
    def _view_query_id(cls, view_urn: UrnStr) -> str:
        return f"view_{DatasetUrn.url_encode(view_urn)}"

    @classmethod
    def _known_lineage_query_id(cls) -> str:
        return f"known_{uuid.uuid4()}"

    @classmethod
    def _is_known_lineage_query_id(cls, query_id: QueryId) -> bool:
        # Our query fingerprints are hex and won't have underscores, so this will
        # never conflict with a real query fingerprint.
        return query_id.startswith("known_")

    def _gen_remaining_queries(
        self, queries_generated: Set[QueryId]
    ) -> Iterable[MetadataChangeProposalWrapper]:
        if not self.generate_queries or not self.generate_query_usage_statistics:
            return

        assert self._query_usage_counts is not None
        for query_id in self._query_usage_counts:
            if query_id in queries_generated:
                continue
            queries_generated.add(query_id)

            yield from self._gen_query(self._query_map[query_id])

    def can_generate_query(self, query_id: QueryId) -> bool:
        return self.generate_queries and not self._is_known_lineage_query_id(query_id)

    def _gen_query(
        self, query: QueryMetadata, downstream_urn: Optional[str] = None
    ) -> Iterable[MetadataChangeProposalWrapper]:
        query_id = query.query_id
        if not self.can_generate_query(query_id):
            return

        # If a query doesn't involve any allowed tables, skip it.
        if downstream_urn is None and not any(
            self.is_allowed_table(urn) for urn in query.upstreams
        ):
            self.report.num_queries_skipped_due_to_filters += 1
            return

        yield from MetadataChangeProposalWrapper.construct_many(
            entityUrn=self._query_urn(query_id),
            aspects=[
                query.make_query_properties(),
                make_query_subjects(
                    query.get_subjects(
                        downstream_urn=downstream_urn,
                        include_fields=self.generate_query_subject_fields,
                    )
                ),
                models.DataPlatformInstanceClass(
                    platform=self.platform.urn(),
                ),
            ],
        )
        self.report.num_queries_entities_generated += 1

        if self._query_usage_counts is not None:
            assert self.usage_config is not None

            # This is slightly lossy, since we only store one unique
            # user per query instead of tracking all of them.
            # We also lose information because we don't keep track
            # of users / lastExecutedAt timestamps per bucket.
            user = query.actor

            query_counter = self._query_usage_counts.get(query_id)
            if not query_counter:
                return

            all_buckets = self.usage_config.buckets()

            for bucket, count in query_counter.items():
                if bucket not in all_buckets:
                    # What happens if we get a query with a timestamp that's outside our configured window?
                    # Theoretically this should never happen, since the audit logs are also fetched
                    # for the window. However, it's useful to have reporting for it, just in case.
                    self.report.num_query_usage_stats_outside_window += 1
                    continue

                yield MetadataChangeProposalWrapper(
                    entityUrn=self._query_urn(query_id),
                    aspect=models.QueryUsageStatisticsClass(
                        timestampMillis=make_ts_millis(bucket),
                        eventGranularity=models.TimeWindowSizeClass(
                            unit=self.usage_config.bucket_duration, multiple=1
                        ),
                        queryCount=count,
                        uniqueUserCount=1,
                        userCounts=(
                            [
                                models.DatasetUserUsageCountsClass(
                                    user=user.urn(),
                                    count=count,
                                )
                            ]
                            if user
                            else None
                        ),
                    ),
                )

                self.report.num_query_usage_stats_generated += 1

    def _resolve_query_with_temp_tables(
        self,
        base_query: QueryMetadata,
    ) -> QueryMetadata:
        # TODO: Add special handling for COPY operations, which should mirror the schema
        # of the thing being copied in order to generate CLL.

        session_id = base_query.session_id

        composed_of_queries = OrderedSet[QueryId]()

        @dataclasses.dataclass
        class QueryLineageInfo:
            upstreams: List[UrnStr]  # this is direct upstreams, with *no temp tables*
            column_lineage: List[ColumnLineageInfo]
            confidence_score: float

            def _merge_lineage_from(self, other_query: "QueryLineageInfo") -> None:
                self.upstreams += other_query.upstreams
                self.column_lineage += other_query.column_lineage
                self.confidence_score = min(
                    self.confidence_score, other_query.confidence_score
                )

        def _recurse_into_query(
            query: QueryMetadata, recursion_path: List[QueryId]
        ) -> QueryLineageInfo:
            if query.query_id in recursion_path:
                # This is a cycle, so we just return the query as-is.
                return QueryLineageInfo(
                    upstreams=query.upstreams,
                    column_lineage=query.column_lineage,
                    confidence_score=query.confidence_score,
                )
            recursion_path = [*recursion_path, query.query_id]
            composed_of_queries.add(query.query_id)

            # Find all the temp tables that this query depends on.
            temp_upstream_queries: Dict[UrnStr, QueryLineageInfo] = {}
            for upstream in query.upstreams:
                upstream_query_ids = self._temp_lineage_map.get(session_id, {}).get(
                    upstream
                )
                if upstream_query_ids:
                    for upstream_query_id in upstream_query_ids:
                        upstream_query = self._query_map.get(upstream_query_id)
                        if (
                            upstream_query
                            and upstream_query.query_id not in composed_of_queries
                        ):
                            temp_query_lineage_info = _recurse_into_query(
                                upstream_query, recursion_path
                            )
                            if upstream in temp_upstream_queries:
                                temp_upstream_queries[upstream]._merge_lineage_from(
                                    temp_query_lineage_info
                                )
                            else:
                                temp_upstream_queries[upstream] = (
                                    temp_query_lineage_info
                                )

            # Compute merged upstreams.
            new_upstreams = OrderedSet[UrnStr]()
            for upstream in query.upstreams:
                if upstream in temp_upstream_queries:
                    new_upstreams.update(temp_upstream_queries[upstream].upstreams)
                else:
                    new_upstreams.add(upstream)

            # Compute merged column lineage.
            new_cll = []
            for lineage_info in query.column_lineage:
                new_column_upstreams: List[ColumnRef] = []
                for existing_col_upstream in lineage_info.upstreams:
                    if existing_col_upstream.table in temp_upstream_queries:
                        new_column_upstreams.extend(
                            [
                                temp_col_upstream
                                for temp_lineage_info in temp_upstream_queries[
                                    existing_col_upstream.table
                                ].column_lineage
                                for temp_col_upstream in temp_lineage_info.upstreams
                                if temp_lineage_info.downstream.column
                                == existing_col_upstream.column
                            ]
                        )
                    else:
                        new_column_upstreams.append(existing_col_upstream)

                new_cll.append(
                    ColumnLineageInfo(
                        downstream=lineage_info.downstream,
                        upstreams=new_column_upstreams,
                    )
                )

            # Compute merged confidence score.
            new_confidence_score = min(
                [
                    query.confidence_score,
                    *[
                        temp_upstream_query.confidence_score
                        for temp_upstream_query in temp_upstream_queries.values()
                    ],
                ]
            )

            return QueryLineageInfo(
                upstreams=list(new_upstreams),
                column_lineage=new_cll,
                confidence_score=new_confidence_score,
            )

        resolved_lineage_info = _recurse_into_query(base_query, [])

        # Fast path if there were no temp tables.
        if len(composed_of_queries) == 1:
            return base_query

        # If this query does actually depend on temp tables:
        # - Clone the query into a new object
        # - Generate a new composite fingerprint
        # - Update the lineage info
        # - Update the query text to combine the queries

        ordered_queries = [
            self._query_map[query_id] for query_id in reversed(composed_of_queries)
        ]
        if all(q.latest_timestamp for q in ordered_queries):
            ordered_queries = sorted(
                ordered_queries,
                key=lambda query: make_ts_millis(query.latest_timestamp) or 0,
            )
        composite_query_id = self._composite_query_id(
            [q.query_id for q in ordered_queries]
        )
        composed_of_queries_truncated: LossyList[str] = LossyList()
        for query_id in composed_of_queries:
            composed_of_queries_truncated.append(query_id)
        self.report.queries_with_temp_upstreams[composite_query_id] = (
            composed_of_queries_truncated
        )

        merged_query_text = ";\n\n".join(
            [q.formatted_query_string for q in ordered_queries]
        )

        resolved_query = dataclasses.replace(
            base_query,
            query_id=composite_query_id,
            formatted_query_string=merged_query_text,
            upstreams=resolved_lineage_info.upstreams,
            column_lineage=resolved_lineage_info.column_lineage,
            confidence_score=resolved_lineage_info.confidence_score,
        )

        return resolved_query

    def _gen_usage_statistics_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        if not self._usage_aggregator:
            return

        for wu in self._usage_aggregator.generate_workunits(
            resource_urn_builder=lambda urn: urn,
            user_urn_builder=lambda urn: urn,
        ):
            # TODO: We should change the usage aggregator to return MCPWs directly.
            yield cast(MetadataChangeProposalWrapper, wu.metadata)

    def _gen_operation_mcps(
        self, queries_generated: Set[QueryId]
    ) -> Iterable[MetadataChangeProposalWrapper]:
        if not self.generate_operations:
            return

        for downstream_urn, query_ids in self._lineage_map.items():
            for query_id in query_ids:
                yield from self._gen_operation_for_downstream(downstream_urn, query_id)

                # Avoid generating the same query twice.
                if query_id in queries_generated:
                    continue
                queries_generated.add(query_id)
                yield from self._gen_query(self._query_map[query_id], downstream_urn)

    def _gen_operation_for_downstream(
        self, downstream_urn: UrnStr, query_id: QueryId
    ) -> Iterable[MetadataChangeProposalWrapper]:
        query = self._query_map[query_id]
        if query.latest_timestamp is None:
            return

        operation_type = query.query_type.to_operation_type()
        if operation_type is None:
            # We don't generate operations for SELECTs.
            return

        if not self.is_allowed_table(downstream_urn):
            self.report.num_operations_skipped_due_to_filters += 1
            return

        self.report.num_operations_generated += 1
        aspect = models.OperationClass(
            timestampMillis=make_ts_millis(datetime.now(tz=timezone.utc)),
            operationType=operation_type,
            lastUpdatedTimestamp=make_ts_millis(query.latest_timestamp),
            actor=query.actor.urn() if query.actor else None,
            sourceType=models.OperationSourceTypeClass.DATA_PLATFORM,
            queries=(
                [self._query_urn(query_id)]
                if self.can_generate_query(query_id)
                else None
            ),
        )
        yield MetadataChangeProposalWrapper(entityUrn=downstream_urn, aspect=aspect)
