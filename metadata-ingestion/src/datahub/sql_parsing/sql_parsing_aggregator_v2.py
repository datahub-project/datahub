import dataclasses
import enum
import itertools
import json
import logging
import pathlib
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional, Set, cast

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import get_sys_time, make_ts_millis
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.sql_parsing_builder import compute_upstream_fields
from datahub.ingestion.api.report import Report
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig, UsageAggregator
from datahub.metadata.urns import (
    CorpUserUrn,
    DataPlatformUrn,
    DatasetUrn,
    QueryUrn,
    SchemaFieldUrn,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver, SchemaResolverInterface
from datahub.sql_parsing.sql_parsing_common import QueryType
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    SqlParsingResult,
    infer_output_schema,
    sqlglot_lineage,
)
from datahub.sql_parsing.sqlglot_utils import generate_hash
from datahub.utilities.file_backed_collections import (
    ConnectionWrapper,
    FileBackedDict,
    FileBackedList,
)
from datahub.utilities.lossy_collections import LossyDict, LossyList
from datahub.utilities.ordered_set import OrderedSet

logger = logging.getLogger(__name__)
QueryId = str
UrnStr = str

_DEFAULT_USER_URN = CorpUserUrn("_ingestion")
_MISSING_SESSION_ID = "__MISSING_SESSION_ID"


class QueryLogSetting(enum.Enum):
    DISABLED = "DISABLED"
    STORE_ALL = "STORE_ALL"
    STORE_FAILED = "STORE_FAILED"


@dataclasses.dataclass
class ViewDefinition:
    # TODO view urn?

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
    actor: Optional[CorpUserUrn]

    upstreams: List[UrnStr]  # this is direct upstreams, which may be temp tables
    column_lineage: List[ColumnLineageInfo]
    confidence_score: float

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


@dataclasses.dataclass
class SqlAggregatorReport(Report):
    _aggregator: "SqlParsingAggregator"
    query_log_path: Optional[str] = None

    num_observed_queries: int = 0
    num_observed_queries_failed: int = 0
    num_observed_queries_column_failed: int = 0
    observed_query_parse_failures = LossyList[str]()

    num_view_definitions: int = 0
    num_views_failed: int = 0
    num_views_column_failed: int = 0
    views_parse_failures = LossyDict[UrnStr, str]()

    num_queries_with_temp_tables_in_session: int = 0

    num_unique_query_fingerprints: Optional[int] = None

    # Lineage-related.
    num_urns_with_lineage: Optional[int] = None
    num_temp_sessions: Optional[int] = None
    num_inferred_temp_schemas: Optional[int] = None
    queries_with_temp_upstreams: LossyDict[QueryId, LossyList] = dataclasses.field(
        default_factory=LossyDict
    )

    num_queries_entities_generated: int = 0

    # Usage-related.
    usage_skipped_missing_timestamp: int = 0

    def compute_stats(self) -> None:
        self.num_unique_query_fingerprints = len(self._aggregator._query_map)

        self.num_urns_with_lineage = len(self._aggregator._lineage_map)
        self.num_temp_sessions = len(self._aggregator._temp_lineage_map)
        self.num_inferred_temp_schemas = len(self._aggregator._inferred_temp_schemas)

        return super().compute_stats()


class SqlParsingAggregator:
    def __init__(
        self,
        *,
        platform: str,
        platform_instance: Optional[str],
        env: str,
        graph: Optional[DataHubGraph] = None,
        generate_lineage: bool = True,
        generate_queries: bool = True,
        generate_usage_statistics: bool = False,
        generate_operations: bool = False,
        usage_config: Optional[BaseUsageConfig] = None,
        is_temp_table: Optional[Callable[[UrnStr], bool]] = None,
        query_log: QueryLogSetting = QueryLogSetting.DISABLED,
    ) -> None:
        self.platform = DataPlatformUrn(platform)
        self.platform_instance = platform_instance
        self.env = env

        self.generate_lineage = generate_lineage
        self.generate_queries = generate_queries
        self.generate_usage_statistics = generate_usage_statistics
        self.generate_operations = generate_operations
        if self.generate_queries and not self.generate_lineage:
            raise ValueError("Queries will only be generated if lineage is enabled")

        self.usage_config = usage_config
        if self.generate_usage_statistics and self.usage_config is None:
            raise ValueError("Usage statistics generation requires a usage config")

        self.report = SqlAggregatorReport(_aggregator=self)

        # can be used by BQ where we have a "temp_table_dataset_prefix"
        self.is_temp_table = is_temp_table

        self.query_log = query_log

        # Set up the schema resolver.
        self._schema_resolver: SchemaResolver
        if graph is None:
            self._schema_resolver = SchemaResolver(
                platform=self.platform.platform_name,
                platform_instance=self.platform_instance,
                env=self.env,
            )
        else:
            self._schema_resolver = None  # type: ignore
            self._initialize_schema_resolver_from_graph(graph)

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
            self._shared_connection = ConnectionWrapper(filename=query_log_path)

        # Stores the logged queries.
        self._logged_queries = FileBackedList[str](
            shared_connection=self._shared_connection, tablename="stored_queries"
        )

        # Map of query_id -> QueryMetadata
        self._query_map = FileBackedDict[QueryMetadata](
            shared_connection=self._shared_connection, tablename="query_map"
        )

        # Map of downstream urn -> { query ids }
        self._lineage_map = FileBackedDict[OrderedSet[QueryId]](
            shared_connection=self._shared_connection, tablename="lineage_map"
        )

        # Map of view urn -> view definition
        self._view_definitions = FileBackedDict[ViewDefinition](
            shared_connection=self._shared_connection, tablename="view_definitions"
        )

        # Map of session ID -> {temp table name -> query id}
        # Needs to use the query_map to find the info about the query.
        # This assumes that a temp table is created at most once per session.
        self._temp_lineage_map = FileBackedDict[Dict[UrnStr, QueryId]](
            shared_connection=self._shared_connection, tablename="temp_lineage_map"
        )

        # Map of query ID -> schema fields, only for query IDs that generate temp tables.
        self._inferred_temp_schemas = FileBackedDict[List[models.SchemaFieldClass]](
            shared_connection=self._shared_connection, tablename="inferred_temp_schemas"
        )

        # Usage aggregator. This will only be initialized if usage statistics are enabled.
        # TODO: Replace with FileBackedDict.
        self._usage_aggregator: Optional[UsageAggregator[UrnStr]] = None
        if self.generate_usage_statistics:
            assert self.usage_config is not None
            self._usage_aggregator = UsageAggregator(config=self.usage_config)

    @property
    def _need_schemas(self) -> bool:
        return self.generate_lineage or self.generate_usage_statistics

    def register_schema(
        self, urn: DatasetUrn, schema: models.SchemaMetadataClass
    ) -> None:
        # If lineage or usage is enabled, adds the schema to the schema resolver
        # by putting the condition in here, we can avoid all the conditional
        # logic that we previously needed in each source

        if self._need_schemas:
            self._schema_resolver.add_schema_metadata(str(urn), schema)

    def _initialize_schema_resolver_from_graph(self, graph: DataHubGraph) -> None:
        # requires a graph instance
        # if no schemas are currently registered in the schema resolver
        # and we need the schema resolver (e.g. lineage or usage is enabled)
        # then use the graph instance to fetch all schemas for the
        # platform/instance/env combo
        if not self._need_schemas:
            return

        if (
            self._schema_resolver is not None
            and self._schema_resolver.schema_count() > 0
        ):
            # TODO: Have a mechanism to override this, e.g. when table ingestion is enabled but view ingestion is not.
            logger.info(
                "Not fetching any schemas from the graph, since "
                f"there are {self._schema_resolver.schema_count()} schemas already registered."
            )
            return

        # TODO: The initialize_schema_resolver_from_datahub method should take in a SchemaResolver
        # that it can populate or add to, rather than creating a new one and dropping any schemas
        # that were already loaded into the existing one.
        self._schema_resolver = graph.initialize_schema_resolver_from_datahub(
            platform=self.platform.urn(),
            platform_instance=self.platform_instance,
            env=self.env,
        )

    def add_view_definition(
        self,
        view_urn: DatasetUrn,
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
        query: str,
        default_db: Optional[str] = None,
        default_schema: Optional[str] = None,
        query_timestamp: Optional[datetime] = None,
        user: Optional[CorpUserUrn] = None,
        session_id: Optional[
            str
        ] = None,  # can only see temp tables with the same session
        usage_multiplier: int = 1,
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
        session_id = session_id or _MISSING_SESSION_ID

        # Load in the temp tables for this session.
        schema_resolver: SchemaResolverInterface = (
            self._make_schema_resolver_for_session(session_id)
        )

        # Run the SQL parser.
        parsed = self._run_sql_parser(
            query,
            default_db=default_db,
            default_schema=default_schema,
            schema_resolver=schema_resolver,
        )
        if parsed.debug_info.error:
            self.report.observed_query_parse_failures.append(
                str(parsed.debug_info.error)
            )
        if parsed.debug_info.table_error:
            self.report.num_observed_queries_failed += 1
            return  # we can't do anything with this query
        elif parsed.debug_info.error:
            self.report.num_observed_queries_column_failed += 1

        # Register the query's usage.
        if not self._usage_aggregator:
            pass  # usage is not enabled
        elif query_timestamp is None:
            self.report.usage_skipped_missing_timestamp += 1
        else:
            # TODO: We need a full list of columns referenced, not just the out tables.
            upstream_fields = compute_upstream_fields(parsed)
            for upstream_urn in parsed.in_tables:
                self._usage_aggregator.aggregate_event(
                    resource=upstream_urn,
                    start_time=query_timestamp,
                    query=query,
                    user=user.urn() if user else None,
                    fields=sorted(upstream_fields.get(upstream_urn, [])),
                    count=usage_multiplier,
                )

        if not parsed.out_tables:
            return
        out_table = parsed.out_tables[0]
        query_fingerprint = parsed.query_fingerprint
        assert query_fingerprint is not None

        # Register the query.
        self._add_to_query_map(
            QueryMetadata(
                query_id=query_fingerprint,
                formatted_query_string=query,  # TODO replace with formatted query string
                session_id=session_id,
                query_type=parsed.query_type,
                lineage_type=models.DatasetLineageTypeClass.TRANSFORMED,
                latest_timestamp=query_timestamp,
                actor=user,
                upstreams=parsed.in_tables,
                column_lineage=parsed.column_lineage or [],
                confidence_score=parsed.debug_info.confidence,
            )
        )

        # Register the query's lineage.
        if (
            is_known_temp_table
            or (
                parsed.query_type.is_create()
                and parsed.query_type_props.get("temporary")
            )
            or (self.is_temp_table and self.is_temp_table(out_table))
            or (
                require_out_table_schema
                and not self._schema_resolver.has_urn(out_table)
            )
        ):
            # Infer the schema of the output table and track it for later.
            inferred_schema = infer_output_schema(parsed)
            if inferred_schema is not None:
                self._inferred_temp_schemas[query_fingerprint] = inferred_schema

            # Also track the lineage for the temp table, for merging purposes later.
            self._temp_lineage_map.for_mutation(session_id, {})[
                out_table
            ] = query_fingerprint

        else:
            # Non-temp tables immediately generate lineage.
            self._lineage_map.for_mutation(out_table, OrderedSet()).add(
                query_fingerprint
            )

    def _make_schema_resolver_for_session(
        self, session_id: str
    ) -> SchemaResolverInterface:
        schema_resolver: SchemaResolverInterface = self._schema_resolver
        if session_id in self._temp_lineage_map:
            temp_table_schemas: Dict[str, Optional[List[models.SchemaFieldClass]]] = {}
            for temp_table_urn, query_id in self._temp_lineage_map[session_id].items():
                temp_table_schemas[temp_table_urn] = self._inferred_temp_schemas.get(
                    query_id
                )

            if temp_table_schemas:
                schema_resolver = self._schema_resolver.with_temp_tables(
                    temp_table_schemas
                )
                self.report.num_queries_with_temp_tables_in_session += 1

        return schema_resolver

    def _process_view_definition(
        self, view_urn: UrnStr, view_definition: ViewDefinition
    ) -> None:
        # Run the SQL parser.
        parsed = self._run_sql_parser(
            view_definition.view_definition,
            default_db=view_definition.default_db,
            default_schema=view_definition.default_schema,
            schema_resolver=self._schema_resolver,
        )
        if parsed.debug_info.error:
            self.report.views_parse_failures[view_urn] = str(parsed.debug_info.error)
        if parsed.debug_info.table_error:
            self.report.num_views_failed += 1
            return  # we can't do anything with this query
        elif parsed.debug_info.error:
            self.report.num_views_column_failed += 1

        # Note that in some cases, the view definition will be a SELECT statement
        # instead of a CREATE VIEW ... AS SELECT statement. In those cases, we can't
        # trust the parsed query type or downstream urn.

        query_fingerprint = self._view_query_id(view_urn)

        # Register the query.
        self._add_to_query_map(
            QueryMetadata(
                query_id=query_fingerprint,
                formatted_query_string=view_definition.view_definition,
                session_id=_MISSING_SESSION_ID,
                query_type=QueryType.CREATE_VIEW,
                lineage_type=models.DatasetLineageTypeClass.VIEW,
                latest_timestamp=None,
                actor=None,
                upstreams=parsed.in_tables,
                column_lineage=parsed.column_lineage or [],
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
    ) -> SqlParsingResult:
        parsed = sqlglot_lineage(
            query,
            schema_resolver=schema_resolver,
            default_db=default_db,
            default_schema=default_schema,
        )

        # Conditionally log the query.
        if self.query_log == QueryLogSetting.STORE_ALL or (
            self.query_log == QueryLogSetting.STORE_FAILED and parsed.debug_info.error
        ):
            self._logged_queries.append(query)

        # Also add some extra logging.
        if parsed.debug_info.error:
            logger.debug(
                f"Error parsing query {query}: {parsed.debug_info.error}",
                exc_info=parsed.debug_info.error,
            )

        return parsed

    def _add_to_query_map(self, new: QueryMetadata) -> None:
        query_fingerprint = new.query_id

        if query_fingerprint in self._query_map:
            current = self._query_map.for_mutation(query_fingerprint)

            # This assumes that queries come in order of increasing timestamps,
            # so the current query is more authoritative than the previous one.
            current.formatted_query_string = new.formatted_query_string
            current.session_id = new.session_id
            current.latest_timestamp = new.latest_timestamp or current.latest_timestamp
            current.actor = new.actor or current.actor

            # An invariant of the fingerprinting is that if two queries have the
            # same fingerprint, they must also have the same lineage. We overwrite
            # here just in case more schemas got registered in the interim.
            current.upstreams = new.upstreams
            current.column_lineage = new.column_lineage
            current.confidence_score = new.confidence_score
        else:
            self._query_map[query_fingerprint] = new

    """
    def add_lineage(self) -> None:
        # A secondary mechanism for adding non-SQL-based lineage
        # e.g. redshift external tables might use this when pointing at s3

        # TODO Add this once we have a use case for it
        pass
    """

    def gen_metadata(self) -> Iterable[MetadataChangeProposalWrapper]:
        # diff from v1 - we generate operations here, and it also
        # generates MCPWs instead of workunits
        yield from self._gen_lineage_mcps()
        yield from self._gen_usage_statistics_mcps()
        yield from self._gen_operation_mcps()

    def _gen_lineage_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        if not self.generate_lineage:
            return

        # Process all views and inject them into the lineage map.
        # The parsing of view definitions is deferred until this point
        # to ensure the availability of all schema metadata.
        for view_urn, view_definition in self._view_definitions.items():
            self._process_view_definition(view_urn, view_definition)
        self._view_definitions.clear()

        # Generate lineage and queries.
        queries_generated: Set[QueryId] = set()
        for downstream_urn in self._lineage_map:
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

        idx = query_precedence.index(query_type)
        if idx == -1:
            return len(query_precedence)
        return idx

    def _gen_lineage_for_downstream(
        self, downstream_urn: str, queries_generated: Set[QueryId]
    ) -> Iterable[MetadataChangeProposalWrapper]:
        query_ids = self._lineage_map[downstream_urn]
        queries: List[QueryMetadata] = [
            self._resolve_query_with_temp_tables(self._query_map[query_id])
            for query_id in query_ids
        ]

        # Sort the queries by highest precedence first, then by latest timestamp.
        # Tricky: by converting the timestamp to a number, we also can ignore the
        # differences between naive and aware datetimes.
        queries = sorted(
            reversed(queries),
            key=lambda query: (
                self._query_type_precedence(query.lineage_type),
                -(make_ts_millis(query.latest_timestamp) or 0),
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
                        self._query_urn(query_id) if self.generate_queries else None
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
            for query_id, upstream_columns_for_query in itertools.groupby(
                sorted(all_upstream_columns.items(), key=lambda x: x[1]),
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
                            self._query_urn(query_id) if self.generate_queries else None
                        ),
                        confidenceScore=queries_map[query_id].confidence_score,
                    )
                )

        yield MetadataChangeProposalWrapper(
            entityUrn=downstream_urn,
            aspect=upstream_aspect,
        )

        if not self.generate_queries:
            return
        for query_id in required_queries:
            # Avoid generating the same query twice.
            if query_id in queries_generated:
                continue
            queries_generated.add(query_id)
            self.report.num_queries_entities_generated += 1

            query = queries_map[query_id]
            yield from MetadataChangeProposalWrapper.construct_many(
                entityUrn=self._query_urn(query_id),
                aspects=[
                    models.QueryPropertiesClass(
                        statement=models.QueryStatementClass(
                            value=query.formatted_query_string,
                            language=models.QueryLanguageClass.SQL,
                        ),
                        source=models.QuerySourceClass.SYSTEM,
                        created=query.make_created_audit_stamp(),
                        lastModified=query.make_last_modified_audit_stamp(),
                    ),
                    models.QuerySubjectsClass(
                        subjects=[
                            models.QuerySubjectClass(entity=dataset_urn)
                            for dataset_urn in itertools.chain(
                                [downstream_urn], query.upstreams
                            )
                        ]
                    ),
                ],
            )

    @classmethod
    def _query_urn(cls, query_id: QueryId) -> str:
        return QueryUrn(query_id).urn()

    @classmethod
    def _composite_query_id(cls, composed_of_queries: Iterable[QueryId]) -> str:
        composed_of_queries = list(composed_of_queries)
        combined = json.dumps(composed_of_queries)
        return f"composite_{generate_hash(combined)}"

    @classmethod
    def _view_query_id(cls, view_urn: UrnStr) -> str:
        return f"view_{DatasetUrn.url_encode(view_urn)}"

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
                upstream_query_id = self._temp_lineage_map.get(session_id, {}).get(
                    upstream
                )
                if upstream_query_id:
                    upstream_query = self._query_map.get(upstream_query_id)
                    if upstream_query:
                        temp_upstream_queries[upstream] = _recurse_into_query(
                            upstream_query, recursion_path
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

        composite_query_id = self._composite_query_id(composed_of_queries)
        self.report.queries_with_temp_upstreams.setdefault(
            composite_query_id, LossyList()
        ).extend(composed_of_queries)

        merged_query_text = ";\n\n".join(
            [
                self._query_map[query_id].formatted_query_string
                for query_id in reversed(composed_of_queries)
            ]
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

    def _gen_operation_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        if not self.generate_operations:
            return

        for downstream_urn, query_ids in self._lineage_map.items():
            for query_id in query_ids:
                yield from self._gen_operation_for_downstream(downstream_urn, query_id)

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

        aspect = models.OperationClass(
            timestampMillis=make_ts_millis(datetime.now(tz=timezone.utc)),
            operationType=operation_type,
            lastUpdatedTimestamp=make_ts_millis(query.latest_timestamp),
            actor=query.actor.urn() if query.actor else None,
            customProperties={
                "query_urn": self._query_urn(query_id),
            },
        )
        yield MetadataChangeProposalWrapper(entityUrn=downstream_urn, aspect=aspect)
