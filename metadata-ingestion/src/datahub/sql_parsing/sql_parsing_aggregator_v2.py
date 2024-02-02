import dataclasses
import enum
import logging
from collections import defaultdict
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Optional, Set

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.metadata.urns import (
    CorpUserUrn,
    DataPlatformUrn,
    DatasetUrn,
    SchemaFieldUrn,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import ColumnLineageInfo, sqlglot_lineage
from datahub.utilities.file_backed_collections import FileBackedDict

logger = logging.getLogger(__name__)
QueryId = str


class StoreQueriesSetting(enum.Enum):
    DISABLED = "DISABLED"
    STORE_ALL = "STORE_ALL"
    STORE_FAILED = "STORE_FAILED"


@dataclasses.dataclass
class QueryMetadata:
    query_id: QueryId

    raw_query_string: str
    # formatted_query_string: str  # TODO add this

    session_id: Optional[str]
    type: str
    latest_timestamp: Optional[datetime]
    actor: Optional[CorpUserUrn]

    upstreams: List[str]  # this is direct upstreams, which may be temp tables
    column_lineage: List[ColumnLineageInfo]  # TODO add an internal representation?
    confidence_score: float


class SqlParsingAggregator:

    def __init__(
        self,
        *,
        platform: str,
        platform_instance: Optional[str],
        env: str,
        graph: Optional[DataHubGraph] = None,
        generate_view_lineage: bool = True,
        generate_observed_lineage: bool = True,
        generate_queries: bool = True,
        generate_usage_statistics: bool = False,
        generate_operations: bool = False,
        usage_config: Optional[BaseUsageConfig] = None,
        is_temp_table: Optional[Callable[[str], bool]] = None,
        store_queries: StoreQueriesSetting = StoreQueriesSetting.DISABLED,
    ) -> None:
        self.platform = DataPlatformUrn(platform)
        self.platform_instance = platform_instance
        self.env = env

        self.generate_view_lineage = generate_view_lineage
        self.generate_observed_lineage = generate_observed_lineage
        self.generate_queries = generate_queries
        self.generate_usage_statistics = generate_usage_statistics
        self.generate_operations = generate_operations
        if self.generate_queries and not self.generate_lineage:
            raise ValueError("Queries will only be generated if lineage is enabled")

        self.usage_config = usage_config
        if self.generate_usage_statistics and self.usage_config is None:
            raise ValueError("Usage statistics generation requires a usage config")

        # can be used by BQ where we have a "temp_table_dataset_prefix"
        self.is_temp_table = is_temp_table

        self.store_queries = (
            store_queries  # TODO: implement + make the name more descriptive
        )

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

        # Map of query_id -> QueryMetadata
        self._query_map = FileBackedDict[QueryMetadata]()

        # Map of downstream urn -> { query ids }
        self._lineage_map = FileBackedDict[Set[QueryId]]()

        # Map of session ID -> {temp table name -> query id}
        # Needs to use the query_map to find the info about the query.
        # This assumes that a temp table is created at most once per session.
        self._temp_lineage_map = FileBackedDict[Dict[str, QueryId]]()

        # Map of query ID -> schema fields, only for query IDs that generate temp tables.
        self._inferred_temp_schemas = FileBackedDict[List[models.SchemaFieldClass]]

        # TODO list of query IDs that we actually want in operations

    @property
    def generate_lineage(self) -> bool:
        return self.generate_view_lineage or self.generate_observed_lineage

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
        view_definition: str,
        default_db: Optional[str] = None,
        default_schema: Optional[str] = None,
        # used in case the query is "SELECT ..." and not
        # "CREATE VIEW ... AS SELECT ..."
        fallback_view_urn: Optional[DatasetUrn] = None,
    ) -> None:
        # view definitions contribute to lineage, but not usage/operations
        # if we have a view definition for a given table, observed queries are ignored

        # One critical detail here - unlike add_observed_query, add_view_definition
        # only enqueues the view_definition. The actual query parsing and lineage
        # generation happens at the time of gen_lineage.
        pass

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
        # TODO: not 100% sure about this flag - it'd basically be here for redshift
        is_known_temp_table: bool = False,
    ) -> None:
        # This may or may not generate lineage.
        # If it produces lineage to a temp table, that gets logged in a separate lineage map
        # If it produces lineage to a non-temp table, it also produces an operation.
        # Either way, it generates usage.

        # Note: this assumes that queries come in order of increasing timestamps

        # TODO load in any temp tables for this session into the schema resolver
        parsed = sqlglot_lineage(
            query,
            schema_resolver=self._schema_resolver,
            default_db=default_db,
            default_schema=default_schema,
        )

        if parsed.debug_info.table_error:
            # TODO replace with better error reporting + logging
            logger.debug(
                f"Error parsing query {query}: {parsed.debug_info.table_error}"
            )
            return

        if not parsed.out_tables:
            # TODO - this is just a SELECT statement, so it only counts towards usage
            # TODO we need a full list of columns referenced, not just the out tables
            breakpoint()

            return

        out_table = parsed.out_tables[0]
        query_fingerprint = parsed.query_fingerprint
        assert query_fingerprint is not None

        upstreams = parsed.in_tables
        column_lineage = parsed.column_lineage

        if (
            is_known_temp_table
            or (
                parsed.query_type.is_create()
                and parsed.query_type_props.get("temporary")
            )
            or (self.is_temp_table and self.is_temp_table(out_table))
            or not self._schema_resolver.has_urn(out_table)
        ):
            # handle temp table

            # TODO add to query_map

            self._temp_lineage_map.for_mutation(session_id, {})[
                out_table
            ] = query_fingerprint
            breakpoint()

        else:
            # Non-temp tables -> immediately generate lineage.

            if query_fingerprint in self._query_map:
                query_metadata = self._query_map.for_mutation(query_fingerprint)

                # This assumes that queries come in order of increasing timestamps,
                # so the current query is more authoritative than the previous one.
                query_metadata.session_id = session_id
                query_metadata.latest_timestamp = (
                    query_timestamp or query_metadata.latest_timestamp
                )
                query_metadata.actor = user or query_metadata.actor

                # An invariant of the fingerprinting is that if two queries have the
                # same fingerprint, they must also have the same lineage. We overwrite
                # here just in case more schemas got registered in the interim.
                query_metadata.upstreams = upstreams
                query_metadata.column_lineage = column_lineage
                query_metadata.confidence_score = parsed.debug_info.confidence
            else:
                self._query_map[query_fingerprint] = QueryMetadata(
                    query_id=query_fingerprint,
                    raw_query_string=query,
                    session_id=session_id,
                    type=models.DatasetLineageTypeClass.TRANSFORMED,
                    latest_timestamp=query_timestamp,
                    actor=user,
                    upstreams=upstreams,
                    column_lineage=column_lineage,
                    confidence_score=parsed.debug_info.confidence,
                )

            self._lineage_map.for_mutation(out_table, set()).add(query_fingerprint)

            # TODO: what happens if a CREATE VIEW query gets passed into this method

            pass

    def add_lineage(self) -> None:
        # A secondary mechanism for adding non-SQL-based lineage
        # e.g. redshift external tables might use this when pointing at s3
        # TODO
        pass

    def gen_metadata(self) -> Iterable[MetadataChangeProposalWrapper]:
        # diff from v1 - we generate operations here, and it also
        # generates MCPWs instead of workunits
        yield from self._gen_lineage_mcps()
        yield from self._gen_usage_statistics_mcps()
        yield from self._gen_operation_mcps()

    def _gen_lineage_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        # the parsing of view definitions is deferred until this method,
        # since all the view schemas will be registered at this point
        # the temp table resolution happens at generation time, not add query time

        # TODO process all views -> inject into the lineage map

        for downstream_urn in self._lineage_map:
            yield from self._gen_lineage_for_downstream(downstream_urn)

    @classmethod
    def _query_type_precedence(cls, query_type: models.DatasetLineageTypeClass) -> int:
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
        self, downstream_urn: str
    ) -> Iterable[MetadataChangeProposalWrapper]:
        query_ids = self._lineage_map[downstream_urn]
        queries: List[QueryMetadata] = [
            self._query_map[query_id] for query_id in query_ids
        ]

        # TODO for some of these queries, they need to be recursively resolved
        # if they have temp tables listed in their upstreams

        # TODO - should we sort by timestamp here? lineage_map using a set loses order info
        queries = sorted(
            queries,
            key=lambda query: (
                self._query_type_precedence(query.type),
                -(query.latest_timestamp or 0),
            ),
        )

        queries_map: Dict[QueryId, QueryMetadata] = {
            query.query_id: query for query in queries
        }

        # mapping of upstream urn -> query id that produced it
        upstreams: Dict[str, QueryId] = {}
        # mapping of downstream column -> { upstream column -> query id that produced it }
        cll: Dict[str, Dict[SchemaFieldUrn, QueryId]] = defaultdict(dict)
        # TODO replace column ref with schema field urn?

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
        required_queries = set()
        upstream_aspect = models.UpstreamLineageClass(
            upstreams=[],
            fineGrainedLineages=[],
        )
        for upstream_urn, query_id in upstreams.items():
            required_queries.add(query_id)
            upstream_aspect.upstreams.append(
                models.UpstreamClass(
                    dataset=upstream_urn,
                    type=queries_map[query_id].type,
                    # TODO audit stamp
                    # TODO query id
                )
            )
        for downstream_column, upstream_columns in cll.items():
            query_id = next(iter(upstream_columns.values()))
            upstream_aspect.fineGrainedLineages.append(
                models.FineGrainedLineageClass(
                    upstreamType=models.FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=[
                        upstream_column.urn() for upstream_column in upstream_columns
                    ],
                    downstreamType=models.FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=[
                        SchemaFieldUrn(downstream_urn, downstream_column).urn()
                    ],
                    # TODO query id
                    confidenceScore=queries_map[query_id].confidence_score,
                )
            )
            required_queries.update(upstream_columns.values())

        yield MetadataChangeProposalWrapper(
            entityUrn=downstream_urn,
            aspect=upstream_aspect,
        )

        # add the required_queries to a global required queries list?
        # or maybe just emit immediately?

    def _gen_usage_statistics_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        # TODO
        yield from []

    def _gen_operation_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        # TODO
        yield from []


# TODO: add a reporter type
# TODO: add a way to pass in fallback lineage info if query parsing fails
