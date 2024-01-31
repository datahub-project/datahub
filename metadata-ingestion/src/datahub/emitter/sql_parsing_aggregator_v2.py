import dataclasses
import enum
import logging
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Optional, Set

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.metadata.urns import CorpUserUrn, DataPlatformUrn, DatasetUrn
from datahub.utilities.file_backed_collections import FileBackedDict
from datahub.utilities.sqlglot_lineage import (
    ColumnLineageInfo,
    SchemaResolver,
    sqlglot_lineage,
)

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
    formatted_query_string: str

    session_id: Optional[str]
    type: str
    latest_timestamp: Optional[datetime]
    actor: Optional[CorpUserUrn]

    upstreams: List[str]  # this is direct upstreams, which may be temp tables
    column_lineage: Optional[
        List[ColumnLineageInfo]
    ]  # TODO add an internal representation?


class SqlParsingAggregator:

    def __init__(
        self,
        *,
        platform: DataPlatformUrn,
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
        self.platform = platform
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
                platform=self.platform.urn(),
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
            breakpoint()

            return

        out_table = parsed.out_tables[0]

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
            breakpoint()

        else:
            # Non-temp tables -> immediately generate lineage.

            query_fingerprint = TODO

            if query_fingerprint in self._query_map:
                pass
            else:
                self._query_map[query_fingerprint] = QueryMetadata(
                    query_id=query_fingerprint,
                    raw_query_string=query,
                    formatted_query_string=parsed.formatted_query,
                    session_id=session_id,
                    type=models.DatasetLineageTypeClass.TRANSFORMED,
                    latest_timestamp=query_timestamp,
                    actor=user,
                )

            # TODO: what happens if a CREATE VIEW query gets passed into this method

            pass

    def add_lineage(self) -> None:
        # A secondary mechanism for adding non-SQL-based lineage
        # e.g. redshift external tables might use this when pointing at s3
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
        pass


# TODO: add a reporter type
# TODO: add a way to pass in fallback lineage info if query parsing fails
