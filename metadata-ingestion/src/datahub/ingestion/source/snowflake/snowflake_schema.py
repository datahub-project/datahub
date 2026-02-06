import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    MutableMapping,
    Optional,
    Tuple,
)

from datahub.configuration.env_vars import get_snowflake_schema_parallelism
from datahub.ingestion.api.report import SupportsAsObj
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.snowflake.constants import (
    SemanticViewColumnSubtype,
    SnowflakeObjectDomain,
)
from datahub.ingestion.source.snowflake.snowflake_connection import SnowflakeConnection
from datahub.ingestion.source.snowflake.snowflake_query import (
    SHOW_COMMAND_MAX_PAGE_SIZE,
    SnowflakeQuery,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.sql.sql_generic import BaseColumn, BaseTable, BaseView
from datahub.ingestion.source.sql.stored_procedures.base import BaseProcedure
from datahub.utilities.file_backed_collections import FileBackedDict
from datahub.utilities.prefix_batch_builder import PrefixGroup, build_prefix_batches
from datahub.utilities.serialized_lru_cache import serialized_lru_cache

logger: logging.Logger = logging.getLogger(__name__)

SCHEMA_PARALLELISM = get_snowflake_schema_parallelism()


@dataclass
class SnowflakePK:
    name: str
    column_names: List[str]


@dataclass
class SnowflakeFK:
    name: str
    column_names: List[str]
    referred_database: str
    referred_schema: str
    referred_table: str
    referred_column_names: List[str]


@dataclass
class SnowflakeTableIdentifier:
    """Identifier for a Snowflake table, view, or semantic view."""

    database: str
    schema: str
    table: str

    def __str__(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"

    @classmethod
    def from_tuple(
        cls, table_tuple: Tuple[str, str, str]
    ) -> "SnowflakeTableIdentifier":
        """Create identifier from (database, schema, table) tuple."""
        return cls(database=table_tuple[0], schema=table_tuple[1], table=table_tuple[2])

    def as_tuple(self) -> Tuple[str, str, str]:
        """Convert to (database, schema, table) tuple for backward compatibility."""
        return (self.database, self.schema, self.table)


@dataclass
class SnowflakeTag:
    database: str
    schema: str
    name: str
    value: str

    def tag_display_name(self) -> str:
        return f"{self.name}: {self.value}"

    def tag_identifier(self) -> str:
        return f"{self._id_prefix_as_str()}:{self.value}"

    def _id_prefix_as_str(self) -> str:
        return f"{self.database}.{self.schema}.{self.name}"

    def structured_property_identifier(self) -> str:
        return f"snowflake.{self.database}.{self.schema}.{self.name}"


@dataclass
class SemanticViewColumnMetadata:
    """Metadata for a single occurrence of a semantic view column (dimension/fact/metric)."""

    name: str
    data_type: str
    comment: Optional[str]
    subtype: SemanticViewColumnSubtype
    table_name: Optional[str]
    synonyms: List[str]
    expression: Optional[str]


@dataclass
class SemanticViewColumnCollection:
    """Collection of column metadata for a semantic view, organized by column name."""

    # Maps uppercase column name to list of metadata occurrences
    columns: Dict[str, List[SemanticViewColumnMetadata]] = field(default_factory=dict)

    def add_column(self, metadata: SemanticViewColumnMetadata) -> None:
        """Add a column metadata occurrence."""
        col_name_upper = metadata.name.upper()
        if col_name_upper not in self.columns:
            self.columns[col_name_upper] = []
        self.columns[col_name_upper].append(metadata)

    def get_occurrences(self, col_name_upper: str) -> List[SemanticViewColumnMetadata]:
        """Get all occurrences of a column by its uppercase name."""
        return self.columns.get(col_name_upper, [])

    def has_duplicates(self, col_name_upper: str) -> bool:
        """Check if a column has multiple occurrences."""
        return len(self.columns.get(col_name_upper, [])) > 1

    def get_duplicate_count(self, col_name_upper: str) -> int:
        """Get the number of duplicate occurrences for a column."""
        occurrences = self.columns.get(col_name_upper, [])
        return max(0, len(occurrences) - 1)


@dataclass
class SnowflakeColumn(BaseColumn):
    character_maximum_length: Optional[int]
    numeric_precision: Optional[int]
    numeric_scale: Optional[int]
    expression: Optional[str] = None  # SQL expression for derived columns

    def get_precise_native_type(self):
        precise_native_type = self.data_type
        # https://docs.snowflake.com/en/sql-reference/data-types-numeric.html
        if (
            self.data_type in ("NUMBER", "NUMERIC", "DECIMAL")
            and self.numeric_precision is not None
            and self.numeric_scale is not None
        ):
            precise_native_type = (
                f"NUMBER({self.numeric_precision},{self.numeric_scale})"
            )
        # https://docs.snowflake.com/en/sql-reference/data-types-text.html
        elif (
            self.data_type in ("TEXT", "STRING", "VARCHAR")
            and self.character_maximum_length is not None
        ):
            precise_native_type = f"VARCHAR({self.character_maximum_length})"
        return precise_native_type


@dataclass
class SnowflakeTable(BaseTable):
    type: Optional[str] = None
    clustering_key: Optional[str] = None
    pk: Optional[SnowflakePK] = None
    columns: List[SnowflakeColumn] = field(default_factory=list)
    foreign_keys: List[SnowflakeFK] = field(default_factory=list)
    tags: Optional[List[SnowflakeTag]] = None
    column_tags: Dict[str, List[SnowflakeTag]] = field(default_factory=dict)
    is_dynamic: bool = False
    is_iceberg: bool = False
    is_hybrid: bool = False

    def get_subtype(self) -> DatasetSubTypes:
        return DatasetSubTypes.TABLE


@dataclass
class SnowflakeDynamicTable(SnowflakeTable):
    definition: Optional[str] = (
        None  # SQL query that defines the dynamic table's content
    )
    target_lag: Optional[str] = None  # Refresh frequency (e.g., "1 HOUR", "30 MINUTES")

    def get_subtype(self) -> DatasetSubTypes:
        return DatasetSubTypes.DYNAMIC_TABLE


@dataclass
class SnowflakeView(BaseView):
    materialized: bool = False
    columns: List[SnowflakeColumn] = field(default_factory=list)
    tags: Optional[List[SnowflakeTag]] = None
    column_tags: Dict[str, List[SnowflakeTag]] = field(default_factory=dict)
    is_secure: bool = False

    def get_subtype(self) -> DatasetSubTypes:
        return DatasetSubTypes.VIEW


@dataclass
class SnowflakeSemanticView(BaseView):
    """Represents a Snowflake Semantic View with dimensions, facts, and metrics."""

    columns: List[SnowflakeColumn] = field(default_factory=list)
    tags: Optional[List[SnowflakeTag]] = None
    column_tags: Dict[str, List[SnowflakeTag]] = field(default_factory=dict)
    # Base tables that this semantic view is built on (for lineage)
    base_tables: List[SnowflakeTableIdentifier] = field(default_factory=list)
    # Column subtypes mapping: column_name -> "DIMENSION" | "FACT" | "METRIC"
    column_subtypes: Dict[str, str] = field(default_factory=dict)
    # Column to logical table mappings: column_name -> [list of logical table names]
    # Used for column-level lineage generation
    column_table_mappings: Dict[str, List[str]] = field(default_factory=dict)
    # Column synonyms: column_name -> [list of alternative names]
    column_synonyms: Dict[str, List[str]] = field(default_factory=dict)
    # Primary key columns: Set of column names that are part of the primary key
    primary_key_columns: set = field(default_factory=set)
    # Table-level synonyms: logical_table_name -> [list of alternative names]
    # These are alternative names for logical tables within the semantic view
    table_synonyms: Dict[str, List[str]] = field(default_factory=dict)
    # Logical to physical table mapping: logical_table_name -> (db, schema, table)
    # Used for column-level lineage generation
    logical_to_physical_table: Dict[str, Tuple[str, str, str]] = field(
        default_factory=dict
    )
    # Pre-computed upstream dataset URNs for column lineage generation
    resolved_upstream_urns: List[str] = field(default_factory=list)

    def get_subtype(self) -> DatasetSubTypes:
        return DatasetSubTypes.SEMANTIC_VIEW


@dataclass
class UserQueryCount:
    """User query count for usage statistics."""

    user_name: str
    query_count: int


@dataclass
class SemanticViewUsageRecord:
    """Aggregated usage statistics for a semantic view within a time bucket."""

    semantic_view_name: str
    bucket_start_time: datetime
    total_queries: int
    unique_users: int
    direct_sql_queries: int
    cortex_analyst_queries: int
    avg_execution_time_ms: float
    total_rows_produced: int
    user_counts: List[UserQueryCount] = field(default_factory=list)
    top_sql_queries: List[str] = field(default_factory=list)


@dataclass
class SemanticViewQuery:
    """Individual query against a semantic view."""

    query_id: str
    query_text: str
    semantic_view_name: str
    user_name: str
    role_name: str
    warehouse_name: str
    start_time: datetime
    total_elapsed_time: int
    rows_produced: int
    query_source: str  # 'DIRECT_SQL' or 'CORTEX_ANALYST'


@dataclass
class SemanticViewProfileCounts:
    """Profile counts for a semantic view (dimensions, facts, metrics)."""

    semantic_view_catalog: str
    semantic_view_schema: str
    semantic_view_name: str
    dimension_count: int
    fact_count: int
    metric_count: int
    table_count: int
    total_column_count: int


@dataclass
class SnowflakeSchema:
    name: str
    created: Optional[datetime]
    last_altered: Optional[datetime]
    comment: Optional[str]
    tables: List[str] = field(default_factory=list)
    views: List[str] = field(default_factory=list)
    semantic_views: List[str] = field(default_factory=list)
    streams: List[str] = field(default_factory=list)
    tags: Optional[List[SnowflakeTag]] = None


@dataclass
class SnowflakeDatabase:
    name: str
    created: Optional[datetime]
    comment: Optional[str]
    last_altered: Optional[datetime] = None
    schemas: List[SnowflakeSchema] = field(default_factory=list)
    tags: Optional[List[SnowflakeTag]] = None


@dataclass
class SnowflakeStream:
    name: str
    created: datetime
    owner: str
    source_type: str
    type: str
    stale: str
    mode: str
    invalid_reason: str
    owner_role_type: str
    database_name: str
    schema_name: str
    table_name: str
    comment: Optional[str]
    columns: List[SnowflakeColumn] = field(default_factory=list)
    stale_after: Optional[datetime] = None
    base_tables: Optional[str] = None
    tags: Optional[List[SnowflakeTag]] = None
    column_tags: Dict[str, List[SnowflakeTag]] = field(default_factory=dict)
    last_altered: Optional[datetime] = None

    def get_subtype(self) -> DatasetSubTypes:
        return DatasetSubTypes.SNOWFLAKE_STREAM


@dataclass
class SnowflakeStreamlitApp:
    """
    Represents a Snowflake Streamlit application.
    Streamlit apps in Snowflake are interactive data applications built with Python
    that are deployed and hosted within Snowflake. These apps are ingested as
    dashboard entities in DataHub to provide visibility into data applications
    alongside traditional dashboards and reports.
    Attributes:
        name: The Snowflake object name of the Streamlit app
        created: Timestamp when the app was created
        owner: The owner role of the app
        database_name: The database containing the app
        schema_name: The schema containing the app
        title: Human-readable title of the app (displayed in Snowflake UI)
        comment: Optional description/comment for the app
        url_id: Unique identifier used in the Snowflake app URL
        owner_role_type: Type of the owner (typically "ROLE")
    """

    name: str
    created: datetime
    owner: str
    database_name: str
    schema_name: str
    title: str
    comment: Optional[str]
    url_id: str
    owner_role_type: str


class _SnowflakeTagCache:
    def __init__(self) -> None:
        # self._database_tags[<database_name>] = list of tags applied to database
        self._database_tags: Dict[str, List[SnowflakeTag]] = defaultdict(list)

        # self._schema_tags[<database_name>][<schema_name>] = list of tags applied to schema
        self._schema_tags: Dict[str, Dict[str, List[SnowflakeTag]]] = defaultdict(
            lambda: defaultdict(list)
        )

        # self._table_tags[<database_name>][<schema_name>][<table_name>] = list of tags applied to table
        self._table_tags: Dict[str, Dict[str, Dict[str, List[SnowflakeTag]]]] = (
            defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        )

        # self._column_tags[<database_name>][<schema_name>][<table_name>][<column_name>] = list of tags applied to column
        self._column_tags: Dict[
            str, Dict[str, Dict[str, Dict[str, List[SnowflakeTag]]]]
        ] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        )

    def add_database_tag(self, db_name: str, tag: SnowflakeTag) -> None:
        self._database_tags[db_name].append(tag)

    def get_database_tags(self, db_name: str) -> List[SnowflakeTag]:
        return self._database_tags[db_name]

    def add_schema_tag(self, schema_name: str, db_name: str, tag: SnowflakeTag) -> None:
        self._schema_tags[db_name][schema_name].append(tag)

    def get_schema_tags(self, schema_name: str, db_name: str) -> List[SnowflakeTag]:
        return self._schema_tags.get(db_name, {}).get(schema_name, [])

    def add_table_tag(
        self, table_name: str, schema_name: str, db_name: str, tag: SnowflakeTag
    ) -> None:
        self._table_tags[db_name][schema_name][table_name].append(tag)

    def get_table_tags(
        self, table_name: str, schema_name: str, db_name: str
    ) -> List[SnowflakeTag]:
        return self._table_tags[db_name][schema_name][table_name]

    def add_column_tag(
        self,
        column_name: str,
        table_name: str,
        schema_name: str,
        db_name: str,
        tag: SnowflakeTag,
    ) -> None:
        self._column_tags[db_name][schema_name][table_name][column_name].append(tag)

    def get_column_tags_for_table(
        self, table_name: str, schema_name: str, db_name: str
    ) -> Dict[str, List[SnowflakeTag]]:
        return (
            self._column_tags.get(db_name, {}).get(schema_name, {}).get(table_name, {})
        )


class SnowflakeDataDictionary(SupportsAsObj):
    def __init__(
        self,
        connection: SnowflakeConnection,
        report: SnowflakeV2Report,
        fetch_views_from_information_schema: bool = False,
    ) -> None:
        self.connection = connection
        self.report = report
        self._fetch_views_from_information_schema = fetch_views_from_information_schema

    def as_obj(self) -> Dict[str, Any]:
        # TODO: Move this into a proper report type that gets computed.

        # Reports how many times we reset in-memory `functools.lru_cache` caches of data,
        # which occurs when we occur a different database / schema.
        # Should not be more than the number of databases / schemas scanned.
        # Maps (function name) -> (stat_name) -> (stat_value)
        lru_cache_functions: List[Callable] = [
            self.get_tables_for_database,
            self.get_views_for_database,
            self.get_semantic_views_for_database,
            self.get_columns_for_schema,
            self.get_streams_for_database,
            self.get_pk_constraints_for_schema,
            self.get_fk_constraints_for_schema,
        ]

        report: Dict[str, Any] = {
            "fetch_views_from_information_schema": self._fetch_views_from_information_schema,
        }
        for func in lru_cache_functions:
            report[func.__name__] = func.cache_info()._asdict()  # type: ignore
        return report

    def show_databases(self) -> List[SnowflakeDatabase]:
        databases: List[SnowflakeDatabase] = []

        cur = self.connection.query(
            SnowflakeQuery.show_databases(),
        )

        for database in cur:
            snowflake_db = SnowflakeDatabase(
                name=database["name"],
                created=database["created_on"],
                comment=database["comment"],
            )
            databases.append(snowflake_db)

        return databases

    def get_databases(self, db_name: str) -> List[SnowflakeDatabase]:
        databases: List[SnowflakeDatabase] = []

        cur = self.connection.query(
            SnowflakeQuery.get_databases(db_name),
        )

        for database in cur:
            snowflake_db = SnowflakeDatabase(
                name=database["DATABASE_NAME"],
                created=database["CREATED"],
                last_altered=database["LAST_ALTERED"],
                comment=database["COMMENT"],
            )
            databases.append(snowflake_db)

        return databases

    def get_schemas_for_database(self, db_name: str) -> List[SnowflakeSchema]:
        snowflake_schemas = []

        cur = self.connection.query(
            SnowflakeQuery.schemas_for_database(db_name),
        )

        for schema in cur:
            snowflake_schema = SnowflakeSchema(
                name=schema["SCHEMA_NAME"],
                created=schema["CREATED"],
                last_altered=schema["LAST_ALTERED"],
                comment=schema["COMMENT"],
            )
            snowflake_schemas.append(snowflake_schema)
        return snowflake_schemas

    @serialized_lru_cache(maxsize=1)
    def get_secure_view_definitions(self) -> Dict[str, Dict[str, Dict[str, str]]]:
        secure_view_definitions: Dict[str, Dict[str, Dict[str, str]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict())
        )
        cur = self.connection.query(SnowflakeQuery.get_secure_view_definitions())
        for view in cur:
            db_name = view["TABLE_CATALOG"]
            schema_name = view["TABLE_SCHEMA"]
            view_name = view["TABLE_NAME"]
            secure_view_definitions[db_name][schema_name][view_name] = view[
                "VIEW_DEFINITION"
            ]

        return secure_view_definitions

    def get_all_tags(self) -> List[SnowflakeTag]:
        cur = self.connection.query(
            SnowflakeQuery.get_all_tags(),
        )

        tags = [
            SnowflakeTag(
                database=tag["TAG_DATABASE"],
                schema=tag["TAG_SCHEMA"],
                name=tag["TAG_NAME"],
                value="",
            )
            for tag in cur
        ]

        return tags

    @serialized_lru_cache(maxsize=1)
    def get_tables_for_database(
        self, db_name: str
    ) -> Optional[Dict[str, List[SnowflakeTable]]]:
        tables: Dict[str, List[SnowflakeTable]] = {}
        try:
            cur = self.connection.query(
                SnowflakeQuery.tables_for_database(db_name),
            )
        except Exception as e:
            logger.debug(
                f"Failed to get all tables for database - {db_name}", exc_info=e
            )
            # Error - Information schema query returned too much data. Please repeat query with more selective predicates.
            return None

        for table in cur:
            if table["TABLE_SCHEMA"] not in tables:
                tables[table["TABLE_SCHEMA"]] = []

            is_dynamic = table.get("IS_DYNAMIC", "NO").upper() == "YES"
            table_cls = SnowflakeDynamicTable if is_dynamic else SnowflakeTable

            tables[table["TABLE_SCHEMA"]].append(
                table_cls(
                    name=table["TABLE_NAME"],
                    type=table["TABLE_TYPE"],
                    created=table["CREATED"],
                    last_altered=table["LAST_ALTERED"],
                    size_in_bytes=table["BYTES"],
                    rows_count=table["ROW_COUNT"],
                    comment=table["COMMENT"],
                    clustering_key=table["CLUSTERING_KEY"],
                    is_dynamic=is_dynamic,
                    is_iceberg=table.get("IS_ICEBERG", "NO").upper() == "YES",
                    is_hybrid=table.get("IS_HYBRID", "NO").upper() == "YES",
                )
            )

        # Populate dynamic table definitions
        self.populate_dynamic_table_definitions(tables, db_name)

        return tables

    def get_tables_for_schema(
        self, schema_name: str, db_name: str
    ) -> List[SnowflakeTable]:
        tables: List[SnowflakeTable] = []

        cur = self.connection.query(
            SnowflakeQuery.tables_for_schema(schema_name, db_name),
        )

        for table in cur:
            is_dynamic = table.get("IS_DYNAMIC", "NO").upper() == "YES"
            table_cls = SnowflakeDynamicTable if is_dynamic else SnowflakeTable

            tables.append(
                table_cls(
                    name=table["TABLE_NAME"],
                    type=table["TABLE_TYPE"],
                    created=table["CREATED"],
                    last_altered=table["LAST_ALTERED"],
                    size_in_bytes=table["BYTES"],
                    rows_count=table["ROW_COUNT"],
                    comment=table["COMMENT"],
                    clustering_key=table["CLUSTERING_KEY"],
                    is_dynamic=is_dynamic,
                    is_iceberg=table.get("IS_ICEBERG", "NO").upper() == "YES",
                    is_hybrid=table.get("IS_HYBRID", "NO").upper() == "YES",
                )
            )

        # Populate dynamic table definitions for just this schema
        schema_tables = {schema_name: tables}
        self.populate_dynamic_table_definitions(schema_tables, db_name)

        return tables

    @serialized_lru_cache(maxsize=1)
    def get_views_for_database(
        self, db_name: str
    ) -> Optional[Dict[str, List[SnowflakeView]]]:
        if self._fetch_views_from_information_schema:
            return self._get_views_for_database_using_information_schema(db_name)
        else:
            return self._get_views_for_database_using_show(db_name)

    def _get_views_for_database_using_show(
        self, db_name: str
    ) -> Dict[str, List[SnowflakeView]]:
        page_limit = SHOW_COMMAND_MAX_PAGE_SIZE

        views: Dict[str, List[SnowflakeView]] = {}

        first_iteration = True
        view_pagination_marker: Optional[str] = None
        while first_iteration or view_pagination_marker is not None:
            cur = self.connection.query(
                SnowflakeQuery.show_views_for_database(
                    db_name,
                    limit=page_limit,
                    view_pagination_marker=view_pagination_marker,
                )
            )

            first_iteration = False
            view_pagination_marker = None

            result_set_size = 0
            for view in cur:
                result_set_size += 1

                view_name = view["name"]
                schema_name = view["schema_name"]
                if schema_name not in views:
                    views[schema_name] = []
                views[schema_name].append(
                    SnowflakeView(
                        name=view_name,
                        created=view["created_on"],
                        comment=view["comment"],
                        view_definition=view["text"],
                        last_altered=view["created_on"],  # TODO: This is not correct.
                        materialized=(
                            view.get("is_materialized", "false").lower() == "true"
                        ),
                        is_secure=(view.get("is_secure", "false").lower() == "true"),
                    )
                )

            if result_set_size >= page_limit:
                logger.info(
                    f"Fetching next page of views for {db_name} - after {view_name}"
                )
                view_pagination_marker = view_name

        # Because this is in a cached function, this will only log once per database.
        view_counts = {schema_name: len(views[schema_name]) for schema_name in views}
        logger.info(
            f"Finished fetching views in {db_name}; counts by schema {view_counts}"
        )
        return views

    def _map_view(self, db_name: str, row: Dict[str, Any]) -> Tuple[str, SnowflakeView]:
        schema_name = row["VIEW_SCHEMA"]
        view_definition = row.get("VIEW_DEFINITION")
        fragment_view_definition = (
            view_definition[:50].strip() if view_definition else None
        )
        logger.info(
            f"Mapping view {db_name}.{schema_name}.{row['VIEW_NAME']} with view definition: {fragment_view_definition}..."
        )

        return schema_name, SnowflakeView(
            name=row["VIEW_NAME"],
            created=row["CREATED"],
            comment=row["COMMENT"],
            view_definition=view_definition,
            last_altered=row["LAST_ALTERED"],
            is_secure=(row.get("IS_SECURE", "false").lower() == "true"),
            # TODO: This doesn't work for materialized views.
            materialized=False,
        )

    def _maybe_populate_empty_view_definitions(
        self,
        db_name: str,
        schema_name: str,
        views_with_empty_definition: List[SnowflakeView],
    ) -> List[SnowflakeView]:
        if not views_with_empty_definition:
            return []

        view_names = [view.name for view in views_with_empty_definition]
        batches = [
            batch[0]
            for batch in build_prefix_batches(
                view_names, max_batch_size=1000, max_groups_in_batch=1
            )
            if batch
            # Skip empty batch if so, also max_groups_in_batch=1 makes it safe to access batch[0]
        ]

        view_map: Dict[str, SnowflakeView] = {
            view.name: view for view in views_with_empty_definition
        }
        views_found_count = 0

        logger.info(
            f"Fetching definitions for {len(view_map)} views in {db_name}.{schema_name} "
            f"using batched 'SHOW VIEWS ... LIKE ...' queries. Found {len(batches)} batch(es)."
        )

        for batch_index, prefix_group in enumerate(batches):
            query = f'SHOW VIEWS LIKE \'{prefix_group.prefix}%\' IN SCHEMA "{db_name}"."{schema_name}"'
            logger.info(f"Processing batch {batch_index + 1}/{len(batches)}: {query}")

            try:
                cur = self.connection.query(query)
                for row in cur:
                    view_name = row["name"]
                    if view_name in view_map:
                        view_definition = row.get("text")
                        if view_definition:  # Ensure definition is not None or empty
                            view_map[view_name].view_definition = view_definition
                            views_found_count += 1
                            logger.debug(
                                f"Fetched view definition for {db_name}.{schema_name}.{view_name}"
                            )
                            # If all targeted views are found, we could theoretically break early,
                            # but SHOW VIEWS doesn't guarantee order, so we must process all results.
                        else:
                            logger.warning(
                                f"'text' field missing or empty in SHOW VIEWS result for {db_name}.{schema_name}.{view_name}"
                            )

            except Exception as e:
                logger.error(
                    f"Failed to execute query for batch {batch_index + 1} ('{query}') for {db_name}.{schema_name} or process its results.",
                    exc_info=e,
                )
                # Returning the original list; some views might still be missing definitions.
                # This also means subsequent batches for this schema (in this call) are skipped.
                return views_with_empty_definition

        logger.info(
            f"Finished processing 'SHOW VIEWS' batches for {db_name}.{schema_name}. "
            f"Fetched definitions for {views_found_count} out of {len(view_map)} targeted views."
        )

        if views_found_count < len(view_map):
            missing_count = len(view_map) - views_found_count
            logger.warning(
                f"Could not fetch definitions for {missing_count} views in {db_name}.{schema_name} after processing all batches."
            )
        # The SnowflakeView objects in the original list were modified in place via view_map
        return views_with_empty_definition

    def _get_views_for_database_using_information_schema(
        self, db_name: str
    ) -> Optional[Dict[str, List[SnowflakeView]]]:
        try:
            cur = self.connection.query(
                SnowflakeQuery.get_views_for_database(db_name),
            )
        except Exception as e:
            logger.debug(f"Failed to get all views for database {db_name}", exc_info=e)
            # Error - Information schema query returned too much data. Please repeat query with more selective predicates.
            return None

        views: Dict[str, List[SnowflakeView]] = {}
        views_with_empty_definition: Dict[str, List[SnowflakeView]] = {}

        for row in cur:
            schema_name, view = self._map_view(db_name, row)
            if view.view_definition is None or view.view_definition == "":
                views_with_empty_definition.setdefault(schema_name, []).append(view)
            else:
                views.setdefault(schema_name, []).append(view)

        for schema_name, empty_views in views_with_empty_definition.items():
            updated_views = self._maybe_populate_empty_view_definitions(
                db_name, schema_name, empty_views
            )
            views.setdefault(schema_name, []).extend(updated_views)

        return views

    def get_views_for_schema_using_information_schema(
        self, *, schema_name: str, db_name: str
    ) -> List[SnowflakeView]:
        cur = self.connection.query(
            SnowflakeQuery.get_views_for_schema(
                db_name=db_name, schema_name=schema_name
            ),
        )

        views: List[SnowflakeView] = []
        views_with_empty_definition: List[SnowflakeView] = []

        for row in cur:
            schema_name, view = self._map_view(db_name, row)
            if view.view_definition is None or view.view_definition == "":
                views_with_empty_definition.append(view)
            else:
                views.append(view)

        if views_with_empty_definition:
            updated_empty_views = self._maybe_populate_empty_view_definitions(
                db_name, schema_name, views_with_empty_definition
            )
            views.extend(updated_empty_views)

        return views

    @serialized_lru_cache(maxsize=1)
    def get_semantic_views_for_database(
        self, db_name: str
    ) -> Optional[Dict[str, List[SnowflakeSemanticView]]]:
        """
        Fetch semantic views for a database using INFORMATION_SCHEMA.SEMANTIC_VIEWS.

        Uses the dedicated information_schema.semantic_views view which provides
        semantic view metadata. At realistic scale (dozens to hundreds of semantic views),
        this approach is reliable and simple.
        """
        try:
            cur = self.connection.query(
                SnowflakeQuery.get_semantic_views_for_database(db_name),
            )
        except Exception as e:
            logger.warning(
                f"Failed to get semantic views for database {db_name}: {e}",
                exc_info=True,
            )
            return None

        semantic_views: Dict[str, List[SnowflakeSemanticView]] = {}

        for row in cur:
            schema_name = row["SEMANTIC_VIEW_SCHEMA"]
            semantic_view = SnowflakeSemanticView(
                name=row["SEMANTIC_VIEW_NAME"],
                created=row["CREATED"],
                comment=row.get("COMMENT"),
                view_definition=None,
                last_altered=row["CREATED"],
            )
            semantic_views.setdefault(schema_name, []).append(semantic_view)

        # Log counts per schema (cached function, logs once per database)
        semantic_view_counts = {
            schema_name: len(semantic_views[schema_name])
            for schema_name in semantic_views
        }
        logger.info(
            f"Finished fetching semantic views in {db_name}; counts by schema {semantic_view_counts}"
        )

        self._populate_semantic_view_definitions(db_name, semantic_views)
        self._populate_semantic_view_base_tables(db_name, semantic_views)
        self._populate_semantic_view_columns(db_name, semantic_views)

        return semantic_views

    def _populate_semantic_view_definitions(
        self, db_name: str, semantic_views: Dict[str, List[SnowflakeSemanticView]]
    ) -> None:
        """Fetch and populate semantic view definitions using GET_DDL."""
        for schema_name, views in semantic_views.items():
            for semantic_view in views:
                try:
                    query = SnowflakeQuery.get_semantic_view_ddl(
                        db_name, schema_name, semantic_view.name
                    )
                    logger.debug(
                        f"Fetching DDL for semantic view {db_name}.{schema_name}.{semantic_view.name}"
                    )

                    cur = self.connection.query(query)
                    row = cur.fetchone()
                    if row:
                        ddl = row.get("DDL") or row.get("ddl")
                        if ddl:
                            semantic_view.view_definition = ddl
                        else:
                            logger.warning(
                                f"GET_DDL returned empty definition for semantic view {db_name}.{schema_name}.{semantic_view.name}"
                            )
                    else:
                        logger.warning(
                            f"GET_DDL returned no rows for semantic view {db_name}.{schema_name}.{semantic_view.name}"
                        )
                except Exception as e:
                    logger.debug(
                        f"Failed to fetch DDL for semantic view {db_name}.{schema_name}.{semantic_view.name}: {e}"
                    )
                    # Continue without definition - it's not critical

    def _parse_json_array(
        self, value: Optional[str], field_name: str, context: str
    ) -> List[str]:
        """
        Safely parse JSON array from Snowflake system views.

        Args:
            value: JSON string to parse (may be None)
            field_name: Name of the field being parsed (for logging)
            context: Context string for logging (e.g., "table_name in view_name")

        Returns:
            List of strings from JSON array, or empty list if parsing fails
        """
        if not value:
            return []
        try:
            parsed = json.loads(value) if isinstance(value, str) else value
            if isinstance(parsed, list):
                # Only accept simple types (str, int, float, bool)
                # Skip complex types (dict, list) to avoid useless stringified objects
                result = []
                for item in parsed:
                    if isinstance(item, (str, int, float, bool)):
                        result.append(str(item))
                    else:
                        logger.debug(
                            f"Skipping complex item in {field_name} ({context}): {type(item).__name__}"
                        )
                return result
            logger.warning(
                f"Expected list for {field_name} in {context}, got {type(parsed).__name__}: {parsed}"
            )
            return []
        except (json.JSONDecodeError, TypeError) as e:
            # Truncate large values to avoid logging sensitive/large data
            truncated_value = (
                f"{str(value)[:100]}..." if value and len(str(value)) > 100 else value
            )
            logger.warning(
                f"Failed to parse {field_name} as JSON in {context}. "
                f"Value: {truncated_value!r}. Error: {e}"
            )
            return []

    def _get_data_type_with_default(
        self, row: Dict, subtype: str, col_name: str, default: str
    ) -> str:
        """Get data type from row, logging debug message if defaulting."""
        data_type = row.get("DATA_TYPE")
        if not data_type:
            logger.debug(
                f"No DATA_TYPE for {subtype} column '{col_name}', defaulting to {default}"
            )
            return default
        return data_type

    def _populate_semantic_view_base_tables(
        self, db_name: str, semantic_views: Dict[str, List[SnowflakeSemanticView]]
    ) -> None:
        """Fetch and populate base table lineage for semantic views using INFORMATION_SCHEMA.SEMANTIC_TABLES."""
        try:
            query = SnowflakeQuery.get_semantic_tables_for_database(db_name)
            logger.debug(f"Fetching semantic tables for database {db_name}")

            # Build a map of semantic_view_name -> semantic_view object for quick lookup
            semantic_view_map: Dict[Tuple[str, str], SnowflakeSemanticView] = {}
            for schema_name, views in semantic_views.items():
                for semantic_view in views:
                    semantic_view_map[(schema_name, semantic_view.name)] = semantic_view

            cur = self.connection.query(query)
            row_count = 0
            for row in cur:
                row_count += 1
                schema_name = row["SEMANTIC_VIEW_SCHEMA"]
                view_name = row["SEMANTIC_VIEW_NAME"]
                logical_table_name = row["SEMANTIC_TABLE_NAME"]
                base_db = row["BASE_TABLE_CATALOG"]
                base_schema = row["BASE_TABLE_SCHEMA"]
                base_table = row["BASE_TABLE_NAME"]

                # Find the corresponding semantic view and add the base table
                semantic_view_obj = semantic_view_map.get((schema_name, view_name))
                if not semantic_view_obj:
                    continue

                base_table_id = SnowflakeTableIdentifier(
                    database=base_db, schema=base_schema, table=base_table
                )
                if base_table_id not in semantic_view_obj.base_tables:
                    semantic_view_obj.base_tables.append(base_table_id)

                # Store the logical-to-physical mapping for lineage generation
                logical_table_upper = logical_table_name.upper()
                semantic_view_obj.logical_to_physical_table[logical_table_upper] = (
                    base_table_id.as_tuple()
                )

                primary_keys_raw = row.get("PRIMARY_KEYS")
                primary_keys = self._parse_json_array(
                    primary_keys_raw, "PRIMARY_KEYS", f"{schema_name}.{view_name}"
                )
                if primary_keys:
                    for pk_col in primary_keys:
                        semantic_view_obj.primary_key_columns.add(pk_col.upper())

                synonyms_raw = row.get("SYNONYMS")
                synonyms = self._parse_json_array(
                    synonyms_raw,
                    "SYNONYMS",
                    f"{logical_table_name} in {schema_name}.{view_name}",
                )
                if synonyms and logical_table_name:
                    logical_table_upper = logical_table_name.upper()
                    if logical_table_upper not in semantic_view_obj.table_synonyms:
                        semantic_view_obj.table_synonyms[logical_table_upper] = []
                    semantic_view_obj.table_synonyms[logical_table_upper].extend(
                        synonyms
                    )

            logger.info(
                f"Populated base tables for {len(semantic_view_map)} semantic views "
                f"in database {db_name} ({row_count} table mappings)"
            )
        except Exception as e:
            logger.warning(
                f"Failed to fetch semantic tables for database {db_name}: {e}"
            )

    def _fetch_semantic_columns(
        self,
        db_name: str,
        column_data: Dict[Tuple[str, str], SemanticViewColumnCollection],
        column_type: SemanticViewColumnSubtype,
        query_func: Callable[[str], str],
        default_data_type: str,
    ) -> int:
        """Fetch columns of a specific type for semantic views in a database."""
        rows = self.connection.query(query_func(db_name))
        count = 0
        type_label = column_type.value.lower()

        for row in rows:
            count += 1
            view_key = (row["SEMANTIC_VIEW_SCHEMA"], row["SEMANTIC_VIEW_NAME"])
            if view_key not in column_data:
                column_data[view_key] = SemanticViewColumnCollection()

            col_name = row["NAME"]
            synonyms = self._parse_json_array(
                row.get("SYNONYMS"), "SYNONYMS", f"{type_label} {col_name}"
            )

            metadata = SemanticViewColumnMetadata(
                name=col_name,
                data_type=self._get_data_type_with_default(
                    row, type_label.upper(), col_name, default_data_type
                ),
                comment=row.get("COMMENT"),
                subtype=column_type,
                table_name=row.get("TABLE_NAME"),
                synonyms=synonyms,
                expression=row.get("EXPRESSION"),
            )
            column_data[view_key].add_column(metadata)

        logger.debug(f"Fetched {count} {type_label}s for database {db_name}")
        return count

    def _deduplicate_synonyms(
        self, occurrences: List[SemanticViewColumnMetadata]
    ) -> List[str]:
        """
        Deduplicate and merge synonyms from multiple column occurrences.

        Returns a sorted list for deterministic output (order doesn't matter semantically).
        """
        # Collect all synonyms and deduplicate case-insensitively
        seen_upper = set()
        unique_synonyms = []
        for occ in occurrences:
            if occ.synonyms:
                for syn in occ.synonyms:
                    syn_upper = syn.upper()
                    if syn_upper not in seen_upper:
                        seen_upper.add(syn_upper)
                        unique_synonyms.append(syn)

        # Sort for deterministic output (synonyms have no inherent order)
        return sorted(unique_synonyms, key=str.lower)

    def _process_column_occurrences(
        self,
        semantic_view: SnowflakeSemanticView,
        col_name_upper: str,
        occurrences: List[SemanticViewColumnMetadata],
        view_name: str,
        ordinal: int,
    ) -> None:
        """Process and deduplicate column occurrences for a semantic view."""
        col_name = occurrences[0].name

        # Merge metadata from all occurrences
        data_type, merged_comment, merged_subtype = self._merge_column_metadata(
            occurrences, col_name, view_name
        )

        # Create deduplicated column
        semantic_view.columns.append(
            SnowflakeColumn(
                name=col_name,
                ordinal_position=ordinal,
                data_type=data_type,
                is_nullable=True,
                comment=merged_comment,
                character_maximum_length=None,
                numeric_precision=None,
                numeric_scale=None,
                expression=occurrences[0].expression,
            )
        )
        semantic_view.column_subtypes[col_name_upper] = merged_subtype

        # Store table mappings for column-level lineage
        table_names: List[str] = [
            occ.table_name for occ in occurrences if occ.table_name
        ]
        if table_names:
            semantic_view.column_table_mappings[col_name_upper] = table_names

        # Store merged synonyms
        unique_synonyms = self._deduplicate_synonyms(occurrences)
        if unique_synonyms:
            semantic_view.column_synonyms[col_name_upper] = unique_synonyms

    def _merge_column_metadata(
        self,
        occurrences: List[SemanticViewColumnMetadata],
        col_name: str,
        view_name: str,
    ) -> Tuple[str, Optional[str], str]:
        """
        Merge metadata from multiple occurrences of the same column.

        Returns: (data_type, merged_comment, merged_subtype)
        """
        # Resolve type conflicts
        types = [occ.data_type for occ in occurrences]
        unique_types = set(types)
        if len(unique_types) > 1:
            logger.warning(
                f"Column '{col_name}' in {view_name} has conflicting types: "
                f"{unique_types}. Using '{types[0]}'."
            )
        data_type = types[0]

        # Merge descriptions
        comments = [occ.comment for occ in occurrences if occ.comment]
        unique_comments = set(comments)

        if len(unique_comments) == 0:
            merged_comment = None
        elif len(unique_comments) == 1:
            merged_comment = list(unique_comments)[0]
            if len(occurrences) > 1:
                # Add context about multiple occurrences
                merged_comment = f"{merged_comment}\n\n(Appears in {len(occurrences)} logical table(s))"
        else:
            # Different descriptions - show all with inline expressions
            comment_parts = []
            for occ in occurrences:
                comment = occ.comment or "(no description)"

                # For FACT and METRIC, include expression inline if available
                if (
                    occ.subtype
                    in (
                        SemanticViewColumnSubtype.FACT,
                        SemanticViewColumnSubtype.METRIC,
                    )
                    and occ.expression
                ):
                    comment_parts.append(
                        f"• {occ.subtype}: {comment} [Expression: {occ.expression}]"
                    )
                else:
                    comment_parts.append(f"• {occ.subtype}: {comment}")
            merged_comment = "\n".join(comment_parts)

        # Collect all subtypes (comma-separated for display)
        subtypes = [occ.subtype for occ in occurrences]
        unique_subtypes = sorted(set(subtypes))  # Deduplicate and sort
        merged_subtype = ",".join(unique_subtypes)

        # For single occurrence FACT/METRIC columns, append expression if available
        # (This handles the case where there's only one description)
        if len(unique_comments) <= 1 and any(
            subtype
            in (SemanticViewColumnSubtype.FACT, SemanticViewColumnSubtype.METRIC)
            for subtype in unique_subtypes
        ):
            expression = occurrences[0].expression
            if expression:
                if merged_comment:
                    merged_comment = f"{merged_comment}\n\n[Expression: {expression}]"
                else:
                    merged_comment = f"[Expression: {expression}]"

        return (data_type, merged_comment, merged_subtype)

    def _populate_semantic_view_columns(
        self, db_name: str, semantic_views: Dict[str, List[SnowflakeSemanticView]]
    ) -> None:
        """
        Fetch and populate columns (dimensions, facts, metrics) for semantic views
        using INFORMATION_SCHEMA system views.
        """
        try:
            # Build a map of (schema_name, semantic_view_name) -> semantic_view object for quick lookup
            semantic_view_map: Dict[Tuple[str, str], SnowflakeSemanticView] = {}
            for schema_name, views in semantic_views.items():
                for semantic_view in views:
                    semantic_view_map[(schema_name, semantic_view.name)] = semantic_view
                    semantic_view.columns = []
                    semantic_view.column_subtypes = {}

            logger.debug(f"Fetching semantic view columns for database {db_name}")

            # Step 1: Collect all column metadata with duplicates
            column_data: Dict[Tuple[str, str], SemanticViewColumnCollection] = {}

            dim_count = self._fetch_semantic_columns(
                db_name,
                column_data,
                SemanticViewColumnSubtype.DIMENSION,
                SnowflakeQuery.get_semantic_dimensions_for_database,
                "VARCHAR",
            )
            fact_count = self._fetch_semantic_columns(
                db_name,
                column_data,
                SemanticViewColumnSubtype.FACT,
                SnowflakeQuery.get_semantic_facts_for_database,
                "NUMBER",
            )
            metric_count = self._fetch_semantic_columns(
                db_name,
                column_data,
                SemanticViewColumnSubtype.METRIC,
                SnowflakeQuery.get_semantic_metrics_for_database,
                "NUMBER",
            )

            # Step 2: Deduplicate and merge metadata for each semantic view
            for view_key, column_collection in column_data.items():
                semantic_view = semantic_view_map.get(view_key)  # type: ignore[assignment]
                if not semantic_view:
                    # Column data exists but semantic view not found - possible data inconsistency
                    schema_name, view_name = view_key
                    column_count = len(column_collection.columns)
                    logger.warning(
                        f"Found {column_count} columns for semantic view {schema_name}.{view_name} "
                        f"but the view was not found in INFORMATION_SCHEMA.SEMANTIC_VIEWS. "
                        f"This may indicate a data inconsistency or permission issue."
                    )
                    continue

                ordinal = 1
                for col_name_upper, occurrences in column_collection.columns.items():
                    self._process_column_occurrences(
                        semantic_view, col_name_upper, occurrences, view_key[1], ordinal
                    )
                    ordinal += 1

            logger.info(
                f"Populated columns for semantic views in database {db_name}. "
                f"Total: {dim_count} dimensions + {fact_count} facts + {metric_count} metrics"
            )

        except Exception as e:
            logger.warning(
                f"Failed to fetch semantic view columns for database {db_name}: {e}"
            )

    def get_semantic_views_for_schema_using_information_schema(
        self, *, schema_name: str, db_name: str
    ) -> List[SnowflakeSemanticView]:
        cur = self.connection.query(
            SnowflakeQuery.get_semantic_views_for_schema(
                db_name=db_name, schema_name=schema_name
            ),
        )

        semantic_views: List[SnowflakeSemanticView] = []

        for row in cur:
            semantic_view = SnowflakeSemanticView(
                name=row["SEMANTIC_VIEW_NAME"],
                created=row["CREATED"],
                comment=row.get("COMMENT"),
                view_definition=None,
                last_altered=row["LAST_ALTERED"],
            )
            semantic_views.append(semantic_view)

        return semantic_views

    @serialized_lru_cache(maxsize=SCHEMA_PARALLELISM)
    def get_columns_for_schema(
        self,
        schema_name: str,
        db_name: str,
        # HACK: This key is excluded from the cache key.
        cache_exclude_all_objects: Iterable[str],
    ) -> MutableMapping[str, List[SnowflakeColumn]]:
        all_objects = list(cache_exclude_all_objects)

        columns: MutableMapping[str, List[SnowflakeColumn]] = {}
        if len(all_objects) > 10000:
            # For massive schemas, use a FileBackedDict to avoid memory issues.
            columns = FileBackedDict()

        # Single prefix table case (for streams)
        if len(all_objects) == 1:
            object_batches = [
                [PrefixGroup(prefix=all_objects[0], names=[], exact_match=True)]
            ]
        else:
            # Build batches for full schema scan
            object_batches = build_prefix_batches(
                all_objects, max_batch_size=10000, max_groups_in_batch=5
            )

        # Process batches
        for batch_index, object_batch in enumerate(object_batches):
            if batch_index > 0:
                logger.info(
                    f"Still fetching columns for {db_name}.{schema_name} - batch {batch_index + 1} of {len(object_batches)}"
                )
            query = SnowflakeQuery.columns_for_schema(
                schema_name, db_name, object_batch
            )

            cur = self.connection.query(query)

            for column in cur:
                if column["TABLE_NAME"] not in columns:
                    columns[column["TABLE_NAME"]] = []
                columns[column["TABLE_NAME"]].append(
                    SnowflakeColumn(
                        name=column["COLUMN_NAME"],
                        ordinal_position=column["ORDINAL_POSITION"],
                        is_nullable=column["IS_NULLABLE"] == "YES",
                        data_type=column["DATA_TYPE"],
                        comment=column["COMMENT"],
                        character_maximum_length=column["CHARACTER_MAXIMUM_LENGTH"],
                        numeric_precision=column["NUMERIC_PRECISION"],
                        numeric_scale=column["NUMERIC_SCALE"],
                    )
                )
        return columns

    @serialized_lru_cache(maxsize=SCHEMA_PARALLELISM)
    def get_pk_constraints_for_schema(
        self, schema_name: str, db_name: str
    ) -> Dict[str, SnowflakePK]:
        constraints: Dict[str, SnowflakePK] = {}
        cur = self.connection.query(
            SnowflakeQuery.show_primary_keys_for_schema(schema_name, db_name),
        )

        for row in cur:
            if row["table_name"] not in constraints:
                constraints[row["table_name"]] = SnowflakePK(
                    name=row["constraint_name"], column_names=[]
                )
            constraints[row["table_name"]].column_names.append(row["column_name"])
        return constraints

    @serialized_lru_cache(maxsize=SCHEMA_PARALLELISM)
    def get_fk_constraints_for_schema(
        self, schema_name: str, db_name: str
    ) -> Dict[str, List[SnowflakeFK]]:
        constraints: Dict[str, List[SnowflakeFK]] = {}
        fk_constraints_map: Dict[str, SnowflakeFK] = {}

        cur = self.connection.query(
            SnowflakeQuery.show_foreign_keys_for_schema(schema_name, db_name),
        )

        for row in cur:
            if row["fk_name"] not in constraints:
                fk_constraints_map[row["fk_name"]] = SnowflakeFK(
                    name=row["fk_name"],
                    column_names=[],
                    referred_database=row["pk_database_name"],
                    referred_schema=row["pk_schema_name"],
                    referred_table=row["pk_table_name"],
                    referred_column_names=[],
                )

            if row["fk_table_name"] not in constraints:
                constraints[row["fk_table_name"]] = []

            fk_constraints_map[row["fk_name"]].column_names.append(
                row["fk_column_name"]
            )
            fk_constraints_map[row["fk_name"]].referred_column_names.append(
                row["pk_column_name"]
            )
            constraints[row["fk_table_name"]].append(fk_constraints_map[row["fk_name"]])

        return constraints

    def get_tags_for_database_without_propagation(
        self,
        db_name: str,
    ) -> _SnowflakeTagCache:
        cur = self.connection.query(
            SnowflakeQuery.get_all_tags_in_database_without_propagation(db_name)
        )

        tags = _SnowflakeTagCache()

        for tag in cur:
            snowflake_tag = SnowflakeTag(
                database=tag["TAG_DATABASE"],
                schema=tag["TAG_SCHEMA"],
                name=tag["TAG_NAME"],
                value=tag["TAG_VALUE"],
            )

            # This is the name of the object, unless the object is a column, in which
            # case the name is in the `COLUMN_NAME` field.
            object_name = tag["OBJECT_NAME"]
            # This will be null if the object is a database or schema
            object_schema = tag["OBJECT_SCHEMA"]
            # This will be null if the object is a database
            object_database = tag["OBJECT_DATABASE"]

            domain = tag["DOMAIN"].lower()
            if domain == SnowflakeObjectDomain.DATABASE:
                tags.add_database_tag(object_name, snowflake_tag)
            elif domain == SnowflakeObjectDomain.SCHEMA:
                tags.add_schema_tag(object_name, object_database, snowflake_tag)
            elif domain == SnowflakeObjectDomain.TABLE:  # including views
                tags.add_table_tag(
                    object_name, object_schema, object_database, snowflake_tag
                )
            elif domain == SnowflakeObjectDomain.COLUMN:
                column_name = tag["COLUMN_NAME"]
                tags.add_column_tag(
                    column_name,
                    object_name,
                    object_schema,
                    object_database,
                    snowflake_tag,
                )
            else:
                # This should never happen.
                logger.error(f"Encountered an unexpected domain: {domain}")
                continue

        return tags

    def get_tags_for_object_with_propagation(
        self,
        domain: str,
        quoted_identifier: str,
        db_name: str,
    ) -> List[SnowflakeTag]:
        tags: List[SnowflakeTag] = []

        cur = self.connection.query(
            SnowflakeQuery.get_all_tags_on_object_with_propagation(
                db_name, quoted_identifier, domain
            ),
        )

        for tag in cur:
            tags.append(
                SnowflakeTag(
                    database=tag["TAG_DATABASE"],
                    schema=tag["TAG_SCHEMA"],
                    name=tag["TAG_NAME"],
                    value=tag["TAG_VALUE"],
                )
            )
        return tags

    def get_tags_on_columns_for_table(
        self, quoted_table_name: str, db_name: str
    ) -> Dict[str, List[SnowflakeTag]]:
        tags: Dict[str, List[SnowflakeTag]] = defaultdict(list)
        cur = self.connection.query(
            SnowflakeQuery.get_tags_on_columns_with_propagation(
                db_name, quoted_table_name
            ),
        )

        for tag in cur:
            column_name = tag["COLUMN_NAME"]
            snowflake_tag = SnowflakeTag(
                database=tag["TAG_DATABASE"],
                schema=tag["TAG_SCHEMA"],
                name=tag["TAG_NAME"],
                value=tag["TAG_VALUE"],
            )
            tags[column_name].append(snowflake_tag)

        return tags

    @serialized_lru_cache(maxsize=1)
    def get_streams_for_database(
        self, db_name: str
    ) -> Dict[str, List[SnowflakeStream]]:
        page_limit = SHOW_COMMAND_MAX_PAGE_SIZE

        streams: Dict[str, List[SnowflakeStream]] = {}

        first_iteration = True
        stream_pagination_marker: Optional[str] = None
        while first_iteration or stream_pagination_marker is not None:
            cur = self.connection.query(
                SnowflakeQuery.streams_for_database(
                    db_name,
                    limit=page_limit,
                    stream_pagination_marker=stream_pagination_marker,
                )
            )

            first_iteration = False
            stream_pagination_marker = None

            result_set_size = 0
            for stream in cur:
                result_set_size += 1

                stream_name = stream["name"]
                schema_name = stream["schema_name"]
                if schema_name not in streams:
                    streams[schema_name] = []
                streams[stream["schema_name"]].append(
                    SnowflakeStream(
                        name=stream["name"],
                        created=stream["created_on"],
                        owner=stream["owner"],
                        comment=stream["comment"],
                        source_type=stream["source_type"],
                        type=stream["type"],
                        stale=stream["stale"],
                        mode=stream["mode"],
                        database_name=stream["database_name"],
                        schema_name=stream["schema_name"],
                        invalid_reason=stream["invalid_reason"],
                        owner_role_type=stream["owner_role_type"],
                        stale_after=stream["stale_after"],
                        table_name=stream["table_name"],
                        base_tables=stream["base_tables"],
                        last_altered=stream["created_on"],
                    )
                )

            if result_set_size >= page_limit:
                logger.info(
                    f"Fetching next page of streams for {db_name} - after {stream_name}"
                )
                stream_pagination_marker = stream_name

        return streams

    @serialized_lru_cache(maxsize=1)
    def get_procedures_for_database(
        self, db_name: str
    ) -> Dict[str, List[BaseProcedure]]:
        procedures: Dict[str, List[BaseProcedure]] = {}
        cur = self.connection.query(
            SnowflakeQuery.procedures_for_database(db_name),
        )

        for procedure in cur:
            if procedure["PROCEDURE_SCHEMA"] not in procedures:
                procedures[procedure["PROCEDURE_SCHEMA"]] = []

            procedures[procedure["PROCEDURE_SCHEMA"]].append(
                BaseProcedure(
                    name=procedure["PROCEDURE_NAME"],
                    language=procedure["PROCEDURE_LANGUAGE"],
                    argument_signature=procedure["ARGUMENT_SIGNATURE"],
                    return_type=procedure["PROCEDURE_RETURN_TYPE"],
                    procedure_definition=procedure["PROCEDURE_DEFINITION"],
                    created=procedure["CREATED"],
                    last_altered=procedure["LAST_ALTERED"],
                    comment=procedure["COMMENT"],
                    extra_properties=None,
                )
            )
        return procedures

    @serialized_lru_cache(maxsize=1)
    def get_streamlit_apps_for_database(
        self, db_name: str
    ) -> Dict[str, List[SnowflakeStreamlitApp]]:
        streamlit_apps: Dict[str, List[SnowflakeStreamlitApp]] = {}
        cur = self.connection.query(
            SnowflakeQuery.streamlit_apps_for_database(db_name),
        )

        for app in cur:
            schema_name = app["schema_name"]
            if schema_name not in streamlit_apps:
                streamlit_apps[schema_name] = []
            streamlit_apps[schema_name].append(
                SnowflakeStreamlitApp(
                    name=app["name"],
                    created=app["created_on"],
                    owner=app["owner"],
                    database_name=app["database_name"],
                    schema_name=app["schema_name"],
                    title=app["title"],
                    comment=app.get("comment"),
                    url_id=app["url_id"],
                    owner_role_type=app["owner_role_type"],
                )
            )

        return streamlit_apps

    @serialized_lru_cache(maxsize=1)
    def get_dynamic_table_graph_info(self, db_name: str) -> Dict[str, Dict[str, Any]]:
        """Get dynamic table dependency information from information schema."""
        dt_graph_info: Dict[str, Dict[str, Any]] = {}
        try:
            cur = self.connection.query(
                SnowflakeQuery.get_dynamic_table_graph_history(db_name)
            )
            for row in cur:
                dt_name = row["NAME"]
                dt_graph_info[dt_name] = {
                    "inputs": row.get("INPUTS"),
                    "target_lag_type": row.get("TARGET_LAG_TYPE"),
                    "target_lag_sec": row.get("TARGET_LAG_SEC"),
                    "scheduling_state": row.get("SCHEDULING_STATE"),
                    "alter_trigger": row.get("ALTER_TRIGGER"),
                }
            logger.debug(
                f"Successfully retrieved graph info for {len(dt_graph_info)} dynamic tables in {db_name}"
            )
        except Exception as e:
            self.report.warning(
                "Failed to get dynamic table graph history",
                db_name,
                exc=e,
            )

        return dt_graph_info

    @serialized_lru_cache(maxsize=1)
    def get_dynamic_tables_with_definitions(
        self, db_name: str
    ) -> Dict[str, List[SnowflakeDynamicTable]]:
        """Get dynamic tables with their definitions using SHOW DYNAMIC TABLES."""
        page_limit = SHOW_COMMAND_MAX_PAGE_SIZE
        dynamic_tables: Dict[str, List[SnowflakeDynamicTable]] = {}

        # Get graph/dependency information (pass db_name)
        dt_graph_info = self.get_dynamic_table_graph_info(db_name)

        first_iteration = True
        dt_pagination_marker: Optional[str] = None

        while first_iteration or dt_pagination_marker is not None:
            try:
                cur = self.connection.query(
                    SnowflakeQuery.show_dynamic_tables_for_database(
                        db_name,
                        limit=page_limit,
                        dynamic_table_pagination_marker=dt_pagination_marker,
                    )
                )

                first_iteration = False
                dt_pagination_marker = None
                result_set_size = 0

                for dt in cur:
                    result_set_size += 1

                    dt_name = dt["name"]
                    schema_name = dt["schema_name"]

                    if schema_name not in dynamic_tables:
                        dynamic_tables[schema_name] = []

                    # Get definition from SHOW result
                    definition = dt.get("text")

                    # Get target lag from SHOW result or graph info
                    target_lag = dt.get("target_lag")
                    if not target_lag and dt_graph_info:
                        qualified_name = f"{db_name}.{schema_name}.{dt_name}"
                        graph_info = dt_graph_info.get(qualified_name, {})
                        if graph_info.get("target_lag_type") and graph_info.get(
                            "target_lag_sec"
                        ):
                            target_lag = f"{graph_info['target_lag_sec']} {graph_info['target_lag_type']}"

                    dynamic_tables[schema_name].append(
                        SnowflakeDynamicTable(
                            name=dt_name,
                            created=dt["created_on"],
                            last_altered=dt.get("created_on"),
                            size_in_bytes=dt.get("bytes", 0),
                            rows_count=dt.get("rows", 0),
                            comment=dt.get("comment"),
                            definition=definition,
                            target_lag=target_lag,
                            is_dynamic=True,
                            type="DYNAMIC TABLE",
                        )
                    )

                if result_set_size >= page_limit:
                    logger.info(
                        f"Fetching next page of dynamic tables for {db_name} - after {dt_name}"
                    )
                    dt_pagination_marker = dt_name

            except Exception as e:
                logger.debug(
                    f"Failed to get dynamic tables for database {db_name}: {e}"
                )
                break

        return dynamic_tables

    def populate_dynamic_table_definitions(
        self, tables: Dict[str, List[SnowflakeTable]], db_name: str
    ) -> None:
        """Populate dynamic table definitions for tables that are marked as dynamic."""
        try:
            # Get dynamic tables with definitions from SHOW command
            dt_with_definitions = self.get_dynamic_tables_with_definitions(db_name)

            for schema_name, table_list in tables.items():
                for table in table_list:
                    if (
                        isinstance(table, SnowflakeDynamicTable)
                        and table.definition is None
                    ):
                        # Find matching dynamic table from SHOW results
                        show_dt_list = dt_with_definitions.get(schema_name, [])
                        for show_dt in show_dt_list:
                            if show_dt.name == table.name:
                                table.definition = show_dt.definition
                                table.target_lag = show_dt.target_lag
                                break
        except Exception as e:
            logger.debug(
                f"Failed to populate dynamic table definitions for {db_name}: {e}"
            )
