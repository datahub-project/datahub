import contextlib
import json
import logging
import pathlib
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Dict,
    Iterable,
    List,
    Optional,
    Protocol,
    Set,
    Tuple,
)

from requests.models import HTTPError
from typing_extensions import TypedDict

from datahub.configuration.common import GraphError
from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import SchemaFieldClass, SchemaMetadataClass
from datahub.metadata.urns import DataPlatformUrn
from datahub.sql_parsing._models import _TableName as _TableName
from datahub.sql_parsing.sql_parsing_common import PLATFORMS_WITH_CASE_SENSITIVE_TABLES
from datahub.utilities.file_backed_collections import ConnectionWrapper, FileBackedDict
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path

logger = logging.getLogger(__name__)

# A lightweight table schema: column -> type mapping.
SchemaInfo = Dict[str, str]


@dataclass
class SchemaResolverReport:
    """Report class for tracking SchemaResolver cache performance."""

    num_schema_cache_hits: int = 0
    num_schema_cache_misses: int = 0


class GraphQLSchemaField(TypedDict):
    fieldPath: str
    nativeDataType: str


class GraphQLSchemaMetadata(TypedDict):
    fields: List[GraphQLSchemaField]


class SchemaResolverInterface(Protocol):
    @property
    def platform(self) -> str: ...

    # Declared as a settable attribute (not a read-only property like `platform`)
    # because implementers assign it in __init__; a Protocol property would
    # install a read-only descriptor and break those assignments.
    platform_instance: Optional[str]

    def includes_temp_tables(self) -> bool: ...

    def resolve_table(self, table: _TableName) -> Tuple[str, Optional[SchemaInfo]]: ...

    def __hash__(self) -> int:
        # Mainly to make lru_cache happy in methods that accept a schema resolver.
        return id(self)


class SchemaResolver(Closeable, SchemaResolverInterface):
    def __init__(
        self,
        *,
        platform: str,
        platform_instance: Optional[str] = None,
        env: str = DEFAULT_ENV,
        graph: Optional["DataHubGraph"] = None,
        _cache_filename: Optional[pathlib.Path] = None,
        report: Optional[SchemaResolverReport] = None,
        keep_platform_instance_case: bool = False,
    ):
        # Also supports platform with an urn prefix.
        self._platform = DataPlatformUrn(platform).platform_name
        self.platform_instance = platform_instance
        self.env = env
        # For sources that emit URNs with the platform_instance as configured, so a
        # URN synthesized for an unknown table matches theirs. Off by default:
        # sources that lowercase whole URNs expect the instance lowercased too.
        self._keep_platform_instance_case = keep_platform_instance_case

        self.graph = graph
        self.report = report

        # Init cache, potentially restoring from a previous run. Without a filename
        # the connection uses a temp directory, removed on close.
        self._conn = ConnectionWrapper(filename=_cache_filename)
        self._schema_cache: FileBackedDict[Optional[SchemaInfo]] = FileBackedDict(
            shared_connection=self._conn,
            extra_columns={"is_missing": lambda v: v is None},
        )
        # {urn: {simplified field path: original field path}}, only for fields whose
        # paths differ (v2 paths). SchemaInfo keys are simplified for the SQL parser,
        # but a schemaField URN must use the dataset's real field path to join.
        self._original_field_paths: FileBackedDict[Dict[str, str]] = FileBackedDict(
            shared_connection=self._conn, tablename="original_field_paths"
        )

    @property
    def platform(self) -> str:
        return self._platform

    def includes_temp_tables(self) -> bool:
        return False

    def get_urns(self) -> Set[str]:
        return {k for k, v in self._schema_cache.items() if v is not None}

    @property
    def closed(self) -> bool:
        """Whether the backing cache has been closed.

        A resolver can be shared with objects that did not create it and will
        not close it, so a borrower cannot infer this from its own state.
        """
        return self._schema_cache.closed

    def schema_count(self) -> int:
        return int(
            self._schema_cache.sql_query(
                f"SELECT COUNT(*) FROM {self._schema_cache.tablename} WHERE NOT is_missing"
            )[0][0]
        )

    def get_urn_for_table(
        self, table: _TableName, lower: bool = False, mixed: bool = False
    ) -> str:
        # TODO: Validate that this is the correct 2/3 layer hierarchy for the platform.

        table_name = ".".join(
            filter(None, [table.database, table.db_schema, table.table])
        )

        platform_instance = self.platform_instance

        if lower:
            table_name = table_name.lower()
            if not mixed:
                platform_instance = (
                    platform_instance.lower() if platform_instance else None
                )

        if self.platform == "bigquery":
            # Normalize shard numbers and other BigQuery weirdness.
            with contextlib.suppress(IndexError):
                table_name = BigqueryTableIdentifier.from_string_name(
                    table_name
                ).get_table_name()

        urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            platform_instance=platform_instance,
            env=self.env,
            name=table_name,
        )
        return urn

    def get_urn_for_unresolved_table(self, table: _TableName) -> str:
        """The URN resolve_table returns for a table it has no schema for."""
        lower = self._prefers_urn_lower()
        return self.get_urn_for_table(
            table, lower=lower, mixed=lower and self._keep_platform_instance_case
        )

    def resolve_urn(self, urn: str) -> Tuple[str, Optional[SchemaInfo]]:
        schema_info = self._resolve_schema_info(urn)
        if schema_info:
            return urn, schema_info

        return urn, None

    def resolve_table(self, table: _TableName) -> Tuple[str, Optional[SchemaInfo]]:
        """Resolve a table to its URN and (best-effort) schema.

        Contract: the returned URN is **best-effort and always non-None** — on a
        cache/graph miss a *synthesized* URN is still returned. The resolution
        signal is the **second element**: ``SchemaInfo is None`` means the table
        was not found in DataHub. Callers deciding whether a table actually
        exists must test the ``SchemaInfo``, never ``urn is None``.
        """
        urn = self.get_urn_for_table(table)
        urn_lower = self.get_urn_for_table(table, lower=True)
        # Our treatment of platform instances when lowercasing urns
        # is inconsistent. In some places (e.g. Snowflake), we lowercase
        # the table names but not the platform instance. In other places
        # (e.g. Databricks), we lowercase everything because it happens
        # via the automatic lowercasing helper.
        # See https://github.com/datahub-project/datahub/pull/8928.
        # While we have this sort of inconsistency, we should also
        # check the mixed case urn, as a last resort.
        urn_mixed = self.get_urn_for_table(table, lower=True, mixed=True)

        urns_to_try = [urn]
        if urn_lower != urn:
            urns_to_try.append(urn_lower)
        if urn_mixed not in {urn, urn_lower}:
            urns_to_try.append(urn_mixed)

        for candidate_urn in urns_to_try:
            if candidate_urn in self._schema_cache:
                schema_info = self._schema_cache[candidate_urn]
                if schema_info is not None:
                    self._track_cache_hit()
                    return candidate_urn, schema_info

        if self.graph:
            # Skip URNs already in cache (None entries included) to avoid repeated API calls.
            urns_to_fetch = [u for u in urns_to_try if u not in self._schema_cache]

            if urns_to_fetch:
                try:
                    entity_results = self.graph.get_entities(
                        entity_name="dataset",
                        urns=urns_to_fetch,
                        aspects=[SchemaMetadataClass.ASPECT_NAME],
                        with_system_metadata=False,
                    )

                    for fetch_urn in urns_to_fetch:
                        schema_metadata: Optional[SchemaMetadataClass] = None

                        if fetch_urn in entity_results:
                            entity_aspects = entity_results[fetch_urn]
                            if SchemaMetadataClass.ASPECT_NAME in entity_aspects:
                                aspect_value, _ = entity_aspects[
                                    SchemaMetadataClass.ASPECT_NAME
                                ]
                                if isinstance(aspect_value, SchemaMetadataClass):
                                    schema_metadata = aspect_value

                        self.add_schema_metadata_from_fetch(fetch_urn, schema_metadata)

                except (
                    TimeoutError,
                    ConnectionError,
                    OSError,
                    HTTPError,
                    json.JSONDecodeError,
                    ValueError,
                    KeyError,
                    GraphError,
                    AssertionError,
                ) as e:
                    logger.warning(
                        f"Batch schema fetch failed ({type(e).__name__}): {e}. "
                        f"Caching {len(urns_to_fetch)} URN(s) as None to avoid repeated lookups.",
                        exc_info=True,
                    )
                    for fetch_urn in urns_to_fetch:
                        self._save_to_cache(fetch_urn, None)

            for candidate_urn in urns_to_try:
                schema_info = self._schema_cache.get(candidate_urn)
                if schema_info is not None:
                    self._track_cache_hit()
                    return candidate_urn, schema_info

        logger.debug(
            f"Schema resolution failed for table {table}. Tried URNs: "
            f"primary={urn}, lower={urn_lower}, mixed={urn_mixed}"
        )
        self._track_cache_miss()

        return self.get_urn_for_unresolved_table(table), None

    def resolve_table_parts(
        self,
        *,
        database: Optional[str],
        db_schema: Optional[str],
        table: str,
    ) -> Tuple[str, Optional[SchemaInfo]]:
        """Resolve catalog/schema/table parts to a URN without importing _TableName.

        Same contract as :meth:`resolve_table`: the URN is always non-None
        (synthesized on a miss); test the returned ``SchemaInfo`` (2nd element)
        to tell whether the table was actually found.
        """
        return self.resolve_table(
            _TableName(database=database, db_schema=db_schema, table=table)
        )

    def _prefers_urn_lower(self) -> bool:
        return self.platform not in PLATFORMS_WITH_CASE_SENSITIVE_TABLES

    def get_original_field_paths(self, urn: str) -> Dict[str, str]:
        """{simplified field path: original field path} for the fields of `urn`
        whose stored path differs from its simplified form (e.g. v2 paths). Fields
        absent from the result use the simplified path as-is."""
        return self._original_field_paths.get(urn) or {}

    def has_urn(self, urn: str) -> bool:
        return self._schema_cache.get(urn) is not None

    def _track_cache_hit(self) -> None:
        """Track a cache hit if reporting is enabled."""
        if self.report is not None:
            self.report.num_schema_cache_hits += 1

    def _track_cache_miss(self) -> None:
        """Track a cache miss if reporting is enabled."""
        if self.report is not None:
            self.report.num_schema_cache_misses += 1

    def _resolve_schema_info(self, urn: str) -> Optional[SchemaInfo]:
        if urn in self._schema_cache:
            return self._schema_cache[urn]

        # TODO: For bigquery partitioned tables, add the pseudo-column _PARTITIONTIME
        # or _PARTITIONDATE where appropriate.

        if self.graph:
            aspect = self.graph.get_aspect(urn, SchemaMetadataClass)
            if aspect:
                schema_info, original_paths = _schema_info_and_original_paths(
                    aspect.fields, urn
                )
                if schema_info:
                    self._save_to_cache(urn, schema_info, original_paths)
                    return schema_info

        self._save_to_cache(urn, None)
        return None

    def add_schema_metadata(
        self, urn: str, schema_metadata: SchemaMetadataClass
    ) -> None:
        self._save_to_cache(
            urn, *_schema_info_and_original_paths(schema_metadata.fields, urn)
        )

    def add_schema_metadata_from_fetch(
        self, urn: str, schema_metadata: Optional[SchemaMetadataClass]
    ) -> None:
        # Always stores a result (including None) to prevent repeated API calls
        # for schemas not found in DataHub.
        if schema_metadata is not None:
            self.add_schema_metadata(urn, schema_metadata)
        else:
            self._save_to_cache(urn, None)

    def add_raw_schema_info(self, urn: str, schema_info: SchemaInfo) -> None:
        self._save_to_cache(urn, schema_info)

    def add_graphql_schema_metadata(
        self, urn: str, schema_metadata: GraphQLSchemaMetadata
    ) -> None:
        self._save_to_cache(
            urn, *_graphql_schema_info_and_original_paths(schema_metadata, urn)
        )

    def with_temp_tables(
        self, temp_tables: Dict[str, Optional[List[SchemaFieldClass]]]
    ) -> SchemaResolverInterface:
        extra_schemas = {
            urn: (
                _convert_schema_field_list_to_info(fields)
                if fields is not None
                else None
            )
            for urn, fields in temp_tables.items()
        }

        return _SchemaResolverWithExtras(
            base_resolver=self, extra_schemas=extra_schemas
        )

    def _save_to_cache(
        self,
        urn: str,
        schema_info: Optional[SchemaInfo],
        original_field_paths: Optional[Dict[str, str]] = None,
    ) -> None:
        self._schema_cache[urn] = schema_info
        if original_field_paths:
            self._original_field_paths[urn] = original_field_paths
        else:
            # One DELETE, rather than a lookup followed by a DELETE.
            with contextlib.suppress(KeyError):
                del self._original_field_paths[urn]

    @classmethod
    def convert_graphql_schema_metadata_to_info(
        cls, schema: GraphQLSchemaMetadata
    ) -> SchemaInfo:
        return _graphql_schema_info_and_original_paths(schema)[0]

    def close(self) -> None:
        # Closes (and flushes) both dicts; a temp-directory connection is removed.
        self._conn.close()


class _SchemaResolverWithExtras(SchemaResolverInterface):
    def __init__(
        self,
        base_resolver: SchemaResolver,
        extra_schemas: Dict[str, Optional[SchemaInfo]],
    ):
        self._base_resolver = base_resolver
        self._extra_schemas = extra_schemas
        self.platform_instance = base_resolver.platform_instance

    @property
    def platform(self) -> str:
        return self._base_resolver.platform

    def includes_temp_tables(self) -> bool:
        return True

    def resolve_table(self, table: _TableName) -> Tuple[str, Optional[SchemaInfo]]:
        urn = self._base_resolver.get_urn_for_unresolved_table(table)
        if urn in self._extra_schemas:
            # Track cache hit for extra schemas
            self._base_resolver._track_cache_hit()
            return urn, self._extra_schemas[urn]
        return self._base_resolver.resolve_table(table)

    def add_temp_tables(
        self, temp_tables: Dict[str, Optional[List[SchemaFieldClass]]]
    ) -> None:
        self._extra_schemas.update(
            {
                urn: (
                    _convert_schema_field_list_to_info(fields)
                    if fields is not None
                    else None
                )
                for urn, fields in temp_tables.items()
            }
        )


def _column_rank(field_path: str) -> Tuple[int, bool, str]:
    # Lower ranks first: fewest [type=...] segments (the column, not a union
    # member), then value fields before Kafka key fields, then lexicographic.
    return (field_path.count("[type="), "[key=True]" in field_path, field_path)


def _simple_to_original_paths(
    fields: Iterable[Tuple[str, str]],
    urn: Optional[str] = None,
) -> Tuple[SchemaInfo, Dict[str, str]]:
    """Build the SchemaInfo (simplified path -> native type) and the
    simplified -> original field path map in one pass over (fieldPath, native type).

    A union column simplifies to the same path as each of its members (e.g.
    `[type=union].u`, `[type=union].[type=int].u`, `[type=union].[type=string].u`
    all become `u`), and a Kafka key field can share its name with a value field.
    The candidate with the lowest `_column_rank` wins: fewest `[type=...]`
    segments (the union column itself), then a value field over a key field,
    then lexicographic order. This is a total order, so the result does not
    depend on field order, which sources that sort fields by fieldPath change. A
    plain (v1) path has no segments and so always wins, and is then dropped from
    the map because it needs no translation."""
    schema_info: SchemaInfo = {}
    candidates: Dict[str, str] = {}
    for field_path, native_type in fields:
        simple = get_simple_field_path_from_v2_field_path(field_path)
        # TODO: We can't generate lineage to columns nested within structs yet.
        if "." in simple:
            continue
        # The actual types are more of a "nice to have".
        schema_info[simple] = native_type or "str"
        current = candidates.get(simple)
        if current is None:
            candidates[simple] = field_path
            continue
        if current == field_path:
            continue
        winner = min(current, field_path, key=_column_rank)
        logger.debug(
            f"Field paths {current!r} and {field_path!r} of {urn} both simplify to "
            f"{simple!r}; using {winner!r}"
        )
        candidates[simple] = winner
    return schema_info, {k: v for k, v in candidates.items() if k != v}


def _schema_info_and_original_paths(
    schema_fields: Iterable[SchemaFieldClass], urn: Optional[str] = None
) -> Tuple[SchemaInfo, Dict[str, str]]:
    return _simple_to_original_paths(
        ((col.fieldPath, col.nativeDataType) for col in schema_fields), urn
    )


def _graphql_schema_info_and_original_paths(
    schema: GraphQLSchemaMetadata, urn: Optional[str] = None
) -> Tuple[SchemaInfo, Dict[str, str]]:
    return _simple_to_original_paths(
        ((field["fieldPath"], field["nativeDataType"]) for field in schema["fields"]),
        urn,
    )


def _convert_schema_field_list_to_info(
    schema_fields: List[SchemaFieldClass],
) -> SchemaInfo:
    return _schema_info_and_original_paths(schema_fields)[0]


def match_columns_to_schema(
    schema_info: SchemaInfo, input_columns: List[str]
) -> List[str]:
    column_from_gms: List[str] = list(schema_info.keys())  # list() to silent lint

    gms_column_map: Dict[str, str] = {
        column.lower(): column for column in column_from_gms
    }

    output_columns: List[str] = [
        gms_column_map.get(column.lower(), column) for column in input_columns
    ]

    return output_columns
