import contextlib
import json
import logging
import pathlib
import shutil
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Protocol, Set, Tuple

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
    ):
        # Init cache, potentially restoring from a previous run.
        shared_conn = None
        if _cache_filename:
            shared_conn = ConnectionWrapper(filename=_cache_filename)
        schema_cache: FileBackedDict[Optional[SchemaInfo]] = FileBackedDict(
            shared_connection=shared_conn,
            extra_columns={"is_missing": lambda v: v is None},
        )

        self._init_components(
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            graph=graph,
            schema_cache=schema_cache,
            readonly_fallback=None,
            report=report,
        )

    def _init_components(
        self,
        *,
        platform: str,
        platform_instance: Optional[str],
        env: str,
        graph: Optional["DataHubGraph"],
        schema_cache: "FileBackedDict[Optional[SchemaInfo]]",
        readonly_fallback: "Optional[FileBackedDict[Optional[SchemaInfo]]]",
        report: Optional[SchemaResolverReport],
    ) -> None:
        """Single place every construction path (``__init__``, ``load_readonly``,
        ``for_worker``) wires up the resolver's attributes.

        Centralized so a new attribute is added once here instead of silently
        missing from the factory methods that bypass ``__init__``.
        """
        # Also supports platform with an urn prefix.
        self._platform = DataPlatformUrn(platform).platform_name
        self.platform_instance = platform_instance
        self.env = env

        self.graph = graph
        self.report = report

        # Optional read-only fallback cache consulted on a primary-cache miss.
        # Used by worker resolvers (see for_worker) to read the bulk of schemas
        # from a shared read-only snapshot while keeping graph-hydrated results and
        # None-miss dedup in the writable primary cache. None → single-tier behavior.
        self._readonly_fallback: Optional[FileBackedDict[Optional[SchemaInfo]]] = (
            readonly_fallback
        )

        self._schema_cache: FileBackedDict[Optional[SchemaInfo]] = schema_cache

    @property
    def platform(self) -> str:
        return self._platform

    def includes_temp_tables(self) -> bool:
        return False

    def get_urns(self) -> Set[str]:
        return {k for k, v in self._schema_cache.items() if v is not None}

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
            if self._cache_contains(candidate_urn):
                schema_info = self._cache_get(candidate_urn)
                if schema_info is not None:
                    self._track_cache_hit()
                    return candidate_urn, schema_info

        if self.graph:
            # Skip URNs already in cache (None entries included) to avoid repeated API calls.
            urns_to_fetch = [u for u in urns_to_try if not self._cache_contains(u)]

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
                schema_info = self._cache_get(candidate_urn)
                if schema_info is not None:
                    self._track_cache_hit()
                    return candidate_urn, schema_info

        logger.debug(
            f"Schema resolution failed for table {table}. Tried URNs: "
            f"primary={urn}, lower={urn_lower}, mixed={urn_mixed}"
        )
        self._track_cache_miss()

        return (urn_lower if self._prefers_urn_lower() else urn), None

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

    def has_urn(self, urn: str) -> bool:
        return self._cache_get(urn) is not None

    def _track_cache_hit(self) -> None:
        """Track a cache hit if reporting is enabled."""
        if self.report is not None:
            self.report.num_schema_cache_hits += 1

    def _track_cache_miss(self) -> None:
        """Track a cache miss if reporting is enabled."""
        if self.report is not None:
            self.report.num_schema_cache_misses += 1

    def _cache_contains(self, urn: str) -> bool:
        """Whether *urn* has an entry (including a cached None) in either tier.

        Reads consult the writable primary cache first, then the optional
        read-only fallback snapshot. Writes only ever touch the primary cache.
        """
        if urn in self._schema_cache:
            return True
        if self._readonly_fallback is not None and urn in self._readonly_fallback:
            return True
        return False

    def _cache_get(self, urn: str) -> Optional[SchemaInfo]:
        """Read *urn*'s schema from the primary cache, falling back to the
        read-only snapshot. Returns None if absent or cached as a miss — callers
        that must distinguish absence from a cached miss should use
        :meth:`_cache_contains`."""
        if urn in self._schema_cache:
            return self._schema_cache[urn]
        if self._readonly_fallback is not None and urn in self._readonly_fallback:
            return self._readonly_fallback[urn]
        return None

    def _resolve_schema_info(self, urn: str) -> Optional[SchemaInfo]:
        if self._cache_contains(urn):
            return self._cache_get(urn)

        # TODO: For bigquery partitioned tables, add the pseudo-column _PARTITIONTIME
        # or _PARTITIONDATE where appropriate.

        if self.graph:
            schema_info = self._fetch_schema_info(self.graph, urn)
            if schema_info:
                self._save_to_cache(urn, schema_info)
                return schema_info

        self._save_to_cache(urn, None)
        return None

    def add_schema_metadata(
        self, urn: str, schema_metadata: SchemaMetadataClass
    ) -> None:
        schema_info = _convert_schema_aspect_to_info(schema_metadata)
        self._save_to_cache(urn, schema_info)

    def add_schema_metadata_from_fetch(
        self, urn: str, schema_metadata: Optional[SchemaMetadataClass]
    ) -> None:
        # Always stores a result (including None) to prevent repeated API calls
        # for schemas not found in DataHub.
        schema_info = (
            _convert_schema_aspect_to_info(schema_metadata)
            if schema_metadata is not None
            else None
        )
        self._save_to_cache(urn, schema_info)

    def add_raw_schema_info(self, urn: str, schema_info: SchemaInfo) -> None:
        self._save_to_cache(urn, schema_info)

    def add_graphql_schema_metadata(
        self, urn: str, schema_metadata: GraphQLSchemaMetadata
    ) -> None:
        schema_info = self.convert_graphql_schema_metadata_to_info(schema_metadata)
        self._save_to_cache(urn, schema_info)

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

    def _save_to_cache(self, urn: str, schema_info: Optional[SchemaInfo]) -> None:
        # Read-only resolvers have no graph and no persistent store to update;
        # writing to the in-memory cache would only dirty it and trigger a write
        # attempt (→ sqlite3.OperationalError) when the cache is flushed at close.
        if self._schema_cache.read_only:
            return
        self._schema_cache[urn] = schema_info

    def _fetch_schema_info(
        self, graph: "DataHubGraph", urn: str
    ) -> Optional[SchemaInfo]:
        aspect = graph.get_aspect(urn, SchemaMetadataClass)
        if not aspect:
            return None

        return _convert_schema_aspect_to_info(aspect)

    @classmethod
    def convert_graphql_schema_metadata_to_info(
        cls, schema: GraphQLSchemaMetadata
    ) -> SchemaInfo:
        return {
            get_simple_field_path_from_v2_field_path(field["fieldPath"]): (
                # The actual types are more of a "nice to have".
                field["nativeDataType"] or "str"
            )
            for field in schema["fields"]
            # TODO: We can't generate lineage to columns nested within structs yet.
            if "." not in get_simple_field_path_from_v2_field_path(field["fieldPath"])
        }

    def snapshot_to(self, path: pathlib.Path) -> None:
        """Flush all in-memory cache entries to SQLite then copy the DB file to *path*.

        The resulting file is a self-contained SQLite database that worker processes
        can open read-only with ``immutable=1``, avoiding all locking overhead.

        The source connection must be quiescent (no concurrent writers) when this is
        called: we flush with ``synchronous=OFF`` so the OS page cache may not have
        been synced to disk; copying mid-write could produce a corrupt snapshot.
        """
        self._schema_cache.flush()
        shutil.copyfile(self._schema_cache.filename, path)

    @classmethod
    def load_readonly(
        cls,
        path: pathlib.Path,
        *,
        platform: str,
        platform_instance: Optional[str] = None,
        env: str = DEFAULT_ENV,
    ) -> "SchemaResolver":
        """Open a snapshot created by :meth:`snapshot_to` as a read-only resolver.

        The returned resolver has ``graph=None`` so it never makes network calls.
        Multiple processes may open the same snapshot file simultaneously via
        ``immutable=1``, which bypasses SQLite locking entirely and allows the OS
        to share the page cache across processes.
        """
        ro_conn = ConnectionWrapper(filename=path, read_only=True)
        # Bypass __init__ — inject the read-only connection without triggering the
        # normal writable-setup path — but route all attribute wiring through the
        # shared _init_components so no field is silently dropped.
        resolver = cls.__new__(cls)
        resolver._init_components(
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            graph=None,
            schema_cache=FileBackedDict(
                shared_connection=ro_conn,
                extra_columns={"is_missing": lambda v: v is None},
            ),
            readonly_fallback=None,
            report=None,
        )
        return resolver

    @classmethod
    def for_worker(
        cls,
        snapshot_path: pathlib.Path,
        *,
        platform: str,
        platform_instance: Optional[str] = None,
        env: str = DEFAULT_ENV,
        graph: Optional["DataHubGraph"] = None,
    ) -> "SchemaResolver":
        """Build a two-tier resolver for a parallel-parsing worker process.

        The bulk of schemas are read from the shared read-only *snapshot_path*
        (opened ``immutable=1`` so the OS page cache is shared across workers,
        keeping memory low). A small **writable** in-memory primary cache sits in
        front of it to hold graph-hydrated results and None-miss dedup, so a
        worker with a ``graph`` attached does not silently drop lineage — unlike
        :meth:`load_readonly`, whose cache is read-only and cannot absorb fetches.

        Use :meth:`load_readonly` instead when there is no graph: it avoids the
        extra writable cache entirely (lowest memory, no writes).
        """
        ro_conn = ConnectionWrapper(filename=snapshot_path, read_only=True)
        # Bypass __init__ so we can wire a writable primary cache in front of a
        # read-only fallback, but route attribute wiring through the shared
        # _init_components so no field is silently dropped.
        resolver = cls.__new__(cls)
        resolver._init_components(
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            graph=graph,
            # Fresh writable in-memory cache (temp file) so _save_to_cache works.
            schema_cache=FileBackedDict(
                extra_columns={"is_missing": lambda v: v is None},
            ),
            readonly_fallback=FileBackedDict(
                shared_connection=ro_conn,
                extra_columns={"is_missing": lambda v: v is None},
            ),
            report=None,
        )
        return resolver

    def close(self) -> None:
        if self._readonly_fallback is not None:
            self._readonly_fallback.close()
        self._schema_cache.close()


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
        urn = self._base_resolver.get_urn_for_table(
            table, lower=self._base_resolver._prefers_urn_lower()
        )
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


def _convert_schema_field_list_to_info(
    schema_fields: List[SchemaFieldClass],
) -> SchemaInfo:
    return {
        get_simple_field_path_from_v2_field_path(col.fieldPath): (
            # The actual types are more of a "nice to have".
            col.nativeDataType or "str"
        )
        for col in schema_fields
        # TODO: We can't generate lineage to columns nested within structs yet.
        if "." not in get_simple_field_path_from_v2_field_path(col.fieldPath)
    }


def _convert_schema_aspect_to_info(schema_metadata: SchemaMetadataClass) -> SchemaInfo:
    return _convert_schema_field_list_to_info(schema_metadata.fields)


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
