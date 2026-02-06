import contextlib
import json
import logging
import pathlib
from dataclasses import dataclass
from typing import Dict, List, Optional, Protocol, Set, Tuple

from requests.models import HTTPError
from typing_extensions import TypedDict

from datahub.configuration.common import GraphError
from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
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
        graph: Optional[DataHubGraph] = None,
        _cache_filename: Optional[pathlib.Path] = None,
        report: Optional[SchemaResolverReport] = None,
    ):
        # Also supports platform with an urn prefix.
        self._platform = DataPlatformUrn(platform).platform_name
        self.platform_instance = platform_instance
        self.env = env

        self.graph = graph
        self.report = report

        # Init cache, potentially restoring from a previous run.
        shared_conn = None
        if _cache_filename:
            shared_conn = ConnectionWrapper(filename=_cache_filename)
        self._schema_cache: FileBackedDict[Optional[SchemaInfo]] = FileBackedDict(
            shared_connection=shared_conn,
            extra_columns={"is_missing": lambda v: v is None},
        )

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

                except (TimeoutError, ConnectionError, OSError) as e:
                    logger.warning(
                        f"Batch fetch failed due to network issue: {e}. "
                        f"Falling back to individual fetches for {len(urns_to_fetch)} URNs.",
                        exc_info=True,
                    )
                    self._fallback_fetch_schemas(urns_to_fetch)

                except HTTPError as e:
                    logger.warning(
                        f"Batch fetch failed due to HTTP error: {e}. "
                        f"Falling back to individual fetches for {len(urns_to_fetch)} URNs.",
                        exc_info=True,
                    )
                    self._fallback_fetch_schemas(urns_to_fetch)

                except (json.JSONDecodeError, ValueError, KeyError) as e:
                    logger.warning(
                        f"Batch fetch failed due to data parsing error: {e}. "
                        f"Falling back to individual fetches for {len(urns_to_fetch)} URNs.",
                        exc_info=True,
                    )
                    self._fallback_fetch_schemas(urns_to_fetch)

                except (GraphError, AssertionError) as e:
                    logger.warning(
                        f"Batch fetch failed due to DataHub error: {e}. "
                        f"Falling back to individual fetches for {len(urns_to_fetch)} URNs.",
                        exc_info=True,
                    )
                    self._fallback_fetch_schemas(urns_to_fetch)

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

        return (urn_lower if self._prefers_urn_lower() else urn), None

    def _prefers_urn_lower(self) -> bool:
        return self.platform not in PLATFORMS_WITH_CASE_SENSITIVE_TABLES

    def has_urn(self, urn: str) -> bool:
        return self._schema_cache.get(urn) is not None

    def _track_cache_hit(self) -> None:
        """Track a cache hit in the report if reporting is enabled."""
        if self.report is not None:
            self.report.num_schema_cache_hits += 1

    def _track_cache_miss(self) -> None:
        """Track a cache miss in the report if reporting is enabled."""
        if self.report is not None:
            self.report.num_schema_cache_misses += 1

    def _resolve_schema_info(self, urn: str) -> Optional[SchemaInfo]:
        if urn in self._schema_cache:
            return self._schema_cache[urn]

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
        """Add schema metadata from ingestion source.

        Ingestion-provided schemas ALWAYS take precedence and overwrite any cached
        schemas (from DataHub or previous runs) because they are fresh from the source.
        """
        schema_info = _convert_schema_aspect_to_info(schema_metadata)
        self._save_to_cache(urn, schema_info)

    def add_schema_metadata_from_fetch(
        self, urn: str, schema_metadata: Optional[SchemaMetadataClass]
    ) -> bool:
        """Cache schema from DataHub API fetch if not already present.

        Respects cache precedence: ingestion schemas take precedence over fetched schemas.
        Always caches the result (even None) to prevent repeated API calls for missing schemas.

        Returns:
            True if schema was cached (new entry), False if skipped (already cached from ingestion).
        """
        if urn in self._schema_cache:
            existing = self._schema_cache[urn]
            if existing is not None:
                logger.debug(
                    f"Skipping DataHub schema for {urn} - already in cache from ingestion"
                )
                return False

        if schema_metadata is not None:
            schema_info = _convert_schema_aspect_to_info(schema_metadata)
            self._save_to_cache(urn, schema_info)
        else:
            # Cache None to prevent repeated lookups for missing schemas
            self._save_to_cache(urn, None)

        return True

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
        self._schema_cache[urn] = schema_info

    def _fallback_fetch_schemas(self, urns: List[str]) -> None:
        """Fetch schemas individually when batch fetch fails.

        Handles network, HTTP, data parsing, and DataHub-specific errors gracefully
        by caching None for failed fetches to prevent repeated attempts.
        """
        if not self.graph:
            return

        for fetch_urn in urns:
            if fetch_urn in self._schema_cache:
                continue

            try:
                schema_metadata = self.graph.get_aspect(fetch_urn, SchemaMetadataClass)
                self.add_schema_metadata_from_fetch(fetch_urn, schema_metadata)

            except (TimeoutError, ConnectionError, OSError) as net_error:
                logger.debug(
                    f"Network error fetching schema for {fetch_urn}: {net_error}"
                )
                self.add_schema_metadata_from_fetch(fetch_urn, None)

            except HTTPError as http_error:
                logger.debug(
                    f"HTTP error fetching schema for {fetch_urn}: {http_error}"
                )
                self.add_schema_metadata_from_fetch(fetch_urn, None)

            except (json.JSONDecodeError, ValueError, KeyError) as data_error:
                logger.debug(
                    f"Data parsing error fetching schema for {fetch_urn}: {data_error}"
                )
                self.add_schema_metadata_from_fetch(fetch_urn, None)

            except (GraphError, AssertionError) as datahub_error:
                logger.debug(
                    f"DataHub error fetching schema for {fetch_urn}: {datahub_error}"
                )
                self.add_schema_metadata_from_fetch(fetch_urn, None)

    def _fetch_schema_info(self, graph: DataHubGraph, urn: str) -> Optional[SchemaInfo]:
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

    def close(self) -> None:
        self._schema_cache.close()


class _SchemaResolverWithExtras(SchemaResolverInterface):
    def __init__(
        self,
        base_resolver: SchemaResolver,
        extra_schemas: Dict[str, Optional[SchemaInfo]],
    ):
        self._base_resolver = base_resolver
        self._extra_schemas = extra_schemas

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
