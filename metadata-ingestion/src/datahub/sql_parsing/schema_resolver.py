import contextlib
import pathlib
from typing import Dict, List, Optional, Protocol, Set, Tuple

from typing_extensions import TypedDict

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

# A lightweight table schema: column -> type mapping.
SchemaInfo = Dict[str, str]


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
    ):
        # Also supports platform with an urn prefix.
        self._platform = DataPlatformUrn(platform).platform_name
        self.platform_instance = platform_instance
        self.env = env

        self.graph = graph

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

        schema_info = self._resolve_schema_info(urn)
        if schema_info:
            return urn, schema_info

        urn_lower = self.get_urn_for_table(table, lower=True)
        if urn_lower != urn:
            schema_info = self._resolve_schema_info(urn_lower)
            if schema_info:
                return urn_lower, schema_info

        # Our treatment of platform instances when lowercasing urns
        # is inconsistent. In some places (e.g. Snowflake), we lowercase
        # the table names but not the platform instance. In other places
        # (e.g. Databricks), we lowercase everything because it happens
        # via the automatic lowercasing helper.
        # See https://github.com/datahub-project/datahub/pull/8928.
        # While we have this sort of inconsistency, we should also
        # check the mixed case urn, as a last resort.
        urn_mixed = self.get_urn_for_table(table, lower=True, mixed=True)
        if urn_mixed not in {urn, urn_lower}:
            schema_info = self._resolve_schema_info(urn_mixed)
            if schema_info:
                return urn_mixed, schema_info

        if self._prefers_urn_lower():
            return urn_lower, None
        else:
            return urn, None

    def _prefers_urn_lower(self) -> bool:
        return self.platform not in PLATFORMS_WITH_CASE_SENSITIVE_TABLES

    def has_urn(self, urn: str) -> bool:
        return self._schema_cache.get(urn) is not None

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
        schema_info = _convert_schema_aspect_to_info(schema_metadata)
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
        self._schema_cache[urn] = schema_info

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
