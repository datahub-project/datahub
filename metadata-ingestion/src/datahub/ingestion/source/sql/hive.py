import json
import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from urllib.parse import urlparse

from pydantic.class_validators import validator
from pydantic.fields import Field

# This import verifies that the dependencies are available.
from pyhive import hive  # noqa: F401
from pyhive.sqlalchemy_hive import HiveDate, HiveDecimal, HiveDialect, HiveTimestamp
from sqlalchemy.engine.reflection import Inspector

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.sql.sql_common import SqlWorkUnit, register_custom_type
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    TimeTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.utilities import config_clean
from datahub.utilities.hive_schema_to_avro import get_avro_schema_for_hive_column

logger = logging.getLogger(__name__)

register_custom_type(HiveDate, DateTypeClass)
register_custom_type(HiveTimestamp, TimeTypeClass)
register_custom_type(HiveDecimal, NumberTypeClass)


class StoragePlatform(Enum):
    """Enumeration of storage platforms supported for lineage"""

    S3 = "s3"
    AZURE = "abs"
    GCS = "gcs"
    DBFS = "dbfs"
    LOCAL = "file"
    HDFS = "hdfs"


# Mapping of URL schemes to storage platforms
STORAGE_SCHEME_MAPPING = {
    # S3 and derivatives
    "s3": StoragePlatform.S3,
    "s3a": StoragePlatform.S3,
    "s3n": StoragePlatform.S3,
    # Azure and derivatives
    "abfs": StoragePlatform.AZURE,
    "abfss": StoragePlatform.AZURE,
    "adl": StoragePlatform.AZURE,
    "adls": StoragePlatform.AZURE,
    "wasb": StoragePlatform.AZURE,
    "wasbs": StoragePlatform.AZURE,
    # GCS and derivatives
    "gs": StoragePlatform.GCS,
    "gcs": StoragePlatform.GCS,
    # DBFS
    "dbfs": StoragePlatform.DBFS,
    # Local filesystem
    "file": StoragePlatform.LOCAL,
    # HDFS
    "hdfs": StoragePlatform.HDFS,
}


class StoragePathParser:
    """Parser for storage paths with platform-specific logic"""

    @staticmethod
    def parse_storage_location(location: str) -> Optional[Tuple[StoragePlatform, str]]:
        """
        Parse a storage location into platform and normalized path.

        Args:
            location: Storage location URI (e.g., s3://bucket/path, abfss://container@account.dfs.core.windows.net/path)

        Returns:
            Tuple of (StoragePlatform, normalized_path) if valid, None if invalid
        """

        try:
            # Handle special case for local files with no scheme
            if location.startswith("/"):
                return StoragePlatform.LOCAL, location

            # Parse the URI
            parsed = urlparse(location)
            scheme = parsed.scheme.lower()

            if not scheme:
                return None

            # Look up the platform
            platform = STORAGE_SCHEME_MAPPING.get(scheme)
            if not platform:
                return None

            # Get normalized path based on platform
            if platform == StoragePlatform.S3:
                # For S3, combine bucket and path
                path = f"{parsed.netloc}/{parsed.path.lstrip('/')}"

            elif platform == StoragePlatform.AZURE:
                if scheme in ("abfs", "abfss"):
                    # Format: abfss://container@account.dfs.core.windows.net/path
                    container = parsed.netloc.split("@")[0]
                    path = f"{container}/{parsed.path.lstrip('/')}"
                else:
                    # Handle other Azure schemes
                    path = f"{parsed.netloc}/{parsed.path.lstrip('/')}"

            elif platform == StoragePlatform.GCS:
                # For GCS, combine bucket and path
                path = f"{parsed.netloc}/{parsed.path.lstrip('/')}"

            elif platform == StoragePlatform.DBFS:
                # For DBFS, use path as-is
                path = parsed.path.lstrip("/")

            elif platform == StoragePlatform.LOCAL:
                # For local files, use full path
                path = f"{parsed.netloc}/{parsed.path.lstrip('/')}"

            elif platform == StoragePlatform.HDFS:
                # For HDFS, use full path
                path = f"{parsed.netloc}/{parsed.path.lstrip('/')}"

            else:
                return None

            # Clean up the path
            path = path.rstrip("/")  # Remove trailing slashes
            path = re.sub(r"/+", "/", path)  # Normalize multiple slashes
            path = f"/{path}"

            return platform, path

        except Exception as exp:
            logger.warning(f"Failed to parse storage location {location}: {exp}")
            return None

    @staticmethod
    def get_platform_name(platform: StoragePlatform) -> str:
        """Get the platform name to use in URNs"""

        platform_names = {
            StoragePlatform.S3: "s3",
            StoragePlatform.AZURE: "adls",
            StoragePlatform.GCS: "gcs",
            StoragePlatform.DBFS: "dbfs",
            StoragePlatform.LOCAL: "file",
            StoragePlatform.HDFS: "hdfs",
        }
        return platform_names[platform]


class HiveStorageLineageConfig:
    """Configuration for Hive storage lineage."""

    def __init__(
        self,
        emit_storage_lineage: bool,
        hive_storage_lineage_direction: str,
        include_column_lineage: bool,
        storage_platform_instance: Optional[str],
    ):
        if hive_storage_lineage_direction.lower() not in ["upstream", "downstream"]:
            raise ValueError(
                "hive_storage_lineage_direction must be either upstream or downstream"
            )

        self.emit_storage_lineage = emit_storage_lineage
        self.hive_storage_lineage_direction = hive_storage_lineage_direction.lower()
        self.include_column_lineage = include_column_lineage
        self.storage_platform_instance = storage_platform_instance


@dataclass
class HiveStorageSourceReport:
    """Report for tracking storage lineage statistics"""

    storage_locations_scanned: int = 0
    filtered_locations: List[str] = Field(default_factory=list)
    failed_locations: List[str] = Field(default_factory=list)

    def report_location_scanned(self) -> None:
        self.storage_locations_scanned += 1

    def report_location_filtered(self, location: str) -> None:
        self.filtered_locations.append(location)

    def report_location_failed(self, location: str) -> None:
        self.failed_locations.append(location)


class HiveStorageLineage:
    """Handles storage lineage for Hive tables"""

    def __init__(
        self,
        config: HiveStorageLineageConfig,
        env: str,
        convert_urns_to_lowercase: bool = False,
    ):
        self.config = config
        self.env = env
        self.convert_urns_to_lowercase = convert_urns_to_lowercase
        self.report = HiveStorageSourceReport()

    def _make_dataset_platform_instance(
        self,
        platform: str,
        instance: Optional[str],
    ) -> DataPlatformInstanceClass:
        """Create DataPlatformInstance aspect"""

        return DataPlatformInstanceClass(
            platform=make_data_platform_urn(platform),
            instance=make_dataplatform_instance_urn(platform, instance)
            if instance
            else None,
        )

    def _make_storage_dataset_urn(
        self,
        storage_location: str,
    ) -> Optional[Tuple[str, str]]:
        """
        Create storage dataset URN from location.
        Returns tuple of (urn, platform) if successful, None otherwise.
        """

        platform_instance = None
        storage_info = StoragePathParser.parse_storage_location(storage_location)
        if not storage_info:
            logger.debug(f"Could not parse storage location: {storage_location}")
            return None

        platform, path = storage_info
        platform_name = StoragePathParser.get_platform_name(platform)

        if self.convert_urns_to_lowercase:
            platform_name = platform_name.lower()
            path = path.lower()
            if self.config.storage_platform_instance:
                platform_instance = self.config.storage_platform_instance.lower()

        try:
            storage_urn = make_dataset_urn_with_platform_instance(
                platform=platform_name,
                name=path,
                env=self.env,
                platform_instance=platform_instance,
            )
            return storage_urn, platform_name
        except Exception as exp:
            logger.error(f"Failed to create URN for {platform_name}:{path}: {exp}")
            return None

    def _get_fine_grained_lineages(
        self,
        dataset_urn: str,
        storage_urn: str,
        dataset_schema: SchemaMetadataClass,
        storage_schema: SchemaMetadataClass,
    ) -> Iterable[FineGrainedLineageClass]:
        """Generate column-level lineage between dataset and storage"""

        if not self.config.include_column_lineage:
            return

        for dataset_field in dataset_schema.fields:
            dataset_path = dataset_field.fieldPath

            # Find matching field in storage schema
            matching_field = next(
                (f for f in storage_schema.fields if f.fieldPath == dataset_path),
                None,
            )

            if matching_field:
                if self.config.hive_storage_lineage_direction == "upstream":
                    yield FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=[
                            make_schema_field_urn(
                                parent_urn=storage_urn,
                                field_path=matching_field.fieldPath,
                            )
                        ],
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[
                            make_schema_field_urn(
                                parent_urn=dataset_urn,
                                field_path=dataset_path,
                            )
                        ],
                    )
                else:
                    yield FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=[
                            make_schema_field_urn(
                                parent_urn=dataset_urn,
                                field_path=dataset_path,
                            )
                        ],
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[
                            make_schema_field_urn(
                                parent_urn=storage_urn,
                                field_path=matching_field.fieldPath,
                            )
                        ],
                    )

    def _create_lineage_mcp(
        self,
        source_urn: str,
        target_urn: str,
        fine_grained_lineages: Optional[Iterable[FineGrainedLineageClass]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Create lineage MCP between source and target datasets"""

        lineages_list = (
            list(fine_grained_lineages) if fine_grained_lineages is not None else None
        )

        upstream_lineage = UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=source_urn, type=DatasetLineageTypeClass.COPY)
            ],
            fineGrainedLineages=lineages_list,
        )

        yield MetadataWorkUnit(
            id=f"{source_urn}-{target_urn}-lineage",
            mcp=MetadataChangeProposalWrapper(
                entityUrn=target_urn, aspect=upstream_lineage
            ),
        )

    def get_storage_dataset_mcp(
        self,
        storage_location: str,
        platform_instance: Optional[str] = None,
        schema_metadata: Optional[SchemaMetadataClass] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate MCPs for storage dataset if needed.
        This creates the storage dataset entity in DataHub.
        """

        storage_info = StoragePathParser.parse_storage_location(
            storage_location,
        )
        if not storage_info:
            return

        platform, path = storage_info
        platform_name = StoragePathParser.get_platform_name(platform)

        if self.convert_urns_to_lowercase:
            platform_name = platform_name.lower()
            path = path.lower()
            if self.config.storage_platform_instance:
                platform_instance = self.config.storage_platform_instance.lower()

        try:
            storage_urn = make_dataset_urn_with_platform_instance(
                platform=platform_name,
                name=path,
                env=self.env,
                platform_instance=platform_instance,
            )

            # Dataset properties
            props = DatasetPropertiesClass(name=path)
            yield MetadataWorkUnit(
                id=f"storage-{storage_urn}-props",
                mcp=MetadataChangeProposalWrapper(
                    entityUrn=storage_urn,
                    aspect=props,
                ),
            )

            # Platform instance
            platform_instance_aspect = self._make_dataset_platform_instance(
                platform=platform_name,
                instance=platform_instance,
            )
            yield MetadataWorkUnit(
                id=f"storage-{storage_urn}-platform",
                mcp=MetadataChangeProposalWrapper(
                    entityUrn=storage_urn, aspect=platform_instance_aspect
                ),
            )

            # Schema if available
            if schema_metadata:
                storage_schema = SchemaMetadataClass(
                    schemaName=f"{platform.value}_schema",
                    platform=f"urn:li:dataPlatform:{platform.value}",
                    version=0,
                    fields=schema_metadata.fields,
                    hash="",
                    platformSchema=OtherSchemaClass(rawSchema=""),
                )
                yield MetadataWorkUnit(
                    id=f"storage-{storage_urn}-schema",
                    mcp=MetadataChangeProposalWrapper(
                        entityUrn=storage_urn, aspect=storage_schema
                    ),
                )

        except Exception as e:
            logger.error(
                f"Failed to create storage dataset MCPs for {storage_location}: {e}"
            )
            return

    def get_lineage_mcp(
        self,
        dataset_urn: str,
        table: Dict[str, Any],
        dataset_schema: Optional[SchemaMetadataClass] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate lineage MCP for a Hive table to its storage location.

        Args:
            dataset_urn: URN of the Hive dataset
            table: Hive table dictionary containing metadata
            dataset_schema: Optional schema metadata for the Hive dataset

        Returns:
            MetadataWorkUnit containing the lineage MCP if successful
        """

        platform_instance = None

        if not self.config.emit_storage_lineage:
            return

        # Get storage location from table
        storage_location = table.get("StorageDescriptor", {}).get("Location")
        if not storage_location:
            return

        # Create storage dataset URN
        storage_info = self._make_storage_dataset_urn(storage_location)
        if not storage_info:
            self.report.report_location_failed(storage_location)
            return

        storage_urn, storage_platform = storage_info
        self.report.report_location_scanned()

        if self.config.storage_platform_instance:
            platform_instance = self.config.storage_platform_instance.lower()

        # Create storage dataset entity
        yield from self.get_storage_dataset_mcp(
            storage_location=storage_location,
            platform_instance=platform_instance,
            schema_metadata=dataset_schema,
        )

        # Get storage schema if available (implement based on storage system)
        storage_schema = (
            self._get_storage_schema(storage_location, dataset_schema)
            if dataset_schema
            else None
        )

        # Generate fine-grained lineage if schemas available
        fine_grained_lineages = (
            None
            if not (dataset_schema and storage_schema)
            else self._get_fine_grained_lineages(
                dataset_urn, storage_urn, dataset_schema, storage_schema
            )
        )

        # Create lineage MCP
        if self.config.hive_storage_lineage_direction == "upstream":
            yield from self._create_lineage_mcp(
                source_urn=storage_urn,
                target_urn=dataset_urn,
                fine_grained_lineages=fine_grained_lineages,
            )
        else:
            yield from self._create_lineage_mcp(
                source_urn=dataset_urn,
                target_urn=storage_urn,
                fine_grained_lineages=fine_grained_lineages,
            )

    def _get_storage_schema(
        self,
        storage_location: str,
        table_schema: Optional[SchemaMetadataClass] = None,
    ) -> Optional[SchemaMetadataClass]:
        """
        Get schema metadata for storage location.
        Currently supports:
        - Delta tables
        - Parquet files
        - Spark tables

        Returns:
            SchemaMetadataClass if schema can be inferred, None otherwise
        """

        if not table_schema:
            return None

        storage_info = StoragePathParser.parse_storage_location(storage_location)
        if not storage_info:
            return None

        platform, _ = storage_info

        return SchemaMetadataClass(
            schemaName=f"{platform.value}_schema",
            platform=f"urn:li:dataPlatform:{platform.value}",
            version=0,
            fields=table_schema.fields,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
        )


try:
    from databricks_dbapi.sqlalchemy_dialects.hive import DatabricksPyhiveDialect
    from pyhive.sqlalchemy_hive import _type_map
    from sqlalchemy import types, util
    from sqlalchemy.engine import reflection

    @reflection.cache  # type: ignore
    def dbapi_get_columns_patched(self, connection, table_name, schema=None, **kw):
        """Patches the get_columns method from dbapi (databricks_dbapi.sqlalchemy_dialects.base) to pass the native type through"""
        rows = self._get_table_columns(connection, table_name, schema)
        # Strip whitespace
        rows = [[col.strip() if col else None for col in row] for row in rows]
        # Filter out empty rows and comment
        rows = [row for row in rows if row[0] and row[0] != "# col_name"]
        result = []
        for col_name, col_type, _comment in rows:
            # Handle both oss hive and Databricks' hive partition header, respectively
            if col_name in ("# Partition Information", "# Partitioning"):
                break
            # Take out the more detailed type information
            # e.g. 'map<int,int>' -> 'map'
            #      'decimal(10,1)' -> decimal
            orig_col_type = col_type  # keep a copy
            col_type = re.search(r"^\w+", col_type).group(0)  # type: ignore
            try:
                coltype = _type_map[col_type]
            except KeyError:
                util.warn(
                    "Did not recognize type '{}' of column '{}'".format(
                        col_type, col_name
                    )
                )
                coltype = types.NullType  # type: ignore
            result.append(
                {
                    "name": col_name,
                    "type": coltype,
                    "nullable": True,
                    "default": None,
                    "full_type": orig_col_type,  # pass it through
                    "comment": _comment,
                }
            )
        return result

    DatabricksPyhiveDialect.get_columns = dbapi_get_columns_patched
except ModuleNotFoundError:
    pass
except Exception as exp:
    logger.warning(f"Failed to patch method due to {exp}")


@reflection.cache  # type: ignore
def get_view_names_patched(self, connection, schema=None, **kw):
    query = "SHOW VIEWS"
    if schema:
        query += " IN " + self.identifier_preparer.quote_identifier(schema)
    return [row[0] for row in connection.execute(query)]


@reflection.cache  # type: ignore
def get_view_definition_patched(self, connection, view_name, schema=None, **kw):
    full_table = self.identifier_preparer.quote_identifier(view_name)
    if schema:
        full_table = "{}.{}".format(
            self.identifier_preparer.quote_identifier(schema),
            self.identifier_preparer.quote_identifier(view_name),
        )
    # Hive responds to the SHOW CREATE TABLE with the full view DDL,
    # including the view definition. However, for multiline view definitions,
    # it returns multiple rows (of one column each), each with a part of the definition.
    # Any whitespace at the beginning/end of each view definition line is lost.
    rows = connection.execute(f"SHOW CREATE TABLE {full_table}").fetchall()
    parts = [row[0] for row in rows]
    return "\n".join(parts)


HiveDialect.get_view_names = get_view_names_patched
HiveDialect.get_view_definition = get_view_definition_patched


class HiveConfig(TwoTierSQLAlchemyConfig):
    # defaults
    scheme: str = Field(default="hive", hidden_from_docs=True)

    # Overriding as table location lineage is richer implementation here than with include_table_location_lineage
    include_table_location_lineage: bool = Field(default=False, hidden_from_docs=True)

    emit_storage_lineage: bool = Field(
        default=False,
        description="Whether to emit storage-to-Hive lineage",
    )
    hive_storage_lineage_direction: str = Field(
        default="upstream",
        description="If 'upstream', storage is upstream to Hive. If 'downstream' storage is downstream to Hive",
    )
    include_column_lineage: bool = Field(
        default=True,
        description="When enabled, column-level lineage will be extracted from storage",
    )
    storage_platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance for the storage system",
    )

    @validator("host_port")
    def clean_host_port(cls, v):
        return config_clean.remove_protocol(v)

    @validator("hive_storage_lineage_direction")
    def _validate_direction(cls, v: str) -> str:
        """Validate the lineage direction."""
        if v.lower() not in ["upstream", "downstream"]:
            raise ValueError(
                "storage_lineage_direction must be either upstream or downstream"
            )
        return v.lower()

    def get_storage_lineage_config(self) -> HiveStorageLineageConfig:
        """Convert base config parameters to HiveStorageLineageConfig"""
        return HiveStorageLineageConfig(
            emit_storage_lineage=self.emit_storage_lineage,
            hive_storage_lineage_direction=self.hive_storage_lineage_direction,
            include_column_lineage=self.include_column_lineage,
            storage_platform_instance=self.storage_platform_instance,
        )


@platform_name("Hive")
@config_class(HiveConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
class HiveSource(TwoTierSQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, and tables
    - Column types associated with each table
    - Detailed table and storage information
    - Table, row, and column statistics via optional SQL profiling.

    """

    _COMPLEX_TYPE = re.compile("^(struct|map|array|uniontype)")

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "hive")
        self.storage_lineage = HiveStorageLineage(
            config=config.get_storage_lineage_config(),
            env=config.env,
            convert_urns_to_lowercase=config.convert_urns_to_lowercase,
        )

    @classmethod
    def create(cls, config_dict, ctx):
        config = HiveConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Generate workunits for tables and their storage lineage."""
        for wu in super().get_workunits_internal():
            yield wu

            if not isinstance(wu, MetadataWorkUnit):
                continue

            # Get dataset URN and required aspects using workunit methods
            try:
                dataset_urn = wu.get_urn()
                dataset_props = wu.get_aspect_of_type(DatasetPropertiesClass)
                schema_metadata = wu.get_aspect_of_type(SchemaMetadataClass)
            except Exception as exp:
                logger.warning(f"Failed to process workunit {wu.id}: {exp}")
                continue

            # Only proceed if we have the necessary properties
            if dataset_props and dataset_props.customProperties:
                table = {
                    "StorageDescriptor": {
                        "Location": dataset_props.customProperties.get("Location")
                    }
                }

                if table.get("StorageDescriptor", {}).get("Location"):
                    yield from self.storage_lineage.get_lineage_mcp(
                        dataset_urn=dataset_urn,
                        table=table,
                        dataset_schema=schema_metadata,
                    )

    def get_schema_names(self, inspector):
        assert isinstance(self.config, HiveConfig)
        # This condition restricts the ingestion to the specified database.
        if self.config.database:
            return [self.config.database]
        else:
            return super().get_schema_names(inspector)

    def get_schema_fields_for_column(
        self,
        dataset_name: str,
        column: Dict[Any, Any],
        inspector: Inspector,
        pk_constraints: Optional[Dict[Any, Any]] = None,
        partition_keys: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
    ) -> List[SchemaFieldClass]:
        fields = super().get_schema_fields_for_column(
            dataset_name,
            column,
            inspector,
            pk_constraints,
            partition_keys=partition_keys,
        )

        if self._COMPLEX_TYPE.match(fields[0].nativeDataType) and isinstance(
            fields[0].type.type, NullTypeClass
        ):
            assert len(fields) == 1
            field = fields[0]
            # Get avro schema for subfields along with parent complex field
            avro_schema = get_avro_schema_for_hive_column(
                column["name"], field.nativeDataType
            )

            new_fields = schema_util.avro_schema_to_mce_fields(
                json.dumps(avro_schema), default_nullable=True
            )

            # First field is the parent complex field
            new_fields[0].nullable = field.nullable
            new_fields[0].description = field.description
            new_fields[0].isPartOfKey = field.isPartOfKey
            return new_fields

        return fields

    # Hive SQLAlchemy connector returns views as tables in get_table_names.
    # See https://github.com/dropbox/PyHive/blob/b21c507a24ed2f2b0cf15b0b6abb1c43f31d3ee0/pyhive/sqlalchemy_hive.py#L270-L273.
    # This override makes sure that we ingest view definitions for views
    def _process_view(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        view: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )

        try:
            view_definition = inspector.get_view_definition(view, schema)
            # Some dialects return a TextClause instead of a raw string, so we need to convert them to a string.
            view_definition = str(view_definition) if view_definition else ""
        except NotImplementedError:
            view_definition = ""

        if view_definition:
            view_properties_aspect = ViewPropertiesClass(
                materialized=False, viewLanguage="SQL", viewLogic=view_definition
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=view_properties_aspect,
            ).as_workunit()

        if view_definition and self.config.include_view_lineage:
            default_db = None
            default_schema = None
            try:
                default_db, default_schema = self.get_db_schema(dataset_name)
            except ValueError:
                logger.warning(f"Invalid view identifier: {dataset_name}")

            self.aggregator.add_view_definition(
                view_urn=dataset_urn,
                view_definition=view_definition,
                default_db=default_db,
                default_schema=default_schema,
            )

    def get_partitions(
        self, inspector: Inspector, schema: str, table: str
    ) -> Optional[List[str]]:
        partition_columns: List[dict] = inspector.get_indexes(
            table_name=table, schema=schema
        )
        for partition_column in partition_columns:
            if partition_column.get("column_names"):
                return partition_column.get("column_names")

        return []
