"""
Shared storage lineage functionality for Hive and Hive Metastore sources.
"""

import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

from pydantic.fields import Field

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    OtherSchemaClass,
    SchemaMetadataClass,
    UpstreamClass,
    UpstreamLineageClass,
)

logger = logging.getLogger(__name__)


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
                if scheme in ("abfs", "abfss", "wasbs"):
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
                path = "/" + parsed.path.lstrip("/")

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
