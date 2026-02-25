import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

from pydantic import Field

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.report import Report
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
from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)


class StoragePlatform(StrEnum):
    """Enumeration of storage platforms supported for lineage"""

    S3 = "s3"
    AZURE = "abs"
    GCS = "gcs"
    DBFS = "dbfs"
    LOCAL = "file"
    HDFS = "hdfs"


class LineageDirection(StrEnum):
    """Direction of lineage relationship between storage and Hive"""

    UPSTREAM = "upstream"
    DOWNSTREAM = "downstream"


# Azure schemes that use container@account format (require container extraction)
AZURE_CONTAINER_SCHEMES = {"abfs", "abfss", "wasb", "wasbs"}

# Azure schemes that use simple path format
AZURE_LEGACY_SCHEMES = {"adl", "adls"}

STORAGE_SCHEME_MAPPING = {
    "s3": StoragePlatform.S3,
    "s3a": StoragePlatform.S3,
    "s3n": StoragePlatform.S3,
    "abfs": StoragePlatform.AZURE,
    "abfss": StoragePlatform.AZURE,
    "adl": StoragePlatform.AZURE,
    "adls": StoragePlatform.AZURE,
    "wasb": StoragePlatform.AZURE,
    "wasbs": StoragePlatform.AZURE,
    "gs": StoragePlatform.GCS,
    "gcs": StoragePlatform.GCS,
    "dbfs": StoragePlatform.DBFS,
    "file": StoragePlatform.LOCAL,
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
            if location.startswith("/"):
                return StoragePlatform.LOCAL, location

            parsed = urlparse(location)
            scheme = parsed.scheme.lower()

            if not scheme:
                return None

            platform = STORAGE_SCHEME_MAPPING.get(scheme)
            if not platform:
                return None

            if platform == StoragePlatform.S3:
                path = f"{parsed.netloc}/{parsed.path.lstrip('/')}"

            elif platform == StoragePlatform.AZURE:
                if scheme in AZURE_CONTAINER_SCHEMES:
                    if "@" in parsed.netloc:
                        container = parsed.netloc.split("@")[0]
                    else:
                        container = parsed.netloc
                    path = f"{container}/{parsed.path.lstrip('/')}"
                else:
                    path = f"{parsed.netloc}/{parsed.path.lstrip('/')}"

            elif platform == StoragePlatform.GCS:
                path = f"{parsed.netloc}/{parsed.path.lstrip('/')}"

            elif platform == StoragePlatform.DBFS:
                path = "/" + parsed.path.lstrip("/")

            elif platform == StoragePlatform.LOCAL or platform == StoragePlatform.HDFS:
                path = f"{parsed.netloc}/{parsed.path.lstrip('/')}"

            else:
                return None

            path = path.rstrip("/")
            path = re.sub(r"/+", "/", path)

            return platform, path
        except ValueError as e:
            logger.debug(
                f"Failed to parse storage location '{location}': {e}", exc_info=True
            )
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


class HiveStorageLineageConfigMixin(ConfigModel):
    """Configuration for Hive storage lineage"""

    emit_storage_lineage: bool = Field(
        default=False,
        description="Whether to emit storage-to-Hive lineage. When enabled, creates lineage relationships "
        "between Hive tables and their underlying storage locations (S3, Azure, GCS, HDFS, etc.).",
    )
    hive_storage_lineage_direction: LineageDirection = Field(
        default=LineageDirection.UPSTREAM,
        description="Direction of storage lineage. If 'upstream', storage is treated as upstream to Hive "
        "(data flows from storage to Hive). If 'downstream', storage is downstream to Hive "
        "(data flows from Hive to storage).",
    )
    include_column_lineage: bool = Field(
        default=True,
        description="When enabled along with emit_storage_lineage, column-level lineage will be extracted "
        "between Hive table columns and storage location fields.",
    )
    storage_platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance for the storage system (e.g., 'my-s3-instance'). "
        "Used when generating URNs for storage datasets.",
    )


@dataclass
class HiveStorageSourceReport(Report):
    """Report for tracking storage lineage statistics"""

    storage_locations_scanned: int = 0
    filtered_locations: List[str] = field(default_factory=list)
    failed_locations: List[str] = field(default_factory=list)

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
        config: HiveStorageLineageConfigMixin,
        env: str,
    ):
        self.config = config
        self.env = env
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
        platform: StoragePlatform,
        path: str,
    ) -> Optional[Tuple[str, str]]:
        """
        Create storage dataset URN from parsed platform and path.
        Returns tuple of (urn, platform_name) if successful, None otherwise.
        """
        if not path or not path.strip():
            logger.warning(f"Empty path provided for platform {platform}")
            return None

        platform_name = StoragePathParser.get_platform_name(platform)
        platform_instance = self.config.storage_platform_instance

        try:
            storage_urn = make_dataset_urn_with_platform_instance(
                platform=platform_name,
                name=path,
                env=self.env,
                platform_instance=platform_instance,
            )
            return storage_urn, platform_name
        except (ValueError, TypeError) as exp:
            logger.warning(
                f"Invalid parameters for storage URN creation - platform: {platform_name}, path: {path}: {exp}",
                exc_info=True,
            )
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

            matching_field = next(
                (f for f in storage_schema.fields if f.fieldPath == dataset_path),
                None,
            )

            if matching_field:
                if (
                    self.config.hive_storage_lineage_direction
                    == LineageDirection.UPSTREAM
                ):
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
        schema_metadata: Optional[SchemaMetadataClass] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Generate MCPs for storage dataset entity"""

        storage_info = StoragePathParser.parse_storage_location(storage_location)
        if not storage_info:
            return
        platform, path = storage_info

        urn_info = self._make_storage_dataset_urn(platform, path)
        if not urn_info:
            return
        storage_urn, platform_name = urn_info

        platform_instance = self.config.storage_platform_instance

        try:
            props = DatasetPropertiesClass(name=path)
            yield MetadataWorkUnit(
                id=f"storage-{storage_urn}-props",
                mcp=MetadataChangeProposalWrapper(
                    entityUrn=storage_urn,
                    aspect=props,
                ),
            )

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

        except ValueError as e:
            logger.warning(
                f"Invalid storage location format {storage_location}: {e}",
                exc_info=True,
            )
            self.report.report_location_failed(storage_location)
            return
        except (TypeError, AttributeError) as e:
            logger.error(
                f"Programming error while creating storage dataset MCPs for {storage_location}: {e}",
                exc_info=True,
            )
            self.report.report_location_failed(storage_location)
            return

    def get_lineage_mcp(
        self,
        dataset_urn: str,
        table: Dict[str, Any],
        dataset_schema: Optional[SchemaMetadataClass] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Generate lineage between Hive table and storage location"""

        if not self.config.emit_storage_lineage:
            return

        storage_location = table.get("StorageDescriptor", {}).get("Location")
        if not storage_location:
            return

        parsed_info = StoragePathParser.parse_storage_location(storage_location)
        if not parsed_info:
            self.report.report_location_failed(storage_location)
            return

        platform, path = parsed_info
        urn_info = self._make_storage_dataset_urn(platform, path)
        if not urn_info:
            self.report.report_location_failed(storage_location)
            return

        storage_urn, storage_platform = urn_info
        self.report.report_location_scanned()

        yield from self.get_storage_dataset_mcp(
            storage_location=storage_location,
            schema_metadata=dataset_schema,
        )

        storage_schema = (
            self._get_storage_schema(storage_location, dataset_schema)
            if dataset_schema
            else None
        )

        fine_grained_lineages = (
            None
            if not (dataset_schema and storage_schema)
            else self._get_fine_grained_lineages(
                dataset_urn, storage_urn, dataset_schema, storage_schema
            )
        )

        if self.config.hive_storage_lineage_direction == LineageDirection.UPSTREAM:
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
        """Get schema metadata for storage location"""

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
