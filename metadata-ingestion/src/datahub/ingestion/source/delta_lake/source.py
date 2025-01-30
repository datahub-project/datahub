import json
import logging
import os
import time
from datetime import datetime
from typing import Dict, Iterable, List
from urllib.parse import urlparse

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from deltalake import DeltaTable

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.s3_boto_utils import get_s3_tags
from datahub.ingestion.source.aws.s3_util import (
    get_bucket_name,
    get_key_prefix,
    strip_s3_prefix,
)
from datahub.ingestion.source.azure.abs_folder_utils import (
    get_abs_properties,
    get_abs_tags,
)
from datahub.ingestion.source.azure.abs_utils import get_container_name
from datahub.ingestion.source.azure.onelake_utils import strip_onelake_prefix
from datahub.ingestion.source.data_lake_common.data_lake_utils import ContainerWUCreator
from datahub.ingestion.source.delta_lake.config import DeltaLakeSourceConfig
from datahub.ingestion.source.delta_lake.delta_lake_utils import (
    get_file_count,
    read_delta_table,
)
from datahub.ingestion.source.delta_lake.report import DeltaLakeSourceReport
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    OperationClass,
    OperationTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
)
from datahub.telemetry import telemetry
from datahub.utilities.delta import delta_type_to_hive_type
from datahub.utilities.hive_schema_to_avro import get_schema_fields_for_hive_column

logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)

config_options_to_report = [
    "platform",
]

OPERATION_STATEMENT_TYPES = {
    "INSERT": OperationTypeClass.INSERT,
    "UPDATE": OperationTypeClass.UPDATE,
    "DELETE": OperationTypeClass.DELETE,
    "MERGE": OperationTypeClass.UPDATE,
    "CREATE": OperationTypeClass.CREATE,
    "CREATE_TABLE_AS_SELECT": OperationTypeClass.CREATE,
    "CREATE_SCHEMA": OperationTypeClass.CREATE,
    "DROP_TABLE": OperationTypeClass.DROP,
    "REPLACE TABLE AS SELECT": OperationTypeClass.UPDATE,
    "COPY INTO": OperationTypeClass.UPDATE,
}


@platform_name("Delta Lake", id="delta-lake")
@config_class(DeltaLakeSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.TAGS,
    "Can extract S3 object/bucket and Azure blob/container tags if enabled",
)
class DeltaLakeSource(Source):
    """
    This plugin extracts:
    - Column types and schema associated with each delta table
    - Custom properties: number_of_files, partition_columns, table_creation_time, location, version etc.

    :::caution

    If you are ingesting datasets from AWS S3, we recommend running the ingestion on a server in the same region to avoid high egress costs.

    :::

    """

    source_config: DeltaLakeSourceConfig
    report: DeltaLakeSourceReport
    profiling_times_taken: List[float]
    container_WU_creator: ContainerWUCreator
    storage_options: Dict[str, str]

    def __init__(self, config: DeltaLakeSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.report = DeltaLakeSourceReport()
        if self.source_config.is_s3:
            if (
                self.source_config.s3 is None
                or self.source_config.s3.aws_config is None
            ):
                raise ValueError("AWS Config must be provided for S3 base path.")
            self.s3_client = self.source_config.s3.aws_config.get_s3_client()
        elif self.source_config.is_azure:
            if self.source_config.azure.azure_config is None:
                raise ValueError("Azure Config must be provided for Azure base path.")

        # self.profiling_times_taken = []
        config_report = {
            config_option: config.dict().get(config_option)
            for config_option in config_options_to_report
        }

        telemetry.telemetry_instance.ping(
            "delta_lake_config",
            config_report,
        )

    def _parse_datatype(self, raw_field_json_str: str) -> List[SchemaFieldClass]:
        raw_field_json = json.loads(raw_field_json_str)

        # get the parent field name and type
        field_name = raw_field_json.get("name")
        field_type = delta_type_to_hive_type(raw_field_json.get("type"))

        return get_schema_fields_for_hive_column(field_name, field_type)

    def get_fields(self, delta_table: DeltaTable) -> List[SchemaField]:
        fields: List[SchemaField] = []

        for raw_field in delta_table.schema().fields:
            parsed_data_list = self._parse_datatype(raw_field.to_json())
            fields = fields + parsed_data_list

        fields = sorted(fields, key=lambda f: f.fieldPath)
        return fields

    def _create_operation_aspect_wu(
        self, delta_table: DeltaTable, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        for hist in delta_table.history(
            limit=self.source_config.version_history_lookback
        ):
            # History schema picked up from https://docs.delta.io/latest/delta-utility.html#retrieve-delta-table-history
            reported_time: int = int(time.time() * 1000)

            # For OneLake/Fabric, the timestamp might be in a different format
            last_updated_timestamp: int = int(hist.get("timestamp", reported_time))
            if isinstance(last_updated_timestamp, str):
                try:
                    # Try to parse ISO format timestamp
                    dt = datetime.fromisoformat(
                        last_updated_timestamp.replace("Z", "+00:00")
                    )
                    last_updated_timestamp = int(dt.timestamp() * 1000)
                except ValueError:
                    # If we can't parse the timestamp, use current time
                    last_updated_timestamp = reported_time

            statement_type = OPERATION_STATEMENT_TYPES.get(
                hist.get("operation", "UNKNOWN"), OperationTypeClass.CUSTOM
            )
            custom_type = (
                hist.get("operation")
                if statement_type == OperationTypeClass.CUSTOM
                else None
            )

            operation_custom_properties = dict()
            for key, val in sorted(hist.items()):
                if val is not None:
                    if isinstance(val, dict):
                        for k, v in sorted(val.items()):
                            if v is not None:
                                operation_custom_properties[f"{key}_{k}"] = str(v)
                    else:
                        operation_custom_properties[key] = str(val)

            # Remove keys that we've handled specially
            operation_custom_properties.pop("timestamp", None)
            operation_custom_properties.pop("operation", None)

            operation_aspect = OperationClass(
                timestampMillis=reported_time,
                lastUpdatedTimestamp=last_updated_timestamp,
                operationType=statement_type,
                customOperationType=custom_type,
                customProperties=operation_custom_properties,
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=operation_aspect,
            ).as_workunit()

    def ingest_table(
        self, delta_table: DeltaTable, path: str
    ) -> Iterable[MetadataWorkUnit]:
        table_name = (
            delta_table.metadata().name
            if delta_table.metadata().name
            else path.split("/")[-1]
        )
        if not self.source_config.table_pattern.allowed(table_name):
            logger.debug(
                f"Skipping table ({table_name}) present at location {path} as table pattern does not match"
            )

        if self.source_config.relative_path is None:
            browse_path: str = (
                strip_s3_prefix(path)
                if self.source_config.is_s3
                else strip_onelake_prefix(path)
                if self.source_config.is_azure
                else path.strip("/")
            )
        else:
            browse_path = path.split(self.source_config.base_path)[1].strip("/")

        data_platform_urn = make_data_platform_urn(self.source_config.platform)
        logger.info(f"Creating dataset urn with name: {browse_path}")
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.source_config.platform,
            browse_path,
            self.source_config.platform_instance,
            self.source_config.env,
        )
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[Status(removed=False)],
        )

        customProperties = {
            "partition_columns": str(delta_table.metadata().partition_columns),
            "table_creation_time": str(delta_table.metadata().created_time),
            "id": str(delta_table.metadata().id),
            "version": str(delta_table.version()),
            "location": self.source_config.complete_path,
        }
        if self.source_config.require_files:
            customProperties["number_of_files"] = str(get_file_count(delta_table))

        dataset_properties = DatasetPropertiesClass(
            description=delta_table.metadata().description,
            name=table_name,
            customProperties=customProperties,
        )
        dataset_snapshot.aspects.append(dataset_properties)

        fields = self.get_fields(delta_table)
        schema_metadata = SchemaMetadata(
            schemaName=table_name,
            platform=data_platform_urn,
            version=delta_table.version(),
            hash="",
            fields=fields,
            platformSchema=OtherSchemaClass(rawSchema=""),
        )
        dataset_snapshot.aspects.append(schema_metadata)

        if (
            self.source_config.is_s3
            and self.source_config.s3
            and (
                self.source_config.s3.use_s3_bucket_tags
                or self.source_config.s3.use_s3_object_tags
            )
        ):
            bucket = get_bucket_name(path)
            key_prefix = get_key_prefix(path)
            s3_tags = get_s3_tags(
                bucket,
                key_prefix,
                dataset_urn,
                self.source_config.s3.aws_config,
                self.ctx,
                self.source_config.s3.use_s3_bucket_tags,
                self.source_config.s3.use_s3_object_tags,
            )
            if s3_tags is not None:
                dataset_snapshot.aspects.append(s3_tags)

        if (
            self.source_config.is_azure
            and self.source_config.azure
            and path.startswith("abfss://")
        ):
            pass
        elif (
            self.source_config.is_azure
            and self.source_config.azure
            and (
                self.source_config.azure.use_abs_container_properties
                or self.source_config.azure.use_abs_blob_properties
                or self.source_config.azure.use_abs_blob_tags
            )
        ):
            # Only do blob operations for regular Azure Storage
            container_name = get_container_name(path)
            blob_name = get_key_prefix(path)

            # Add Azure blob properties
            if (
                self.source_config.azure.use_abs_container_properties
                or self.source_config.azure.use_abs_blob_properties
            ):
                azure_properties = get_abs_properties(
                    container_name=container_name,
                    blob_name=blob_name,
                    full_path=path,
                    number_of_files=get_file_count(delta_table)
                    if self.source_config.require_files
                    else 0,
                    size_in_bytes=0,  # Could calculate if needed
                    sample_files=False,
                    azure_config=self.source_config.azure.azure_config,
                    use_abs_container_properties=self.source_config.azure.use_abs_container_properties,
                    use_abs_blob_properties=self.source_config.azure.use_abs_blob_properties,
                )
                customProperties.update(azure_properties)

            # Add Azure blob tags
            if self.source_config.azure.use_abs_blob_tags:
                azure_tags = get_abs_tags(
                    container_name=container_name,
                    key_name=blob_name,
                    dataset_urn=dataset_urn,
                    azure_config=self.source_config.azure.azure_config,
                    ctx=self.ctx,
                    use_abs_blob_tags=True,
                )
                if azure_tags is not None:
                    dataset_snapshot.aspects.append(azure_tags)

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        yield MetadataWorkUnit(id=str(delta_table.metadata().id), mce=mce)

        yield from self.container_WU_creator.create_container_hierarchy(
            browse_path, dataset_urn
        )

        yield from self._create_operation_aspect_wu(delta_table, dataset_urn)

    def get_storage_options(self) -> Dict[str, str]:
        if (
            self.source_config.is_s3
            and self.source_config.s3 is not None
            and self.source_config.s3.aws_config is not None
        ):
            aws_config = self.source_config.s3.aws_config
            creds = aws_config.get_credentials()
            opts = {
                "AWS_ACCESS_KEY_ID": creds.get("aws_access_key_id") or "",
                "AWS_SECRET_ACCESS_KEY": creds.get("aws_secret_access_key") or "",
                "AWS_SESSION_TOKEN": creds.get("aws_session_token") or "",
                # Allow http connections, this is required for minio
                "AWS_STORAGE_ALLOW_HTTP": "true",  # for delta-lake < 0.11.0
                "AWS_ALLOW_HTTP": "true",  # for delta-lake >= 0.11.0
            }
            if aws_config.aws_region:
                opts["AWS_REGION"] = aws_config.aws_region
            if aws_config.aws_endpoint_url:
                opts["AWS_ENDPOINT_URL"] = aws_config.aws_endpoint_url
            return opts
        elif self.source_config.is_azure:
            logger.debug("Getting Azure OneLake storage options")
            if (
                not self.source_config.azure
                or not self.source_config.azure.azure_config
            ):
                raise ValueError("Azure config is required for Azure paths")

            azure_config = self.source_config.azure.azure_config
            creds = ClientSecretCredential(
                tenant_id=azure_config.tenant_id,
                client_id=azure_config.client_id,
                client_secret=azure_config.client_secret,
            )
            token = creds.get_token("https://onelake.fabric.microsoft.com/.default")

            storage_opts = {
                "bearer_token": token.token,
                "onelake_endpoint": "https://onelake.dfs.fabric.microsoft.com",
            }

            logger.debug(f"Using Azure OneLake storage options: {storage_opts}")
            return storage_opts
        return {}

    def process_folder(self, path: str) -> Iterable[MetadataWorkUnit]:
        logger.debug(f"Processing folder: {path}")
        delta_table = read_delta_table(path, self.storage_options, self.source_config)
        if delta_table:
            logger.debug(f"Delta table found at: {path}")
            yield from self.ingest_table(delta_table, path.rstrip("/"))
        else:
            logger.debug(f"No delta table found at {path}, scanning subfolders")
            for folder in self.get_folders(path):
                yield from self.process_folder(folder)

    def get_folders(self, path: str) -> Iterable[str]:
        """Get subfolders based on the source configuration type."""
        logger.debug(f"Getting folders for path: {path}")
        if self.source_config.is_azure:
            return self.azure_get_folders(path)
        elif self.source_config.is_s3:
            return self.s3_get_folders(path)
        else:
            return self.local_get_folders(path)

    def s3_get_folders(self, path: str) -> Iterable[str]:
        parse_result = urlparse(path)
        for page in self.s3_client.get_paginator("list_objects_v2").paginate(
            Bucket=parse_result.netloc, Prefix=parse_result.path[1:], Delimiter="/"
        ):
            for o in page.get("CommonPrefixes", []):
                yield f"{parse_result.scheme}://{parse_result.netloc}/{o.get('Prefix')}"

    def azure_get_folders(self, path: str) -> Iterable[str]:
        """List folders in Azure OneLake."""
        logger.debug(f"Looking for Azure folders in: {path}")
        if not path.startswith("abfss://"):
            raise ValueError(f"Invalid Azure path format: {path}")

        try:
            # Parse ABFSS URL for OneLake
            parts = path.split("/")
            container_guid = parts[2].split("@")[
                0
            ]  # Extract the GUID from container part
            workspace_guid = container_guid  # In Fabric, these are the same

            # Check if we have valid GUIDs
            if not (
                len(container_guid) == 36
                and all(c in "0123456789abcdef-" for c in container_guid.lower())
            ):
                raise ValueError(f"Invalid GUID format in path: {container_guid}")

            # Use DFS endpoint for OneLake
            datalake_service_client = DataLakeServiceClient(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                credential=self.source_config.azure.azure_config.get_credentials(),
            )

            # Get filesystem client with the workspace GUID
            filesystem_client = datalake_service_client.get_file_system_client(
                file_system=workspace_guid
            )

            # Look in the Tables directory within the specific path
            path_prefix = "Tables"
            if len(parts) > 4:  # If there's a more specific path
                path_prefix = "/".join(["Tables"] + parts[4:])

            logger.debug(f"Listing paths with prefix: {path_prefix}")
            paths = filesystem_client.get_paths(path=path_prefix)

            for path_prop in paths:
                if path_prop.is_directory:
                    # Construct the full path properly maintaining the original structure
                    folder_path = path_prop.name
                    full_path = f"abfss://{container_guid}@onelake.dfs.fabric.microsoft.com/{folder_path}"
                    logger.debug(f"Found folder: {full_path}")
                    yield full_path

        except Exception as e:
            logger.error(f"Error listing Azure folders: {str(e)}")
            raise

    def local_get_folders(self, path: str) -> Iterable[str]:
        if not os.path.isdir(path):
            raise FileNotFoundError(
                f"{path} does not exist or is not a directory. Please check base_path configuration."
            )
        for folder in os.listdir(path):
            yield os.path.join(path, folder)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self.container_WU_creator = ContainerWUCreator(
            self.source_config.platform,
            self.source_config.platform_instance,
            self.source_config.env,
        )
        self.storage_options = self.get_storage_options()
        yield from self.process_folder(self.source_config.complete_path)

    def get_report(self) -> SourceReport:
        return self.report
