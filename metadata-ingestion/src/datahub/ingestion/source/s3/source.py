import dataclasses
import functools
import logging
import os
import pathlib
import re
import time
from datetime import datetime
from pathlib import PurePath
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

import smart_open.compression as so_compression
from more_itertools import peekable
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException
from smart_open import open as smart_open

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
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
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.s3_boto_utils import (
    get_s3_tags,
    list_folders,
    list_folders_path,
    list_objects_recursive,
    list_objects_recursive_path,
)
from datahub.ingestion.source.aws.s3_util import (
    get_bucket_name,
    get_bucket_relative_path,
    get_key_prefix,
    strip_s3_prefix,
)
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier
from datahub.ingestion.source.data_lake_common.data_lake_utils import (
    ContainerWUCreator,
    add_partition_columns_to_schema,
)
from datahub.ingestion.source.data_lake_common.object_store import (
    create_object_store_adapter,
)
from datahub.ingestion.source.data_lake_common.path_spec import FolderTraversalMethod
from datahub.ingestion.source.s3.config import DataLakeSourceConfig, PathSpec
from datahub.ingestion.source.s3.report import DataLakeSourceReport
from datahub.ingestion.source.schema_inference import avro, csv_tsv, json, parquet
from datahub.ingestion.source.schema_inference.base import SchemaInferenceBase
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import TimeStamp
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    OperationClass,
    OperationTypeClass,
    OtherSchemaClass,
    PartitionsSummaryClass,
    PartitionSummaryClass,
    _Aspect,
)
from datahub.telemetry import stats, telemetry
from datahub.utilities.perf_timer import PerfTimer

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import Bucket

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)

# Hack to support the .gzip extension with smart_open.
so_compression.register_compressor(".gzip", so_compression._COMPRESSOR_REGISTRY[".gz"])

# config flags to emit telemetry for
config_options_to_report = [
    "platform",
    "use_relative_path",
    "ignore_dotfiles",
]

# profiling flags to emit telemetry for
profiling_flags_to_report = [
    "profile_table_level_only",
    "include_field_null_count",
    "include_field_min_value",
    "include_field_max_value",
    "include_field_mean_value",
    "include_field_median_value",
    "include_field_stddev_value",
    "include_field_quantiles",
    "include_field_distinct_value_frequencies",
    "include_field_histogram",
    "include_field_sample_values",
]


# LOCAL_BROWSE_PATH_TRANSFORMER_CONFIG = AddDatasetBrowsePathConfig(
#     path_templates=["/ENV/PLATFORMDATASET_PARTS"], replace_existing=True
# )
#
# LOCAL_BROWSE_PATH_TRANSFORMER = AddDatasetBrowsePathTransformer(
#     ctx=None, config=LOCAL_BROWSE_PATH_TRANSFORMER_CONFIG
# )


def partitioned_folder_comparator(folder1: str, folder2: str) -> int:
    # Try to convert to number and compare if the folder name is a number
    try:
        # Stripping = from the folder names as it most probably partition name part like year=2021
        if "=" in folder1 and "=" in folder2:
            if folder1.rsplit("=", 1)[0] == folder2.rsplit("=", 1)[0]:
                folder1 = folder1.rsplit("=", 1)[-1]
                folder2 = folder2.rsplit("=", 1)[-1]

        num_folder1 = int(folder1)
        num_folder2 = int(folder2)
        if num_folder1 == num_folder2:
            return 0
        else:
            return 1 if num_folder1 > num_folder2 else -1
    except Exception:
        # If folder name is not a number then do string comparison
        if folder1 == folder2:
            return 0
        else:
            return 1 if folder1 > folder2 else -1


@dataclasses.dataclass
class Folder:
    creation_time: datetime
    modification_time: datetime
    size: int
    sample_file: str
    partition_id: Optional[List[Tuple[str, str]]] = None
    is_partition: bool = False

    def partition_id_text(self) -> Optional[str]:
        return (
            "/".join([f"{k}={v}" for k, v in self.partition_id])
            if self.partition_id
            else None
        )


@dataclasses.dataclass
class FolderInfo:
    objects: List[Any]
    total_size: int
    min_time: datetime
    max_time: datetime
    latest_obj: Any


@dataclasses.dataclass
class BrowsePath:
    file: str
    timestamp: datetime
    size: int
    partitions: List[Folder]
    content_type: Optional[str] = None


@dataclasses.dataclass
class TableData:
    display_name: str
    is_s3: bool
    full_path: str
    timestamp: datetime
    table_path: str
    size_in_bytes: int
    number_of_files: int
    partitions: Optional[List[Folder]] = None
    max_partition: Optional[Folder] = None
    min_partition: Optional[Folder] = None
    content_type: Optional[str] = None


@platform_name("S3 / Local Files", id="s3")
@config_class(DataLakeSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    subtype_modifier=[
        SourceCapabilityModifier.FOLDER,
        SourceCapabilityModifier.S3_BUCKET,
    ],
)
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.SCHEMA_METADATA, "Can infer schema from supported file types"
)
@capability(SourceCapability.TAGS, "Can extract S3 object/bucket tags if enabled")
class S3Source(StatefulIngestionSourceBase):
    source_config: DataLakeSourceConfig
    report: DataLakeSourceReport
    profiling_times_taken: List[float]
    container_WU_creator: ContainerWUCreator
    object_store_adapter: Any

    def __init__(self, config: DataLakeSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.report = DataLakeSourceReport()
        self.profiling_times_taken = []
        self.container_WU_creator = ContainerWUCreator(
            self.source_config.platform,
            self.source_config.platform_instance,
            self.source_config.env,
        )

        # Create an object store adapter for handling external URLs and paths
        if self.is_s3_platform():
            # Get the AWS region from config, if available
            aws_region = None
            if self.source_config.aws_config:
                aws_region = self.source_config.aws_config.aws_region

                # For backward compatibility with tests: if we're using a test endpoint, use us-east-1
                if self.source_config.aws_config.aws_endpoint_url and (
                    "localstack"
                    in self.source_config.aws_config.aws_endpoint_url.lower()
                    or "storage.googleapis.com"
                    in self.source_config.aws_config.aws_endpoint_url.lower()
                ):
                    aws_region = "us-east-1"

            # Create an S3 adapter with the configured region
            self.object_store_adapter = create_object_store_adapter(
                "s3", aws_region=aws_region
            )

            # Special handling for GCS via S3 (via boto compatibility layer)
            if (
                self.source_config.aws_config
                and self.source_config.aws_config.aws_endpoint_url
                and "storage.googleapis.com"
                in self.source_config.aws_config.aws_endpoint_url.lower()
            ):
                # We need to preserve the S3-style paths but use GCS external URL generation
                self.object_store_adapter = create_object_store_adapter("gcs")
                # Override create_s3_path to maintain S3 compatibility
                self.object_store_adapter.register_customization(
                    "create_s3_path", lambda bucket, key: f"s3://{bucket}/{key}"
                )
        else:
            # For local files, create a default adapter
            self.object_store_adapter = create_object_store_adapter(
                self.source_config.platform or "file"
            )

        config_report = {
            config_option: config.dict().get(config_option)
            for config_option in config_options_to_report
        }
        config_report = {
            **config_report,
            "profiling_enabled": config.is_profiling_enabled(),
        }

        telemetry.telemetry_instance.ping(
            "data_lake_config",
            config_report,
        )

        if config.is_profiling_enabled():
            telemetry.telemetry_instance.ping(
                "data_lake_profiling_config",
                {
                    config_flag: config.profiling.dict().get(config_flag)
                    for config_flag in profiling_flags_to_report
                },
            )
            self.init_spark()

    def init_spark(self):
        os.environ.setdefault("SPARK_VERSION", "3.5")
        spark_version = os.environ["SPARK_VERSION"]

        # Importing here to avoid Deequ dependency for non profiling use cases
        # Deequ fails if Spark is not available which is not needed for non profiling use cases
        import pydeequ

        conf = SparkConf()
        conf.set(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.hadoop:hadoop-aws:3.0.3",
                    # Spark's avro version needs to be matched with the Spark version
                    f"org.apache.spark:spark-avro_2.12:{spark_version}{'.0' if spark_version.count('.') == 1 else ''}",
                    pydeequ.deequ_maven_coord,
                ]
            ),
        )

        if self.source_config.aws_config is not None:
            credentials = self.source_config.aws_config.get_credentials()

            aws_access_key_id = credentials.get("aws_access_key_id")
            aws_secret_access_key = credentials.get("aws_secret_access_key")
            aws_session_token = credentials.get("aws_session_token")

            aws_provided_credentials = [
                aws_access_key_id,
                aws_secret_access_key,
                aws_session_token,
            ]

            if any(x is not None for x in aws_provided_credentials):
                # see https://hadoop.apache.org/docs/r3.0.3/hadoop-aws/tools/hadoop-aws/index.html#Changing_Authentication_Providers
                if all(x is not None for x in aws_provided_credentials):
                    conf.set(
                        "spark.hadoop.fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
                    )

                else:
                    conf.set(
                        "spark.hadoop.fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                    )

                if aws_access_key_id is not None:
                    conf.set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
                if aws_secret_access_key is not None:
                    conf.set(
                        "spark.hadoop.fs.s3a.secret.key",
                        aws_secret_access_key,
                    )
                if aws_session_token is not None:
                    conf.set(
                        "spark.hadoop.fs.s3a.session.token",
                        aws_session_token,
                    )
            else:
                # if no explicit AWS config is provided, use a default AWS credentials provider
                conf.set(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
                )

            if self.source_config.aws_config.aws_endpoint_url is not None:
                conf.set(
                    "fs.s3a.endpoint", self.source_config.aws_config.aws_endpoint_url
                )
            if self.source_config.aws_config.aws_region is not None:
                conf.set(
                    "fs.s3a.endpoint.region", self.source_config.aws_config.aws_region
                )

        conf.set("spark.jars.excludes", pydeequ.f2j_maven_coord)
        conf.set("spark.driver.memory", self.source_config.spark_driver_memory)

        if self.source_config.spark_config:
            for key, value in self.source_config.spark_config.items():
                conf.set(key, value)
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()

    @classmethod
    def create(cls, config_dict, ctx):
        config = DataLakeSourceConfig.parse_obj(config_dict)

        return cls(config, ctx)

    def read_file_spark(self, file: str, ext: str) -> Optional[DataFrame]:
        logger.debug(f"Opening file {file} for profiling in spark")
        if "s3://" in file:
            # replace s3:// with s3a://, and make sure standalone bucket names always end with a slash.
            # Spark will fail if given a path like `s3a://mybucket`, and requires it to be `s3a://mybucket/`.
            file = f"s3a://{get_bucket_name(file)}/{get_bucket_relative_path(file)}"

        telemetry.telemetry_instance.ping("data_lake_file", {"extension": ext})

        if ext.endswith(".parquet"):
            df = self.spark.read.parquet(file)
        elif ext.endswith(".csv"):
            # see https://sparkbyexamples.com/pyspark/pyspark-read-csv-file-into-dataframe
            df = self.spark.read.csv(
                file,
                header="True",
                inferSchema="True",
                sep=",",
                ignoreLeadingWhiteSpace=True,
                ignoreTrailingWhiteSpace=True,
            )
        elif ext.endswith(".tsv"):
            df = self.spark.read.csv(
                file,
                header="True",
                inferSchema="True",
                sep="\t",
                ignoreLeadingWhiteSpace=True,
                ignoreTrailingWhiteSpace=True,
            )
        elif ext.endswith(".json") or ext.endswith(".jsonl"):
            df = self.spark.read.json(file)
        elif ext.endswith(".avro"):
            try:
                df = self.spark.read.format("avro").load(file)
            except AnalysisException as e:
                self.report.report_warning(
                    file,
                    f"Avro file reading failed with exception. The error was: {e}",
                )
                return None

        # TODO: add support for more file types
        # elif file.endswith(".orc"):
        # df = self.spark.read.orc(file)
        else:
            self.report.report_warning(file, f"file {file} has unsupported extension")
            return None
        logger.debug(f"dataframe read for file {file} with row count {df.count()}")
        # replace periods in names because they break PyDeequ
        # see https://mungingdata.com/pyspark/avoid-dots-periods-column-names/
        return df.toDF(*(c.replace(".", "_") for c in df.columns))

    def get_fields(self, table_data: TableData, path_spec: PathSpec) -> List:
        if self.is_s3_platform():
            if self.source_config.aws_config is None:
                raise ValueError("AWS config is required for S3 file sources")

            s3_client = self.source_config.aws_config.get_s3_client(
                self.source_config.verify_ssl
            )

            file = smart_open(
                table_data.full_path, "rb", transport_params={"client": s3_client}
            )
        else:
            # We still use smart_open here to take advantage of the compression
            # capabilities of smart_open.
            file = smart_open(table_data.full_path, "rb")

        extension = pathlib.Path(table_data.full_path).suffix
        from datahub.ingestion.source.data_lake_common.path_spec import (
            SUPPORTED_COMPRESSIONS,
        )

        if path_spec.enable_compression and (extension[1:] in SUPPORTED_COMPRESSIONS):
            # Removing the compression extension and using the one before that like .json.gz -> .json
            extension = pathlib.Path(table_data.full_path).with_suffix("").suffix
        if extension == "" and path_spec.default_extension:
            extension = f".{path_spec.default_extension}"

        fields = []
        inferrer = self._get_inferrer(extension, table_data.content_type)
        if inferrer:
            try:
                fields = inferrer.infer_schema(file)
                logger.debug(f"Extracted fields in schema: {fields}")
            except Exception as e:
                self.report.report_warning(
                    table_data.full_path,
                    f"could not infer schema for file {table_data.full_path}: {e}",
                )
        else:
            self.report.report_warning(
                table_data.full_path,
                f"file {table_data.full_path} has unsupported extension",
            )
        file.close()

        if self.source_config.sort_schema_fields:
            fields = sorted(fields, key=lambda f: f.fieldPath)

        if self.source_config.add_partition_columns_to_schema and table_data.partitions:
            add_partition_columns_to_schema(
                fields=fields, path_spec=path_spec, full_path=table_data.full_path
            )

        return fields

    def _get_inferrer(
        self, extension: str, content_type: Optional[str]
    ) -> Optional[SchemaInferenceBase]:
        if content_type == "application/vnd.apache.parquet":
            return parquet.ParquetInferrer()
        elif content_type == "text/csv":
            return csv_tsv.CsvInferrer(max_rows=self.source_config.max_rows)
        elif content_type == "text/tab-separated-values":
            return csv_tsv.TsvInferrer(max_rows=self.source_config.max_rows)
        elif content_type == "application/json":
            return json.JsonInferrer()
        elif content_type == "application/avro":
            return avro.AvroInferrer()
        elif extension == ".parquet":
            return parquet.ParquetInferrer()
        elif extension == ".csv":
            return csv_tsv.CsvInferrer(max_rows=self.source_config.max_rows)
        elif extension == ".tsv":
            return csv_tsv.TsvInferrer(max_rows=self.source_config.max_rows)
        elif extension == ".jsonl":
            return json.JsonInferrer(
                max_rows=self.source_config.max_rows, format="jsonl"
            )
        elif extension == ".json":
            return json.JsonInferrer()
        elif extension == ".avro":
            return avro.AvroInferrer()
        else:
            return None

    def get_table_profile(
        self, table_data: TableData, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        # Importing here to avoid Deequ dependency for non profiling use cases
        # Deequ fails if Spark is not available which is not needed for non profiling use cases
        from pydeequ.analyzers import AnalyzerContext

        from datahub.ingestion.source.s3.profiling import _SingleTableProfiler

        # read in the whole table with Spark for profiling
        table = None
        try:
            if table_data.partitions:
                table = self.read_file_spark(
                    table_data.table_path, os.path.splitext(table_data.full_path)[1]
                )
            else:
                table = self.read_file_spark(
                    table_data.full_path, os.path.splitext(table_data.full_path)[1]
                )
        except Exception as e:
            logger.error(e)

        # if table is not readable, skip
        if table is None:
            self.report.report_warning(
                table_data.display_name,
                f"unable to read table {table_data.display_name} from file {table_data.full_path}",
            )
            return

        with PerfTimer() as timer:
            # init PySpark analysis object
            logger.debug(
                f"Profiling {table_data.full_path}: reading file and computing nulls+uniqueness {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
            )
            table_profiler = _SingleTableProfiler(
                table,
                self.spark,
                self.source_config.profiling,
                self.report,
                table_data.full_path,
            )

            logger.debug(
                f"Profiling {table_data.full_path}: preparing profilers to run {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
            )
            # instead of computing each profile individually, we run them all in a single analyzer.run() call
            # we use a single call because the analyzer optimizes the number of calls to the underlying profiler
            # since multiple profiles reuse computations, this saves a lot of time
            table_profiler.prepare_table_profiles()

            # compute the profiles
            logger.debug(
                f"Profiling {table_data.full_path}: computing profiles {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
            )
            analysis_result = table_profiler.analyzer.run()
            analysis_metrics = AnalyzerContext.successMetricsAsDataFrame(
                self.spark, analysis_result
            )

            logger.debug(
                f"Profiling {table_data.full_path}: extracting profiles {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
            )
            table_profiler.extract_table_profiles(analysis_metrics)

            time_taken = timer.elapsed_seconds()

            logger.info(
                f"Finished profiling {table_data.full_path}; took {time_taken:.3f} seconds"
            )

            self.profiling_times_taken.append(time_taken)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=table_profiler.profile,
        ).as_workunit()

    def _create_table_operation_aspect(self, table_data: TableData) -> OperationClass:
        reported_time = int(time.time() * 1000)

        operation = OperationClass(
            timestampMillis=reported_time,
            lastUpdatedTimestamp=int(table_data.timestamp.timestamp() * 1000),
            # actor=make_user_urn(table_data.created_by),
            operationType=OperationTypeClass.UPDATE,
        )

        return operation

    def __create_partition_summary_aspect(
        self, partitions: List[Folder]
    ) -> Optional[PartitionsSummaryClass]:
        min_partition = min(partitions, key=lambda x: x.creation_time)
        max_partition = max(partitions, key=lambda x: x.creation_time)

        max_partition_summary: Optional[PartitionSummaryClass] = None

        max_partition_id = max_partition.partition_id_text()
        if max_partition_id is not None:
            max_partition_summary = PartitionSummaryClass(
                partition=max_partition_id,
                createdTime=int(max_partition.creation_time.timestamp() * 1000),
                lastModifiedTime=int(
                    max_partition.modification_time.timestamp() * 1000
                ),
            )

        min_partition_summary: Optional[PartitionSummaryClass] = None
        min_partition_id = min_partition.partition_id_text()
        if min_partition_id is not None:
            min_partition_summary = PartitionSummaryClass(
                partition=min_partition_id,
                createdTime=int(min_partition.creation_time.timestamp() * 1000),
                lastModifiedTime=int(
                    min_partition.modification_time.timestamp() * 1000
                ),
            )

        return PartitionsSummaryClass(
            maxPartition=max_partition_summary, minPartition=min_partition_summary
        )

    def get_external_url(self, table_data: TableData) -> Optional[str]:
        """
        Get the external URL for a table using the configured object store adapter.

        Args:
            table_data: Table data containing path information

        Returns:
            An external URL or None if not applicable
        """
        # The adapter handles all the URL generation with proper region handling
        return self.object_store_adapter.get_external_url(table_data)

    def ingest_table(
        self, table_data: TableData, path_spec: PathSpec
    ) -> Iterable[MetadataWorkUnit]:
        aspects: List[Optional[_Aspect]] = []

        logger.info(f"Extracting table schema from file: {table_data.full_path}")
        browse_path: str = (
            self.strip_s3_prefix(table_data.table_path)
            if self.is_s3_platform()
            else table_data.table_path.strip("/")
        )

        data_platform_urn = make_data_platform_urn(self.source_config.platform)
        logger.info(f"Creating dataset urn with name: {browse_path}")
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.source_config.platform,
            browse_path,
            self.source_config.platform_instance,
            self.source_config.env,
        )

        if self.source_config.platform_instance:
            data_platform_instance = DataPlatformInstanceClass(
                platform=data_platform_urn,
                instance=make_dataplatform_instance_urn(
                    self.source_config.platform, self.source_config.platform_instance
                ),
            )
            aspects.append(data_platform_instance)

        customProperties = {"schema_inferred_from": str(table_data.full_path)}

        min_partition: Optional[Folder] = None
        max_partition: Optional[Folder] = None
        if table_data.partitions:
            min_partition = min(table_data.partitions, key=lambda x: x.creation_time)
            max_partition = max(table_data.partitions, key=lambda x: x.creation_time)

        if not path_spec.sample_files:
            customProperties.update(
                {
                    "number_of_files": str(table_data.number_of_files),
                    "size_in_bytes": str(table_data.size_in_bytes),
                }
            )
        else:
            if table_data.partitions:
                customProperties.update(
                    {
                        "number_of_partitions": str(
                            len(table_data.partitions) if table_data.partitions else 0
                        ),
                    }
                )

        dataset_properties = DatasetPropertiesClass(
            description="",
            name=table_data.display_name,
            customProperties=customProperties,
            created=(
                TimeStamp(time=int(min_partition.creation_time.timestamp() * 1000))
                if min_partition
                else None
            ),
            lastModified=(
                TimeStamp(time=int(max_partition.modification_time.timestamp() * 1000))
                if max_partition
                else None
            ),
            externalUrl=self.get_external_url(table_data),
        )
        aspects.append(dataset_properties)
        if table_data.size_in_bytes > 0:
            try:
                fields = self.get_fields(table_data, path_spec)
                schema_metadata = SchemaMetadata(
                    schemaName=table_data.display_name,
                    platform=data_platform_urn,
                    version=0,
                    hash="",
                    fields=fields,
                    platformSchema=OtherSchemaClass(rawSchema=""),
                )
                aspects.append(schema_metadata)
            except Exception as e:
                logger.error(
                    f"Failed to extract schema from file {table_data.full_path}. The error was:{e}"
                )
        else:
            logger.info(
                f"Skipping schema extraction for empty file {table_data.full_path}"
            )

        if (
            self.source_config.use_s3_bucket_tags
            or self.source_config.use_s3_object_tags
        ):
            bucket = get_bucket_name(table_data.table_path)
            key_prefix = (
                get_key_prefix(table_data.table_path)
                if table_data.full_path == table_data.table_path
                else None
            )
            s3_tags = get_s3_tags(
                bucket,
                key_prefix,
                dataset_urn,
                self.source_config.aws_config,
                self.ctx,
                self.source_config.use_s3_bucket_tags,
                self.source_config.use_s3_object_tags,
                self.source_config.verify_ssl,
            )
            if s3_tags:
                aspects.append(s3_tags)

        operation = self._create_table_operation_aspect(table_data)
        aspects.append(operation)

        if table_data.partitions and self.source_config.generate_partition_aspects:
            aspects.append(
                self.__create_partition_summary_aspect(table_data.partitions)
            )

        for mcp in MetadataChangeProposalWrapper.construct_many(
            entityUrn=dataset_urn,
            aspects=aspects,
        ):
            yield mcp.as_workunit()

        yield from self.container_WU_creator.create_container_hierarchy(
            table_data.table_path, dataset_urn
        )

        if self.source_config.is_profiling_enabled():
            yield from self.get_table_profile(table_data, dataset_urn)

    def get_prefix(self, relative_path: str) -> str:
        index = re.search(r"[\*|\{]", relative_path)
        if index:
            return relative_path[: index.start()]
        else:
            return relative_path

    def extract_table_name(self, path_spec: PathSpec, named_vars: dict) -> str:
        if path_spec.table_name is None:
            raise ValueError("path_spec.table_name is not set")
        return path_spec.table_name.format_map(named_vars)

    def extract_table_data(
        self,
        path_spec: PathSpec,
        browse_path: BrowsePath,
    ) -> TableData:
        path = browse_path.file
        partitions = browse_path.partitions
        logger.debug(f"Getting table data for path: {path}")
        table_name, table_path = path_spec.extract_table_name_and_path(path)
        return TableData(
            display_name=table_name,
            is_s3=self.is_s3_platform(),
            full_path=path,
            partitions=partitions,
            max_partition=partitions[-1] if partitions else None,
            min_partition=partitions[0] if partitions else None,
            timestamp=browse_path.timestamp,
            table_path=table_path,
            number_of_files=1,
            size_in_bytes=(
                browse_path.size
                if browse_path.size
                else sum(
                    [
                        partition.size if partition.size else 0
                        for partition in partitions
                    ]
                )
            ),
            content_type=browse_path.content_type,
        )

    def resolve_templated_folders(self, prefix: str) -> Iterable[str]:
        folder_split: List[str] = prefix.split("*", 1)
        # If the len of split is 1 it means we don't have * in the prefix
        if len(folder_split) == 1:
            yield prefix
            return

        basename_startswith = folder_split[0].split("/")[-1]
        dirname = folder_split[0].removesuffix(basename_startswith)

        folders = list_folders_path(
            dirname,
            startswith=basename_startswith,
            aws_config=self.source_config.aws_config,
        )
        for folder in folders:
            # Ensure proper path joining - folders from list_folders path never include a
            # trailing slash, but we need to handle the case where folder_split[1] might
            # start with a slash
            remaining_pattern = folder_split[1]
            if remaining_pattern.startswith("/"):
                remaining_pattern = remaining_pattern[1:]

            yield from self.resolve_templated_folders(
                f"{folder.path}/{remaining_pattern}"
            )

    def get_dir_to_process(
        self,
        bucket_name: str,
        folder: str,
        path_spec: PathSpec,
        protocol: str,
        min: bool = False,
    ) -> List[str]:
        # if len(path_spec.include.split("/")) == len(f"{protocol}{bucket_name}/{folder}".split("/")):
        #    return [f"{protocol}{bucket_name}/{folder}"]

        iterator = list_folders(
            bucket_name=bucket_name,
            prefix=folder,
            aws_config=self.source_config.aws_config,
        )
        iterator = peekable(iterator)
        if iterator:
            sorted_dirs = sorted(
                iterator,
                key=functools.cmp_to_key(partitioned_folder_comparator),
                reverse=not min,
            )
            folders = []
            for dir in sorted_dirs:
                if path_spec.dir_allowed(f"{protocol}{bucket_name}/{dir}/"):
                    folders_list = self.get_dir_to_process(
                        bucket_name=bucket_name,
                        folder=dir + "/",
                        path_spec=path_spec,
                        protocol=protocol,
                        min=min,
                    )
                    folders.extend(folders_list)
                    if path_spec.traversal_method != FolderTraversalMethod.ALL:
                        return folders
            if folders:
                return folders
            else:
                return [f"{protocol}{bucket_name}/{folder}"]
        return [f"{protocol}{bucket_name}/{folder}"]

    def get_folder_info(
        self,
        path_spec: PathSpec,
        bucket: "Bucket",
        prefix: str,
    ) -> Iterable[Folder]:
        """
        Retrieves all the folders in a path by listing all the files in the prefix.
        If the prefix is a full path then only that folder will be extracted.

        A folder has creation and modification times, size, and a sample file path.
        - Creation time is the earliest creation time of all files in the folder.
        - Modification time is the latest modification time of all files in the folder.
        - Size is the sum of all file sizes in the folder.
        - Sample file path is used for schema inference later. (sample file is the latest created file in the folder)

        Parameters:
        path_spec (PathSpec): The path specification used to determine partitioning.
        bucket (Bucket): The S3 bucket object.
        prefix (str): The prefix path in the S3 bucket to list objects from.

        Returns:
        List[Folder]: A list of Folder objects representing the partitions found.
        """

        def _is_allowed_path(path_spec_: PathSpec, s3_uri: str) -> bool:
            # Normalize URI for pattern matching
            normalized_uri = self._normalize_uri_for_pattern_matching(s3_uri)

            allowed = path_spec_.allowed(normalized_uri)
            if not allowed:
                logger.debug(f"File {s3_uri} not allowed and skipping")
                self.report.report_file_dropped(s3_uri)
            return allowed

        # Process objects in a memory-efficient streaming fashion
        # Instead of loading all objects into memory, we'll accumulate folder data incrementally
        folder_data: Dict[str, FolderInfo] = {}  # dirname -> FolderInfo

        for obj in list_objects_recursive(
            bucket.name, prefix, self.source_config.aws_config
        ):
            s3_path = self.create_s3_path(obj.bucket_name, obj.key)

            if not _is_allowed_path(path_spec, s3_path):
                continue

            # Extract the directory name (folder) from the object key
            dirname = obj.key.rsplit("/", 1)[0]

            # Initialize folder data if we haven't seen this directory before
            if dirname not in folder_data:
                folder_data[dirname] = FolderInfo(
                    objects=[],
                    total_size=0,
                    min_time=obj.last_modified,
                    max_time=obj.last_modified,
                    latest_obj=obj,
                )

            # Update folder statistics incrementally
            folder_info = folder_data[dirname]
            folder_info.objects.append(obj)
            folder_info.total_size += obj.size

            # Track min/max times and latest object
            if obj.last_modified < folder_info.min_time:
                folder_info.min_time = obj.last_modified
            if obj.last_modified > folder_info.max_time:
                folder_info.max_time = obj.last_modified
                folder_info.latest_obj = obj

        # Yield folders after processing all objects
        for _dirname, folder_info in folder_data.items():
            latest_obj = folder_info.latest_obj
            max_file_s3_path = self.create_s3_path(
                latest_obj.bucket_name, latest_obj.key
            )

            # If partition_id is None, it means the folder is not a partition
            partition_id = path_spec.get_partition_from_path(max_file_s3_path)

            yield Folder(
                partition_id=partition_id,
                is_partition=bool(partition_id),
                creation_time=folder_info.min_time,
                modification_time=folder_info.max_time,
                sample_file=max_file_s3_path,
                size=folder_info.total_size,
            )

    def create_s3_path(self, bucket_name: str, key: str) -> str:
        return f"s3://{bucket_name}/{key}"

    def s3_browser(self, path_spec: PathSpec, sample_size: int) -> Iterable[BrowsePath]:
        """
        Main entry point for browsing S3 objects and creating table-level datasets.

        This method determines whether to use templated processing (for paths with {table})
        or simple file-by-file processing (for paths without templates).

        Args:
            path_spec: Configuration specifying the S3 path pattern to scan
            sample_size: Number of files to sample (used in simple processing)

        Returns:
            Iterator of BrowsePath objects representing datasets to be created

        Examples:
            - Templated: s3://bucket/data/*/{table}/** -> Groups files by table
            - Simple: s3://bucket/data/*.csv -> Processes individual files
        """
        if self.source_config.aws_config is None:
            raise ValueError("aws_config not set. Cannot browse s3")

        logger.info(f"Processing path spec: {path_spec.include}")

        # Check if we have {table} template in the path
        has_table_template = "{table}" in path_spec.include

        logger.info(f"Has table template: {has_table_template}")

        if has_table_template:
            logger.info("Using templated path processing")
            # Always use templated processing when {table} is present
            # This groups files under table-level datasets
            yield from self._process_templated_path(path_spec)
        else:
            logger.info("Using simple path processing")
            # Only use simple processing for non-templated paths
            # This creates individual file-level datasets
            yield from self._process_simple_path(path_spec)

    def _process_templated_path(self, path_spec: PathSpec) -> Iterable[BrowsePath]:  # noqa: C901
        """
        Process S3 paths containing {table} templates to create table-level datasets.

        This method handles complex path patterns with wildcards and templates by:
        1. Replacing template placeholders with stars (except {table})
        2. Resolving wildcards in the path up to the {table} marker
        3. Finding all potential table folders under each resolved path
        4. Applying configurable partition traversal strategy (ALL, MAX, MIN_MAX)
        5. Aggregating files from selected partitions under each table
        6. Creating one dataset per table (not per file)

        Args:
            path_spec: Path specification with {table} template

        Yields:
            BrowsePath: One per table (not per file), containing aggregated metadata
        """

        if self.source_config.aws_config is None:
            raise ValueError("aws_config not set. Cannot browse s3")
        s3 = self.source_config.aws_config.get_s3_resource(
            self.source_config.verify_ssl
        )

        # Find the part before {table}
        table_marker = "{table}"
        if table_marker not in path_spec.include:
            logger.info("No {table} marker found in path")
            return

        # STEP 1: Replace template placeholders with stars (except {table}) to enable folder resolution
        # This is the crucial missing logic from the original implementation
        matches = re.finditer(r"{\s*\w+\s*}", path_spec.include, re.MULTILINE)
        matches_list = list(matches)

        if matches_list:
            # Replace all templates with stars except keep {table} as the marker
            max_start: int = -1
            include: str = path_spec.include
            max_match: str = ""

            for match in matches_list:
                pos = include.find(match.group())
                if pos > max_start:
                    if max_match:
                        include = include.replace(max_match, "*")
                    max_start = match.start()
                    max_match = match.group()
                    # We stop at {table}
                    if max_match == "{table}":
                        break

            logger.info(f"Template replacement: {path_spec.include} -> {include}")
        else:
            include = path_spec.include

        # Split the path at {table} to get the prefix that needs wildcard resolution
        prefix_before_table = include.split(table_marker)[0]
        logger.info(f"Prefix before table: {prefix_before_table}")

        try:
            # STEP 2: Resolve ALL wildcards in the path up to {table}
            # This converts patterns like "s3://data/*/logs/" to actual paths like ["s3://data/2023/logs/", "s3://data/2024/logs/"]
            resolved_prefixes = list(
                self.resolve_templated_folders(prefix_before_table)
            )
            logger.info(f"Resolved prefixes: {resolved_prefixes}")

            # STEP 3: Process each resolved prefix to find table folders
            for resolved_prefix in resolved_prefixes:
                logger.info(f"Processing resolved prefix: {resolved_prefix}")

                # Get all folders that could be tables under this resolved prefix
                # These are the actual table names (e.g., "users", "events", "logs")
                table_folders = list(
                    list_folders_path(
                        resolved_prefix, aws_config=self.source_config.aws_config
                    )
                )
                logger.debug(
                    f"Found table folders under {resolved_prefix}: {[folder.name for folder in table_folders]}"
                )

                # STEP 4: Process each table folder to create a table-level dataset
                for folder in table_folders:
                    bucket_name = get_bucket_name(folder.path)
                    table_folder = get_bucket_relative_path(folder.path)
                    bucket = s3.Bucket(bucket_name)

                    # Create the full S3 path for this table
                    table_s3_path = self.create_s3_path(bucket_name, table_folder)
                    logger.info(
                        f"Processing table folder: {table_folder} -> {table_s3_path}"
                    )

                    # Extract table name using the ORIGINAL path spec pattern matching (not the modified one)
                    # This uses the compiled regex pattern to extract the table name from the full path
                    table_name, table_path = path_spec.extract_table_name_and_path(
                        table_s3_path
                    )

                    # Apply table name filtering if configured
                    if not path_spec.tables_filter_pattern.allowed(table_name):
                        logger.debug(f"Table '{table_name}' not allowed and skipping")
                        continue

                    # STEP 5: Handle partition traversal based on configuration
                    # Get all partition folders first
                    all_partition_folders = list(
                        list_folders(
                            bucket_name, table_folder, self.source_config.aws_config
                        )
                    )
                    logger.info(
                        f"Found {len(all_partition_folders)} partition folders under table {table_name} using method {path_spec.traversal_method}"
                    )

                    if all_partition_folders:
                        # Apply the same traversal logic as the original code
                        dirs_to_process = []

                        if path_spec.traversal_method == FolderTraversalMethod.ALL:
                            # Process ALL partitions (original behavior)
                            dirs_to_process = all_partition_folders
                            logger.debug(
                                f"Processing ALL {len(all_partition_folders)} partitions"
                            )

                        else:
                            # Use the original get_dir_to_process logic for MIN/MAX
                            protocol = "s3://"  # Default protocol for S3

                            if (
                                path_spec.traversal_method
                                == FolderTraversalMethod.MIN_MAX
                                or path_spec.traversal_method
                                == FolderTraversalMethod.MAX
                            ):
                                # Get MAX partition using original logic
                                dirs_to_process_max = self.get_dir_to_process(
                                    bucket_name=bucket_name,
                                    folder=table_folder + "/",
                                    path_spec=path_spec,
                                    protocol=protocol,
                                    min=False,
                                )
                                if dirs_to_process_max:
                                    # Convert full S3 paths back to relative paths for processing
                                    dirs_to_process.extend(
                                        [
                                            d.replace(f"{protocol}{bucket_name}/", "")
                                            for d in dirs_to_process_max
                                        ]
                                    )
                                    logger.debug(
                                        f"Added MAX partition: {dirs_to_process_max}"
                                    )

                            if (
                                path_spec.traversal_method
                                == FolderTraversalMethod.MIN_MAX
                            ):
                                # Get MIN partition using original logic
                                dirs_to_process_min = self.get_dir_to_process(
                                    bucket_name=bucket_name,
                                    folder=table_folder + "/",
                                    path_spec=path_spec,
                                    protocol=protocol,
                                    min=True,
                                )
                                if dirs_to_process_min:
                                    # Convert full S3 paths back to relative paths for processing
                                    dirs_to_process.extend(
                                        [
                                            d.replace(f"{protocol}{bucket_name}/", "")
                                            for d in dirs_to_process_min
                                        ]
                                    )
                                    logger.debug(
                                        f"Added MIN partition: {dirs_to_process_min}"
                                    )

                        # Process the selected partitions
                        all_folders = []
                        for partition_folder in dirs_to_process:
                            # Ensure we have a clean folder path
                            clean_folder = partition_folder.rstrip("/")

                            logger.info(f"Scanning files in partition: {clean_folder}")
                            partition_files = list(
                                self.get_folder_info(path_spec, bucket, clean_folder)
                            )
                            all_folders.extend(partition_files)

                        if all_folders:
                            # Use the most recent file across all processed partitions
                            latest_file = max(
                                all_folders, key=lambda x: x.modification_time
                            )

                            # Get partition information
                            partitions = [f for f in all_folders if f.is_partition]

                            # Calculate total size of processed partitions
                            total_size = sum(f.size for f in all_folders)

                            # Create ONE BrowsePath per table
                            # The key insight: we need to provide the sample file for schema inference
                            # but the table path should be extracted correctly by extract_table_name_and_path
                            yield BrowsePath(
                                file=latest_file.sample_file,  # Sample file for schema inference
                                timestamp=latest_file.modification_time,  # Latest timestamp
                                size=total_size,  # Size of processed partitions
                                partitions=partitions,  # Partition metadata
                            )
                        else:
                            logger.warning(
                                f"No files found in processed partitions for table {table_name}"
                            )
                    else:
                        logger.warning(
                            f"No partition folders found under table {table_name}"
                        )

        except Exception as e:
            if isinstance(e, s3.meta.client.exceptions.NoSuchBucket):
                self.get_report().report_warning(
                    "Missing bucket",
                    f"No bucket found {e.response['Error'].get('BucketName')}",
                )
                return
            logger.error(f"Error in _process_templated_path: {e}")
            raise e

    def _process_simple_path(self, path_spec: PathSpec) -> Iterable[BrowsePath]:
        """
        Process simple S3 paths without {table} templates to create file-level datasets.

        This method handles straightforward file patterns by:
        1. Listing all files matching the pattern
        2. Creating one dataset per file
        3. No aggregation or grouping is performed

        Use Cases:
        - Individual file processing: s3://bucket/data/*.csv
        - Direct file paths: s3://bucket/data/myfile.json
        - Patterns without table grouping: s3://bucket/logs/*.log

        Args:
            path_spec: Path specification without {table} template

        Yields:
            BrowsePath: One per file, containing individual file metadata

        Example Output:
            - BrowsePath(file="data/file1.csv", size=1000, partitions=[])
            - BrowsePath(file="data/file2.csv", size=2000, partitions=[])
        """

        if self.source_config.aws_config is None:
            raise ValueError("aws_config not set")
        s3 = self.source_config.aws_config.get_s3_resource(
            self.source_config.verify_ssl
        )

        path_spec.sample_files = False  # Disable sampling for simple paths

        # Extract the prefix from the path spec (stops at first wildcard)
        prefix = self.get_prefix(path_spec.include)

        basename_startswith = prefix.split("/")[-1]
        dirname = prefix.removesuffix(basename_startswith)

        # Iterate through all objects in the bucket matching the prefix
        for obj in list_objects_recursive_path(
            dirname,
            startswith=basename_startswith,
            aws_config=self.source_config.aws_config,
        ):
            s3_path = self.create_s3_path(obj.bucket_name, obj.key)

            # Get content type if configured
            content_type = None
            if self.source_config.use_s3_content_type:
                content_type = s3.Object(obj.bucket_name, obj.key).content_type

            # Create one BrowsePath per file
            yield BrowsePath(
                file=s3_path,
                timestamp=obj.last_modified,
                size=obj.size,
                partitions=[],  # No partitions in simple mode
                content_type=content_type,
            )

    def local_browser(self, path_spec: PathSpec) -> Iterable[BrowsePath]:
        prefix = self.get_prefix(path_spec.include)
        if os.path.isfile(prefix):
            logger.debug(f"Scanning single local file: {prefix}")
            yield BrowsePath(
                file=prefix,
                timestamp=datetime.utcfromtimestamp(os.path.getmtime(prefix)),
                size=os.path.getsize(prefix),
                partitions=[],
            )
        else:
            logger.debug(f"Scanning files under local folder: {prefix}")
            for root, dirs, files in os.walk(prefix):
                dirs.sort(key=functools.cmp_to_key(partitioned_folder_comparator))

                for file in sorted(files):
                    # We need to make sure the path is in posix style which is not true on windows
                    full_path = PurePath(
                        os.path.normpath(os.path.join(root, file))
                    ).as_posix()
                    yield BrowsePath(
                        file=full_path,
                        timestamp=datetime.utcfromtimestamp(
                            os.path.getmtime(full_path)
                        ),
                        size=os.path.getsize(full_path),
                        partitions=[],
                    )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        with PerfTimer() as timer:
            assert self.source_config.path_specs
            for path_spec in self.source_config.path_specs:
                file_browser = (
                    self.s3_browser(
                        path_spec, self.source_config.number_of_files_to_sample
                    )
                    if self.is_s3_platform()
                    else self.local_browser(path_spec)
                )
                table_dict: Dict[str, TableData] = {}
                for browse_path in file_browser:
                    # Normalize URI for pattern matching
                    normalized_file_path = self._normalize_uri_for_pattern_matching(
                        browse_path.file
                    )

                    if not path_spec.allowed(
                        normalized_file_path,
                        ignore_ext=self.is_s3_platform()
                        and self.source_config.use_s3_content_type,
                    ):
                        continue
                    table_data = self.extract_table_data(path_spec, browse_path)
                    if table_data.table_path not in table_dict:
                        table_dict[table_data.table_path] = table_data
                    else:
                        table_dict[table_data.table_path].number_of_files = (
                            table_dict[table_data.table_path].number_of_files + 1
                        )
                        table_dict[table_data.table_path].size_in_bytes = (
                            table_dict[table_data.table_path].size_in_bytes
                            + table_data.size_in_bytes
                        )
                        if (
                            table_dict[table_data.table_path].timestamp
                            < table_data.timestamp
                        ) and (table_data.size_in_bytes > 0):
                            table_dict[
                                table_data.table_path
                            ].full_path = table_data.full_path
                            table_dict[
                                table_data.table_path
                            ].timestamp = table_data.timestamp

                for _, table_data in table_dict.items():
                    yield from self.ingest_table(table_data, path_spec)

            if not self.source_config.is_profiling_enabled():
                return

            total_time_taken = timer.elapsed_seconds()

            logger.info(
                f"Profiling {len(self.profiling_times_taken)} table(s) finished in {total_time_taken:.3f} seconds"
            )

            time_percentiles: Dict[str, float] = {}

            if len(self.profiling_times_taken) > 0:
                percentiles = [50, 75, 95, 99]
                percentile_values = stats.calculate_percentiles(
                    self.profiling_times_taken, percentiles
                )

                time_percentiles = {
                    f"table_time_taken_p{percentile}": stats.discretize(
                        percentile_values[percentile]
                    )
                    for percentile in percentiles
                }

            telemetry.telemetry_instance.ping(
                "data_lake_profiling_summary",
                # bucket by taking floor of log of time taken
                {
                    "total_time_taken": stats.discretize(total_time_taken),
                    "count": stats.discretize(len(self.profiling_times_taken)),
                    "platform": self.source_config.platform,
                    **time_percentiles,
                },
            )

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def is_s3_platform(self):
        return self.source_config.platform == "s3"

    def strip_s3_prefix(self, s3_uri: str) -> str:
        """Strip S3 prefix from URI. Can be overridden by adapters for other platforms."""
        return strip_s3_prefix(s3_uri)

    def _normalize_uri_for_pattern_matching(self, uri: str) -> str:
        """Normalize URI for pattern matching. Can be overridden by adapters for other platforms."""
        return uri

    def get_report(self):
        return self.report
