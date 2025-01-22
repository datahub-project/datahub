import dataclasses
import functools
import logging
import os
import pathlib
import re
import time
from datetime import datetime
from pathlib import PurePath
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

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
from datahub.ingestion.source.aws.s3_boto_utils import get_s3_tags, list_folders
from datahub.ingestion.source.aws.s3_util import (
    get_bucket_name,
    get_bucket_relative_path,
    get_key_prefix,
    strip_s3_prefix,
)
from datahub.ingestion.source.data_lake_common.data_lake_utils import ContainerWUCreator
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
    SchemaField,
    SchemaMetadata,
    StringTypeClass,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    OperationClass,
    OperationTypeClass,
    OtherSchemaClass,
    PartitionsSummaryClass,
    PartitionSummaryClass,
    SchemaFieldDataTypeClass,
    _Aspect,
)
from datahub.telemetry import stats, telemetry
from datahub.utilities.groupby import groupby_unsorted
from datahub.utilities.perf_timer import PerfTimer

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import Bucket

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)

PAGE_SIZE = 1000

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
@capability(SourceCapability.CONTAINERS, "Enabled by default")
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

    def __init__(self, config: DataLakeSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.report = DataLakeSourceReport()
        self.profiling_times_taken = []
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
        file = file.replace("s3://", "s3a://")

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
            self.add_partition_columns_to_schema(
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

    def add_partition_columns_to_schema(
        self, path_spec: PathSpec, full_path: str, fields: List[SchemaField]
    ) -> None:
        is_fieldpath_v2 = False
        for field in fields:
            if field.fieldPath.startswith("[version=2.0]"):
                is_fieldpath_v2 = True
                break
        partition_keys = path_spec.get_partition_from_path(full_path)
        if not partition_keys:
            return None

        for partition_key in partition_keys:
            fields.append(
                SchemaField(
                    fieldPath=(
                        f"{partition_key[0]}"
                        if not is_fieldpath_v2
                        else f"[version=2.0].[type=string].{partition_key[0]}"
                    ),
                    nativeDataType="string",
                    type=SchemaFieldDataTypeClass(StringTypeClass()),
                    isPartitioningKey=True,
                    nullable=True,
                    recursive=False,
                )
            )

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

    def ingest_table(
        self, table_data: TableData, path_spec: PathSpec
    ) -> Iterable[MetadataWorkUnit]:
        aspects: List[Optional[_Aspect]] = []

        logger.info(f"Extracting table schema from file: {table_data.full_path}")
        browse_path: str = (
            strip_s3_prefix(table_data.table_path)
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

    def resolve_templated_folders(self, bucket_name: str, prefix: str) -> Iterable[str]:
        folder_split: List[str] = prefix.split("*", 1)
        # If the len of split is 1 it means we don't have * in the prefix
        if len(folder_split) == 1:
            yield prefix
            return

        folders: Iterable[str] = list_folders(
            bucket_name, folder_split[0], self.source_config.aws_config
        )
        for folder in folders:
            yield from self.resolve_templated_folders(
                bucket_name, f"{folder}{folder_split[1]}"
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
                    if not path_spec.traversal_method == FolderTraversalMethod.ALL:
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
    ) -> List[Folder]:
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
        partitions: List[Folder] = []
        s3_objects = bucket.objects.filter(Prefix=prefix).page_size(PAGE_SIZE)
        grouped_s3_objects_by_dirname = groupby_unsorted(
            s3_objects,
            key=lambda obj: obj.key.rsplit("/", 1)[0],
        )
        for key, group in grouped_s3_objects_by_dirname:
            file_size = 0
            creation_time = None
            modification_time = None

            for item in group:
                file_path = self.create_s3_path(item.bucket_name, item.key)
                if not path_spec.allowed(file_path):
                    logger.debug(f"File {file_path} not allowed and skipping")
                    continue
                file_size += item.size
                if creation_time is None or item.last_modified < creation_time:
                    creation_time = item.last_modified
                if modification_time is None or item.last_modified > modification_time:
                    modification_time = item.last_modified
                    max_file = item

            if modification_time is None:
                logger.warning(
                    f"Unable to find any files in the folder {key}. Skipping..."
                )
                continue

            id = path_spec.get_partition_from_path(
                self.create_s3_path(max_file.bucket_name, max_file.key)
            )

            # If id is None, it means the folder is not a partition
            partitions.append(
                Folder(
                    partition_id=id,
                    is_partition=bool(id),
                    creation_time=creation_time if creation_time else None,  # type: ignore[arg-type]
                    modification_time=modification_time,
                    sample_file=self.create_s3_path(max_file.bucket_name, max_file.key),
                    size=file_size,
                )
            )

        return partitions

    def s3_browser(self, path_spec: PathSpec, sample_size: int) -> Iterable[BrowsePath]:
        if self.source_config.aws_config is None:
            raise ValueError("aws_config not set. Cannot browse s3")
        s3 = self.source_config.aws_config.get_s3_resource(
            self.source_config.verify_ssl
        )
        bucket_name = get_bucket_name(path_spec.include)
        logger.debug(f"Scanning bucket: {bucket_name}")
        bucket = s3.Bucket(bucket_name)
        prefix = self.get_prefix(get_bucket_relative_path(path_spec.include))
        logger.debug(f"Scanning objects with prefix:{prefix}")
        matches = re.finditer(r"{\s*\w+\s*}", path_spec.include, re.MULTILINE)
        matches_list = list(matches)
        if matches_list and path_spec.sample_files:
            # Replace the patch_spec include's templates with star because later we want to resolve all the stars
            # to actual directories.
            # For example:
            # "s3://my-test-bucket/*/{dept}/*/{table}/*/*.*" -> "s3://my-test-bucket/*/*/*/{table}/*/*.*"
            # We only keep the last template as a marker to know the point util we need to resolve path.
            # After the marker we can safely get sample files for sampling because it is not used in the
            # table name, so we don't need all the files.
            # This speed up processing but we won't be able to get a precise modification date/size/number of files.
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

            table_index = include.find(max_match)
            for folder in self.resolve_templated_folders(
                bucket_name, get_bucket_relative_path(include[:table_index])
            ):
                try:
                    for f in list_folders(
                        bucket_name, f"{folder}", self.source_config.aws_config
                    ):
                        dirs_to_process = []
                        logger.info(f"Processing folder: {f}")
                        if path_spec.traversal_method == FolderTraversalMethod.ALL:
                            dirs_to_process.append(f)
                        else:
                            if (
                                path_spec.traversal_method
                                == FolderTraversalMethod.MIN_MAX
                                or path_spec.traversal_method
                                == FolderTraversalMethod.MAX
                            ):
                                protocol = ContainerWUCreator.get_protocol(
                                    path_spec.include
                                )
                                dirs_to_process_max = self.get_dir_to_process(
                                    bucket_name=bucket_name,
                                    folder=f + "/",
                                    path_spec=path_spec,
                                    protocol=protocol,
                                )
                                dirs_to_process.append(dirs_to_process_max[0])

                            if (
                                path_spec.traversal_method
                                == FolderTraversalMethod.MIN_MAX
                            ):
                                dirs_to_process_min = self.get_dir_to_process(
                                    bucket_name=bucket_name,
                                    folder=f + "/",
                                    path_spec=path_spec,
                                    protocol=protocol,
                                    min=True,
                                )
                                dirs_to_process.append(dirs_to_process_min[0])
                        folders = []
                        for dir in dirs_to_process:
                            logger.info(f"Getting files from folder: {dir}")
                            prefix_to_process = urlparse(dir).path.lstrip("/")

                            folders.extend(
                                self.get_folder_info(
                                    path_spec, bucket, prefix_to_process
                                )
                            )
                        max_folder = None
                        if folders:
                            max_folder = max(folders, key=lambda x: x.modification_time)
                        if not max_folder:
                            logger.warning(
                                f"Unable to find any files in the folder {dir}. Skipping..."
                            )
                            continue

                        partitions = list(filter(lambda x: x.is_partition, folders))
                        yield BrowsePath(
                            file=max_folder.sample_file,
                            timestamp=max_folder.modification_time,
                            size=max_folder.size,
                            partitions=partitions,
                            # TODO: Support content type inference for partitions
                        )
                except Exception as e:
                    # This odd check if being done because boto does not have a proper exception to catch
                    # The exception that appears in stacktrace cannot actually be caught without a lot more work
                    # https://github.com/boto/boto3/issues/1195
                    if "NoSuchBucket" in repr(e):
                        logger.debug(f"Got NoSuchBucket exception for {bucket_name}", e)
                        self.get_report().report_warning(
                            "Missing bucket", f"No bucket found {bucket_name}"
                        )
                    else:
                        raise e
        else:
            logger.debug(
                "No template in the pathspec can't do sampling, fallbacking to do full scan"
            )
            path_spec.sample_files = False
            for obj in bucket.objects.filter(Prefix=prefix).page_size(PAGE_SIZE):
                s3_path = self.create_s3_path(obj.bucket_name, obj.key)
                logger.debug(f"Path: {s3_path}")

                content_type = None
                if self.source_config.use_s3_content_type:
                    content_type = s3.Object(obj.bucket_name, obj.key).content_type

                yield BrowsePath(
                    file=s3_path,
                    timestamp=obj.last_modified,
                    size=obj.size,
                    partitions=[],
                    content_type=content_type,
                )

    def create_s3_path(self, bucket_name: str, key: str) -> str:
        return f"s3://{bucket_name}/{key}"

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
        self.container_WU_creator = ContainerWUCreator(
            self.source_config.platform,
            self.source_config.platform_instance,
            self.source_config.env,
        )
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
                    if not path_spec.allowed(
                        browse_path.file,
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

    def get_report(self):
        return self.report
