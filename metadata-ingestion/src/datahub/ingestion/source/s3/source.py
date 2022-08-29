import dataclasses
import logging
import os
import pathlib
import re
from collections import OrderedDict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pydeequ
from pydeequ.analyzers import AnalyzerContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.utils import AnalysisException
from smart_open import open as smart_open

import datahub.ingestion.source.s3.config
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
from datahub.ingestion.source.aws.s3_boto_utils import get_s3_tags, list_folders
from datahub.ingestion.source.aws.s3_util import (
    get_bucket_name,
    get_bucket_relative_path,
    get_key_prefix,
    strip_s3_prefix,
)
from datahub.ingestion.source.data_lake.data_lake_utils import ContainerWUCreator
from datahub.ingestion.source.s3.config import DataLakeSourceConfig, PathSpec
from datahub.ingestion.source.s3.profiling import _SingleTableProfiler
from datahub.ingestion.source.s3.report import DataLakeSourceReport
from datahub.ingestion.source.schema_inference import avro, csv_tsv, json, parquet
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetPropertiesClass,
    MapTypeClass,
    OtherSchemaClass,
)
from datahub.telemetry import stats, telemetry
from datahub.utilities.perf_timer import PerfTimer

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)

# for a list of all types, see https://spark.apache.org/docs/3.0.3/api/python/_modules/pyspark/sql/types.html
_field_type_mapping = {
    NullType: NullTypeClass,
    StringType: StringTypeClass,
    BinaryType: BytesTypeClass,
    BooleanType: BooleanTypeClass,
    DateType: DateTypeClass,
    TimestampType: TimeTypeClass,
    DecimalType: NumberTypeClass,
    DoubleType: NumberTypeClass,
    FloatType: NumberTypeClass,
    ByteType: BytesTypeClass,
    IntegerType: NumberTypeClass,
    LongType: NumberTypeClass,
    ShortType: NumberTypeClass,
    ArrayType: NullTypeClass,
    MapType: MapTypeClass,
    StructField: RecordTypeClass,
    StructType: RecordTypeClass,
}
SAMPLE_SIZE = 100
PAGE_SIZE = 1000


def get_column_type(
    report: SourceReport, dataset_name: str, column_type: str
) -> SchemaFieldDataType:
    """
    Maps known Spark types to datahub types
    """
    TypeClass: Any = None

    for field_type, type_class in _field_type_mapping.items():
        if isinstance(column_type, field_type):
            TypeClass = type_class
            break

    # if still not found, report the warning
    if TypeClass is None:
        report.report_warning(
            dataset_name, f"unable to map type {column_type} to metadata schema"
        )
        TypeClass = NullTypeClass

    return SchemaFieldDataType(type=TypeClass())


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

S3_PREFIXES = ("s3://", "s3n://", "s3a://")


# LOCAL_BROWSE_PATH_TRANSFORMER_CONFIG = AddDatasetBrowsePathConfig(
#     path_templates=["/ENV/PLATFORMDATASET_PARTS"], replace_existing=True
# )
#
# LOCAL_BROWSE_PATH_TRANSFORMER = AddDatasetBrowsePathTransformer(
#     ctx=None, config=LOCAL_BROWSE_PATH_TRANSFORMER_CONFIG
# )


@dataclasses.dataclass
class TableData:
    display_name: str
    is_s3: bool
    full_path: str
    partitions: Optional[OrderedDict]
    timestamp: datetime
    table_path: str
    size_in_bytes: int
    number_of_files: int


@platform_name("S3 Data Lake", id="s3")
@config_class(DataLakeSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.TAGS, "Can extract S3 object/bucket tags if enabled")
class S3Source(Source):
    """
    This plugin extracts:

    - Row and column counts for each table
    - For each column, if profiling is enabled:
      - null counts and proportions
      - distinct counts and proportions
      - minimum, maximum, mean, median, standard deviation, some quantile values
      - histograms or frequencies of unique values

    This connector supports both local files as well as those stored on AWS S3 (which must be identified using the prefix `s3://`). Supported file types are as follows:

    - CSV
    - TSV
    - JSON
    - Parquet
    - Apache Avro

    Schemas for Parquet and Avro files are extracted as provided.

    Schemas for schemaless formats (CSV, TSV, JSON) are inferred. For CSV and TSV files, we consider the first 100 rows by default, which can be controlled via the `max_rows` recipe parameter (see [below](#config-details))
    JSON file schemas are inferred on the basis of the entire file (given the difficulty in extracting only the first few objects of the file), which may impact performance.
    We are working on using iterator-based JSON parsers to avoid reading in the entire JSON object.

    Note that because the profiling is run with PySpark, we require Spark 3.0.3 with Hadoop 3.2 to be installed (see [compatibility](#compatibility) for more details). If profiling, make sure that permissions for **s3a://** access are set because Spark and Hadoop use the s3a:// protocol to interface with AWS (schema inference outside of profiling requires s3:// access).
    Enabling profiling will slow down ingestion runs.
    """

    source_config: DataLakeSourceConfig
    report: DataLakeSourceReport
    profiling_times_taken: List[float]
    container_WU_creator: ContainerWUCreator

    def __init__(self, config: DataLakeSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.report = DataLakeSourceReport()
        self.profiling_times_taken = []
        config_report = {
            config_option: config.dict().get(config_option)
            for config_option in config_options_to_report
        }
        config_report = {**config_report, "profiling_enabled": config.profiling.enabled}

        telemetry.telemetry_instance.ping(
            "data_lake_config",
            config_report,
        )

        if config.profiling.enabled:
            telemetry.telemetry_instance.ping(
                "data_lake_profiling_config",
                {
                    config_flag: config.profiling.dict().get(config_flag)
                    for config_flag in profiling_flags_to_report
                },
            )
            self.init_spark()

    def init_spark(self):

        conf = SparkConf()

        conf.set(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.hadoop:hadoop-aws:3.0.3",
                    "org.apache.spark:spark-avro_2.12:3.0.3",
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

        conf.set("spark.jars.excludes", pydeequ.f2j_maven_coord)
        conf.set("spark.driver.memory", self.source_config.spark_driver_memory)

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
        elif ext.endswith(".json"):
            df = self.spark.read.json(file)
        elif ext.endswith(".avro"):
            try:
                df = self.spark.read.format("avro").load(file)
            except AnalysisException:
                self.report.report_warning(
                    file,
                    "To ingest avro files, please install the spark-avro package: https://mvnrepository.com/artifact/org.apache.spark/spark-avro_2.12/3.0.3",
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
        if table_data.is_s3:
            if self.source_config.aws_config is None:
                raise ValueError("AWS config is required for S3 file sources")

            s3_client = self.source_config.aws_config.get_s3_client()

            file = smart_open(
                table_data.full_path, "rb", transport_params={"client": s3_client}
            )
        else:

            file = open(table_data.full_path, "rb")

        fields = []

        extension = pathlib.Path(table_data.full_path).suffix
        if path_spec.enable_compression and (
            extension[1:]
            in datahub.ingestion.source.aws.path_spec.SUPPORTED_COMPRESSIONS
        ):
            # Removing the compression extension and using the one before that like .json.gz -> .json
            extension = pathlib.Path(table_data.full_path).with_suffix("").suffix
        if extension == "" and path_spec.default_extension:
            extension = f".{path_spec.default_extension}"

        try:
            if extension == ".parquet":
                fields = parquet.ParquetInferrer().infer_schema(file)
            elif extension == ".csv":
                fields = csv_tsv.CsvInferrer(
                    max_rows=self.source_config.max_rows
                ).infer_schema(file)
            elif extension == ".tsv":
                fields = csv_tsv.TsvInferrer(
                    max_rows=self.source_config.max_rows
                ).infer_schema(file)
            elif extension == ".json":
                fields = json.JsonInferrer().infer_schema(file)
            elif extension == ".avro":
                fields = avro.AvroInferrer().infer_schema(file)
            else:
                self.report.report_warning(
                    table_data.full_path,
                    f"file {table_data.full_path} has unsupported extension",
                )
            file.close()
        except Exception as e:
            self.report.report_warning(
                table_data.full_path,
                f"could not infer schema for file {table_data.full_path}: {e}",
            )
            file.close()
        logger.debug(f"Extracted fields in schema: {fields}")
        fields = sorted(fields, key=lambda f: f.fieldPath)

        return fields

    def get_table_profile(
        self, table_data: TableData, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
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

        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=dataset_urn,
            changeType=ChangeTypeClass.UPSERT,
            aspectName="datasetProfile",
            aspect=table_profiler.profile,
        )
        wu = MetadataWorkUnit(
            id=f"profile-{self.source_config.platform}-{table_data.table_path}", mcp=mcp
        )
        self.report.report_workunit(wu)
        yield wu

    def ingest_table(
        self, table_data: TableData, path_spec: PathSpec
    ) -> Iterable[MetadataWorkUnit]:

        logger.info(f"Extracting table schema from file: {table_data.full_path}")
        browse_path: str = (
            strip_s3_prefix(table_data.table_path)
            if table_data.is_s3
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

        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[],
        )

        customProperties: Optional[Dict[str, str]] = None
        if not path_spec.sample_files:
            customProperties = {
                "number_of_files": str(table_data.number_of_files),
                "size_in_bytes": str(table_data.size_in_bytes),
            }

        dataset_properties = DatasetPropertiesClass(
            description="",
            name=table_data.display_name,
            customProperties=customProperties,
        )
        dataset_snapshot.aspects.append(dataset_properties)

        fields = self.get_fields(table_data, path_spec)
        schema_metadata = SchemaMetadata(
            schemaName=table_data.display_name,
            platform=data_platform_urn,
            version=0,
            hash="",
            fields=fields,
            platformSchema=OtherSchemaClass(rawSchema=""),
        )
        dataset_snapshot.aspects.append(schema_metadata)
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
            )
            if s3_tags is not None:
                dataset_snapshot.aspects.append(s3_tags)

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = MetadataWorkUnit(id=table_data.table_path, mce=mce)
        self.report.report_workunit(wu)
        yield wu

        container_wus = self.container_WU_creator.create_container_hierarchy(
            table_data.table_path, table_data.is_s3, dataset_urn
        )
        for wu in container_wus:
            self.report.report_workunit(wu)
            yield wu

        if self.source_config.profiling.enabled:
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
        self, path_spec: PathSpec, path: str, timestamp: datetime, size: int
    ) -> TableData:

        logger.debug(f"Getting table data for path: {path}")
        table_name, table_path = path_spec.extract_table_name_and_path(path)
        table_data = None
        table_data = TableData(
            display_name=table_name,
            is_s3=path_spec.is_s3(),
            full_path=path,
            partitions=None,
            timestamp=timestamp,
            table_path=table_path,
            number_of_files=1,
            size_in_bytes=size,
        )
        return table_data

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

    def s3_browser(self, path_spec: PathSpec) -> Iterable[Tuple[str, datetime, int]]:
        if self.source_config.aws_config is None:
            raise ValueError("aws_config not set. Cannot browse s3")
        s3 = self.source_config.aws_config.get_s3_resource()
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

            table_index = include.find(max_match)
            for folder in self.resolve_templated_folders(
                bucket_name, get_bucket_relative_path(include[:table_index])
            ):
                for f in list_folders(
                    bucket_name, f"{folder}", self.source_config.aws_config
                ):
                    logger.info(f"Processing folder: {f}")

                    for obj in (
                        bucket.objects.filter(Prefix=f"{f}")
                        .page_size(PAGE_SIZE)
                        .limit(SAMPLE_SIZE)
                    ):
                        s3_path = f"s3://{obj.bucket_name}/{obj.key}"
                        logger.debug(f"Samping file: {s3_path}")
                        yield s3_path, obj.last_modified, obj.size,
        else:
            logger.debug(
                "No template in the pathspec can't do sampling, fallbacking to do full scan"
            )
            path_spec.sample_files = False
            for obj in bucket.objects.filter(Prefix=prefix).page_size(PAGE_SIZE):
                s3_path = f"s3://{obj.bucket_name}/{obj.key}"
                logger.debug(f"Path: {s3_path}")
                yield s3_path, obj.last_modified, obj.size,

    def local_browser(self, path_spec: PathSpec) -> Iterable[Tuple[str, datetime, int]]:
        prefix = self.get_prefix(path_spec.include)
        if os.path.isfile(prefix):
            logger.debug(f"Scanning single local file: {prefix}")
            yield prefix, datetime.utcfromtimestamp(
                os.path.getmtime(prefix)
            ), os.path.getsize(prefix)
        else:
            logger.debug(f"Scanning files under local folder: {prefix}")
            for root, dirs, files in os.walk(prefix):
                for file in sorted(files):
                    full_path = os.path.join(root, file)
                    yield full_path, datetime.utcfromtimestamp(
                        os.path.getmtime(full_path)
                    ), os.path.getsize(full_path)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        self.container_WU_creator = ContainerWUCreator(
            self.source_config.platform,
            self.source_config.platform_instance,
            self.source_config.env,
        )
        with PerfTimer() as timer:
            assert self.source_config.path_specs
            for path_spec in self.source_config.path_specs:
                file_browser = (
                    self.s3_browser(path_spec)
                    if self.source_config.platform == "s3"
                    else self.local_browser(path_spec)
                )
                table_dict: Dict[str, TableData] = {}
                for file, timestamp, size in file_browser:
                    if not path_spec.allowed(file):
                        continue
                    table_data = self.extract_table_data(
                        path_spec, file, timestamp, size
                    )
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
                        ):
                            table_dict[
                                table_data.table_path
                            ].timestamp = table_data.timestamp

                for guid, table_data in table_dict.items():
                    yield from self.ingest_table(table_data, path_spec)

            if not self.source_config.profiling.enabled:
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

    def get_report(self):
        return self.report

    def close(self):
        pass
