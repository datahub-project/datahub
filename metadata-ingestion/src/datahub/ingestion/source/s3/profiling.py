import dataclasses
import logging
import os
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from pandas import DataFrame
from pydeequ.analyzers import (
    AnalysisRunBuilder,
    AnalysisRunner,
    AnalyzerContext,
    ApproxCountDistinct,
    ApproxQuantile,
    ApproxQuantiles,
    Histogram,
    Maximum,
    Mean,
    Minimum,
    StandardDeviation,
)
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, count, isnan, when
from pyspark.sql.types import (
    DataType as SparkDataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NullType,
    ShortType,
    StringType,
    TimestampType,
)

from datahub.emitter.mce_builder import get_sys_time
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_util import (
    get_bucket_name,
    get_bucket_relative_path,
)
from datahub.ingestion.source.profiling.common import (
    Cardinality,
    convert_to_cardinality,
)
from datahub.ingestion.source.s3.datalake_profiler_config import DataLakeProfilerConfig
from datahub.ingestion.source.s3.report import DataLakeSourceReport
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
    HistogramClass,
    QuantileClass,
    ValueFrequencyClass,
)
from datahub.telemetry import stats, telemetry
from datahub.utilities.perf_timer import PerfTimer

NUM_SAMPLE_ROWS = 20
QUANTILES = [0.05, 0.25, 0.5, 0.75, 0.95]
MAX_HIST_BINS = 25

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)


def null_str(value: Any) -> Optional[str]:
    # str() with a passthrough for None.
    return str(value) if value is not None else None


@dataclasses.dataclass
class _SingleColumnSpec:
    column: str
    column_profile: DatasetFieldProfileClass

    # if the histogram is a list of value frequencies (discrete data) or bins (continuous data)
    histogram_distinct: Optional[bool] = None

    type_: SparkDataType = NullType  # type:ignore

    unique_count: Optional[int] = None
    non_null_count: Optional[int] = None
    cardinality: Optional[Cardinality] = None


class _SingleTableProfiler:
    spark: "SparkSession"
    dataframe: DataFrame
    analyzer: AnalysisRunBuilder
    column_specs: List[_SingleColumnSpec]
    row_count: int
    profiling_config: DataLakeProfilerConfig
    file_path: str
    columns_to_profile: List[str]
    ignored_columns: List[str]
    profile: DatasetProfileClass
    report: DataLakeSourceReport

    def __init__(
        self,
        dataframe: DataFrame,
        spark: "SparkSession",
        profiling_config: DataLakeProfilerConfig,
        report: DataLakeSourceReport,
        file_path: str,
    ):
        self.spark = spark
        self.dataframe = dataframe
        self.analyzer = AnalysisRunner(spark).onData(dataframe)
        self.column_specs = []
        self.row_count = dataframe.count()
        self.profiling_config = profiling_config
        self.file_path = file_path
        self.columns_to_profile = []
        self.ignored_columns = []
        self.profile = DatasetProfileClass(timestampMillis=get_sys_time())
        self.report = report

        self.profile.rowCount = self.row_count
        self.profile.columnCount = len(dataframe.columns)

        column_types = {x.name: x.dataType for x in dataframe.schema.fields}

        if self.profiling_config.profile_table_level_only:
            return

        # get column distinct counts
        for column in dataframe.columns:
            if not self.profiling_config._allow_deny_patterns.allowed(column):
                self.ignored_columns.append(column)
                continue

            self.columns_to_profile.append(column)
            # Normal CountDistinct is ridiculously slow
            self.analyzer.addAnalyzer(ApproxCountDistinct(column))

        if self.profiling_config.max_number_of_fields_to_profile is not None:
            if (
                len(self.columns_to_profile)
                > self.profiling_config.max_number_of_fields_to_profile
            ):
                columns_being_dropped = self.columns_to_profile[
                    self.profiling_config.max_number_of_fields_to_profile :
                ]
                self.columns_to_profile = self.columns_to_profile[
                    : self.profiling_config.max_number_of_fields_to_profile
                ]

                self.report.report_file_dropped(
                    f"The max_number_of_fields_to_profile={self.profiling_config.max_number_of_fields_to_profile} reached. Profile of columns {self.file_path}({', '.join(sorted(columns_being_dropped))})"
                )

        analysis_result = self.analyzer.run()
        analysis_metrics = AnalyzerContext.successMetricsAsJson(
            self.spark, analysis_result
        )

        # reshape distinct counts into dictionary
        column_distinct_counts = {
            x["instance"]: int(x["value"])
            for x in analysis_metrics
            if x["name"] == "ApproxCountDistinct"
        }

        select_numeric_null_counts = [
            count(
                when(
                    isnan(c) | col(c).isNull(),
                    c,
                )
            ).alias(c)
            for c in self.columns_to_profile
            if column_types[column] in [DoubleType, FloatType]
        ]

        # PySpark doesn't support isnan() on non-float/double columns
        select_nonnumeric_null_counts = [
            count(
                when(
                    col(c).isNull(),
                    c,
                )
            ).alias(c)
            for c in self.columns_to_profile
            if column_types[column] not in [DoubleType, FloatType]
        ]

        null_counts = dataframe.select(
            select_numeric_null_counts + select_nonnumeric_null_counts
        )
        column_null_counts = null_counts.toPandas().T[0].to_dict()
        column_null_fractions = {
            c: column_null_counts[c] / self.row_count if self.row_count != 0 else 0
            for c in self.columns_to_profile
        }
        column_nonnull_counts = {
            c: self.row_count - column_null_counts[c] for c in self.columns_to_profile
        }

        column_unique_proportions = {
            c: (
                column_distinct_counts[c] / column_nonnull_counts[c]
                if column_nonnull_counts[c] > 0
                else 0
            )
            for c in self.columns_to_profile
        }

        if self.profiling_config.include_field_sample_values:
            # take sample and convert to Pandas DataFrame
            if self.row_count < NUM_SAMPLE_ROWS:
                # if row count is less than number to sample, just take all rows
                rdd_sample = dataframe.rdd.take(self.row_count)
            else:
                rdd_sample = dataframe.rdd.takeSample(False, NUM_SAMPLE_ROWS, seed=0)

        # init column specs with profiles
        for column in self.columns_to_profile:
            column_profile = DatasetFieldProfileClass(fieldPath=column)

            column_spec = _SingleColumnSpec(column, column_profile)

            column_profile.uniqueCount = column_distinct_counts.get(column)
            column_profile.uniqueProportion = column_unique_proportions.get(column)
            column_profile.nullCount = column_null_counts.get(column)
            column_profile.nullProportion = column_null_fractions.get(column)
            if self.profiling_config.include_field_sample_values:
                column_profile.sampleValues = sorted(
                    [str(x[column]) for x in rdd_sample]
                )

            column_spec.type_ = column_types[column]
            column_spec.cardinality = convert_to_cardinality(
                column_distinct_counts[column],
                column_null_fractions[column],
            )

            self.column_specs.append(column_spec)

    def prep_min_value(self, column: str) -> None:
        if self.profiling_config.include_field_min_value:
            self.analyzer.addAnalyzer(Minimum(column))

    def prep_max_value(self, column: str) -> None:
        if self.profiling_config.include_field_max_value:
            self.analyzer.addAnalyzer(Maximum(column))

    def prep_mean_value(self, column: str) -> None:
        if self.profiling_config.include_field_mean_value:
            self.analyzer.addAnalyzer(Mean(column))

    def prep_median_value(self, column: str) -> None:
        if self.profiling_config.include_field_median_value:
            self.analyzer.addAnalyzer(ApproxQuantile(column, 0.5))

    def prep_stdev_value(self, column: str) -> None:
        if self.profiling_config.include_field_stddev_value:
            self.analyzer.addAnalyzer(StandardDeviation(column))

    def prep_quantiles(self, column: str) -> None:
        if self.profiling_config.include_field_quantiles:
            self.analyzer.addAnalyzer(ApproxQuantiles(column, QUANTILES))

    def prep_distinct_value_frequencies(self, column: str) -> None:
        if self.profiling_config.include_field_distinct_value_frequencies:
            self.analyzer.addAnalyzer(Histogram(column))

    def prep_field_histogram(self, column: str) -> None:
        if self.profiling_config.include_field_histogram:
            self.analyzer.addAnalyzer(Histogram(column, maxDetailBins=MAX_HIST_BINS))

    def prepare_table_profiles(self) -> None:
        row_count = self.row_count

        telemetry.telemetry_instance.ping(
            "profile_data_lake_table",
            {"rows_profiled": stats.discretize(row_count)},
        )

        # loop through the columns and add the analyzers
        for column_spec in self.column_specs:
            column = column_spec.column
            column_profile = column_spec.column_profile
            type_ = column_spec.type_
            cardinality = column_spec.cardinality

            non_null_count = column_spec.non_null_count
            unique_count = column_spec.unique_count

            if (
                self.profiling_config.include_field_null_count
                and non_null_count is not None
            ):
                null_count = row_count - non_null_count
                assert null_count >= 0
                column_profile.nullCount = null_count
                if row_count > 0:
                    column_profile.nullProportion = null_count / row_count

            if unique_count is not None:
                column_profile.uniqueCount = unique_count
                if non_null_count is not None and non_null_count > 0:
                    column_profile.uniqueProportion = unique_count / non_null_count

            if isinstance(
                type_,
                (
                    DecimalType,
                    DoubleType,
                    FloatType,
                    IntegerType,
                    LongType,
                    ShortType,
                ),
            ):
                if cardinality == Cardinality.UNIQUE:
                    pass
                elif cardinality in [
                    Cardinality.ONE,
                    Cardinality.TWO,
                    Cardinality.VERY_FEW,
                    Cardinality.FEW,
                ]:
                    column_spec.histogram_distinct = True
                    self.prep_distinct_value_frequencies(column)
                elif cardinality in [
                    Cardinality.MANY,
                    Cardinality.VERY_MANY,
                    Cardinality.UNIQUE,
                ]:
                    column_spec.histogram_distinct = False
                    self.prep_min_value(column)
                    self.prep_max_value(column)
                    self.prep_mean_value(column)
                    self.prep_median_value(column)
                    self.prep_stdev_value(column)
                    self.prep_quantiles(column)
                    self.prep_field_histogram(column)
                else:  # unknown cardinality - skip
                    pass

            elif isinstance(type_, StringType):
                if cardinality in [
                    Cardinality.ONE,
                    Cardinality.TWO,
                    Cardinality.VERY_FEW,
                    Cardinality.FEW,
                ]:
                    column_spec.histogram_distinct = True
                    self.prep_distinct_value_frequencies(
                        column,
                    )

            elif isinstance(type_, (DateType, TimestampType)):
                self.prep_min_value(column)
                self.prep_max_value(column)

                # FIXME: Re-add histogram once kl_divergence has been modified to support datetimes

                if cardinality in [
                    Cardinality.ONE,
                    Cardinality.TWO,
                    Cardinality.VERY_FEW,
                    Cardinality.FEW,
                ]:
                    self.prep_distinct_value_frequencies(
                        column,
                    )

    def extract_table_profiles(
        self,
        analysis_metrics: DataFrame,
    ) -> None:
        self.profile.fieldProfiles = []

        analysis_metrics = analysis_metrics.toPandas()
        # DataFrame with following columns:
        #   entity: "Column" for column profile, "Table" for table profile
        #   instance: name of column being profiled. "*" for table profiles
        #   name: name of metric. Histogram metrics are formatted as "Histogram.<metric>.<value>"
        #   value: value of metric

        column_metrics = analysis_metrics[analysis_metrics["entity"] == "Column"]

        # resolve histogram types for grouping
        column_metrics["kind"] = column_metrics["name"].apply(
            lambda x: "Histogram" if x.startswith("Histogram.") else x
        )

        column_histogram_metrics = column_metrics[column_metrics["kind"] == "Histogram"]
        column_nonhistogram_metrics = column_metrics[
            column_metrics["kind"] != "Histogram"
        ]

        histogram_columns = set()

        if len(column_histogram_metrics) > 0:
            # we only want the absolute counts for each histogram for now
            column_histogram_metrics = column_histogram_metrics[
                column_histogram_metrics["name"].apply(
                    lambda x: x.startswith("Histogram.abs.")
                )
            ]
            # get the histogram bins by chopping off the "Histogram.abs." prefix
            column_histogram_metrics["bin"] = column_histogram_metrics["name"].apply(
                lambda x: x[14:]
            )

            # reshape histogram counts for easier access
            histogram_counts = column_histogram_metrics.set_index(["instance", "bin"])[
                "value"
            ]

            histogram_columns = set(histogram_counts.index.get_level_values(0))

        profiled_columns = set()

        if len(column_nonhistogram_metrics) > 0:
            # reshape other metrics for easier access
            nonhistogram_metrics = column_nonhistogram_metrics.set_index(
                ["instance", "name"]
            )["value"]

            profiled_columns = set(nonhistogram_metrics.index.get_level_values(0))
        # histogram_columns = set(histogram_counts.index.get_level_values(0))

        for column_spec in self.column_specs:
            column = column_spec.column
            column_profile = column_spec.column_profile

            if column not in profiled_columns:
                continue

            # convert to Dict so we can use .get
            deequ_column_profile = nonhistogram_metrics.loc[column].to_dict()

            # uniqueCount, uniqueProportion, nullCount, nullProportion, sampleValues already set in TableWrapper
            column_profile.min = null_str(deequ_column_profile.get("Minimum"))
            column_profile.max = null_str(deequ_column_profile.get("Maximum"))
            column_profile.mean = null_str(deequ_column_profile.get("Mean"))
            column_profile.median = null_str(
                deequ_column_profile.get("ApproxQuantiles-0.5")
            )
            column_profile.stdev = null_str(
                deequ_column_profile.get("StandardDeviation")
            )
            if all(
                deequ_column_profile.get(f"ApproxQuantiles-{quantile}") is not None
                for quantile in QUANTILES
            ):
                column_profile.quantiles = [
                    QuantileClass(
                        quantile=str(quantile),
                        value=str(deequ_column_profile[f"ApproxQuantiles-{quantile}"]),
                    )
                    for quantile in QUANTILES
                ]

            if column in histogram_columns:
                column_histogram = histogram_counts.loc[column]
                # sort so output is deterministic
                column_histogram = column_histogram.sort_index()

                if column_spec.histogram_distinct:
                    column_profile.distinctValueFrequencies = [
                        ValueFrequencyClass(
                            value=value, frequency=int(column_histogram.loc[value])
                        )
                        for value in column_histogram.index
                    ]
                    # sort so output is deterministic
                    column_profile.distinctValueFrequencies = sorted(
                        column_profile.distinctValueFrequencies, key=lambda x: x.value
                    )

                else:
                    column_profile.histogram = HistogramClass(
                        [str(x) for x in column_histogram.index],
                        [float(x) for x in column_histogram],
                    )

            # append the column profile to the dataset profile
            self.profile.fieldProfiles.append(column_profile)


class SparkProfiler:
    """
    Handles Spark-based profiling for S3/data lake sources.

    This class encapsulates all PySpark and Deequ dependencies, allowing them to be
    optional for non-profiling use cases.
    """

    def __init__(
        self,
        aws_config: Optional[AwsConnectionConfig],
        spark_driver_memory: str,
        spark_config: Dict[str, Any],
        report: DataLakeSourceReport,
        profiling_times_taken: List[float],
        profiling_config: DataLakeProfilerConfig,
    ):
        # TODO: Try to avoid passing a list reference here (limiting side effects)
        self.profiling_times_taken = profiling_times_taken
        self.profiling_config = profiling_config
        self.aws_config = aws_config
        self.spark_driver_memory = spark_driver_memory
        self.spark_config = spark_config
        self.report = report
        self.spark = self.init_spark()

    def init_spark(self) -> "SparkSession":
        """Initialize Spark session with appropriate configuration for S3 access."""
        # Importing here to avoid PySpark/Deequ dependency for non profiling use cases
        import pydeequ
        from pyspark.conf import SparkConf
        from pyspark.sql import SparkSession

        spark_version = os.environ["SPARK_VERSION"]

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

        if self.aws_config is not None:
            credentials = self.aws_config.get_credentials()

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

            if self.aws_config.aws_endpoint_url is not None:
                conf.set("fs.s3a.endpoint", self.aws_config.aws_endpoint_url)
            if self.aws_config.aws_region is not None:
                conf.set("fs.s3a.endpoint.region", self.aws_config.aws_region)

        conf.set("spark.jars.excludes", pydeequ.f2j_maven_coord)
        conf.set("spark.driver.memory", self.spark_driver_memory)

        if self.spark_config:
            for key, value in self.spark_config.items():
                conf.set(key, value)

        return SparkSession.builder.config(conf=conf).getOrCreate()

    def read_file_spark(self, file: str, ext: str) -> Optional["SparkDataFrame"]:
        """Read a file using Spark and return a DataFrame."""
        from pyspark.sql.utils import AnalysisException

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

    def get_table_profile(
        self, table_data: Any, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate profiling metadata for a table.

        Args:
            table_data: TableData object containing information about the table
            dataset_urn: URN for the dataset

        Yields:
            MetadataWorkUnit containing profile information
        """
        # Importing here to avoid Deequ dependency for non profiling use cases
        from pydeequ.analyzers import AnalyzerContext

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
                self.profiling_config,
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
