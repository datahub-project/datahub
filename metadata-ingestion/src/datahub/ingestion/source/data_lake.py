import dataclasses
import logging
import os
from dataclasses import field as dataclass_field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional

import boto3
import pydantic
import pydeequ
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
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, count, isnan, when
from pyspark.sql.types import ArrayType, BinaryType, BooleanType, ByteType
from pyspark.sql.types import DataType as SparkDataType
from pyspark.sql.types import (
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

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.emitter.mce_builder import DEFAULT_ENV, get_sys_time
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import AwsSourceConfig
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetFieldProfileClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    HistogramClass,
    MapTypeClass,
    OtherSchemaClass,
    QuantileClass,
    ValueFrequencyClass,
)

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)


NUM_SAMPLE_ROWS = 3
QUANTILES = [0.05, 0.25, 0.5, 0.75, 0.95]
MAX_HIST_BINS = 25


class Cardinality(Enum):
    NONE = 0
    ONE = 1
    TWO = 2
    VERY_FEW = 3
    FEW = 4
    MANY = 5
    VERY_MANY = 6
    UNIQUE = 7


def _convert_to_cardinality(
    unique_count: Optional[int], pct_unique: Optional[float]
) -> Optional[Cardinality]:
    """
    Resolve the cardinality of a column based on the unique count and the percentage of unique values.

    Logic adopted from Great Expectations.
    See https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/profile/base.py

    Args:
        unique_count: raw number of unique values
        pct_unique: raw proportion of unique values

    Returns:
        Optional[Cardinality]: resolved cardinality
    """

    if unique_count is None:
        return Cardinality.NONE

    if pct_unique == 1.0:
        cardinality = Cardinality.UNIQUE
    elif unique_count == 1:
        cardinality = Cardinality.ONE
    elif unique_count == 2:
        cardinality = Cardinality.TWO
    elif 0 < unique_count < 20:
        cardinality = Cardinality.VERY_FEW
    elif 0 < unique_count < 60:
        cardinality = Cardinality.FEW
    elif unique_count is None or unique_count == 0 or pct_unique is None:
        cardinality = Cardinality.NONE
    elif pct_unique > 0.1:
        cardinality = Cardinality.VERY_MANY
    else:
        cardinality = Cardinality.MANY
    return cardinality


def null_str(value: Any) -> Optional[str]:
    # str() with a passthrough for None.
    return str(value) if value is not None else None


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


def get_column_type(
    report: SourceReport, dataset_name: str, column_type: str
) -> SchemaFieldDataType:
    """
    Maps known Spark types to datahub types
    """
    TypeClass: Any = _field_type_mapping.get(column_type)

    # if still not found, report the warning
    if TypeClass is None:

        report.report_warning(
            dataset_name, f"unable to map type {column_type} to metadata schema"
        )
        TypeClass = NullTypeClass

    return SchemaFieldDataType(type=TypeClass())


@dataclasses.dataclass
class _SingleColumnSpec:
    column: str
    column_profile: DatasetFieldProfileClass

    # if the histogram is a list of value frequencies (discrete data) or bins (continuous data)
    histogram_distinct: Optional[bool] = None

    type_: SparkDataType = NullType

    unique_count: Optional[int] = None
    non_null_count: Optional[int] = None
    cardinality: Optional[Cardinality] = None


class DataLakeProfilerConfig(ConfigModel):
    enabled: bool = False

    # These settings will override the ones below.
    turn_off_expensive_profiling_metrics: bool = False
    profile_table_level_only: bool = False

    allow_deny_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()

    include_field_null_count: bool = True
    include_field_min_value: bool = True
    include_field_max_value: bool = True
    include_field_mean_value: bool = True
    include_field_median_value: bool = True
    include_field_stddev_value: bool = True
    include_field_quantiles: bool = True
    include_field_distinct_value_frequencies: bool = True
    include_field_histogram: bool = True
    include_field_sample_values: bool = True

    max_number_of_fields_to_profile: Optional[pydantic.PositiveInt] = None

    @pydantic.root_validator()
    def ensure_field_level_settings_are_normalized(
        cls: "DataLakeProfilerConfig", values: Dict[str, Any]
    ) -> Dict[str, Any]:
        max_num_fields_to_profile_key = "max_number_of_fields_to_profile"
        table_level_profiling_only_key = "profile_table_level_only"
        max_num_fields_to_profile = values.get(max_num_fields_to_profile_key)
        if values.get(table_level_profiling_only_key):
            all_field_level_metrics: List[str] = [
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
            # Suppress all field-level metrics
            for field_level_metric in all_field_level_metrics:
                values[field_level_metric] = False
            assert (
                max_num_fields_to_profile is None
            ), f"{max_num_fields_to_profile_key} should be set to None"

        if values.get("turn_off_expensive_profiling_metrics"):
            if not values.get(table_level_profiling_only_key):
                expensive_field_level_metrics: List[str] = [
                    "include_field_quantiles",
                    "include_field_distinct_value_frequencies",
                    "include_field_histogram",
                    "include_field_sample_values",
                ]
                for expensive_field_metric in expensive_field_level_metrics:
                    values[expensive_field_metric] = False
            if max_num_fields_to_profile is None:
                # We currently profile up to 10 non-filtered columns in this mode by default.
                values[max_num_fields_to_profile_key] = 10

        return values


class DataLakeSourceConfig(ConfigModel):

    env: str = DEFAULT_ENV
    platform: str
    base_path: str

    aws_config: Optional[AwsSourceConfig] = None

    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    profile_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()

    profiling: DataLakeProfilerConfig = DataLakeProfilerConfig()

    @pydantic.root_validator()
    def ensure_profiling_pattern_is_passed_to_profiling(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        profiling = values.get("profiling")
        if profiling is not None and profiling.enabled:
            profiling.allow_deny_patterns = values["profile_pattern"]
        return values


@dataclasses.dataclass
class DataLakeSourceReport(SourceReport):
    files_scanned = 0
    filtered: List[str] = dataclass_field(default_factory=list)

    def report_file_scanned(self) -> None:
        self.files_scanned += 1

    def report_file_dropped(self, file: str) -> None:
        self.filtered.append(file)


class _SingleTableProfiler:
    spark: SparkSession
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
        spark: SparkSession,
        source_config: DataLakeProfilerConfig,
        report: DataLakeSourceReport,
        file_path: str,
    ):
        self.spark = spark
        self.dataframe = dataframe
        self.analyzer = AnalysisRunner(spark).onData(dataframe)
        self.column_specs = []
        self.row_count = dataframe.count()
        self.profiling_config = source_config
        self.file_path = file_path
        self.columns_to_profile = []
        self.ignored_columns = []
        self.profile = DatasetProfileClass(timestampMillis=get_sys_time())
        self.report = report

        self.profile.rowCount = self.row_count
        self.profile.columnCount = len(dataframe.columns)

        if self.profiling_config.profile_table_level_only:

            return

        # get column distinct counts
        for column in dataframe.columns:

            if not self.profiling_config.allow_deny_patterns.allowed(column):
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

        # compute null counts and fractions manually since Deequ only reports fractions
        # cast to integer to allow isnan() – this is what Deequ does for completeness
        # see https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/Completeness.scala
        # (this works for strings somehow)
        null_counts = dataframe.select(
            [
                count(
                    when(
                        isnan(col(c).astype("int")) | col(c).isNull(),
                        c,
                    )
                ).alias(c)
                for c in self.columns_to_profile
            ]
        )
        column_null_counts = null_counts.toPandas().T[0].to_dict()
        column_null_fractions = {
            c: column_null_counts[c] / self.row_count for c in self.columns_to_profile
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
            rdd_sample = dataframe.rdd.takeSample(False, NUM_SAMPLE_ROWS, seed=0)

        column_types = {x.name: x.dataType for x in dataframe.schema.fields}

        # init column specs with profiles
        for column in self.columns_to_profile:
            column_profile = DatasetFieldProfileClass(fieldPath=column)

            column_spec = _SingleColumnSpec(column, column_profile)

            column_profile.uniqueCount = column_distinct_counts.get(column)
            column_profile.uniqueProportion = column_unique_proportions.get(column)
            column_profile.nullCount = column_null_counts.get(column)
            column_profile.nullProportion = column_null_fractions.get(column)
            if self.profiling_config.include_field_sample_values:
                column_profile.sampleValues = [str(x[column]) for x in rdd_sample]

            column_spec.type_ = column_types[column]
            column_spec.cardinality = _convert_to_cardinality(
                column_distinct_counts[column],
                column_null_fractions[column],
            )

            self.column_specs.append(column_spec)

    def prep_min_value(self, column: str) -> None:
        if self.profiling_config.include_field_min_value:
            self.analyzer.addAnalyzer(Minimum(column))
        return

    def prep_max_value(self, column: str) -> None:
        if self.profiling_config.include_field_max_value:
            self.analyzer.addAnalyzer(Maximum(column))
        return

    def prep_mean_value(self, column: str) -> None:
        if self.profiling_config.include_field_mean_value:
            self.analyzer.addAnalyzer(Mean(column))
        return

    def prep_median_value(self, column: str) -> None:
        if self.profiling_config.include_field_median_value:
            self.analyzer.addAnalyzer(ApproxQuantile(column, 0.5))
        return

    def prep_stdev_value(self, column: str) -> None:
        if self.profiling_config.include_field_stddev_value:
            self.analyzer.addAnalyzer(StandardDeviation(column))
        return

    def prep_quantiles(self, column: str) -> None:
        if self.profiling_config.include_field_quantiles:
            self.analyzer.addAnalyzer(ApproxQuantiles(column, QUANTILES))
        return

    def prep_distinct_value_frequencies(self, column: str) -> None:
        if self.profiling_config.include_field_distinct_value_frequencies:
            self.analyzer.addAnalyzer(Histogram(column))
        return

    def prep_field_histogram(self, column: str) -> None:
        if self.profiling_config.include_field_histogram:
            self.analyzer.addAnalyzer(Histogram(column, maxDetailBins=MAX_HIST_BINS))
        return

    def prepare_table_profiles(self) -> None:

        row_count = self.row_count

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

                if column_spec.histogram_distinct:

                    column_profile.distinctValueFrequencies = [
                        ValueFrequencyClass(
                            value=value, frequency=int(column_histogram.loc[value])
                        )
                        for value in column_histogram.index
                    ]

                else:

                    column_profile.histogram = HistogramClass(
                        [str(x) for x in column_histogram.index],
                        [float(x) for x in column_histogram],
                    )

            # append the column profile to the dataset profile
            self.profile.fieldProfiles.append(column_profile)


class DataLakeSource(Source):
    source_config: DataLakeSourceConfig
    report = DataLakeSourceReport()

    def __init__(self, config: DataLakeSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config

        conf = SparkConf()

        conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.0.3")

        if self.source_config.aws_config is not None:
            conf.set(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
            )

            if self.source_config.aws_config.aws_access_key_id is not None:
                conf.set(
                    "fs.s3a.access.key", self.source_config.aws_config.aws_access_key_id
                )
            if self.source_config.aws_config.aws_secret_access_key is not None:
                conf.set(
                    "fs.s3a.secret.key",
                    self.source_config.aws_config.aws_secret_access_key,
                )
            if self.source_config.aws_config.aws_session_token is not None:
                conf.set(
                    "fs.s3a.session.token",
                    self.source_config.aws_config.aws_session_token,
                )
        else:
            # if no AWS config is provided, use a default AWS credentials provider
            conf.set(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
            )

        self.spark = (
            SparkSession.builder.config(
                "spark.jars.packages", pydeequ.deequ_maven_coord
            )
            .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
            .config("spark.driver.memory", "8g")
            .config(conf=conf)
            .getOrCreate()
        )

    @classmethod
    def create(cls, config_dict, ctx):
        config = DataLakeSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def read_file(self, file: str) -> Optional[DataFrame]:

        if file.endswith(".parquet"):
            df = self.spark.read.parquet(file)
        elif file.endswith(".csv"):
            # see https://sparkbyexamples.com/pyspark/pyspark-read-csv-file-into-dataframe
            df = self.spark.read.csv(
                file,
                header="True",
                inferSchema="True",
                sep=",",
                ignoreLeadingWhiteSpace=True,
                ignoreTrailingWhiteSpace=True,
            )
        elif file.endswith(".tsv"):
            df = self.spark.read.csv(
                file,
                header="True",
                inferSchema="True",
                sep="\t",
                ignoreLeadingWhiteSpace=True,
                ignoreTrailingWhiteSpace=True,
            )
        elif file.endswith(".json"):
            df = self.spark.read.json(file)
        elif file.endswith(".orc"):
            df = self.spark.read.orc(file)
        elif file.endswith(".avro"):
            df = self.spark.read.avro(file)
        else:
            self.report.report_warning(file, f"file {file} has unsupported extension")
            return None

        # replace periods in names because they break PyDeequ
        # see https://mungingdata.com/pyspark/avoid-dots-periods-column-names/
        return df.toDF(*(c.replace(".", "_") for c in df.columns))

    def get_table_schema(
        self, dataframe: DataFrame, file_path: str
    ) -> Iterable[MetadataWorkUnit]:

        datasetUrn = f"urn:li:dataset:(urn:li:dataPlatform:{self.source_config.platform},{file_path},{self.source_config.env})"
        dataset_snapshot = DatasetSnapshot(
            urn=datasetUrn,
            aspects=[],
        )

        dataset_properties = DatasetPropertiesClass(
            description="",
            customProperties={},
        )
        dataset_snapshot.aspects.append(dataset_properties)

        column_fields = []

        for field in dataframe.schema.fields:

            field = SchemaField(
                fieldPath=field.name,
                type=get_column_type(self.report, "test", field.dataType),
                nativeDataType=str(field.dataType),
                recursive=False,
            )

            column_fields.append(field)

        schema_metadata = SchemaMetadata(
            schemaName="test",
            platform=f"urn:li:dataPlatform:{self.source_config.platform}",
            version=0,
            hash="",
            fields=column_fields,
            platformSchema=OtherSchemaClass(rawSchema=""),
        )

        dataset_snapshot.aspects.append(schema_metadata)

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = MetadataWorkUnit(id=file_path, mce=mce)
        self.report.report_workunit(wu)
        yield wu

    def ingest_table(self, file: str) -> Iterable[MetadataWorkUnit]:

        table = self.read_file(file)

        # if table is not readable, skip
        if table is None:
            return

        # yield the table schema first
        logger.debug(
            f"Ingesting {file}: making table schemas {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
        )
        yield from self.get_table_schema(table, file)

        # If profiling is not enabled, skip the rest
        if not self.source_config.profiling.enabled:
            return

        # init PySpark analysis object
        logger.debug(
            f"Profiling {file}: reading file and computing nulls+uniqueness {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
        )
        table_profiler = _SingleTableProfiler(
            table, self.spark, self.source_config.profiling, self.report, file
        )

        # TODO: implement ignored columns and max number of fields to profile
        logger.debug(
            f"Profiling {file}: preparing profilers to run {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
        )
        table_profiler.prepare_table_profiles()

        # compute the profiles
        logger.debug(
            f"Profiling {file}: computing profiles {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
        )
        analysis_result = table_profiler.analyzer.run()
        analysis_metrics = AnalyzerContext.successMetricsAsDataFrame(
            self.spark, analysis_result
        )

        logger.debug(
            f"Profiling {file}: extracting profiles {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
        )
        table_profiler.extract_table_profiles(analysis_metrics)

        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:{self.source_config.platform},{file},{self.source_config.env})",
            changeType=ChangeTypeClass.UPSERT,
            aspectName="datasetProfile",
            aspect=table_profiler.profile,
        )
        wu = MetadataWorkUnit(
            id=f"profile-{self.source_config.platform}-{file}", mcp=mcp
        )
        self.report.report_workunit(wu)
        yield wu

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:

        s3_prefixes = ["s3://", "s3n://", "s3a://"]

        if any(
            self.source_config.base_path.startswith(s3_prefix)
            for s3_prefix in s3_prefixes
        ):

            for s3_prefix in s3_prefixes:
                if self.source_config.base_path.startswith(s3_prefix):
                    clean_path = self.source_config.base_path[len(s3_prefix) :]
                    break

            s3 = boto3.resource("s3")
            bucket = s3.Bucket(clean_path.split("/")[0])

            for obj in bucket.objects.filter(
                Prefix=clean_path.split("/", maxsplit=1)[1]
            ):

                # if the file is a directory, skip it
                if obj.key.endswith("/"):
                    continue

                obj_path = f"s3a://{obj.bucket_name}/{obj.key}"

                yield from self.ingest_table(obj_path)
        else:
            for root, dirs, files in os.walk(self.source_config.base_path):
                for file in files:
                    yield from self.ingest_table(os.path.join(root, file))

    def get_report(self):
        return self.report

    def close(self):
        pass
