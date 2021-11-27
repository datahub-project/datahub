import dataclasses
import logging
from dataclasses import field as dataclass_field
from enum import Enum
from typing import Any, Iterable, List, Optional

import pydantic
import pydeequ
from pydeequ.analyzers import (
    AnalysisRunBuilder,
    AnalysisRunner,
    AnalyzerContext,
    ApproxQuantile,
    ApproxQuantiles,
    CountDistinct,
    Histogram,
    Maximum,
    Mean,
    Minimum,
    StandardDeviation,
)
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

logging.getLogger("py4j").setLevel(logging.ERROR)


class FileType(Enum):
    PARQUET = "parquet"
    CSV = "csv"


class DataLakeSourceConfig(ConfigModel):

    env: str = DEFAULT_ENV

    file: str
    file_type: Optional[FileType] = None
    platform: str

    limit: Optional[int] = None
    offset: Optional[int] = None

    # These settings will override the ones below.
    turn_off_expensive_profiling_metrics: bool = False
    profile_table_level_only: bool = False

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

    allow_deny_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()
    max_number_of_fields_to_profile: Optional[pydantic.PositiveInt] = None


@dataclasses.dataclass
class DataLakeSourceReport(SourceReport):
    files_scanned = 0
    filtered: List[str] = dataclass_field(default_factory=list)

    def report_file_scanned(self) -> None:
        self.files_scanned += 1

    def report_file_dropped(self, file: str) -> None:
        self.filtered.append(file)


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
    # Logic adopted from Great Expectations.

    # See https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/profile/base.py

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


def null_str(value: Any) -> Optional[str]:
    # str() with a passthrough for None.
    return str(value) if value is not None else None


class TableWrapper:
    spark: SparkSession
    dataframe: DataFrame
    analyzer: AnalysisRunBuilder
    column_specs: List[_SingleColumnSpec]
    row_count: int

    def __init__(self, dataframe, spark):
        self.spark = spark
        self.dataframe = dataframe
        self.analyzer = AnalysisRunner(spark).onData(dataframe)
        self.column_specs = []
        self.row_count = dataframe.count()

        # get column distinct counts
        for column in dataframe.columns:
            # TODO: add option for ApproxCountDistinct
            self.analyzer.addAnalyzer(CountDistinct(column))
        analysis_result = self.analyzer.run()
        analysis_metrics = AnalyzerContext.successMetricsAsJson(
            self.spark, analysis_result
        )

        # reshape distinct counts into dictionary
        column_distinct_counts = {
            x["instance"]: int(x["value"])
            for x in analysis_metrics
            if x["name"] == "CountDistinct"
        }

        # compute null counts and fractions manually since Deequ only reports fractions
        # cast to integer to allow isnan() – this is what Deequ does for completeness
        # see https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/Completeness.scala
        # (this works for strings somehow)
        null_counts = dataframe.select(
            [
                count(when(isnan(col(c).astype("int")) | col(c).isNull(), c)).alias(c)
                for c in dataframe.columns
            ]
        )
        column_null_counts = null_counts.toPandas().T[0].to_dict()
        column_null_fractions = {
            c: column_null_counts[c] / self.row_count for c in dataframe.columns
        }
        column_nonnull_counts = {
            c: self.row_count - column_null_counts[c] for c in dataframe.columns
        }
        column_unique_proportions = {
            c: column_distinct_counts[c] / column_nonnull_counts[c]
            for c in dataframe.columns
        }

        # init column specs with profiles
        for column in dataframe.schema.fields:
            column_profile = DatasetFieldProfileClass(fieldPath=column.name)

            column_spec = _SingleColumnSpec(column.name, column_profile)

            column_profile.uniqueCount = column_distinct_counts.get(column.name)
            column_profile.uniqueProportion = column_unique_proportions.get(column.name)
            column_profile.nullCount = column_null_counts.get(column.name)
            column_profile.nullProportion = column_null_fractions.get(column.name)

            column_spec.type_ = column.dataType
            column_spec.cardinality = _convert_to_cardinality(
                column_distinct_counts[column.name], column_null_fractions[column.name]
            )

            self.column_specs.append(column_spec)


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

# _deequ_profiler_to_profile = {
#     "ApproxCountDistinct" : "uniqueCount",
#     "ApproxQuantile": "median",
#     "ApproxQuantiles": "quantiles",
#     "CountDistinct": "",
#     # "Histogram" : "histogram",
#     "Maximum" : "max",
#     "Mean" : "mean",
#     "Minimum" :
#     "StandardDeviation" :
# }

QUANTILES = [0.05, 0.25, 0.5, 0.75, 0.95]


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


class DataLakeSource(Source):
    source_config: DataLakeSourceConfig
    report = DataLakeSourceReport()

    def __init__(self, config: DataLakeSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.spark = (
            SparkSession.builder.config(
                "spark.jars.packages", pydeequ.deequ_maven_coord
            )
            .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
            .getOrCreate()
        )

    @classmethod
    def create(cls, config_dict, ctx):
        config = DataLakeSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def prep_min_value(self, table: TableWrapper, column: str) -> None:
        if self.source_config.include_field_min_value:
            table.analyzer.addAnalyzer(Minimum(column))
        return

    def prep_max_value(self, table: TableWrapper, column: str) -> None:
        if self.source_config.include_field_max_value:
            table.analyzer.addAnalyzer(Maximum(column))
        return

    def prep_mean_value(self, table: TableWrapper, column: str) -> None:
        if self.source_config.include_field_mean_value:
            table.analyzer.addAnalyzer(Mean(column))
        return

    def prep_median_value(self, table: TableWrapper, column: str) -> None:
        if self.source_config.include_field_median_value:
            table.analyzer.addAnalyzer(ApproxQuantile(column, 0.5))
        return

    def prep_stdev_value(self, table: TableWrapper, column: str) -> None:
        if self.source_config.include_field_stddev_value:
            table.analyzer.addAnalyzer(StandardDeviation(column))
        return

    def prep_quantiles(self, table: TableWrapper, column: str) -> None:
        if self.source_config.include_field_quantiles:
            table.analyzer.addAnalyzer(ApproxQuantiles(column, QUANTILES))
        return

    def prep_distinct_value_frequencies(self, table: TableWrapper, column: str) -> None:
        if self.source_config.include_field_distinct_value_frequencies:
            table.analyzer.addAnalyzer(Histogram(column))
        return

    def prep_field_histogram(self, table: TableWrapper, column: str) -> None:
        if self.source_config.include_field_histogram:
            table.analyzer.addAnalyzer(Histogram(column, maxDetailBins=100))
        return

    def calculate_sample_values(self, table: TableWrapper) -> None:
        num_rows_to_sample = 3

        if self.source_config.include_field_sample_values:
            rdd_sample = table.dataframe.rdd.takeSample(
                False, num_rows_to_sample, seed=0
            )

        return rdd_sample

    def read_file(self, file: str, file_type: Optional[FileType]) -> DataFrame:

        if file_type is FileType.PARQUET:
            df = self.spark.read.parquet(file)
        elif file_type is FileType.CSV:
            # see https://sparkbyexamples.com/pyspark/pyspark-read-csv-file-into-dataframe
            df = self.spark.read.csv(file, header="True", inferSchema="True")
        else:
            raise ValueError("File type not found")

        return df

    def get_table_schema(
        self, analysis_table: TableWrapper
    ) -> Iterable[MetadataWorkUnit]:

        datasetUrn = f"urn:li:dataset:(urn:li:dataPlatform:{self.source_config.platform},{self.source_config.file},{self.source_config.env})"
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

        for field in analysis_table.dataframe.schema.fields:

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
        wu = MetadataWorkUnit(id=self.source_config.file, mce=mce)
        self.report.report_workunit(wu)
        yield wu

    def generate_dataset_profile(self) -> Iterable[MetadataWorkUnit]:

        profile = DatasetProfileClass(timestampMillis=get_sys_time())
        profile.fieldProfiles = []

        table = self.read_file(self.source_config.file, self.source_config.file_type)

        # init PySpark analysis object
        analysis_table = TableWrapper(table, self.spark)

        profile.rowCount = analysis_table.row_count
        profile.columnCount = len(analysis_table.column_specs)

        # yield the table schema first
        yield from self.get_table_schema(analysis_table)

        # TODO: implement ignored columns and max number of fields to profile

        row_count = analysis_table.row_count

        # loop through the columns and add the analyzers
        for column_spec in analysis_table.column_specs:
            column = column_spec.column
            column_profile = column_spec.column_profile
            type_ = column_spec.type_
            cardinality = column_spec.cardinality

            non_null_count = column_spec.non_null_count
            unique_count = column_spec.unique_count

            if (
                self.source_config.include_field_null_count
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

            # TODO: get sample values
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
                    self.prep_distinct_value_frequencies(analysis_table, column)
                elif cardinality in [
                    Cardinality.MANY,
                    Cardinality.VERY_MANY,
                    Cardinality.UNIQUE,
                ]:
                    column_spec.histogram_distinct = False
                    self.prep_min_value(analysis_table, column)
                    self.prep_max_value(analysis_table, column)
                    self.prep_mean_value(analysis_table, column)
                    self.prep_median_value(analysis_table, column)
                    self.prep_stdev_value(analysis_table, column)
                    self.prep_quantiles(analysis_table, column)
                    self.prep_field_histogram(analysis_table, column)
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
                        analysis_table,
                        column,
                    )

            elif isinstance(type_, (DateType, TimestampType)):
                self.prep_min_value(analysis_table, column)
                self.prep_max_value(analysis_table, column)

                # FIXME: Re-add histogram once kl_divergence has been modified to support datetimes

                if cardinality in [
                    Cardinality.ONE,
                    Cardinality.TWO,
                    Cardinality.VERY_FEW,
                    Cardinality.FEW,
                ]:
                    self.prep_distinct_value_frequencies(
                        analysis_table,
                        column,
                    )

        # run the analysis
        analysis_result = analysis_table.analyzer.run()
        analysis_metrics = AnalyzerContext.successMetricsAsDataFrame(
            self.spark, analysis_result
        )
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
        # reshape other metrics for easier access
        nonhistogram_metrics = column_nonhistogram_metrics.set_index(
            ["instance", "name"]
        )["value"]

        profiled_columns = set(nonhistogram_metrics.index.get_level_values(0))
        # histogram_columns = set(histogram_counts.index.get_level_values(0))

        for column_spec in analysis_table.column_specs:
            column = column_spec.column
            column_profile = column_spec.column_profile

            if column not in profiled_columns:
                continue

            # breakpoint()

            # convert to Dict so we can use .get
            deequ_column_profile = nonhistogram_metrics.loc[column].to_dict()

            # uniqueCount, uniqueProportion, nullCount, nullProportion already set in TableWrapper
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

            if column in histogram_counts.index.get_level_values(0):

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

                # column_profile.distinctValueFrequencies = deequ_column_profile.get("DistinctValueFrequencies")
                # column_profile.histogram = deequ_column_profile.get("Histogram")

            # column_profile.sampleValues = deequ_column_profile.get("SampleValues")

            # append the column profile to the dataset profile
            profile.fieldProfiles.append(column_profile)

        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:{self.source_config.platform},{'test'},{self.source_config.env})",
            changeType=ChangeTypeClass.UPSERT,
            aspectName="datasetProfile",
            aspect=profile,
        )
        wu = MetadataWorkUnit(id=f"profile-{'test'}", mcp=mcp)
        self.report.report_workunit(wu)
        yield wu

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self.generate_dataset_profile()

    def get_report(self):
        return self.report

    def close(self):
        pass
