import dataclasses
import logging
import os
from dataclasses import field as dataclass_field
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional

import pydantic
import pydeequ
from pydeequ.analyzers import (
    AnalysisRunBuilder,
    AnalysisRunner,
    ApproxCountDistinct,
    ApproxQuantile,
    ApproxQuantiles,
    Histogram,
    Maximum,
    Mean,
    Minimum,
    Size,
    StandardDeviation,
)
from pyspark.sql import Row, SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, count, isnan, when

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import DatasetPropertiesClass

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
        self.file_scanned += 1

    def report_file_dropped(self, file: str) -> None:
        self.filtered.append(file)


class TableWrapper:
    spark: SparkSession
    dataframe: DataFrame
    analyzer: AnalysisRunBuilder
    columns: List[str]
    
    def __init__(self, table, spark):
        self.spark = spark
        self.dataframe = table
        self.analyzer = AnalysisRunner(spark).onData(table).addAnalyzer(Size())
        self.columns = table.columns


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

    def calculate_null_count(self, table: TableWrapper):
        if self.config.include_field_null_count:
            # Deequ only has methods for getting null fraction, so we use Spark to get null count
            null_counts = table.dataframe.select(
                [count(when(isnan(c), c)).alias(c) for c in table.columns]
            ).show()

        return

    def prep_min_value(self, table: TableWrapper):
        if self.config.include_field_min_value:
            for column in table.columns:
                table.analyzer.addAnalyzer(Minimum(column))
        return

    def prep_max_value(self, table: TableWrapper):
        if self.config.include_field_max_value:
            for column in table.columns:
                table.analyzer.addAnalyzer(Maximum(column))
        return

    def prep_mean_value(self, table: TableWrapper):
        if self.config.include_field_mean_value:
            for column in table.columns:
                table.analyzer.addAnalyzer(Mean(column))
        return

    def prep_median_value(self, table: TableWrapper):
        if self.config.include_field_median_value:
            for column in table.columns:
                table.analyzer.addAnalyzer(ApproxQuantile(column, 0.5))
        return

    def prep_stdev_value(self, table: TableWrapper):
        if self.config.include_field_stddev_value:
            for column in table.columns:
                table.analyzer.addAnalyzer(StandardDeviation(column))
        return

    def prep_quantiles(self, table: TableWrapper):
        if self.config.include_field_quantiles:
            for column in table.columns:
                table.analyzer.addAnalyzer(
                    ApproxQuantiles(column, [0.05, 0.25, 0.5, 0.75, 0.95])
                )
        return

    def prep_distinct_value_frequencies(self, table: TableWrapper):
        if self.config.include_field_distinct_value_frequencies:
            for column in table.columns:
                table.analyzer.addAnalyzer(ApproxCountDistinct(column))
        return

    def prep_field_histogram(self, table: TableWrapper):
        if self.config.include_field_histogram:
            for column in table.columns:
                table.analyzer.addAnalyzer(Histogram(column))
        return

    def calculate_sample_values(self, table: TableWrapper):
        num_rows_to_sample = 3

        if self.config.include_field_sample_values:
            rdd_sample = table.dataframe.rdd.takeSample(False, num_rows_to_sample, seed=0)

        return

    def read_file(self, file: str, file_type: FileType):

        if file_type is FileType.PARQUET:
            df = self.spark.read.parquet(file)
        elif file_type is FileType.CSV:
            df = self.spark.read.csv(file)
        else:
            raise ValueError("File type not found")

        return df

    def generate_profiles(self):

        table = self.read_file(self.source_config.file, self.source_config.file_type)

        analysis_table = TableWrapper(table, self.spark)

        analyzer_result = analysis_table.analyzer.run()
        
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
        
        # schema_metadata = SchemaMetadata(
        #     schemaName=dataset_name,
        #     platform=f"urn:li:dataPlatform:{platform}",
        #     version=0,
        #     hash="",
        #     platformSchema=MySqlDDL(tableSchema=""),
        #     fields=canonical_schema,
        # )
        
        # field = SchemaField(
        #     fieldPath=column["name"],
        #     type=get_column_type(self.report, dataset_name, column["type"]),
        #     nativeDataType=column.get("full_type", repr(column["type"])),
        #     description=column.get("comment", None),
        #     nullable=column["nullable"],
        #     recursive=False,
        # )
        
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = MetadataWorkUnit(id=self.source_config.file, mce=mce)
        self.report.report_workunit(wu)
        yield wu

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self.generate_profiles()

    def get_report(self):
        return self.report

    def close(self):
        pass
