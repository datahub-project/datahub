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
from pyspark.sql.functions import col, count, isnan, when

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit

logging.getLogger("py4j").setLevel(logging.ERROR)


class FileType(Enum):
    PARQUET = "parquet"
    CSV = "csv"


class DataLakeSourceConfig(ConfigModel):

    file: str
    file_type: Optional[FileType] = None

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

    # The default of (5 * cpu_count) is adopted from the default max_workers
    # parameter of ThreadPoolExecutor. Given that profiling is often an I/O-bound
    # task, it may make sense to increase this default value in the future.
    # https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
    max_workers: int = 5 * (os.cpu_count() or 4)


@dataclasses.dataclass
class DataLakeSourceReport(SourceReport):
    files_scanned = 0
    filtered: List[str] = dataclass_field(default_factory=list)

    def report_file_scanned(self) -> None:
        self.file_scanned += 1

    def report_file_dropped(self, file: str) -> None:
        self.filtered.append(file)


class TableWrapper:
    table: pydeequ.Table
    analyzer: AnalysisRunBuilder
    columns: List[str]

    def __init__(self, table):
        self.table = table
        self.analyzer = AnalysisRunner(self.spark).onData(table).addAnalyzer(Size())
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
            null_counts = table.table.select(
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
            rdd_sample = table.table.rdd.takeSample(False, num_rows_to_sample, seed=0)

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

        analysis_table = TableWrapper(table)

        analyzer_result = analysis_table.analyer.run()

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        self.generate_profiles()
        return []

    def get_report(self):
        return self.report

    def close(self):
        pass
