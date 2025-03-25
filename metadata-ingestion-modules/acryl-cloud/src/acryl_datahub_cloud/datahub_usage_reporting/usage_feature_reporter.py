import logging
import math
import os
import re
import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from itertools import chain
from tempfile import TemporaryDirectory
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple, Union

import numpy
import polars
import pyarrow as pa
import pyarrow.parquet as pq
from elasticsearch.client import Elasticsearch
from opensearchpy import OpenSearch
from polars.datatypes import DataTypeClass
from pydantic import Field
from scipy.stats import expon

from acryl_datahub_cloud.datahub_usage_reporting.query_builder import QueryBuilder
from acryl_datahub_cloud.datahub_usage_reporting.usage_feature_patch_builder import (
    UsageFeaturePatchBuilder,
)
from acryl_datahub_cloud.elasticsearch.config import ElasticSearchClientConfig
from acryl_datahub_cloud.metadata.schema_classes import (
    QueryUsageFeaturesClass,
    UsageFeaturesClass,
)
from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit_reporter
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DatahubClientConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionReport,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)

platform_regexp = re.compile(r"urn:li:dataset:\(urn:li:dataPlatform:(.+?),.*")
dashboard_chart_platform_regexp = re.compile(r"urn:li:(?:dashboard|chart):\((.+?),.*")
dbt_platform_regexp = re.compile(r"urn:li:dataset:\(urn:li:dataPlatform:dbt,.*\)")


class S3ClientConfig(ConfigModel):
    bucket: str = os.getenv("DATA_BUCKET", "")
    path: str = os.getenv("RDS_DATA_PATH", "rds_backup/metadata_aspect_v2")


class FreshnessFactor(ConfigModel):
    age_in_days: List[int]
    value: float


class RegexpFactor(ConfigModel):
    regexp: str
    value: float


class UsagePercentileFactor(ConfigModel):
    percentile: List[int]
    value: float


@dataclass
class SearchRankingMultipliers:
    usageSearchScoreMultiplier: Optional[float] = 1.0
    usageFreshnessScoreMultiplier: Optional[float] = 1.0
    customDatahubScoreMultiplier: Optional[float] = 1.0
    combinedSearchRankingMultiplier: Optional[float] = 1.0


class RankingPolicy(ConfigModel):
    freshness_factors: List[FreshnessFactor] = []
    usage_percentile_factors: List[UsagePercentileFactor] = []
    regexp_based_factors: List[RegexpFactor] = []


class DataHubUsageFeatureReportingSourceConfig(
    ConfigModel, StatefulIngestionConfigBase
):
    lookback_days: int = Field(
        30, description="Number of days to look back for usage data."
    )

    server: Optional[DatahubClientConfig] = Field(
        None, description="Optional configuration for the DataHub server connection."
    )
    search_index: ElasticSearchClientConfig = Field(
        default_factory=ElasticSearchClientConfig,
        description="Configuration for the Elasticsearch or OpenSearch index.",
    )
    query_timeout: int = Field(
        30, description="Timeout in seconds for the search queries."
    )
    extract_batch_size: int = Field(
        1000,
        description="The number of documents to retrieve in each batch from ElasticSearch or OpenSearch.",
    )

    extract_delay: Optional[float] = Field(
        0.25,
        description="The delay in seconds between each batch extraction from ElasticSearch or OpenSearch.",
    )

    use_exp_cdf: bool = Field(
        True,
        description="Flag to determine whether to use the exponential cumulative distribution function for calculating percentiles.",
    )
    ranking_policy: RankingPolicy = Field(
        default_factory=RankingPolicy,
        description="Configuration for the ranking policy.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        None,
        description="Optional configuration for stateful ingestion, including stale metadata removal.",
    )
    dataset_usage_enabled: bool = Field(
        True,
        description="Flag to enable or disable dataset usage statistics collection.",
    )
    dashboard_usage_enabled: bool = Field(
        True,
        description="Flag to enable or disable dashboard usage statistics collection.",
    )
    chart_usage_enabled: bool = Field(
        True, description="Flag to enable or disable chart usage statistics collection."
    )

    query_usage_enabled: bool = Field(
        default=False,
        description="Flag to enable or disable query usage statistics collection.",
    )

    sibling_usage_enabled: bool = Field(
        True,
        description="Flag to enable or disable the setting dataset usage statistics for sibling entities (only DBT siblings are set).",
    )

    use_server_side_aggregation: bool = Field(
        False,
        description="Flag to enable server side aggregation for write usage statistics.",
    )

    set_upstream_table_max_modification_time_for_views: bool = Field(
        True,
        description="Flag to enable setting the max modification time for views based on their upstream tables' modification time.'",
    )

    streaming_mode: bool = Field(
        True,
        description="Flag to enable polars streaming mode.'",
    )

    # Running the whole pipeline in streaming mode was very unstable in the past.
    # It seems like with the latest version of Polars it is much more stable.
    # This option is only needed here until we are sure that the streaming mode is stable.
    # then we can remove it and control it with the streaming_mode option.
    experimental_full_streaming: bool = Field(
        False,
        description="Flag to enable full streaming mode.'",
    )

    disable_write_usage: bool = Field(
        True,
        description="Flag to disable write usage statistics collection.'",
    )

    generate_patch: bool = Field(
        True,
        description="Flag to generate MCP patch for usage features.'",
    )


def exp_cdf(series: polars.Series) -> polars.Series:
    with PerfTimer() as timer:
        if series.is_empty():
            return polars.Series([])

        numpy_array = series.to_numpy()
        fit_array = numpy_array[~numpy.isnan(numpy_array)]
        if fit_array.size == 0:
            return polars.Series([0] * len(numpy_array))

        loc, scale = expon.fit(fit_array, floc=0)
        # percentiles = [int(round(expon.cdf(count, loc, scale) * 100)) for count in numpy_array]
        percentiles = []
        for count in numpy_array:
            if math.isnan(count):
                percentiles.append(0)
            else:
                try:
                    exp_cdf_value = round(expon.cdf(count, loc, scale) * 100)
                except Exception as e:
                    logger.warning(
                        f"Expcdf calculation failed on array: {numpy_array}, count: {count}, loc: {loc}, scale: {scale}, Error: {e}"
                    )
                    return polars.Series([0] * len(numpy_array))
                if math.isnan(exp_cdf_value):
                    percentiles.append(0)
                else:
                    percentiles.append(int(exp_cdf_value))

        logger.debug(f"Percentiles: {percentiles}")
        time_taken = timer.elapsed_seconds()
        logger.debug(f"Exp CDF processing took {time_taken:.3f} seconds")
    return polars.Series(percentiles)


@dataclass
class DatahubUsageFeatureReport(IngestionStageReport, StatefulIngestionReport):
    dataset_platforms_count: Dict[str, int] = field(
        default_factory=lambda: defaultdict(lambda: 0)
    )
    dashboard_platforms_count: Dict[str, int] = field(
        default_factory=lambda: defaultdict(lambda: 0)
    )
    sibling_usage_count: int = 0

    report_es_extraction_time: Dict[str, PerfTimer] = field(
        default_factory=lambda: defaultdict(lambda: PerfTimer())
    )

    dataset_usage_processing_time: PerfTimer = PerfTimer()
    dashboard_usage_processing_time: PerfTimer = PerfTimer()
    chart_usage_processing_time: PerfTimer = PerfTimer()
    query_usage_processing_time: PerfTimer = PerfTimer()
    query_platforms_count: Dict[str, int] = field(
        default_factory=lambda: defaultdict(lambda: 0)
    )


@platform_name(id="datahub", platform_name="DataHub")
@config_class(DataHubUsageFeatureReportingSourceConfig)
@support_status(SupportStatus.INCUBATING)
class DataHubUsageFeatureReportingSource(StatefulIngestionSourceBase):
    platform = "datahub"
    temp_files_to_clean: List[str] = []
    temp_dir: Optional[TemporaryDirectory] = None

    def __init__(
        self, ctx: PipelineContext, config: DataHubUsageFeatureReportingSourceConfig
    ):
        super().__init__(config, ctx)
        # super().__init__(ctx)
        self.config: DataHubUsageFeatureReportingSourceConfig = config
        self.report: DatahubUsageFeatureReport = DatahubUsageFeatureReport()
        self.ctx = ctx

        # We compile regexpes in advance for faster matching
        self.compiled_regexp_factor: List[Tuple[re.Pattern[str], float]] = []
        num = 0
        for rfactor in self.config.ranking_policy.regexp_based_factors:
            self.compiled_regexp_factor.append(
                (re.compile(rfactor.regexp), rfactor.value)
            )
            num += 1

        if num > 0:
            logger.info(f"Compiled {num} regexp factors")

        if self.config.streaming_mode:
            self.temp_dir = tempfile.TemporaryDirectory(prefix="datahub-usage-")
            logger.info(f"Using temp dir: {self.temp_dir.name}")

    def soft_deleted_batch(self, results: Iterable) -> Iterable[Dict]:
        with PerfTimer() as timer:
            for doc in results:
                try:
                    if "urn" not in doc["_source"]:
                        logger.warning(f"Urn not found in ES doc {doc}. Skipping...")
                        continue

                    yield {
                        "entity_urn": doc["_source"]["urn"],
                        "last_modified_at": (
                            doc["_source"]["lastModifiedAt"]
                            if "lastModifiedAt" in doc["_source"]
                            and doc["_source"]["lastModifiedAt"]
                            else (
                                doc["_source"]["lastModifiedAt"]
                                if "lastModifiedAt" in doc["_source"]
                                and doc["_source"]["lastModifiedAt"]
                                else None
                            )
                        ),
                        "removed": (
                            doc["_source"]["removed"]
                            if "removed" in doc["_source"] and doc["_source"]["removed"]
                            else False
                        ),
                        "siblings": (
                            doc["_source"]["siblings"]
                            if "siblings" in doc["_source"]
                            and doc["_source"]["siblings"]
                            else []
                        ),
                        "combinedSearchRankingMultiplier": (
                            doc["_source"]["combinedSearchRankingMultiplier"]
                            if "combinedSearchRankingMultiplier" in doc["_source"]
                            and doc["_source"]["combinedSearchRankingMultiplier"]
                            else None
                        ),
                        "isView": (
                            "View" in doc["_source"]["typeNames"]
                            if "typeNames" in doc["_source"]
                            and doc["_source"]["typeNames"]
                            else False
                        ),
                    }
                except KeyError as e:
                    logger.warning(
                        f"Unable to process row {doc} from ES. It failed with {e}"
                    )
                    continue
            time_taken = timer.elapsed_seconds()
            logger.info(f"Entities processing took {time_taken:.3f} seconds")

    def write_stat_batch(self, results: Iterable) -> Iterable[Dict]:
        with PerfTimer() as timer:
            for doc in results:
                match = re.match(
                    platform_regexp, doc["key"]["dataset_operationaspect_v1"]
                )
                if match:
                    platform = match.group(1)
                else:
                    logger.warning("Platform not found in urn. Skipping...")
                    continue

                yield {
                    "urn": doc["key"]["dataset_operationaspect_v1"],
                    "platform": platform,
                    "write_count": doc["doc_count"],
                }
            time_taken = timer.elapsed_seconds()
            logger.info(
                f"Write Operation aspect processing took {time_taken:.3f} seconds"
            )

    def write_stat_raw_batch(self, results: Iterable) -> Iterable[Dict]:
        with PerfTimer() as timer:
            for doc in results:
                match = re.match(platform_regexp, doc["_source"]["urn"])
                if match:
                    platform = match.group(1)
                else:
                    logger.warning("Platform not found in urn. Skipping...")
                    continue

                yield {
                    "urn": doc["_source"]["urn"],
                    "platform": platform,
                }
            time_taken = timer.elapsed_seconds()
            logger.info(
                f"Write Operation aspect processing took {time_taken:.3f} seconds"
            )

    def queries_entities_batch(self, results: Iterable) -> Iterable[Dict]:
        with PerfTimer() as timer:
            for doc in results:
                if "platform" not in doc["_source"] or not doc["_source"]["platform"]:
                    logger.warning(
                        f"Platform not found in query {doc['_source']['urn']}. Skipping..."
                    )
                    continue

                self.report.query_platforms_count[doc["_source"]["platform"]] = (
                    self.report.query_platforms_count[doc["_source"]["platform"]] + 1
                )

                yield {
                    "entity_urn": doc["_source"]["urn"],
                    "last_modified_at": (
                        doc["_source"]["lastModifiedAt"]
                        if "lastModifiedAt" in doc["_source"]
                        else (
                            doc["_source"]["lastModifiedAt"]
                            if "lastModifiedAt" in doc["_source"]
                            else None
                        )
                    ),
                    "platform": doc["_source"]["platform"],
                    "removed": (
                        doc["_source"]["removed"]
                        if "removed" in doc["_source"]
                        else False
                    ),
                }

            time_taken = timer.elapsed_seconds()
            logger.info(f"Query entities processing took {time_taken:.3f} seconds")

    def process_dashboard_usage(self, results: Iterable) -> Iterable[Dict]:
        for doc in results:
            match = re.match(dashboard_chart_platform_regexp, doc["_source"]["urn"])
            if match:
                platform = match.group(1)
                self.report.dashboard_platforms_count[platform] += 1
            else:
                logger.warning("Platform not found in urn. Skipping...")
                continue

            yield {
                "timestampMillis": doc["_source"].get("timestampMillis"),
                "lastObserved": doc["_source"]
                .get("systemMetadata", {})
                .get("lastObserved"),
                "urn": doc["_source"].get("urn"),
                "eventGranularity": doc["_source"].get("eventGranularity"),
                "viewsCount": doc["_source"].get("viewsCount", 0),
                "uniqueUserCount": doc["_source"].get("uniqueUserCount"),
                "userCounts": doc["_source"].get("event", {}).get("userCounts", []),
                "platform": platform,
            }

    def process_query_usage(self, results: Iterable) -> Iterable[Dict]:
        for doc in results:
            yield {
                "timestampMillis": doc["_source"].get("timestampMillis"),
                "lastObserved": doc["_source"]
                .get("systemMetadata", {})
                .get("lastObserved"),
                "urn": doc["_source"].get("urn"),
                "eventGranularity": doc["_source"].get("eventGranularity"),
                "queryCount": doc["_source"].get("queryCount", 0),
                "uniqueUserCount": doc["_source"].get("uniqueUserCount"),
                "userCounts": doc["_source"].get("event", {}).get("userCounts", []),
            }

    def upstream_lineage_batch(self, results: Iterable) -> Iterable[Dict]:
        for doc in results:
            if (
                not doc["_source"]["source"]["urn"]
                or not doc["_source"]["destination"]["urn"]
            ):
                logger.warning("Source urn not found in upstream lineage. Skipping...")
                continue

            source_platform_match = re.match(
                platform_regexp, doc["_source"]["source"]["urn"]
            )
            if source_platform_match:
                source_platform = source_platform_match.group(1)
            else:
                logger.warning("Source Platform not found in urn. Skipping...")
                continue

            destination_platform_match = re.match(
                platform_regexp, doc["_source"]["destination"]["urn"]
            )
            if destination_platform_match:
                destination_platform = destination_platform_match.group(1)
            else:
                logger.warning("Destination Platform not found in urn. Skipping...")
                continue

            # In some case like Tableau there is dataset which marked as view and points to a dataset on another platform
            # We drop these now
            if source_platform != destination_platform:
                continue

            yield {
                "source_urn": doc["_source"]["source"]["urn"],
                "destination_urn": doc["_source"]["destination"]["urn"],
            }

    def process_batch(self, results: Iterable) -> Iterable[Dict]:
        with PerfTimer() as timer:
            for doc in results:
                if "urn" not in doc["_source"]:
                    logger.warning(f"Urn not found in ES doc {doc}. Skipping...")
                    continue
                match = re.match(platform_regexp, doc["_source"]["urn"])
                if match:
                    platform = match.group(1)
                    self.report.dataset_platforms_count[platform] += 1
                else:
                    logger.warning(
                        f"Platform not found in urn  {doc['_source']['urn']} in doc {doc}. Skipping..."
                    )
                    continue

                try:
                    yield {
                        "timestampMillis": doc["_source"]["timestampMillis"],
                        "urn": doc["_source"]["urn"],
                        "eventGranularity": doc["_source"].get("eventGranularity"),
                        "totalSqlQueries": doc["_source"].get("totalSqlQueries", 0),
                        "uniqueUserCount": doc["_source"].get("uniqueUserCount", 0),
                        "userCounts": (
                            doc["_source"]["event"]["userCounts"]
                            if "userCounts" in doc["_source"]["event"]
                            else None
                        ),
                        "platform": platform,
                    }
                except KeyError as e:
                    logger.warning(
                        f"Unable to process row {doc} from ES. The error was: {e}"
                    )
                    continue

            time_taken = timer.elapsed_seconds()
            logger.info(f"DatasetUsage processing took {time_taken:.3f} seconds")

    def search_score(
        self, urn: str, last_update_time: int, usage_percentile: int
    ) -> SearchRankingMultipliers:
        usage_search_score_multiplier = 1.0
        freshness_factor = 1.0
        regexp_factor = 1.0

        current_time = datetime.now().timestamp() * 1000
        age_in_millis = current_time - last_update_time
        age_in_days = age_in_millis / (1000 * 60 * 60 * 24)

        bucket = 0
        for factor in self.config.ranking_policy.freshness_factors:
            if len(factor.age_in_days) == 2:
                if bucket == 0:
                    if factor.age_in_days[0] <= age_in_days <= factor.age_in_days[1]:
                        freshness_factor = factor.value
                        break
                else:
                    if factor.age_in_days[0] < age_in_days <= factor.age_in_days[1]:
                        freshness_factor = factor.value
                        break
            elif age_in_days > factor.age_in_days[0]:
                freshness_factor = factor.value

        bucket = 0
        for pfactor in self.config.ranking_policy.usage_percentile_factors:
            bucket += 1
            if len(pfactor.percentile) == 2:
                # The first bucket min should be inclusive
                if bucket == 1:
                    if (
                        pfactor.percentile[0]
                        <= usage_percentile
                        <= pfactor.percentile[1]
                    ):
                        usage_search_score_multiplier = pfactor.value
                        break
                else:
                    if (
                        pfactor.percentile[0]
                        < usage_percentile
                        <= pfactor.percentile[1]
                    ):
                        usage_search_score_multiplier = pfactor.value
                        break
            elif usage_percentile > pfactor.percentile[0]:
                usage_search_score_multiplier = pfactor.value

        for rfactor in self.compiled_regexp_factor:
            if rfactor[0].match(urn):
                regexp_factor = rfactor[1]

        return SearchRankingMultipliers(
            usageSearchScoreMultiplier=usage_search_score_multiplier,
            usageFreshnessScoreMultiplier=freshness_factor,
            customDatahubScoreMultiplier=regexp_factor,
            # We make sure the combinedSearchRankingMultiplier is never less than 1
            combinedSearchRankingMultiplier=max(
                1, (usage_search_score_multiplier * freshness_factor * regexp_factor)
            ),
        )

    def load_data_from_es(
        self,
        index: str,
        query: Dict,
        process_function: Callable,
        aggregation_key: Optional[str] = None,
    ) -> Iterable[Dict]:
        with self.report.report_es_extraction_time[index]:
            query_copy = query.copy()
            endpoint = ""
            if self.config.search_index:
                if self.config.search_index.host and not self.config.search_index.port:
                    endpoint = f"{self.config.search_index.host}"
                elif self.config.search_index.host and self.config.search_index.port:
                    endpoint = f"{self.config.search_index.host}:{self.config.search_index.port}"

                index_prefix = (
                    self.config.search_index.index_prefix
                    if self.config.search_index
                    else ""
                )

                index = f"{index_prefix}{index}" if index_prefix else index
                user = self.config.search_index.username
                password = self.config.search_index.password
                batch_size = self.config.extract_batch_size
                delay = self.config.extract_delay
                server: Union[Elasticsearch, OpenSearch]

                if self.config.search_index.opensearch_dialect:
                    server = OpenSearch(
                        [endpoint],
                        http_auth=(user, password),
                        use_ssl=(
                            True
                            if self.config.search_index
                            and self.config.search_index.use_ssl
                            else False
                        ),
                    )

                    response = server.create_pit(index, keep_alive="10m")

                    # TODO: Save PIT, we can resume processing based on <pit, search_after> tuple
                    pit = response.get("pit_id")
                    query_copy.update({"pit": {"id": pit, "keep_alive": "10m"}})
                else:
                    server = Elasticsearch(
                        [endpoint],
                        http_auth=(user, password),
                        use_ssl=(
                            True
                            if self.config.search_index
                            and self.config.search_index.use_ssl
                            else False
                        ),
                    )

                yield from self.load_es_data(
                    query_copy,
                    server,
                    index,
                    process_function,
                    batch_size=batch_size,
                    delay=delay,
                    aggregation_key=aggregation_key,
                )

    def gen_rank_and_percentile(
        self,
        lf: polars.LazyFrame,
        count_field: str,
        urn_field: str = "urn",
        platform_field: str = "platform",
        prefix: Optional[str] = None,
        use_exp_cdf: Optional[bool] = None,
    ) -> polars.LazyFrame:
        logger.debug(f"Generating rank and percentile for {count_field} field")
        lf = lf.with_columns(
            polars.col(count_field)
            .rank(descending=True, method="max")
            .over(platform_field)
            .alias(f"{prefix}rank")
        )

        use_exp_cdf = self.config.use_exp_cdf if use_exp_cdf is None else use_exp_cdf
        if use_exp_cdf:
            lf = lf.with_columns(
                polars.col(count_field)
                .map_batches(exp_cdf, return_dtype=polars.Int64)
                .over(platform_field)
                .alias(f"{prefix}rank_percentile")
            )
        else:
            lf = lf.with_columns(
                polars.when(
                    polars.col(count_field)
                    > 0  # This is slightly modified percentile rank calculation as we zero out zero usage
                )
                .then(
                    (
                        1
                        - (
                            (polars.col(f"{prefix}rank") - 1)
                            / (
                                polars.max_horizontal(
                                    polars.col(urn_field).count(), polars.lit(2)
                                )
                                - 1
                            )  # If we only have 1 item then we have to set the percentile to 100
                        )
                    )
                    * 100
                )
                .otherwise(0)
                .over(platform_field)
                .alias(f"{prefix}rank_percentile")
            )

        return lf

    @staticmethod
    def polars_to_arrow_schema(
        polars_schema: Dict[str, Union[DataTypeClass, polars.DataType]],
    ) -> pa.Schema:
        def convert_dtype(
            polars_dtype: Union[DataTypeClass, polars.DataType],
        ) -> pa.DataType:
            type_mapping: Dict[Union[DataTypeClass, polars.DataType], pa.DataType] = {
                polars.Boolean(): pa.bool_(),
                polars.Int8(): pa.int8(),
                polars.Int16(): pa.int16(),
                polars.Int32(): pa.int32(),
                polars.Int64(): pa.int64(),
                polars.UInt8(): pa.uint8(),
                polars.UInt16(): pa.uint16(),
                polars.UInt32(): pa.uint32(),
                polars.UInt64(): pa.uint64(),
                polars.Float32(): pa.float32(),
                polars.Float64(): pa.float64(),
                polars.Utf8(): pa.string(),
                polars.Utf8(): pa.utf8(),
                polars.String(): pa.string(),
                polars.Date(): pa.date32(),
                polars.Datetime(): pa.timestamp("ns"),
                polars.Time(): pa.time64("ns"),
                polars.Duration(): pa.duration("ns"),
            }

            if polars_dtype in [type(key) for key in type_mapping.keys()]:
                return type_mapping[polars_dtype]
            elif polars_dtype == polars.Categorical:
                return pa.dictionary(index_type=pa.int32(), value_type=pa.string())
            elif isinstance(polars_dtype, polars.Struct):
                return pa.struct(
                    {
                        field.name: convert_dtype(field.dtype)
                        for field in polars_dtype.fields
                    }
                )
            elif isinstance(polars_dtype, polars.List):
                return pa.list_(convert_dtype(polars_dtype.inner))
            else:
                raise ValueError(f"Unsupported Polars dtype: {polars_dtype}")

        fields = [(name, convert_dtype(dtype)) for name, dtype in polars_schema.items()]
        return pa.schema(fields)

    def batch_write_parquet(
        self,
        data_iterator: Iterable[Dict[Any, Any]],
        pl_schema: Dict,
        output_path: str,
        batch_size: int = 50000,
        append: bool = False,
        parquet_writer: Optional[pq.ParquetWriter] = None,
    ) -> None:
        """
        Write data in batches to a file with support for appending to existing files.

        Args:
            data_iterator: Iterator of dictionaries containing the data
            pa_schema: PyArrow schema for the data
            output_path: Path for the output file
            format_type: One of "ipc", "feather", "csv", "parquet", "pl_parquet"
            batch_size: Number of rows per batch
            append: If True, append to existing file. If False, create new file.
            parquet_writer: Parquet doesn't let to append to existing file, so we need to pass the writer object
        Returns:
            LazyFrame pointing to the written data
        """
        arrow_schema = self.polars_to_arrow_schema(pl_schema)

        total_rows = 0
        total_batches = 0

        try:
            if parquet_writer:
                writer = parquet_writer
            else:
                writer = pq.ParquetWriter(output_path, arrow_schema)

            try:
                for batch in self._get_batches(data_iterator, batch_size):
                    table = pa.Table.from_pylist(batch, schema=arrow_schema)
                    writer.write_table(table)
                    total_rows += len(batch)
                    total_batches += 1
                    logger.debug(f"Wrote batch {total_batches} ({len(batch)} rows)")
            finally:
                if not parquet_writer:
                    writer.close()
        except Exception as e:
            logger.exception(f"Error during batch writing: {str(e)}", exc_info=True)
            raise

    def _get_batches(
        self, iterator: Iterable[Dict], batch_size: int
    ) -> Iterator[List[Dict]]:
        """Helper generator to create batches from an iterator."""
        current_batch = []
        for item in iterator:
            current_batch.append(item)
            if len(current_batch) >= batch_size:
                yield current_batch
                current_batch = []

        if current_batch:
            yield current_batch

    def load_write_usage(
        self, soft_deleted_entities_df: polars.LazyFrame
    ) -> polars.LazyFrame:
        wdf = self.load_data_from_es_to_lf(
            index="dataset_operationaspect_v1",
            query=QueryBuilder.get_dataset_write_usage_raw_query(
                self.config.lookback_days
            ),
            process_function=self.write_stat_raw_batch,
            schema={"urn": polars.Categorical, "platform": polars.Categorical},
        )
        wdf = wdf.cast({polars.String: polars.Categorical})

        wdf = wdf.group_by(polars.col("urn"), polars.col("platform")).agg(
            polars.col("urn").count().alias("write_count"),
        )

        wdf = (
            wdf.join(
                soft_deleted_entities_df,
                left_on="urn",
                right_on="entity_urn",
                how="inner",
            )
            .filter(polars.col("removed") == False)  # noqa: E712
            .drop(["removed"])
        )

        return wdf.collect(streaming=self.config.streaming_mode).lazy()

    def load_write_usage_server_side_aggregation(
        self, soft_deleted_entities_df: polars.LazyFrame
    ) -> polars.LazyFrame:
        wdf = polars.LazyFrame(
            self.load_data_from_es(
                "dataset_operationaspect_v1",
                QueryBuilder.get_dataset_write_usage_composite_query(
                    self.config.lookback_days
                ),
                self.write_stat_batch,
                aggregation_key="urn_count",
            ),
            schema={
                "urn": polars.Categorical,
                "platform": polars.Categorical,
                "write_count": polars.Int64,
            },
            strict=True,
        )

        wdf = (
            wdf.join(
                soft_deleted_entities_df,
                left_on="urn",
                right_on="entity_urn",
                how="inner",
            )
            .filter(polars.col("removed") == False)  # noqa: E712
            .drop(["removed"])
        )

        return wdf

    def set_table_modification_time_for_views(
        self, datasets_df: polars.LazyFrame
    ) -> polars.LazyFrame:
        schema = {
            "source_urn": polars.Categorical,
            "destination_urn": polars.Categorical,
        }

        upstreams_lf = self.load_data_from_es_to_lf(
            schema=schema,
            index="graph_service_v1",
            query=QueryBuilder.get_upstreams_query(),
            process_function=self.upstream_lineage_batch,
        )

        wdf = (
            (
                upstreams_lf.join(
                    datasets_df.filter(polars.col("isView") == True),  # noqa: E712
                    left_on="destination_urn",
                    right_on="entity_urn",
                    how="inner",
                )
            )
            .join(
                datasets_df.filter(polars.col("isView") == False),  # noqa: E712
                left_on="source_urn",
                right_on="entity_urn",
            )
            .group_by(
                "destination_urn",
            )
            .agg(
                polars.col("last_modified_at_right")
                .max()
                .alias("inherited_last_modified_at"),
                polars.col("last_modified_at").first().alias("last_modified_at"),
            )
        )

        dataset_df = (
            datasets_df.join(
                wdf, left_on="entity_urn", right_on="destination_urn", how="left"
            )
            .with_columns(
                polars.coalesce("inherited_last_modified_at", "last_modified_at").alias(
                    "last_modified_at"
                )
            )
            .drop(["inherited_last_modified_at", "last_modified_at_right"])
        )

        return dataset_df

    def generate_dataset_usage_mcps(self) -> Iterable[MetadataWorkUnit]:
        with polars.StringCache():
            dataset_usage_df = self.generate_dataset_usage()
            logger.info("Generate Dataset Usage")
            yield from self.generate_mcp_from_lazyframe(dataset_usage_df)
            logger.info("End Generate Dataset Usage")

    def generate_dashboard_usage_mcps(self) -> Iterable[MetadataWorkUnit]:
        with polars.StringCache():
            logger.info("Generate Dashboard Usage")
            dashboard_usage_df = self.generate_dashboard_usage()
            yield from self.generate_mcp_from_lazyframe(dashboard_usage_df)

    def generate_chart_usage_mcps(self) -> Iterable[MetadataWorkUnit]:
        with polars.StringCache():
            logger.info("Generate Chart Usage")
            chart_usage_df = self.generate_chart_usage()
            yield from self.generate_mcp_from_lazyframe(chart_usage_df)

    def generate_query_usage_mcps(self) -> Iterable[MetadataWorkUnit]:
        with polars.StringCache():
            logger.info("Generate Query Usage")
            query_usage_df = self.generate_query_usage()
            yield from self.generate_query_usage_mcp_from_lazyframe(query_usage_df)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        """A list of functions that transforms the workunits produced by this source.
        Run in order, first in list is applied first. Be careful with order when overriding.
        """

        return [
            partial(auto_workunit_reporter, self.get_report()),
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        if self.config.dataset_usage_enabled:
            with self.report.dataset_usage_processing_time as timer:
                self.report.new_stage("generate dataset usage")
                yield from self.generate_dataset_usage_mcps()
                time_taken = timer.elapsed_seconds()
                logger.info(f"Dataset Usage generation took {time_taken:.3f} seconds")

        if self.config.dashboard_usage_enabled:
            with self.report.dashboard_usage_processing_time as timer:
                self.report.new_stage("generate dashboard usage")
                yield from self.generate_dashboard_usage_mcps()

                time_taken = timer.elapsed_seconds()
                logger.info(f"Dashboard Usage generation took {time_taken:.3f}")

        if self.config.chart_usage_enabled:
            with self.report.chart_usage_processing_time as timer:
                self.report.new_stage("generate chart usage")

                yield from self.generate_chart_usage_mcps()

                time_taken = timer.elapsed_seconds()
                logger.info(f"Chart Usage generation took {time_taken:.3f}")

        if self.config.query_usage_enabled:
            with self.report.query_usage_processing_time as timer:
                self.report.new_stage("generate query usage")

                yield from self.generate_query_usage_mcps()

                time_taken = timer.elapsed_seconds()
                logger.info(f"Query Usage generation took {time_taken:.3f}")

    def generate_mcp_from_lazyframe(
        self, lazy_frame: polars.LazyFrame
    ) -> Iterable[MetadataWorkUnit]:
        num = 0
        for row in lazy_frame.collect(
            streaming=self.config.experimental_full_streaming
        ).to_struct():
            num += 1

            if "siblings" in row and row["siblings"]:
                logger.info(f"Siblings found for urn: {row['urn']} -> row['siblings']")

            search_ranking_multipliers: SearchRankingMultipliers = (
                SearchRankingMultipliers()
            )

            if "queries_rank_percentile" in row:
                # If usage data is missing we set the search ranking multipliers to 1
                search_ranking_multipliers = (
                    self.search_score(
                        urn=row["urn"],
                        last_update_time=row.get("last_modified_at", 0) or 0,
                        usage_percentile=row.get("queries_rank_percentile", 0) or 0,
                    )
                    if row.get("queries_rank_percentile", 0)
                    else SearchRankingMultipliers()
                )
            elif "viewsCount30Days_rank_percentile" in row:
                # If usage data is missing we set the search ranking multipliers to 1
                search_ranking_multipliers = (
                    self.search_score(
                        urn=row["urn"],
                        last_update_time=row.get("last_modified_at", 0) or 0,
                        usage_percentile=row.get("viewsCount30Days_rank_percentile", 0)
                        or 0,
                    )
                    if row.get("viewsCount30Days_rank_percentile", 0)
                    else SearchRankingMultipliers()
                )
                logger.debug(f"Urn: {row['urn']} Score: {search_ranking_multipliers}")

            usage_feature = UsageFeaturesClass(
                queryCountLast30Days=int(row.get("totalSqlQueries", 0) or 0),
                usageCountLast30Days=int(row.get("totalSqlQueries", 0) or 0),
                queryCountRankLast30Days=int(row.get("queries_rank"))
                if row.get("queries_rank")
                else None,
                queryCountPercentileLast30Days=row.get("queries_rank_percentile", 0)
                or 0,
                # queryCountPercentileLast30Days=int(
                #   row["queries_rank_percentile"]) if "queries_rank_percentile" in row and row[
                #   "queries_rank_percentile"] else 0,
                topUsersLast30Days=(
                    list(chain.from_iterable(row.get("top_users")))
                    if row.get("top_users")
                    else None
                ),
                uniqueUserCountLast30Days=int(row.get("distinct_user", 0) or 0),
                uniqueUserRankLast30Days=int(row.get("distinct_user_rank"))
                if row.get("distinct_user_rank")
                else None,
                uniqueUserPercentileLast30Days=int(
                    row.get("distinct_user_rank_percentile", 0) or 0
                ),
                writeCountLast30Days=int(row.get("write_rank_percentile", 0) or 0)
                if not self.config.disable_write_usage
                else None,
                writeCountPercentileLast30Days=int(
                    row.get("write_rank_percentile", 0) or 0
                )
                if not self.config.disable_write_usage
                else None,
                writeCountRankLast30Days=int(row.get("write_rank") or 0)
                if not self.config.disable_write_usage
                else None,
                viewCountTotal=int(row.get("viewsTotal", 0) or 0),
                viewCountLast30Days=int(row.get("viewsCount30Days", 0) or 0),
                viewCountPercentileLast30Days=int(
                    row.get("viewsCount30Days_rank_percentile", 0) or 0
                ),
                usageSearchScoreMultiplier=search_ranking_multipliers.usageSearchScoreMultiplier,
                usageFreshnessScoreMultiplier=search_ranking_multipliers.usageFreshnessScoreMultiplier,
                customDatahubScoreMultiplier=search_ranking_multipliers.customDatahubScoreMultiplier,
                combinedSearchRankingMultiplier=search_ranking_multipliers.combinedSearchRankingMultiplier,
            )

            yield from self.generate_usage_feature_mcp(row["urn"], usage_feature)

            if row.get("siblings") and self.config.sibling_usage_enabled:
                for sibling in row["siblings"]:
                    if dbt_platform_regexp.match(sibling):
                        yield from self.generate_usage_feature_mcp(
                            sibling, usage_feature
                        )

    def generate_query_usage_mcp_from_lazyframe(
        self, lazy_frame: polars.LazyFrame
    ) -> Iterable[MetadataWorkUnit]:
        num = 0
        for row in lazy_frame.collect().iter_rows(named=True):
            num += 1

            query_usage_features = QueryUsageFeaturesClass(
                queryCountLast30Days=int(row.get("totalSqlQueries", 0) or 0),
                queryCountTotal=None,  # This is not implemented
                runsPercentileLast30days=int(
                    row.get("queries_rank_percentile", 0) or 0
                ),
                lastExecutedAt=int(row.get("last_modified_at", 0) or 0),
                topUsersLast30Days=(
                    list(chain.from_iterable(row.get("top_users", [])))
                    if row.get("top_users")
                    else None
                ),
                queryCostLast30Days=None,  # Not implemented yet
            )

            yield from self.generate_query_usage_feature_mcp(
                row["urn"], query_usage_features
            )

    def generate_usage_feature_mcp(
        self, urn: str, usage_feature: UsageFeaturesClass
    ) -> Iterable[MetadataWorkUnit]:
        if self.config.generate_patch:
            usage_feature_patch_builder = UsageFeaturePatchBuilder(urn=urn)
            usage_feature_patch_builder.apply_usage_features(usage_feature)
            for mcp in usage_feature_patch_builder.build():
                yield MetadataWorkUnit(
                    id=MetadataWorkUnit.generate_workunit_id(mcp),
                    mcp_raw=mcp,
                    is_primary_source=False,
                )
        else:
            mcw = MetadataChangeProposalWrapper(entityUrn=urn, aspect=usage_feature)
            yield mcw.as_workunit(is_primary_source=False)

    def generate_query_usage_feature_mcp(
        self, urn: str, query_usage_features: QueryUsageFeaturesClass
    ) -> Iterable[MetadataWorkUnit]:
        mcw = MetadataChangeProposalWrapper(entityUrn=urn, aspect=query_usage_features)
        yield mcw.as_workunit(is_primary_source=False)

    def generate_chart_usage(self) -> polars.LazyFrame:
        entity_index = "chartindex_v2"
        usage_index = "chart_chartusagestatisticsaspect_v1"

        return self.generate_dashboard_chart_usage(entity_index, usage_index)

    def generate_dashboard_usage(self) -> polars.LazyFrame:
        usage_index = "dashboard_dashboardusagestatisticsaspect_v1"
        entity_index = "dashboardindex_v2"

        return self.generate_dashboard_chart_usage(entity_index, usage_index)

    def generate_dashboard_chart_usage(
        self, entity_index: str, usage_index: str
    ) -> polars.LazyFrame:
        entity_schema = {
            "entity_urn": polars.Categorical,
            "removed": polars.Boolean,
            "last_modified_at": polars.Int64,
            "siblings": polars.List(polars.String),
            "combinedSearchRankingMultiplier": polars.Float64,
            "isView": polars.Boolean,
        }

        entities_df = self.load_data_from_es_to_lf(
            schema=entity_schema,
            index=entity_index,
            query=QueryBuilder.get_dataset_entities_query(),
            process_function=self.soft_deleted_batch,
        )

        dashboard_usage_schema = {
            "timestampMillis": polars.Int64,
            "lastObserved": polars.Int64,
            "urn": polars.Categorical,
            "platform": polars.Categorical,
            "eventGranularity": polars.String,
            "viewsCount": polars.Int64,
            "userCounts": polars.List(
                polars.Struct(
                    {
                        "usageCount": polars.Int64,
                        "user": polars.String,
                    }
                )
            ),
        }

        lf = self.load_data_from_es_to_lf(
            schema=dashboard_usage_schema,
            index=usage_index,
            query=QueryBuilder.get_dashboard_usage_query(self.config.lookback_days),
            process_function=self.process_dashboard_usage,
        )

        lf = (
            lf.join(entities_df, left_on="urn", right_on="entity_urn", how="inner")
            .filter(polars.col("removed") == False)  # noqa: E712
            .drop(["removed"])
        )

        lf = lf.with_columns(
            polars.col("lastObserved")
            .rank(descending=True, method="ordinal")
            .over("urn", "timestampMillis")
            .alias("row_num")
        ).filter(polars.col("row_num") == 1)

        # lf = lf.filter(polars.col("urn") == "urn:li:dashboard:(looker,dashboards.8)")
        # "urn:li:dashboard:(looker,dashboards.8)"

        top_users = self.generate_top_users(
            lf.filter(polars.col("eventGranularity").is_not_null()),
            count_field_name="usageCount",
        )

        views_sum_with_top_users = (
            lf.group_by("urn")
            .agg(
                [
                    polars.max("last_modified_at").alias("last_modified_at"),
                    polars.first("siblings").alias("siblings"),
                ]
            )
            .join(top_users, on="urn", how="left")
        )
        # views_sum_with_top_users = views_sum_with_top_users.drop(["userCounts"])

        incremental_views_sum = (
            lf.filter(polars.col("eventGranularity").is_null())
            .group_by("urn")
            .agg(
                polars.col("viewsCount").min().alias("first_viewsCount"),
                polars.col("viewsCount").max().alias("viewsTotal"),
                polars.col("last_modified_at").max().alias("last_modified_at"),
                polars.col("siblings").first().alias("siblings"),
            )
            .with_columns(
                (polars.col("viewsTotal") - polars.col("first_viewsCount")).alias(
                    "viewsCountTotal30Days"
                )
            )
            .drop(["first_viewsCount"])
        )
        views_with_inceremental_sum = views_sum_with_top_users.join(
            incremental_views_sum, on="urn", how="left"
        )
        total_views = views_with_inceremental_sum.with_columns(
            polars.when(
                polars.col("total_user_count")
                .is_null()
                .or_(polars.col("total_user_count") <= 0)
            )
            .then(polars.col("viewsCountTotal30Days"))
            .otherwise(polars.col("total_user_count"))
            .alias("viewsCount30Days")
        )

        total_views_with_rank_and_percentiles = self.gen_rank_and_percentile(
            total_views, "viewsCount30Days", "urn", "platform", "viewsCount30Days_"
        ).drop(["siblings_right"])

        total_views_with_rank_and_percentiles_with_zeroed_stale_usages = (
            self.generate_empty_usage_for_stale_entities(
                entities_df, total_views_with_rank_and_percentiles
            )
        )

        return total_views_with_rank_and_percentiles_with_zeroed_stale_usages

    def generate_empty_usage_for_stale_entities(
        self, entities_lf: polars.LazyFrame, usages_lf: polars.LazyFrame
    ) -> polars.LazyFrame:
        # We need to merge datasets with existing search scores to make sure we can downrank them if there were no usage in the last n days
        # We drop last_modified_at to not use it in merge because we are getting last_modified_at from the usage index
        df_with_search_scores = (
            entities_lf.filter(
                polars.col("combinedSearchRankingMultiplier")
                .is_not_null()
                # We only want to downrank datasets that have a search score multiplier greater than 1. 1 is the minimum score of a dataset
                .and_(polars.col("combinedSearchRankingMultiplier").ne(1))
            )  # noqa: E712
            .filter(polars.col("removed") == False)  # noqa: E712
            .drop(["removed"])
            .drop(["last_modified_at"])
            # We set this to 0 because we want to downrank datasets that have no usage
            .with_columns(polars.lit(0).alias("combinedSearchRankingMultiplier"))
            .rename({"entity_urn": "urn"})
        )
        common_fields = list(
            set(usages_lf.columns).intersection(set(df_with_search_scores.columns))
        )
        usages_lf = df_with_search_scores.join(
            usages_lf, on="urn", how="full", suffix="_right"
        )
        ## Merge all common fields automatically
        for common_field in common_fields:
            right_col = f"{common_field}_right"
            usages_lf = usages_lf.with_columns(
                [
                    polars.col(common_field)
                    .fill_null(polars.col(right_col))
                    .alias(common_field)
                ]
            ).drop(right_col)
        return usages_lf

    def generate_query_usage(self) -> polars.LazyFrame:
        usage_index = "query_queryusagestatisticsaspect_v1"
        entity_index = "queryindex_v2"
        query_entities_schema = {
            "entity_urn": polars.Categorical,
            "last_modified_at": polars.Int64,
            "platform": polars.Categorical,
            "removed": polars.Boolean,
        }

        query_entities = self.load_data_from_es_to_lf(
            schema=query_entities_schema,
            index=entity_index,
            query=QueryBuilder.get_query_entities_query(),
            process_function=self.queries_entities_batch,
        )

        query_usage_schema = {
            "timestampMillis": polars.Int64,
            "lastObserved": polars.Int64,
            "urn": polars.Categorical,
            "eventGranularity": polars.String,
            "queryCount": polars.Int64,
            "userCounts": polars.List(
                polars.Struct(
                    {
                        "usageCount": polars.Int64,
                        "user": polars.String,
                    }
                )
            ),
        }

        lf = self.load_data_from_es_to_lf(
            schema=query_usage_schema,
            index=usage_index,
            query=QueryBuilder.get_query_usage_query(self.config.lookback_days),
            process_function=self.process_query_usage,
        )

        lf = query_entities.join(
            lf, left_on="entity_urn", right_on="urn", how="left", coalesce=False
        ).filter(
            polars.col("removed") == False  # noqa: E712
        )

        total_queries = lf.group_by("urn", "platform").agg(
            polars.col("queryCount").sum().alias("totalSqlQueries"),
            polars.col("last_modified_at").max().alias("last_modified_at"),
        )

        top_users = self.generate_top_users(lf, "usageCount")

        usage_with_top_users = top_users.join(total_queries, on="urn", how="inner")

        usage_with_top_users_with_ranks = self.gen_rank_and_percentile(
            lf=usage_with_top_users,
            count_field="totalSqlQueries",
            urn_field="urn",
            platform_field="platform",
            prefix="queries_",
            use_exp_cdf=False,
        )

        usage_with_top_users_with_ranks = usage_with_top_users_with_ranks.sort(
            by=["platform", "queries_rank"], descending=[False, False]
        )

        return usage_with_top_users_with_ranks

    def generate_dataset_usage(self) -> polars.LazyFrame:
        datasets_lf = self.get_datasets()
        if self.config.set_upstream_table_max_modification_time_for_views:
            datasets_lf = self.set_table_modification_time_for_views(datasets_lf)

        lf = self.load_dataset_usage()

        # Polaris/pandas join merges the join column into one column and that's why we need to filter based on the removed column
        lf = (
            lf.join(datasets_lf, left_on="urn", right_on="entity_urn", how="left")
            .filter(polars.col("removed") == False)  # noqa: E712
            .drop(["removed"])
        )

        total_queries = lf.group_by("urn", "platform").agg(
            polars.col("totalSqlQueries").sum(),
            polars.col("last_modified_at").max().alias("last_modified_at"),
            polars.col("siblings").first().alias("siblings"),
        )

        total_queries = self.generate_empty_usage_for_stale_entities(
            datasets_lf, total_queries
        )

        top_users = self.generate_top_users(lf)

        usage_with_top_users = total_queries.join(top_users, on="urn", how="left")

        usage_with_top_users_with_ranks = self.gen_rank_and_percentile(
            usage_with_top_users, "totalSqlQueries", "urn", "platform", "queries_"
        )

        usage_with_top_users_with_ranks = usage_with_top_users_with_ranks.sort(
            by=["platform", "queries_rank"], descending=[False, False]
        )

        if not self.config.disable_write_usage:
            # Calculate write usage
            if self.config.use_server_side_aggregation:
                write_lf = self.load_write_usage_server_side_aggregation(datasets_lf)
            else:
                write_lf = self.load_write_usage(datasets_lf)
        else:
            logger.info("Write usage disabled")
            write_lf = polars.LazyFrame(
                schema={
                    "urn": polars.Categorical,
                    "platform": polars.Categorical,
                    "write_count": polars.Int64,
                }
            )

        usage_and_write_lf = (
            usage_with_top_users_with_ranks.join(
                write_lf, on="urn", how="full", suffix="_write"
            )
            .with_columns(polars.col("write_count").fill_null(polars.lit(0)))
            .with_columns(polars.col("totalSqlQueries").fill_null(polars.lit(0)))
        )

        # If we get a dataset from the operation aspect index only then we have to use its urn and platform
        usage_and_write_lf = usage_and_write_lf.with_columns(
            polars.col("urn").fill_null(polars.col("urn_write"))
        )
        usage_and_write_lf = usage_and_write_lf.with_columns(
            polars.col("platform").fill_null(polars.col("platform_write"))
        )

        usage_and_write_lf = self.gen_rank_and_percentile(
            usage_and_write_lf, "write_count", "urn", "platform", "write_"
        )
        return usage_and_write_lf

    def load_data_from_es_to_lf(
        self,
        index: str,
        schema: Dict,
        query: Dict,
        process_function: Callable,
        aggregation_key: Optional[str] = None,
        file_to_load: Optional[str] = None,
    ) -> polars.LazyFrame:
        data = self.load_data_from_es(
            index=index,
            query=query,
            process_function=process_function,
            aggregation_key=aggregation_key,
        )

        if not self.config.streaming_mode:
            return polars.LazyFrame(data, schema)
        else:
            assert self.temp_dir is not None, (
                "In Streaming mode temp dir should be set. Normally this should not happen..."
            )

            with tempfile.NamedTemporaryFile(
                delete=False,
                mode="wb",
                dir=self.temp_dir.name,
                prefix=f"{index}_",
                suffix=".parquet",
            ) as temp_file:
                tempfile_name = temp_file.name
                with pq.ParquetWriter(
                    tempfile_name, self.polars_to_arrow_schema(schema)
                ) as writer:
                    logger.debug(f"Creating temporary file {tempfile_name}")

                    self.batch_write_parquet(
                        data,
                        schema,
                        temp_file.name,
                        parquet_writer=writer,
                    )
                # Scan parquet fails in some cases with
            # thread 'polars-1' panicked at crates/polars-parquet/src/arrow/read/deserialize/dictionary_encoded/required_masked_dense.rs:113:72:
            # called `Option::unwrap()` on a `None` value
            # Which only happens if we don't collect immediately
            # return polars.scan_parquet(temp_file.name, schema=schema, low_memory=True).collect().lazy()
            return (
                polars.scan_parquet(temp_file.name, schema=schema, low_memory=True)
                .collect()
                .lazy()
            )

    def load_dataset_usage(self) -> polars.LazyFrame:
        index = "dataset_datasetusagestatisticsaspect_v1"
        schema = {
            "timestampMillis": polars.Int64,
            "urn": polars.Categorical,
            "platform": polars.Categorical,
            "eventGranularity": polars.String,
            "totalSqlQueries": polars.Int64,
            "uniqueUserCount": polars.Int64,
            "userCounts": polars.List(
                polars.Struct(
                    {
                        "count": polars.Int64,
                        "user": polars.String,
                        "userEmail": polars.String,
                    }
                )
            ),
        }

        return self.load_data_from_es_to_lf(
            schema=schema,
            index=index,
            query=QueryBuilder.get_dataset_usage_query(self.config.lookback_days),
            process_function=self.process_batch,
        )

    def get_datasets(self) -> polars.LazyFrame:
        schema = {
            "entity_urn": polars.Categorical,
            "removed": polars.Boolean,
            "last_modified_at": polars.Int64,
            "siblings": polars.List(polars.String),
            "combinedSearchRankingMultiplier": polars.Float64,
            "isView": polars.Boolean,
        }

        return self.load_data_from_es_to_lf(
            schema=schema,
            index="datasetindex_v2",
            query=QueryBuilder.get_dataset_entities_query(),
            process_function=self.soft_deleted_batch,
        )

    def generate_top_users(
        self, lf: polars.LazyFrame, count_field_name: str = "count"
    ) -> polars.LazyFrame:
        #  Getting top users

        top_users = (
            lf.explode("userCounts")
            .unnest("userCounts")
            .filter(polars.col("user").is_not_null())
        )

        top_users = (
            top_users.group_by("urn", "platform", "user")
            .agg(polars.col(count_field_name).sum().alias("count"))
            .sort(by=["urn"], descending=[False])
        )
        top_users = top_users.with_columns(
            polars.col("user")
            .unique()
            .count()
            .over("platform", "urn")
            .alias("distinct_user")
        )

        top_users = top_users.with_columns(
            polars.col("count").sum().over("urn").alias("total_user_count")
        )

        top_users = top_users.with_columns(
            polars.col("count")
            .rank(descending=True, method="ordinal")
            .over("platform", "urn")
            .alias("user_rank")
        )

        top_users = top_users.filter(polars.col("user_rank") <= 10).sort(
            by=["urn", "user_rank"], descending=[False, False]
        )

        top_users = (
            top_users.group_by("urn", "platform", "distinct_user", "total_user_count")
            .agg(
                # polars.concat_list(polars.col("user")).alias("top_users")
                polars.when(polars.col("user").count() == 0)
                .then(polars.concat_list(polars.lit([])))
                .otherwise(polars.concat_list(polars.col("user")))
                .alias("top_users")
                # polars.when(polars.col("user").count() == 0).then(polars.concat_list(polars.lit(polars.Series([], dtype=polars.Int64)))).otherwise(polars.concat_list(polars.col("user"))).alias("top_users")
            )
            .select(
                ["urn", "platform", "top_users", "distinct_user", "total_user_count"]
            )
        )

        top_users = self.gen_rank_and_percentile(
            top_users, "distinct_user", "urn", "platform", "distinct_user_"
        )
        top_users = top_users.filter(polars.col("urn").is_not_null())
        return top_users

    def load_es_data(
        self,
        query: Dict,
        server: Union[OpenSearch, Elasticsearch],
        index: str,
        process_function: Callable,
        aggregation_key: Optional[str] = None,
        batch_size: int = 1000,
        delay: Optional[float] = None,
    ) -> Iterable[Dict[str, Any]]:
        processed_count = 0
        while True:
            with PerfTimer() as timer:
                logger.debug(f"ES query: {query}")
                results = server.search(
                    body=query,
                    size=batch_size,
                    index=(
                        index
                        if not self.config.search_index.opensearch_dialect
                        else None
                    ),
                    params=(
                        {"timeout": self.config.query_timeout}
                        if self.config.search_index.opensearch_dialect
                        else {"request_timeout": self.config.query_timeout}
                    ),
                )
                if not aggregation_key:
                    yield from process_function(results["hits"]["hits"])

                    time_taken = timer.elapsed_seconds()
                    processed_count += len(results["hits"]["hits"])
                    logger.info(
                        f"Processed {len(results['hits']['hits'])} data from {index} index in {time_taken:.3f} seconds. Total: {processed_count} processed."
                    )
                    if len(results["hits"]["hits"]) < batch_size:
                        break
                    query.update({"search_after": results["hits"]["hits"][-1]["sort"]})
                else:
                    yield from process_function(
                        results["aggregations"][aggregation_key]["buckets"]
                    )
                    if (
                        len((results["aggregations"][aggregation_key]["buckets"]))
                        < batch_size
                    ):
                        break
                    if "after_key" in results["aggregations"][aggregation_key]:
                        query["aggs"][aggregation_key]["composite"]["after"] = results[
                            "aggregations"
                        ][aggregation_key]["after_key"]

                if delay:
                    logger.debug(
                        f"Sleeping for {delay} seconds before getting next batch from ES"
                    )
                    time.sleep(delay)

    def get_report(self) -> SourceReport:
        return self.report
