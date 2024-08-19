import logging
import math
import os
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from itertools import chain
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import numpy
import polars
from elasticsearch.client import Elasticsearch
from opensearchpy import OpenSearch
from pydantic import Field
from scipy.stats import expon

from acryl_datahub_cloud.elasticsearch.config import ElasticSearchClientConfig
from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import SourceReport
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
from datahub.metadata._schema_classes import UsageFeaturesClass
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)

platform_regexp = re.compile(r"urn:li:dataset:\(urn:li:dataPlatform:(.+?),.*")
dashboard_chart_platform_regexp = re.compile(r"urn:li:(?:dashboard|chart):\((.+?),.*")
dbt_platform_regexp = re.compile(r"urn:li:dataset:\(urn:li:dataPlatform:dbt,.*\)")

GET_SOFT_DELETED_ENTITIES = {
    "sort": [{"urn": {"order": "asc"}}],
}

GET_UPSTREAMS = {
    "sort": [{"destination.urn": {"order": "asc"}}],
    "query": {
        "bool": {
            "must": [
                {"terms": {"destination.entityType": ["dataset"]}},
                {"terms": {"source.entityType": ["dataset"]}},
            ]
        }
    },
}

GET_DASHBOARD_USAGE_QUERY = {
    "sort": [{"urn": {"order": "asc"}}],
    "query": {
        "bool": {
            "filter": {
                "bool": {
                    "must": [
                        {"range": {"@timestamp": {"gte": "now-30d", "lt": "now/d"}}},
                        {"term": {"isExploded": False}},
                    ]
                }
            }
        }
    },
}

GET_DATASET_USAGE_QUERY = {
    "sort": [{"urn": {"order": "asc"}}],
    "query": {
        "bool": {
            "filter": {
                "bool": {
                    "must": [
                        {"range": {"@timestamp": {"gte": "now-30d/d", "lt": "now/d"}}},
                        {"term": {"isExploded": False}},
                        {"range": {"totalSqlQueries": {"gt": 0}}},
                    ]
                }
            }
        }
    },
}

DATASET_WRITE_USAGE_RAW_QUERY = {
    "sort": [{"urn": {"order": "asc"}}, {"@timestamp": {"order": "asc"}}],
    "query": {
        "bool": {
            "must": [
                {"range": {"@timestamp": {"gte": "now-30d/d", "lte": "now/d"}}},
                {"terms": {"operationType": ["INSERT", "UPDATE", "CREATE"]}},
            ]
        }
    },
    "_source": {
        "includes": ["urn", "@timestamp"],
    },
}

DATASET_WRITE_USAGE_COMPOSITE_QUERY = {
    "query": {
        "bool": {
            "must": [
                {"range": {"@timestamp": {"gte": "now-30d/d", "lte": "now/d"}}},
                {"terms": {"operationType": ["INSERT", "UPDATE", "CREATE"]}},
            ]
        }
    },
    "aggs": {
        "urn_count": {
            "composite": {
                "sources": [{"dataset_operationaspect_v1": {"terms": {"field": "urn"}}}]
            }
        }
    },
}


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


class DataHubUsageFeatureReportingSourceConfig(StatefulIngestionConfigBase):
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


@platform_name(id="datahub", platform_name="DataHub")
@config_class(DataHubUsageFeatureReportingSourceConfig)
@support_status(SupportStatus.INCUBATING)
class DataHubUsageFeatureReportingSource(StatefulIngestionSourceBase):
    platform = "datahub"

    def __init__(
        self, ctx: PipelineContext, config: DataHubUsageFeatureReportingSourceConfig
    ):
        super().__init__(config, ctx)
        # super().__init__(ctx)
        self.config: DataHubUsageFeatureReportingSourceConfig = config
        self.report: DatahubUsageFeatureReport = DatahubUsageFeatureReport()

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

    def soft_deleted_batch(self, results: Iterable) -> Iterable[Dict]:
        with PerfTimer() as timer:
            for doc in results:
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
                    "removed": (
                        doc["_source"]["removed"]
                        if "removed" in doc["_source"]
                        else False
                    ),
                    "siblings": (
                        doc["_source"]["siblings"]
                        if "siblings" in doc["_source"]
                        else []
                    ),
                    "isView": (
                        "View" in doc["_source"]["typeNames"]
                        if "typeNames" in doc["_source"]
                        else False
                    ),
                }
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
                    logging.warning("Platform not found in urn. Skipping...")
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
                    logging.warning("Platform not found in urn. Skipping...")
                    continue

                yield {
                    "urn": doc["_source"]["urn"],
                    "platform": platform,
                }
            time_taken = timer.elapsed_seconds()
            logger.info(
                f"Write Operation aspect processing took {time_taken:.3f} seconds"
            )

    def process_dashboard_usage(self, results: Iterable) -> Iterable[Dict]:
        for doc in results:
            match = re.match(dashboard_chart_platform_regexp, doc["_source"]["urn"])
            if match:
                platform = match.group(1)
                self.report.dashboard_platforms_count[platform] += 1
            else:
                logging.warning("Platform not found in urn. Skipping...")
                continue

            yield {
                "timestampMillis": doc["_source"]["timestampMillis"],
                "lastObserved": doc["_source"]["systemMetadata"]["lastObserved"],
                "urn": doc["_source"]["urn"],
                "eventGranularity": (
                    doc["_source"]["eventGranularity"]
                    if "eventGranularity" in doc["_source"]
                    else None
                ),
                "partitionSpec": doc["_source"]["partitionSpec"],
                "viewsCount": (
                    doc["_source"]["viewsCount"]
                    if "viewsCount" in doc["_source"]
                    else 0
                ),
                "uniqueUserCount": (
                    doc["_source"]["uniqueUserCount"]
                    if "uniqueUserCount" in doc["_source"]
                    else None
                ),
                "userCounts": (
                    doc["_source"]["event"]["userCounts"]
                    if "userCounts" in doc["_source"]["event"]
                    else []
                ),
                "platform": platform,
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
                logging.warning("Source Platform not found in urn. Skipping...")
                continue

            destination_platform_match = re.match(
                platform_regexp, doc["_source"]["destination"]["urn"]
            )
            if destination_platform_match:
                destination_platform = destination_platform_match.group(1)
            else:
                logging.warning("Destination Platform not found in urn. Skipping...")
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
                match = re.match(platform_regexp, doc["_source"]["urn"])
                if match:
                    platform = match.group(1)
                    self.report.dataset_platforms_count[platform] += 1
                else:
                    logging.warning("Platform not found in urn. Skipping...")
                    continue

                yield {
                    "timestampMillis": doc["_source"]["timestampMillis"],
                    "urn": doc["_source"]["urn"],
                    "eventGranularity": doc["_source"]["eventGranularity"],
                    "partitionSpec": doc["_source"]["partitionSpec"],
                    "totalSqlQueries": doc["_source"]["totalSqlQueries"],
                    "uniqueUserCount": doc["_source"]["uniqueUserCount"],
                    "userCounts": (
                        doc["_source"]["event"]["userCounts"]
                        if "userCounts" in doc["_source"]["event"]
                        else None
                    ),
                    "platform": platform,
                }

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
            combinedSearchRankingMultiplier=usage_search_score_multiplier
            * freshness_factor
            * regexp_factor,
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
    ) -> polars.LazyFrame:

        logger.debug(f"Generating rank and percentile for {count_field} field")
        lf = lf.with_columns(
            polars.col(count_field)
            .rank(descending=True, method="max")
            .over(platform_field)
            .alias(f"{prefix}rank")
        )

        if self.config.use_exp_cdf:
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

    def load_write_usage(
        self, soft_deleted_entities_df: polars.LazyFrame
    ) -> polars.LazyFrame:
        wdf = polars.LazyFrame(
            self.load_data_from_es(
                "dataset_operationaspect_v1",
                DATASET_WRITE_USAGE_RAW_QUERY,
                self.write_stat_raw_batch,
            ),
            schema={"urn": polars.Categorical, "platform": polars.Categorical},
            strict=True,
        )
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

        return wdf

    def load_write_usage_server_side_aggregation(
        self, soft_deleted_entities_df: polars.LazyFrame
    ) -> polars.LazyFrame:
        query: Dict = DATASET_WRITE_USAGE_COMPOSITE_QUERY
        query["aggs"]["urn_count"]["composite"]["size"] = self.config.extract_batch_size
        wdf = polars.LazyFrame(
            self.load_data_from_es(
                "dataset_operationaspect_v1",
                DATASET_WRITE_USAGE_COMPOSITE_QUERY,
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
        upstreams_lf = polars.LazyFrame(
            self.load_data_from_es(
                "graph_service_v1",
                GET_UPSTREAMS,
                self.upstream_lineage_batch,
            ),
            schema={
                "source_urn": polars.Categorical,
                "destination_urn": polars.Categorical,
            },
            strict=True,
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

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        if self.config.dataset_usage_enabled:
            with self.report.dataset_usage_processing_time as timer:
                self.report.report_ingestion_stage_start("generate dataset usage")
                yield from self.generate_dataset_usage_mcps()
                time_taken = timer.elapsed_seconds()
                logger.info(f"Dataset Usage generation took {time_taken:.3f} seconds")

        if self.config.dashboard_usage_enabled:
            with self.report.dashboard_usage_processing_time as timer:
                self.report.report_ingestion_stage_start("generate dashboard usage")
                yield from self.generate_dashboard_usage_mcps()

                time_taken = timer.elapsed_seconds()
                logger.info(f"Dashboard Usage generation took {time_taken:.3f}")

        if self.config.chart_usage_enabled:
            with self.report.chart_usage_processing_time as timer:
                self.report.report_ingestion_stage_start("generate chart usage")

                yield from self.generate_chart_usage_mcps()

                time_taken = timer.elapsed_seconds()
                logger.info(f"Chart Usage generation took {time_taken:.3f}")

    def generate_mcp_from_lazyframe(
        self, lazy_frame: polars.LazyFrame
    ) -> Iterable[MetadataWorkUnit]:
        num = 0
        for row in lazy_frame.collect().to_struct():
            num += 1

            if "siblings" in row and row["siblings"]:
                logger.info(f"Siblings found for urn: {row['urn']} -> row['siblings']")

            search_ranking_multipliers: SearchRankingMultipliers = (
                SearchRankingMultipliers()
            )

            if "queries_rank_percentile" in row:
                search_ranking_multipliers = self.search_score(
                    urn=row["urn"],
                    last_update_time=(
                        row["last_modified_at"]
                        if "last_modified_at" in row and row["last_modified_at"]
                        else 0
                    ),
                    usage_percentile=(
                        row["queries_rank_percentile"]
                        if row["queries_rank_percentile"]
                        else 0
                    ),
                )
            elif "viewsCount30Days_rank_percentile" in row:
                search_ranking_multipliers = self.search_score(
                    urn=row["urn"],
                    last_update_time=(
                        row["last_modified_at"]
                        if "last_modified_at" in row and row["last_modified_at"]
                        else 0
                    ),
                    usage_percentile=(
                        row["viewsCount30Days_rank_percentile"]
                        if row["viewsCount30Days_rank_percentile"]
                        else 0
                    ),
                )
                logger.debug(f"Urn: {row['urn']} Score: {search_ranking_multipliers}")

            usage_feature = UsageFeaturesClass(
                queryCountLast30Days=(
                    int(row["totalSqlQueries"])
                    if "totalSqlQueries" in row and row["totalSqlQueries"]
                    else 0
                ),
                usageCountLast30Days=(
                    int(row["totalSqlQueries"])
                    if "totalSqlQueries" in row and row["totalSqlQueries"]
                    else 0
                ),
                queryCountRankLast30Days=(
                    int(row["queries_rank"])
                    if "queries_rank" in row and row["queries_rank"] is not None
                    else None
                ),
                queryCountPercentileLast30Days=(
                    int(row["queries_rank_percentile"])
                    if "queries_rank_percentile" in row
                    and row["queries_rank_percentile"]
                    else 0
                ),
                # queryCountPercentileLast30Days=int(
                #   row["queries_rank_percentile"]) if "queries_rank_percentile" in row and row[
                #   "queries_rank_percentile"] else 0,
                topUsersLast30Days=(
                    list(chain.from_iterable(row["top_users"]))
                    if row["top_users"]
                    else None
                ),
                uniqueUserCountLast30Days=(
                    int(row["distinct_user"]) if row["distinct_user"] else 0
                ),
                uniqueUserRankLast30Days=(
                    int(row["distinct_user_rank"])
                    if "distinct_user_rank" in row
                    and row["distinct_user_rank"] is not None
                    else None
                ),
                uniqueUserPercentileLast30Days=(
                    int(row["distinct_user_rank_percentile"])
                    if "distinct_user_rank_percentile" in row
                    and row["distinct_user_rank_percentile"]
                    else 0
                ),
                writeCountLast30Days=(
                    int(row["write_count"])
                    if "write_count" in row and row["write_count"]
                    else 0
                ),
                writeCountPercentileLast30Days=(
                    int(row["write_rank_percentile"])
                    if "write_count" in row and row["write_rank_percentile"]
                    else 0
                ),
                writeCountRankLast30Days=(
                    int(row["write_rank"])
                    if "write_rank" in row and row["write_rank"]
                    else None
                ),
                viewCountTotal=(
                    int(row["viewsTotal"])
                    if "viewsTotal" in row and row["viewsTotal"]
                    else 0
                ),
                viewCountLast30Days=(
                    int(row["viewsCount30Days"])
                    if "viewsCount30Days" in row and row["viewsCount30Days"]
                    else 0
                ),
                viewCountPercentileLast30Days=(
                    int(row["viewsCount30Days_rank_percentile"])
                    if "viewsCount30Days_rank_percentile" in row
                    else 0
                ),
                usageSearchScoreMultiplier=search_ranking_multipliers.usageSearchScoreMultiplier,
                usageFreshnessScoreMultiplier=search_ranking_multipliers.usageFreshnessScoreMultiplier,
                customDatahubScoreMultiplier=search_ranking_multipliers.customDatahubScoreMultiplier,
                combinedSearchRankingMultiplier=search_ranking_multipliers.combinedSearchRankingMultiplier,
            )

            mcp = MetadataChangeProposalWrapper(
                entityUrn=row["urn"], aspect=usage_feature
            )
            yield mcp.as_workunit(is_primary_source=False)

            if (
                "siblings" in row
                and row["siblings"]
                and self.config.sibling_usage_enabled
            ):
                for sibling in row["siblings"]:
                    if dbt_platform_regexp.match(sibling):
                        dbt_sibling_mcp = MetadataChangeProposalWrapper(
                            entityUrn=sibling, aspect=usage_feature
                        )
                        self.report.sibling_usage_count += 1
                        yield dbt_sibling_mcp.as_workunit(is_primary_source=False)

    def generate_chart_usage(self) -> polars.LazyFrame:
        usage_index = "chartindex_v2"
        entity_index = "chart_chartusagestatisticsaspect_v1"

        return self.generate_dashboard_chart_usage(entity_index, usage_index)

    def generate_dashboard_usage(self) -> polars.LazyFrame:
        usage_index = "dashboard_dashboardusagestatisticsaspect_v1"
        entity_index = "dashboardindex_v2"

        return self.generate_dashboard_chart_usage(entity_index, usage_index)

    def generate_dashboard_chart_usage(
        self, entity_index: str, usage_index: str
    ) -> polars.LazyFrame:
        soft_deleted_df = polars.LazyFrame(
            self.load_data_from_es(
                index=entity_index,
                query=GET_SOFT_DELETED_ENTITIES,
                process_function=self.soft_deleted_batch,
            ),
            schema={
                "entity_urn": polars.Categorical,
                "removed": bool,
                "last_modified_at": polars.Int64,
                "siblings": polars.List(polars.String),
                "isView": polars.Boolean,
            },
            strict=True,
        )

        lf: polars.LazyFrame = polars.LazyFrame(
            self.load_data_from_es(
                index=usage_index,
                query=GET_DASHBOARD_USAGE_QUERY,
                process_function=self.process_dashboard_usage,
            ),
            schema={
                "timestampMillis": polars.Int64,
                "lastObserved": polars.Int64,
                "urn": polars.Categorical,
                "platform": polars.Categorical,
                "eventGranularity": polars.String,
                "partitionSpec": polars.Struct(
                    {
                        "partition": polars.String,
                    }
                ),
                "viewsCount": polars.Int64,
                "userCounts": polars.List(
                    polars.Struct(
                        {
                            "usageCount": polars.Int64,
                            "user": polars.String,
                        }
                    )
                ),
            },
        )

        lf = (
            lf.join(soft_deleted_df, left_on="urn", right_on="entity_urn", how="inner")
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
        lf = views_sum_with_top_users.join(incremental_views_sum, on="urn", how="left")
        lf = lf.with_columns(
            polars.when(
                polars.col("total_user_count")
                .is_null()
                .or_(polars.col("total_user_count") <= 0)
            )
            .then(polars.col("viewsCountTotal30Days"))
            .otherwise(polars.col("total_user_count"))
            .alias("viewsCount30Days")
        )

        lf = self.gen_rank_and_percentile(
            lf, "viewsCount30Days", "urn", "platform", "viewsCount30Days_"
        )

        return lf

    def generate_dataset_usage(self) -> polars.LazyFrame:
        datasets_lf = self.get_datasets()
        if self.config.set_upstream_table_max_modification_time_for_views:
            datasets_lf = self.set_table_modification_time_for_views(datasets_lf)

        index = "dataset_datasetusagestatisticsaspect_v1"
        lf: polars.LazyFrame = polars.LazyFrame(
            self.load_data_from_es(
                index=index,
                query=GET_DATASET_USAGE_QUERY,
                process_function=self.process_batch,
            ),
            schema={
                "timestampMillis": polars.Int64,
                "urn": polars.Categorical,
                "platform": polars.Categorical,
                "eventGranularity": polars.String,
                "partitionSpec": polars.Struct(
                    {
                        "partition": polars.String,
                    }
                ),
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
            },
        )

        # Polaris/pandas join merges the join column into one column and that's why we need to filter based on the removed column
        lf = (
            lf.join(datasets_lf, left_on="urn", right_on="entity_urn", how="inner")
            .filter(polars.col("removed") == False)  # noqa: E712
            .drop(["removed"])
        )
        total_queries = lf.group_by("urn", "platform").agg(
            polars.col("totalSqlQueries").sum(),
            polars.col("last_modified_at").max().alias("last_modified_at"),
            polars.col("siblings").first().alias("siblings"),
        )

        top_users = self.generate_top_users(lf)

        usage_with_top_users = top_users.join(total_queries, on="urn", how="inner")

        usage_with_top_users_with_ranks = self.gen_rank_and_percentile(
            usage_with_top_users, "totalSqlQueries", "urn", "platform", "queries_"
        )

        usage_with_top_users_with_ranks = usage_with_top_users_with_ranks.sort(
            by=["platform", "queries_rank"], descending=[False, False]
        )

        # Calculate write usage
        if self.config.use_server_side_aggregation:
            write_lf = self.load_write_usage_server_side_aggregation(datasets_lf)
        else:
            write_lf = self.load_write_usage(datasets_lf)

        usage_and_write_lf = (
            usage_with_top_users_with_ranks.join(
                write_lf, on="urn", how="full", suffix="_write"
            )
            .with_columns("write_count")
            .fill_null(polars.lit(0))
            .with_columns("totalSqlQueries")
            .fill_null(polars.lit(0))
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

    def get_datasets(self) -> polars.LazyFrame:
        datasets_df = polars.LazyFrame(
            self.load_data_from_es(
                index="datasetindex_v2",
                query=GET_SOFT_DELETED_ENTITIES,
                process_function=self.soft_deleted_batch,
            ),
            schema={
                "entity_urn": polars.Categorical,
                "removed": bool,
                "last_modified_at": polars.Int64,
                "siblings": polars.List(polars.String),
                "isView": polars.Boolean,
            },
            strict=True,
        )
        return datasets_df

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
                    logger.info(
                        f"Processed {len(results['hits']['hits'''])} data from {index} index in {time_taken:.3f} seconds"
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
