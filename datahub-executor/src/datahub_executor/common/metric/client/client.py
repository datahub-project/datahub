import logging
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from datahub.configuration.common import OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DataHubMetricCubeEventClass,
    DatasetProfileClass,
    OperationClass,
    OperationTypeClass,
    SystemMetadataClass,
)

from datahub_executor.common.metric.types import (
    Metric,
    Operation,
)

logger = logging.getLogger(__name__)
_DEFAULT_METRIC_LIMIT = 200


class MetricClient:
    """
    A client that interacts with DataHubGraph to fetch and store time-series metric data.
    """

    def __init__(self, graph: DataHubGraph) -> None:
        self.graph = graph

    def save_metric_value(self, metric_urn: str, metric: Metric) -> None:
        """
        Save a new metric record.
        """
        logger.debug("Saving metric %s for entity %s", metric, metric_urn)

        timestamp = int(time.time() * 1000)
        run_id = f"save-metric-{metric_urn}-{timestamp}"

        metric_event = DataHubMetricCubeEventClass(
            timestampMillis=timestamp,
            measure=metric.value,
            reportedTimeMillis=timestamp,
        )

        mcpw = MetadataChangeProposalWrapper(
            entityUrn=metric_urn,
            aspect=metric_event,
            systemMetadata=SystemMetadataClass(runId=run_id, lastObserved=timestamp),
        )

        self.graph.emit_mcp(mcpw)

    def fetch_metric_values(
        self, metric_urn: str, lookback: timedelta, limit: int = _DEFAULT_METRIC_LIMIT
    ) -> List[Metric]:
        """
        Fetch historical metrics based on a time duration.
        """
        end_time = datetime.now(timezone.utc)
        start_time = end_time - lookback

        return self.fetch_metric_values_by_time(
            metric_urn,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

    def fetch_metric_values_by_time(
        self,
        metric_urn: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = _DEFAULT_METRIC_LIMIT,
    ) -> List[Metric]:
        """
        Fetch historical metric values within a given time range.

        Args:
            metric_urn (str): The metric's URN.
            start_time_ms (int): Start timestamp (milliseconds).
            end_time_ms (int): End timestamp (milliseconds).

        Returns:
            List[Metric]: A list of metric values.
        """
        logger.debug("Fetching historical metrics for entity %s", metric_urn)

        metric_value_aspects = self.graph.get_timeseries_values(
            entity_urn=metric_urn,
            aspect_type=DataHubMetricCubeEventClass,
            filter={
                "or": [
                    {
                        "and": self._build_timeseries_filter(start_time, end_time),
                    }
                ]
            },
            limit=limit,
        )

        return [
            Metric(
                timestamp_ms=metric_event.timestampMillis,
                value=metric_event.measure,
            )
            for metric_event in metric_value_aspects or []
        ]

    def fetch_row_count_metric_values(
        self,
        metric_urn: Optional[str],
        dataset_urn: Optional[str],
        start_time: datetime,
        end_time: datetime,
        limit: int = _DEFAULT_METRIC_LIMIT,
        dataset_profiles_fallback: bool = True,
    ) -> List[Metric]:
        metrics = []
        if metric_urn is not None:
            try:
                metrics = self.fetch_metric_values_by_time(
                    metric_urn=metric_urn,
                    start_time=start_time,
                    end_time=end_time,
                    limit=limit,
                )
            except OperationalError as e:
                if (
                    dataset_profiles_fallback
                    and "Failed to find entity with name dataHubMetricCube"
                    in str(e.info)
                ):
                    # Gracefully handle the case where the backend doesn't have the metric cube entity yet.
                    # This should generally only happen when testing against older remote instances,
                    # but should never happen in production.
                    pass
                else:
                    raise e

        if (
            dataset_profiles_fallback
            and len(metrics) < limit
            and dataset_urn is not None
        ):
            # If we have started producing metric cube metrics, then we should only
            # consider datasetProfile for historical data prior to the oldest metric cube metric.
            # This way, we can avoid duplicate data in case we due wrote metric values to
            # both the metric cube entity and the dataset profile.
            updated_end_time = min(
                [
                    end_time,
                    *[metric.timestamp() for metric in metrics],
                ]
            )
            # If we got partial results from the metric cube, fetch the rest from the dataset profile.
            metrics.extend(
                self.fetch_row_counts_from_dataset_profile(
                    dataset_urn=dataset_urn,
                    start_time=start_time,
                    end_time=updated_end_time,
                    limit=(limit - len(metrics)),
                )
            )
            metrics.sort(key=lambda x: x.timestamp_ms)

        return metrics

    def fetch_row_counts_from_dataset_profile(
        self,
        dataset_urn: str,
        *,
        start_time: datetime,
        end_time: datetime,
        limit: int = _DEFAULT_METRIC_LIMIT,
    ) -> List[Metric]:
        profiles = self.graph.get_timeseries_values(
            entity_urn=dataset_urn,
            aspect_type=DatasetProfileClass,
            filter={
                "or": [
                    {
                        "and": self._build_timeseries_filter(start_time, end_time),
                    }
                ]
            },
            limit=limit,
        )

        # TODO: We should be pushing down the rowCount filter so that the limit is applied before the profile is fetched.
        metrics = []
        for profile in profiles:
            if profile.rowCount is not None:
                metrics.append(
                    Metric(
                        timestamp_ms=profile.timestampMillis,
                        value=profile.rowCount,
                    )
                )

        return metrics

    def fetch_operations(
        self,
        entity_urn: str,
        lookback: timedelta,
        limit: int = 50,
        ignore_types: Optional[List[str]] = None,
    ) -> List[Operation]:
        """
        Fetch historical metrics based on a time duration.
        """
        end_time = datetime.now(timezone.utc)
        start_time = end_time - lookback

        logger.info(
            f"Fetching operations between start time {start_time} and end time {end_time}"
        )

        return self.fetch_operations_by_time(
            entity_urn,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            ignore_types=ignore_types,
        )

    def fetch_operations_by_time(
        self,
        entity_urn: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 50,
        ignore_types: Optional[List[str]] = None,
    ) -> List[Operation]:
        """
        Fetch historical operations (INSERT, UPDATE, DELETEs) for a specific urn within a given time range.

        Args:
            entity_urn (str): The entity's URN.
            start_time_ms (int): Start timestamp (milliseconds).
            end_time_ms (int): End timestamp (milliseconds).

        Returns:
            List[Operation]: A list of operation values for each update operation.
        """
        logger.debug("Fetching historical operations for entity %s", entity_urn)

        operation_aspects = self.graph.get_timeseries_values(
            entity_urn=entity_urn,
            aspect_type=OperationClass,
            filter={
                "or": [
                    {
                        "and": [
                            {
                                "field": "timestampMillis",
                                "condition": "GREATER_THAN_OR_EQUAL_TO",
                                "value": str(int(start_time.timestamp() * 1000)),
                            },
                            {
                                "field": "timestampMillis",
                                "condition": "LESS_THAN_OR_EQUAL_TO",
                                "value": str(int(end_time.timestamp() * 1000)),
                            },
                            {
                                "field": "operationType",
                                "condition": "EQUAL",
                                "values": ignore_types,
                                "negated": True,
                            },
                            {
                                "field": "customOperationType",
                                "condition": "EQUAL",
                                "values": ignore_types,
                                "negated": True,
                            },
                        ]
                    }
                ]
            },
            limit=limit,
        )

        logger.info(
            f"Fetched operation aspects {len(operation_aspects)} operation aspects"
        )

        return [
            Operation(
                timestamp_ms=operation.timestampMillis,
                type=str(
                    operation.operationType
                    if operation.operationType is not OperationTypeClass.CUSTOM
                    else operation.customOperationType
                ),
            )
            for operation in operation_aspects or []
        ]

    @classmethod
    def _build_timeseries_filter(
        cls, start_time: datetime, end_time: datetime
    ) -> list[dict]:
        # TODO: Migrate this to use the new Search SDK filters dsl once that's available in acryl-main.
        return [
            {
                "field": "timestampMillis",
                "condition": "GREATER_THAN_OR_EQUAL_TO",
                "value": str(int(start_time.timestamp() * 1000)),
            },
            {
                "field": "timestampMillis",
                "condition": "LESS_THAN_OR_EQUAL_TO",
                "value": str(int(end_time.timestamp() * 1000)),
            },
        ]
