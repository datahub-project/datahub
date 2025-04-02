import logging
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AssertionEvaluationContextClass,
    AssertionEvaluationParametersClass,
    AssertionEvaluationSpecClass,
    AssertionInfoClass,
    AuditLogSpecClass,
    CronScheduleClass,
    DatasetFieldAssertionParametersClass,
    DatasetFreshnessAssertionParametersClass,
    DatasetVolumeAssertionParametersClass,
    FreshnessFieldSpecClass,
    MonitorAnomalyEventClass,
    MonitorErrorClass,
    MonitorStateClass,
    MonitorTypeClass,
    SystemMetadataClass,
)

from datahub_executor.common.metric.types import Metric
from datahub_executor.common.monitor.client.patch_builder import MonitorPatchBuilder
from datahub_executor.common.types import Anomaly, AssertionEvaluationSpec, CronSchedule

logger = logging.getLogger(__name__)

_DEFAULT_ANOMALY_LIMIT = 200


class MonitorClient:
    """
    Shell class that uses a DataHubGraph client to update Assertions
    based on an inference step.
    """

    def __init__(self, graph: DataHubGraph) -> None:
        self.graph = graph

    def fetch_monitor_anomalies(
        self,
        urn: str,
        lookback: timedelta,
        limit: int,
    ) -> List[Anomaly]:
        """
        Fetch anomaly events for a given monitor urn.
        """
        end_time = datetime.now(timezone.utc)
        start_time = end_time - lookback

        return self.fetch_monitor_anomalies_by_time(
            urn,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

    def fetch_monitor_anomalies_by_time(
        self,
        monitor_urn: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = _DEFAULT_ANOMALY_LIMIT,
    ) -> List[Anomaly]:
        """
        Fetch anomaly events for a given monitor urn.

        Args:
            monitor_urn (str): The monitor's URN.
            start_time_ms (int): Start timestamp (milliseconds).
            end_time_ms (int): End timestamp (milliseconds).

        Returns:
            List[Anomaly]: A list of anomalies, if there are any.
        """
        logger.info("Fetching historical anomalies for monitor %s", monitor_urn)

        monitor_event_aspects = self.graph.get_timeseries_values(
            entity_urn=monitor_urn,
            aspect_type=MonitorAnomalyEventClass,
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
            Anomaly(
                timestamp_ms=monitor_event.timestampMillis,
                metric=self._get_monitor_anomaly_event_metric(monitor_event),
            )
            for monitor_event in monitor_event_aspects or []
        ]

    def update_assertion_info(
        self,
        urn: str,
        assertion_info: AssertionInfoClass,
    ) -> None:
        """
        Persist an updated assertion to DataHub or other stores.
        """
        logger.info(
            f"AssertionsClient: Saving assertion info {assertion_info} for urn {urn}"
        )

        timestamp = int(time.time() * 1000)
        run_id = f"save-assertion-{urn}-{timestamp}"

        mcpws = []

        # Info MCP
        mcpws.append(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=assertion_info,
                systemMetadata=SystemMetadataClass(
                    runId=run_id, lastObserved=timestamp
                ),
            )
        )

        self.graph.emit_mcps(mcpws)

    def patch_volume_monitor_evaluation_context(
        self,
        monitor_urn: str,
        assertion_urn: str,
        new_assertion_evaluation_context: AssertionEvaluationContextClass,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        """
        Patch the evaluation context for a volume monitor
        """
        logger.info(
            "AssertionsClient: Patching volume assertion monitor context %s",
            new_assertion_evaluation_context,
        )

        self._validate_volume_evaluation_spec(evaluation_spec)

        # Build monitor patch builder and apply the patch
        monitor_patch_builder = self._create_base_monitor_patch_builder(monitor_urn)

        volume_assertion_spec = self._build_volume_assertion_evaluation_spec(
            assertion_urn, evaluation_spec, new_assertion_evaluation_context
        )

        monitor_patch_builder.set_assertion_monitor_assertions(
            assertions=[volume_assertion_spec]
        )
        mcps = monitor_patch_builder.build()
        self.graph.emit_mcps(mcps)

    def patch_freshness_monitor_evaluation_spec(
        self,
        monitor_urn: str,
        assertion_urn: str,
        new_assertion_evaluation_context: AssertionEvaluationContextClass,
        new_schedule: CronSchedule,
        current_evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        """
        Patch the evaluation context for the freshness monitor
        """
        logger.info(
            f"AssertionsClient: Patching freshness assertion {assertion_urn} monitor urn {monitor_urn} monitor context %s",
            new_assertion_evaluation_context,
        )

        self._validate_freshness_evaluation_spec(current_evaluation_spec)

        # Build monitor patch builder and apply the patch
        monitor_patch_builder = self._create_base_monitor_patch_builder(monitor_urn)

        freshness_assertion_spec = self._build_freshness_assertion_evaluation_spec(
            assertion_urn,
            current_evaluation_spec,
            new_schedule,
            new_assertion_evaluation_context,
        )

        monitor_patch_builder.set_assertion_monitor_assertions(
            assertions=[freshness_assertion_spec]
        )
        mcps = monitor_patch_builder.build()

        logger.info(
            f"Emitting patch {assertion_urn} monitor urn {monitor_urn} monitor context %s",
            new_assertion_evaluation_context,
        )
        self.graph.emit_mcps(mcps)

    def patch_field_metric_monitor_evaluation_context(
        self,
        monitor_urn: str,
        assertion_urn: str,
        new_assertion_evaluation_context: AssertionEvaluationContextClass,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        """
        Patch the evaluation context for the field metric monitor
        """
        logger.info(
            f"AssertionsClient: Patching field metric assertion {assertion_urn} monitor urn {monitor_urn} monitor context %s",
            new_assertion_evaluation_context,
        )

        self._validate_field_metric_evaluation_spec(evaluation_spec)

        # Build monitor patch builder and apply the patch
        monitor_patch_builder = self._create_base_monitor_patch_builder(monitor_urn)

        field_metric_assertion_spec = (
            self._build_field_metric_assertion_evaluation_spec(
                assertion_urn, evaluation_spec, new_assertion_evaluation_context
            )
        )

        monitor_patch_builder.set_assertion_monitor_assertions(
            assertions=[field_metric_assertion_spec]
        )
        mcps = monitor_patch_builder.build()

        logger.info(
            f"Emitting patch {assertion_urn} monitor urn {monitor_urn} monitor context %s",
            new_assertion_evaluation_context,
        )
        self.graph.emit_mcps(mcps)

    def patch_monitor_state(
        self,
        monitor_urn: str,
        new_state: MonitorStateClass | str,
        error: Optional[MonitorErrorClass],
    ) -> None:
        """
        Patch the state for the monitor
        """
        logger.debug(
            "AssertionsClient: Patching monitor state info %s",
            new_state,
        )
        monitor_patch_builder = MonitorPatchBuilder(
            urn=monitor_urn,
        )

        monitor_patch_builder.set_state(new_state)

        if error:
            monitor_patch_builder.set_error(error)

        mcps = monitor_patch_builder.build()
        self.graph.emit_mcps(mcps)

    # Helper methods for validation

    def _validate_volume_evaluation_spec(
        self, evaluation_spec: AssertionEvaluationSpec | None
    ) -> None:
        """Validate that the evaluation spec has the required parameters for volume assertions"""
        if (
            not evaluation_spec
            or not evaluation_spec.schedule
            or not evaluation_spec.parameters.dataset_volume_parameters
        ):
            raise Exception(
                "Failed to update volume assertion monitor evaluation context. Existing context is missing required evaluation parameters 'schedule' and 'parameters'"
            )

    def _validate_freshness_evaluation_spec(
        self, evaluation_spec: AssertionEvaluationSpec | None
    ) -> None:
        """Validate that the evaluation spec has the required parameters for freshness assertions"""
        if (
            not evaluation_spec
            or not evaluation_spec.schedule
            or not evaluation_spec.parameters.dataset_freshness_parameters
        ):
            raise Exception(
                "Failed to update freshness assertion monitor evaluation context. Existing context is missing required evaluation parameters 'schedule' and 'parameters'"
            )

    def _validate_field_metric_evaluation_spec(
        self, evaluation_spec: AssertionEvaluationSpec | None
    ) -> None:
        """Validate that the evaluation spec has the required parameters for field metric assertions"""
        if (
            not evaluation_spec
            or not evaluation_spec.schedule
            or not evaluation_spec.parameters.dataset_field_parameters
        ):
            raise Exception(
                "Failed to update field metric assertion monitor evaluation context. Existing context is missing required evaluation parameters 'schedule' and 'parameters'"
            )

    # Helper methods for creating assertion evaluation specs

    def _create_base_monitor_patch_builder(
        self, monitor_urn: str
    ) -> MonitorPatchBuilder:
        """Create a base monitor patch builder with the type set to ASSERTION"""
        monitor_patch_builder = MonitorPatchBuilder(urn=monitor_urn)
        monitor_patch_builder.set_type(MonitorTypeClass.ASSERTION)
        return monitor_patch_builder

    def _build_volume_assertion_evaluation_spec(
        self,
        assertion_urn: str,
        evaluation_spec: AssertionEvaluationSpec,
        assertion_evaluation_context: AssertionEvaluationContextClass,
    ) -> AssertionEvaluationSpecClass:
        """Build an assertion evaluation spec for volume assertions"""
        return AssertionEvaluationSpecClass(
            assertion=assertion_urn,
            schedule=CronScheduleClass(
                cron=evaluation_spec.schedule.cron,
                timezone=evaluation_spec.schedule.timezone,
            ),
            parameters=AssertionEvaluationParametersClass(
                type=models.AssertionEvaluationParametersTypeClass.DATASET_VOLUME,
                datasetVolumeParameters=DatasetVolumeAssertionParametersClass(
                    sourceType=evaluation_spec.parameters.dataset_volume_parameters.source_type.value  # type: ignore
                ),
            ),
            context=assertion_evaluation_context,
        )

    def _build_freshness_field_spec(
        self, evaluation_spec: AssertionEvaluationSpec
    ) -> Optional[FreshnessFieldSpecClass]:
        """Build a freshness field spec from the evaluation spec"""
        if not evaluation_spec.parameters.dataset_freshness_parameters.field:  # type: ignore
            return None

        return FreshnessFieldSpecClass(
            kind=evaluation_spec.parameters.dataset_freshness_parameters.field.kind.value  # type: ignore
            if evaluation_spec.parameters.dataset_freshness_parameters.field.kind  # type: ignore
            else None,
            path=evaluation_spec.parameters.dataset_freshness_parameters.field.path,  # type: ignore
            type=evaluation_spec.parameters.dataset_freshness_parameters.field.type,  # type: ignore
            nativeType=evaluation_spec.parameters.dataset_freshness_parameters.field.native_type,  # type: ignore
        )

    def _build_freshness_assertion_evaluation_spec(
        self,
        assertion_urn: str,
        evaluation_spec: AssertionEvaluationSpec,
        schedule: CronSchedule,
        assertion_evaluation_context: AssertionEvaluationContextClass,
    ) -> AssertionEvaluationSpecClass:
        """Build an assertion evaluation spec for freshness assertions"""
        field = self._build_freshness_field_spec(evaluation_spec)

        audit_log = None
        if evaluation_spec.parameters.dataset_freshness_parameters.audit_log:  # type: ignore
            audit_log = AuditLogSpecClass(
                operationTypes=evaluation_spec.parameters.dataset_freshness_parameters.audit_log.operation_types,  # type: ignore
                userName=evaluation_spec.parameters.dataset_freshness_parameters.audit_log.user_name,  # type: ignore
            )

        return AssertionEvaluationSpecClass(
            assertion=assertion_urn,
            schedule=CronScheduleClass(
                cron=schedule.cron,
                timezone=schedule.timezone,
            ),
            parameters=AssertionEvaluationParametersClass(
                type=models.AssertionEvaluationParametersTypeClass.DATASET_FRESHNESS,
                datasetFreshnessParameters=DatasetFreshnessAssertionParametersClass(
                    sourceType=evaluation_spec.parameters.dataset_freshness_parameters.source_type.value,  # type: ignore
                    field=field,
                    auditLog=audit_log,
                ),
            ),
            context=assertion_evaluation_context,
        )

    def _build_field_metric_changed_rows_field_spec(
        self, evaluation_spec: AssertionEvaluationSpec
    ) -> Optional[FreshnessFieldSpecClass]:
        """Build a field spec for changed rows from the evaluation spec"""
        if not evaluation_spec.parameters.dataset_field_parameters.changed_rows_field:  # type: ignore
            return None

        return FreshnessFieldSpecClass(
            kind=evaluation_spec.parameters.dataset_field_parameters.changed_rows_field.kind.value  # type: ignore
            if evaluation_spec.parameters.dataset_field_parameters.changed_rows_field.kind  # type: ignore
            else None,
            path=evaluation_spec.parameters.dataset_field_parameters.changed_rows_field.path,  # type: ignore
            type=evaluation_spec.parameters.dataset_field_parameters.changed_rows_field.type,  # type: ignore
            nativeType=evaluation_spec.parameters.dataset_field_parameters.changed_rows_field.native_type,  # type: ignore
        )

    def _build_field_metric_assertion_evaluation_spec(
        self,
        assertion_urn: str,
        evaluation_spec: AssertionEvaluationSpec,
        assertion_evaluation_context: AssertionEvaluationContextClass,
    ) -> AssertionEvaluationSpecClass:
        """Build an assertion evaluation spec for field metric assertions"""
        field = self._build_field_metric_changed_rows_field_spec(evaluation_spec)

        return AssertionEvaluationSpecClass(
            assertion=assertion_urn,
            schedule=CronScheduleClass(
                cron=evaluation_spec.schedule.cron,
                timezone=evaluation_spec.schedule.timezone,
            ),
            parameters=AssertionEvaluationParametersClass(
                type=models.AssertionEvaluationParametersTypeClass.DATASET_FIELD,
                datasetFieldParameters=DatasetFieldAssertionParametersClass(
                    sourceType=evaluation_spec.parameters.dataset_field_parameters.source_type.value,  # type: ignore
                    changedRowsField=field,
                ),
            ),
            context=assertion_evaluation_context,
        )

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

    def _get_monitor_anomaly_event_metric(
        self, monitor_anomaly_event: MonitorAnomalyEventClass
    ) -> Optional[Metric]:
        if (
            monitor_anomaly_event.source
            and monitor_anomaly_event.source.properties
            and monitor_anomaly_event.source.properties.assertionMetric
        ):
            value = monitor_anomaly_event.source.properties.assertionMetric.value
            timestamp_ms = (
                monitor_anomaly_event.source.properties.assertionMetric.timestampMs
            )
            return Metric(value=value, timestamp_ms=timestamp_ms)
        return None
