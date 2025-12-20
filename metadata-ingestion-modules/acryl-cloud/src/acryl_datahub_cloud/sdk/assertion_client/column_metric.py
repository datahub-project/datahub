from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional, Union

from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    AssertionMode,
    _HasColumnMetricFunctionality,
)
from acryl_datahub_cloud.sdk.assertion.column_metric_assertion import (
    ColumnMetricAssertion,
)
from acryl_datahub_cloud.sdk.assertion_client.helpers import (
    DEFAULT_CREATED_BY,
    _merge_field,
    _validate_required_field,
    retrieve_assertion_and_monitor_by_urn,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    AssertionIncidentBehaviorInputTypes,
    DetectionMechanismInputTypes,
)
from acryl_datahub_cloud.sdk.assertion_input.column_metric_assertion_input import (
    ColumnMetricAssertionParameters,
    _ColumnMetricAssertionInput,
)
from acryl_datahub_cloud.sdk.assertion_input.column_metric_constants import (
    MetricInputType,
    OperatorInputType,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion, TagsInputType
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, MonitorUrn

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient

logger = logging.getLogger(__name__)


class ColumnMetricAssertionClient:
    """Client for managing column metric assertions."""

    def __init__(self, client: "DataHubClient"):
        self.client = client

    def _validate_required_column_fields_for_creation(
        self,
        column_name: Optional[str],
        metric_type: Optional[MetricInputType],
        operator: Optional[OperatorInputType],
    ) -> None:
        """Validate required fields for column metric assertion creation."""
        _validate_required_field(
            column_name, "column_name", "when creating a new assertion (urn is None)"
        )
        _validate_required_field(
            metric_type, "metric_type", "when creating a new assertion (urn is None)"
        )
        _validate_required_field(
            operator, "operator", "when creating a new assertion (urn is None)"
        )

    def _validate_required_column_fields_for_update(
        self,
        column_name: Optional[str],
        metric_type: Optional[MetricInputType],
        operator: Optional[OperatorInputType],
        assertion_urn: Union[str, AssertionUrn],
    ) -> None:
        """Validate required fields after attempting to fetch from existing assertion."""
        context = f"and not found in existing assertion {assertion_urn}. The existing assertion may be invalid or corrupted."
        _validate_required_field(column_name, "column_name", context)
        _validate_required_field(metric_type, "metric_type", context)
        _validate_required_field(operator, "operator", context)

    def _retrieve_assertion_and_monitor(
        self,
        urn: Union[str, AssertionUrn],
        dataset_urn: Union[str, DatasetUrn],
    ) -> tuple[Optional[Assertion], MonitorUrn, Optional[Monitor]]:
        """Retrieve the assertion and monitor entities from the DataHub instance.

        Args:
            urn: The assertion URN.
            dataset_urn: The dataset URN.

        Returns:
            The assertion and monitor entities.
        """
        return retrieve_assertion_and_monitor_by_urn(self.client, urn, dataset_urn)

    def sync_column_metric_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        column_name: Optional[str] = None,
        metric_type: Optional[MetricInputType] = None,
        operator: Optional[OperatorInputType] = None,
        criteria_parameters: Optional[ColumnMetricAssertionParameters] = None,
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
    ) -> ColumnMetricAssertion:
        now_utc = datetime.now(timezone.utc)

        if updated_by is None:
            logger.warning(
                f"updated_by is not set, using {DEFAULT_CREATED_BY} as a placeholder"
            )
            updated_by = DEFAULT_CREATED_BY

        # 1. Retrieve and merge the assertion input with any existing assertion and monitor entities,
        # or build a new assertion input if the assertion does not exist:
        assertion_input = self._retrieve_and_merge_column_metric_assertion_and_monitor(
            dataset_urn=dataset_urn,
            urn=urn,
            column_name=column_name,
            metric_type=metric_type,
            operator=operator,
            criteria_parameters=criteria_parameters,
            display_name=display_name,
            enabled=enabled,
            detection_mechanism=detection_mechanism,
            incident_behavior=incident_behavior,
            tags=tags,
            updated_by=updated_by,
            now_utc=now_utc,
            schedule=schedule,
        )

        # 2. Upsert the assertion and monitor entities:
        assertion_entity, monitor_entity = (
            assertion_input.to_assertion_and_monitor_entities()
        )
        # If assertion upsert fails, we won't try to upsert the monitor
        self.client.entities.upsert(assertion_entity)
        # TODO: Wrap monitor upsert in a try-except and delete the assertion if monitor upsert fails (once delete is implemented https://linear.app/acryl-data/issue/OBS-1350/add-delete-method-to-entity-clientpy)
        # try:
        self.client.entities.upsert(monitor_entity)
        # except Exception as e:
        #     logger.error(f"Error upserting monitor: {e}")
        #     self.client.entities.delete(assertion_entity)
        #     raise e

        return ColumnMetricAssertion._from_entities(assertion_entity, monitor_entity)

    def _extract_fields_from_existing_assertion(
        self,
        maybe_assertion_entity: Optional[Assertion],
        column_name: Optional[str],
        metric_type: Optional[MetricInputType],
        operator: Optional[OperatorInputType],
        criteria_parameters: Optional[ColumnMetricAssertionParameters],
    ) -> tuple[
        Optional[str],
        Optional[MetricInputType],
        Optional[OperatorInputType],
        Optional[ColumnMetricAssertionParameters],
        Optional[tuple],
    ]:
        """Extract missing fields from existing assertion entity."""
        gms_criteria_type_info = None

        if maybe_assertion_entity is None:
            return (
                column_name,
                metric_type,
                operator,
                criteria_parameters,
                gms_criteria_type_info,
            )

        assertion_info = maybe_assertion_entity.info
        if (
            hasattr(assertion_info, "fieldMetricAssertion")
            and assertion_info.fieldMetricAssertion
        ):
            field_metric_assertion = assertion_info.fieldMetricAssertion

            # Extract column_name
            if (
                column_name is None
                and hasattr(field_metric_assertion, "field")
                and hasattr(field_metric_assertion.field, "path")
            ):
                column_name = field_metric_assertion.field.path

            # Extract metric_type
            if metric_type is None and hasattr(field_metric_assertion, "metric"):
                metric_type = field_metric_assertion.metric

            # Extract operator
            if operator is None and hasattr(field_metric_assertion, "operator"):
                operator = field_metric_assertion.operator

            # Extract criteria_parameters
            if criteria_parameters is None and hasattr(
                field_metric_assertion, "parameters"
            ):
                params = field_metric_assertion.parameters
                if params and hasattr(params, "value") and params.value:
                    criteria_parameters = params.value.value
                elif (
                    params
                    and hasattr(params, "minValue")
                    and hasattr(params, "maxValue")
                    and params.minValue
                    and params.maxValue
                ):
                    criteria_parameters = (params.minValue.value, params.maxValue.value)

        # Extract gms_criteria_type_info (outside the fieldMetricAssertion check)
        gms_criteria_type_info = (
            _HasColumnMetricFunctionality._get_criteria_parameters_with_type(
                maybe_assertion_entity
            )
        )

        return (
            column_name,
            metric_type,
            operator,
            criteria_parameters,
            gms_criteria_type_info,
        )

    def _create_existing_assertion_from_entities(
        self,
        maybe_assertion_entity: Optional[Assertion],
        maybe_monitor_entity: Optional[Monitor],
        monitor_urn: MonitorUrn,
        enabled: Optional[bool],
    ) -> Optional[ColumnMetricAssertion]:
        """Create existing assertion object from entities, handling missing monitor case."""
        if maybe_assertion_entity and maybe_monitor_entity:
            return ColumnMetricAssertion._from_entities(
                maybe_assertion_entity, maybe_monitor_entity
            )

        if maybe_assertion_entity and not maybe_monitor_entity:
            monitor_mode = (
                "ACTIVE" if enabled else "INACTIVE" if enabled is not None else "ACTIVE"
            )
            return ColumnMetricAssertion._from_entities(
                maybe_assertion_entity,
                Monitor(id=monitor_urn, info=("ASSERTION", monitor_mode)),
            )

        return None

    def _retrieve_and_merge_column_metric_assertion_and_monitor(
        self,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]],
        column_name: Optional[str],
        metric_type: Optional[MetricInputType],
        operator: Optional[OperatorInputType],
        criteria_parameters: Optional[ColumnMetricAssertionParameters],
        display_name: Optional[str],
        enabled: Optional[bool],
        detection_mechanism: DetectionMechanismInputTypes,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        updated_by: Union[str, CorpUserUrn],
        now_utc: datetime,
        schedule: Optional[Union[str, models.CronScheduleClass]],
    ) -> _ColumnMetricAssertionInput:
        # 1. If urn is not provided, validate required fields and build a new assertion input directly
        if urn is None:
            logger.info("URN is not set, building a new assertion input")
            self._validate_required_column_fields_for_creation(
                column_name, metric_type, operator
            )
            assert (
                column_name is not None
                and metric_type is not None
                and operator is not None
            ), "Fields guaranteed non-None after validation"
            return _ColumnMetricAssertionInput(
                urn=None,
                entity_client=self.client.entities,
                dataset_urn=dataset_urn,
                column_name=column_name,
                metric_type=metric_type,
                operator=operator,
                criteria_parameters=criteria_parameters,
                display_name=display_name,
                enabled=enabled if enabled is not None else True,
                detection_mechanism=detection_mechanism,
                incident_behavior=incident_behavior,
                tags=tags,
                created_by=updated_by,
                created_at=now_utc,
                updated_by=updated_by,
                updated_at=now_utc,
                schedule=schedule,
                gms_criteria_type_info=None,
            )

        # 2. Retrieve any existing assertion and monitor entities
        maybe_assertion_entity, monitor_urn, maybe_monitor_entity = (
            self._retrieve_assertion_and_monitor(urn, dataset_urn)
        )

        # 3. Extract missing required fields and gms_criteria_type_info from existing assertion
        (
            column_name,
            metric_type,
            operator,
            criteria_parameters,
            gms_criteria_type_info,
        ) = self._extract_fields_from_existing_assertion(
            maybe_assertion_entity,
            column_name,
            metric_type,
            operator,
            criteria_parameters,
        )

        # 4. Validate required fields
        if maybe_assertion_entity is None:
            self._validate_required_column_fields_for_creation(
                column_name, metric_type, operator
            )
        else:
            self._validate_required_column_fields_for_update(
                column_name, metric_type, operator, urn
            )

        assert (
            column_name is not None and metric_type is not None and operator is not None
        ), "Fields guaranteed non-None after validation"

        # 5. If the assertion does not exist, build and return a new assertion input
        if not maybe_assertion_entity:
            logger.info(
                f"No existing assertion entity found for assertion urn {urn}, building a new assertion input"
            )
            return _ColumnMetricAssertionInput(
                urn=urn,
                entity_client=self.client.entities,
                dataset_urn=dataset_urn,
                column_name=column_name,
                metric_type=metric_type,
                operator=operator,
                criteria_parameters=criteria_parameters,
                display_name=display_name,
                enabled=enabled if enabled is not None else True,
                detection_mechanism=detection_mechanism,
                incident_behavior=incident_behavior,
                tags=tags,
                created_by=updated_by,
                created_at=now_utc,
                updated_by=updated_by,
                updated_at=now_utc,
                schedule=schedule,
                gms_criteria_type_info=None,
            )

        # 6. Build assertion input for validation
        assertion_input = _ColumnMetricAssertionInput(
            urn=urn,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            column_name=column_name,
            metric_type=metric_type,
            operator=operator,
            criteria_parameters=criteria_parameters,
            display_name=display_name,
            detection_mechanism=detection_mechanism,
            incident_behavior=incident_behavior,
            tags=tags,
            created_by=updated_by,
            created_at=now_utc,
            updated_by=updated_by,
            updated_at=now_utc,
            schedule=schedule,
            gms_criteria_type_info=gms_criteria_type_info,
        )

        # 7. Create existing assertion from entities
        existing_assertion = self._create_existing_assertion_from_entities(
            maybe_assertion_entity, maybe_monitor_entity, monitor_urn, enabled
        )
        assert existing_assertion is not None, (
            "existing_assertion is guaranteed non-None after early return check"
        )

        # 8. Check for any issues e.g. different dataset urns
        if (
            existing_assertion
            and hasattr(existing_assertion, "dataset_urn")
            and existing_assertion.dataset_urn != assertion_input.dataset_urn
        ):
            raise SDKUsageError(
                f"Dataset URN mismatch, existing assertion: {existing_assertion.dataset_urn} != new assertion: {dataset_urn}"
            )

        # 9. Merge the existing assertion with the validated input
        merged_assertion_input = self._merge_column_metric_input(
            dataset_urn=dataset_urn,
            column_name=column_name,
            metric_type=metric_type,
            operator=operator,
            criteria_parameters=criteria_parameters,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            schedule=schedule,
            detection_mechanism=detection_mechanism,
            incident_behavior=incident_behavior,
            tags=tags,
            now_utc=now_utc,
            assertion_input=assertion_input,
            maybe_assertion_entity=maybe_assertion_entity,
            maybe_monitor_entity=maybe_monitor_entity,
            existing_assertion=existing_assertion,
            gms_criteria_type_info=gms_criteria_type_info,
        )

        return merged_assertion_input

    def _merge_column_metric_input(
        self,
        dataset_urn: Union[str, DatasetUrn],
        column_name: str,
        metric_type: MetricInputType,
        operator: OperatorInputType,
        criteria_parameters: Optional[ColumnMetricAssertionParameters],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        schedule: Optional[Union[str, models.CronScheduleClass]],
        detection_mechanism: DetectionMechanismInputTypes,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        now_utc: datetime,
        assertion_input: _ColumnMetricAssertionInput,
        maybe_assertion_entity: Optional[Assertion],
        maybe_monitor_entity: Optional[Monitor],
        existing_assertion: ColumnMetricAssertion,
        gms_criteria_type_info: Optional[tuple] = None,
    ) -> _ColumnMetricAssertionInput:
        """Merge the input with the existing assertion and monitor entities.

        Args:
            dataset_urn: The urn of the dataset to be monitored.
            column_name: The name of the column to be monitored.
            metric_type: The type of the metric to be monitored.
            operator: The operator to be used for the assertion.
            criteria_parameters: The criteria parameters for the assertion.
            urn: The urn of the assertion.
            display_name: The display name of the assertion.
            enabled: Whether the assertion is enabled.
            schedule: The schedule to be applied to the assertion.
            detection_mechanism: The detection mechanism to be used for the assertion.
            incident_behavior: The incident behavior to be applied to the assertion.
            tags: The tags to be applied to the assertion.
            now_utc: The current UTC time from when the function is called.
            assertion_input: The validated input to the function.
            maybe_assertion_entity: The existing assertion entity from the DataHub instance.
            maybe_monitor_entity: The existing monitor entity from the DataHub instance.
            existing_assertion: The existing assertion from the DataHub instance.
            gms_criteria_type_info: The criteria type info from the existing assertion.

        Returns:
            The merged assertion input.
        """
        merged_assertion_input = _ColumnMetricAssertionInput(
            urn=urn,
            entity_client=assertion_input.entity_client,
            dataset_urn=dataset_urn,
            column_name=column_name,
            metric_type=metric_type,
            operator=operator,
            criteria_parameters=criteria_parameters,
            display_name=_merge_field(
                display_name,
                "display_name",
                assertion_input,
                existing_assertion,
                maybe_assertion_entity.description if maybe_assertion_entity else None,
            ),
            enabled=_merge_field(
                enabled,
                "enabled",
                assertion_input,
                existing_assertion,
                existing_assertion.mode == AssertionMode.ACTIVE
                if existing_assertion
                else None,
            ),
            schedule=_merge_field(
                schedule,
                "schedule",
                assertion_input,
                existing_assertion,
                existing_assertion.schedule if existing_assertion else None,
            ),
            detection_mechanism=_merge_field(
                detection_mechanism,
                "detection_mechanism",
                assertion_input,
                existing_assertion,
                ColumnMetricAssertion._get_detection_mechanism(
                    maybe_assertion_entity, maybe_monitor_entity, default=None
                )
                if maybe_assertion_entity and maybe_monitor_entity
                else None,
            ),
            incident_behavior=_merge_field(
                incident_behavior,
                "incident_behavior",
                assertion_input,
                existing_assertion,
                ColumnMetricAssertion._get_incident_behavior(maybe_assertion_entity)
                if maybe_assertion_entity
                else None,
            ),
            tags=_merge_field(
                tags,
                "tags",
                assertion_input,
                existing_assertion,
                maybe_assertion_entity.tags if maybe_assertion_entity else None,
            ),
            created_by=existing_assertion.created_by
            or DEFAULT_CREATED_BY,  # Override with the existing assertion's created_by or the default created_by if not set
            created_at=existing_assertion.created_at
            or now_utc,  # Override with the existing assertion's created_at or now if not set
            updated_by=assertion_input.updated_by,  # Override with the input's updated_by
            updated_at=assertion_input.updated_at,  # Override with the input's updated_at (now)
            gms_criteria_type_info=gms_criteria_type_info,
        )

        return merged_assertion_input
