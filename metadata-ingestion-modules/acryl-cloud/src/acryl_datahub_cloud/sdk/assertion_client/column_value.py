"""
Column value assertion client module.

This module provides the ColumnValueAssertionClient for creating and managing
column value assertions that validate individual row values against semantic constraints.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional, Union

from acryl_datahub_cloud.sdk.assertion.assertion_base import AssertionMode
from acryl_datahub_cloud.sdk.assertion.column_value_assertion import (
    ColumnValueAssertion,
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
from acryl_datahub_cloud.sdk.assertion_input.column_metric_constants import (
    OperatorInputType,
)
from acryl_datahub_cloud.sdk.assertion_input.column_value_assertion_input import (
    ColumnValueAssertionParameters,
    FailThresholdInputType,
    FieldTransformInputType,
    _ColumnValueAssertionInput,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion, TagsInputType
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, MonitorUrn

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient

logger = logging.getLogger(__name__)


class ColumnValueAssertionClient:
    """Client for managing column value assertions."""

    def __init__(self, client: "DataHubClient"):
        self.client = client

    def _validate_required_column_value_fields_for_creation(
        self,
        column_name: Optional[str],
        operator: Optional[OperatorInputType],
    ) -> None:
        """Validate required fields for column value assertion creation."""
        _validate_required_field(
            column_name, "column_name", "when creating a new assertion (urn is None)"
        )
        _validate_required_field(
            operator, "operator", "when creating a new assertion (urn is None)"
        )

    def _validate_required_column_value_fields_for_update(
        self,
        column_name: Optional[str],
        operator: Optional[OperatorInputType],
        assertion_urn: Union[str, AssertionUrn],
    ) -> None:
        """Validate required fields after attempting to fetch from existing assertion."""
        context = (
            f"and not found in existing assertion {assertion_urn}. "
            "The existing assertion may be invalid or corrupted."
        )
        _validate_required_field(column_name, "column_name", context)
        _validate_required_field(operator, "operator", context)

    def _retrieve_assertion_and_monitor(
        self,
        urn: Union[str, AssertionUrn],
        dataset_urn: Union[str, DatasetUrn],
    ) -> tuple[Optional[Assertion], MonitorUrn, Optional[Monitor]]:
        """Retrieve the assertion and monitor entities from the DataHub instance."""
        return retrieve_assertion_and_monitor_by_urn(self.client, urn, dataset_urn)

    def sync_column_value_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        column_name: Optional[str] = None,
        operator: Optional[OperatorInputType] = None,
        criteria_parameters: Optional[ColumnValueAssertionParameters] = None,
        transform: Optional[FieldTransformInputType] = None,
        fail_threshold_type: Optional[FailThresholdInputType] = None,
        fail_threshold_value: Optional[int] = None,
        exclude_nulls: Optional[bool] = None,
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
    ) -> ColumnValueAssertion:
        """Create or update a column value assertion.

        See AssertionsClient.sync_column_value_assertion for full documentation.
        """
        now_utc = datetime.now(timezone.utc)

        if updated_by is None:
            logger.warning(
                f"updated_by is not set, using {DEFAULT_CREATED_BY} as a placeholder"
            )
            updated_by = DEFAULT_CREATED_BY

        assertion_input = self._retrieve_and_merge_column_value_assertion_and_monitor(
            dataset_urn=dataset_urn,
            urn=urn,
            column_name=column_name,
            operator=operator,
            criteria_parameters=criteria_parameters,
            transform=transform,
            fail_threshold_type=fail_threshold_type,
            fail_threshold_value=fail_threshold_value,
            exclude_nulls=exclude_nulls,
            display_name=display_name,
            enabled=enabled,
            detection_mechanism=detection_mechanism,
            incident_behavior=incident_behavior,
            tags=tags,
            updated_by=updated_by,
            now_utc=now_utc,
            schedule=schedule,
        )

        assertion_entity, monitor_entity = (
            assertion_input.to_assertion_and_monitor_entities()
        )
        self.client.entities.upsert(assertion_entity)
        self.client.entities.upsert(monitor_entity)

        return ColumnValueAssertion._from_entities(assertion_entity, monitor_entity)

    def _extract_fields_from_existing_assertion(
        self,
        maybe_assertion_entity: Optional[Assertion],
        column_name: Optional[str],
        operator: Optional[OperatorInputType],
        criteria_parameters: Optional[ColumnValueAssertionParameters],
        transform: Optional[FieldTransformInputType],
        fail_threshold_type: Optional[FailThresholdInputType],
        fail_threshold_value: Optional[int],
        exclude_nulls: Optional[bool],
    ) -> tuple[
        Optional[str],
        Optional[OperatorInputType],
        Optional[ColumnValueAssertionParameters],
        Optional[FieldTransformInputType],
        Optional[FailThresholdInputType],
        Optional[int],
        Optional[bool],
        Optional[tuple],
    ]:
        """Extract missing fields from existing assertion entity."""
        gms_criteria_type_info = None

        if maybe_assertion_entity is None:
            return (
                column_name,
                operator,
                criteria_parameters,
                transform,
                fail_threshold_type,
                fail_threshold_value,
                exclude_nulls,
                gms_criteria_type_info,
            )

        assertion_info = maybe_assertion_entity.info
        field_values_assertion = getattr(assertion_info, "fieldValuesAssertion", None)
        if field_values_assertion:
            column_name, operator, transform = self._extract_basic_fields(
                field_values_assertion, column_name, operator, transform
            )
            fail_threshold_type, fail_threshold_value, exclude_nulls = (
                self._extract_threshold_fields(
                    field_values_assertion,
                    fail_threshold_type,
                    fail_threshold_value,
                    exclude_nulls,
                )
            )
            if criteria_parameters is None:
                criteria_parameters = self._extract_criteria_parameters(
                    field_values_assertion
                )

        gms_criteria_type_info = (
            ColumnValueAssertion._get_criteria_parameters_with_type(
                maybe_assertion_entity
            )
        )

        return (
            column_name,
            operator,
            criteria_parameters,
            transform,
            fail_threshold_type,
            fail_threshold_value,
            exclude_nulls,
            gms_criteria_type_info,
        )

    def _extract_basic_fields(
        self,
        field_values_assertion: models.FieldValuesAssertionClass,
        column_name: Optional[str],
        operator: Optional[OperatorInputType],
        transform: Optional[FieldTransformInputType],
    ) -> tuple[
        Optional[str], Optional[OperatorInputType], Optional[FieldTransformInputType]
    ]:
        """Extract column_name, operator, and transform from field values assertion."""
        if column_name is None:
            field = getattr(field_values_assertion, "field", None)
            if field:
                column_name = getattr(field, "path", None)

        if operator is None:
            operator = getattr(field_values_assertion, "operator", None)

        if transform is None:
            fv_transform = getattr(field_values_assertion, "transform", None)
            if fv_transform:
                transform = getattr(fv_transform, "type", None)

        return column_name, operator, transform

    def _extract_threshold_fields(
        self,
        field_values_assertion: models.FieldValuesAssertionClass,
        fail_threshold_type: Optional[FailThresholdInputType],
        fail_threshold_value: Optional[int],
        exclude_nulls: Optional[bool],
    ) -> tuple[Optional[FailThresholdInputType], Optional[int], Optional[bool]]:
        """Extract fail threshold and exclude_nulls from field values assertion."""
        threshold = getattr(field_values_assertion, "failThreshold", None)
        if threshold:
            if fail_threshold_type is None:
                fail_threshold_type = getattr(threshold, "type", None)
            if fail_threshold_value is None:
                fail_threshold_value = getattr(threshold, "value", None)

        if exclude_nulls is None:
            exclude_nulls = getattr(field_values_assertion, "excludeNulls", None)

        return fail_threshold_type, fail_threshold_value, exclude_nulls

    def _extract_criteria_parameters(
        self,
        field_values_assertion: models.FieldValuesAssertionClass,
    ) -> Optional[ColumnValueAssertionParameters]:
        """Extract criteria parameters from field values assertion."""
        params = getattr(field_values_assertion, "parameters", None)
        if not params:
            return None

        value_param = getattr(params, "value", None)
        if value_param:
            return getattr(value_param, "value", None)

        min_value = getattr(params, "minValue", None)
        max_value = getattr(params, "maxValue", None)
        if min_value and max_value:
            return (min_value.value, max_value.value)

        return None

    def _create_existing_assertion_from_entities(
        self,
        maybe_assertion_entity: Optional[Assertion],
        maybe_monitor_entity: Optional[Monitor],
        monitor_urn: MonitorUrn,
        enabled: Optional[bool],
    ) -> Optional[ColumnValueAssertion]:
        """Create existing assertion object from entities, handling missing monitor case."""
        if maybe_assertion_entity and maybe_monitor_entity:
            return ColumnValueAssertion._from_entities(
                maybe_assertion_entity, maybe_monitor_entity
            )

        if maybe_assertion_entity and not maybe_monitor_entity:
            monitor_mode = (
                "ACTIVE" if enabled else "INACTIVE" if enabled is not None else "ACTIVE"
            )
            return ColumnValueAssertion._from_entities(
                maybe_assertion_entity,
                Monitor(id=monitor_urn, info=("ASSERTION", monitor_mode)),
            )

        return None

    def _retrieve_and_merge_column_value_assertion_and_monitor(
        self,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]],
        column_name: Optional[str],
        operator: Optional[OperatorInputType],
        criteria_parameters: Optional[ColumnValueAssertionParameters],
        transform: Optional[FieldTransformInputType],
        fail_threshold_type: Optional[FailThresholdInputType],
        fail_threshold_value: Optional[int],
        exclude_nulls: Optional[bool],
        display_name: Optional[str],
        enabled: Optional[bool],
        detection_mechanism: DetectionMechanismInputTypes,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        updated_by: Union[str, CorpUserUrn],
        now_utc: datetime,
        schedule: Optional[Union[str, models.CronScheduleClass]],
    ) -> _ColumnValueAssertionInput:
        if urn is None:
            logger.info("URN is not set, building a new assertion input")
            self._validate_required_column_value_fields_for_creation(
                column_name, operator
            )
            assert column_name is not None and operator is not None, (
                "Fields guaranteed non-None after validation"
            )
            return _ColumnValueAssertionInput(
                urn=None,
                entity_client=self.client.entities,
                dataset_urn=dataset_urn,
                column_name=column_name,
                operator=operator,
                criteria_parameters=criteria_parameters,
                transform=transform,
                fail_threshold_type=fail_threshold_type,
                fail_threshold_value=fail_threshold_value
                if fail_threshold_value is not None
                else 0,
                exclude_nulls=exclude_nulls if exclude_nulls is not None else True,
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

        maybe_assertion_entity, monitor_urn, maybe_monitor_entity = (
            self._retrieve_assertion_and_monitor(urn, dataset_urn)
        )

        # Fail fast: Check for dataset URN mismatch before expensive operations
        if maybe_assertion_entity and str(maybe_assertion_entity.dataset) != str(
            dataset_urn
        ):
            raise SDKUsageError(
                f"Dataset URN mismatch: assertion {urn} is for dataset "
                f"{maybe_assertion_entity.dataset}, but you're trying to update it "
                f"on dataset {dataset_urn}"
            )

        (
            column_name,
            operator,
            criteria_parameters,
            transform,
            fail_threshold_type,
            fail_threshold_value,
            exclude_nulls,
            gms_criteria_type_info,
        ) = self._extract_fields_from_existing_assertion(
            maybe_assertion_entity,
            column_name,
            operator,
            criteria_parameters,
            transform,
            fail_threshold_type,
            fail_threshold_value,
            exclude_nulls,
        )

        if maybe_assertion_entity is None:
            self._validate_required_column_value_fields_for_creation(
                column_name, operator
            )
        else:
            self._validate_required_column_value_fields_for_update(
                column_name, operator, urn
            )

        assert column_name is not None and operator is not None, (
            "Fields guaranteed non-None after validation"
        )

        if not maybe_assertion_entity:
            logger.info(
                f"No existing assertion entity found for assertion urn {urn}, "
                "building a new assertion input"
            )
            return _ColumnValueAssertionInput(
                urn=urn,
                entity_client=self.client.entities,
                dataset_urn=dataset_urn,
                column_name=column_name,
                operator=operator,
                criteria_parameters=criteria_parameters,
                transform=transform,
                fail_threshold_type=fail_threshold_type,
                fail_threshold_value=fail_threshold_value
                if fail_threshold_value is not None
                else 0,
                exclude_nulls=exclude_nulls if exclude_nulls is not None else True,
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

        assertion_input = _ColumnValueAssertionInput(
            urn=urn,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            column_name=column_name,
            operator=operator,
            criteria_parameters=criteria_parameters,
            transform=transform,
            fail_threshold_type=fail_threshold_type,
            fail_threshold_value=fail_threshold_value
            if fail_threshold_value is not None
            else 0,
            exclude_nulls=exclude_nulls if exclude_nulls is not None else True,
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

        existing_assertion = self._create_existing_assertion_from_entities(
            maybe_assertion_entity, maybe_monitor_entity, monitor_urn, enabled
        )
        assert existing_assertion is not None, (
            "existing_assertion is guaranteed non-None after early return check"
        )

        merged_assertion_input = self._merge_column_value_input(
            dataset_urn=dataset_urn,
            column_name=column_name,
            operator=operator,
            criteria_parameters=criteria_parameters,
            transform=transform,
            fail_threshold_type=fail_threshold_type,
            fail_threshold_value=fail_threshold_value,
            exclude_nulls=exclude_nulls,
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

    def _merge_column_value_input(
        self,
        dataset_urn: Union[str, DatasetUrn],
        column_name: str,
        operator: OperatorInputType,
        criteria_parameters: Optional[ColumnValueAssertionParameters],
        transform: Optional[FieldTransformInputType],
        fail_threshold_type: Optional[FailThresholdInputType],
        fail_threshold_value: Optional[int],
        exclude_nulls: Optional[bool],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        schedule: Optional[Union[str, models.CronScheduleClass]],
        detection_mechanism: DetectionMechanismInputTypes,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        now_utc: datetime,
        assertion_input: _ColumnValueAssertionInput,
        maybe_assertion_entity: Optional[Assertion],
        maybe_monitor_entity: Optional[Monitor],
        existing_assertion: ColumnValueAssertion,
        gms_criteria_type_info: Optional[tuple] = None,
    ) -> _ColumnValueAssertionInput:
        """Merge the input with the existing assertion and monitor entities."""
        merged_assertion_input = _ColumnValueAssertionInput(
            urn=urn,
            entity_client=assertion_input.entity_client,
            dataset_urn=dataset_urn,
            column_name=column_name,
            operator=operator,
            criteria_parameters=criteria_parameters,
            transform=_merge_field(
                transform,
                "transform",
                assertion_input,
                existing_assertion,
                existing_assertion.transform if existing_assertion else None,
            ),
            fail_threshold_type=_merge_field(
                fail_threshold_type,
                "fail_threshold_type",
                assertion_input,
                existing_assertion,
                existing_assertion.fail_threshold_type if existing_assertion else None,
            ),
            fail_threshold_value=_merge_field(
                fail_threshold_value,
                "fail_threshold_value",
                assertion_input,
                existing_assertion,
                existing_assertion.fail_threshold_value if existing_assertion else 0,
            ),
            exclude_nulls=_merge_field(
                exclude_nulls,
                "exclude_nulls",
                assertion_input,
                existing_assertion,
                existing_assertion.exclude_nulls if existing_assertion else True,
            ),
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
                ColumnValueAssertion._get_detection_mechanism(
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
                ColumnValueAssertion._get_incident_behavior(maybe_assertion_entity)
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
            created_by=existing_assertion.created_by or DEFAULT_CREATED_BY,
            created_at=existing_assertion.created_at or now_utc,
            updated_by=assertion_input.updated_by,
            updated_at=assertion_input.updated_at,
            gms_criteria_type_info=gms_criteria_type_info,
        )

        return merged_assertion_input
