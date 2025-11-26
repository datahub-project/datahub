from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional, Union

from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    _AssertionPublic,
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
    _AssertionInput,
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
        assertion_input: Union[_AssertionInput, dict],
    ) -> tuple[Optional[Assertion], MonitorUrn, Optional[Monitor]]:
        """Retrieve the assertion and monitor entities from the DataHub instance.

        Args:
            assertion_input: The validated input to the function or minimal lookup info.

        Returns:
            The assertion and monitor entities.
        """
        # Extract URN and dataset URN from input
        _urn: Union[str, AssertionUrn]
        _dataset_urn: Union[str, DatasetUrn]
        if isinstance(assertion_input, dict):
            _urn = assertion_input["urn"]
            _dataset_urn = assertion_input["dataset_urn"]
        else:
            assert assertion_input.urn is not None, "URN is required"
            _urn = assertion_input.urn
            _dataset_urn = assertion_input.dataset_urn

        return retrieve_assertion_and_monitor_by_urn(self.client, _urn, _dataset_urn)

    def sync_column_metric_assertion(  # noqa: C901  # TODO: Refactor
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
        """Upsert and merge a column metric assertion.

        Note:
            Keyword arguments are required.

        Upsert and merge is a combination of create and update. If the assertion does not exist,
        it will be created. If it does exist, it will be updated.

        Existing assertion fields will be updated if the input value is not None. If the input value is None, the existing value
        will be preserved. If the input value can be un-set (e.g. by passing an empty list or
        empty string), it will be unset.

        Schedule behavior:
            - Create case: Uses default schedule of every 6 hours or provided schedule
            - Update case: Uses existing schedule or provided schedule.

        Examples:
            # Using enum values (recommended for type safety)
            from acryl_datahub_cloud.sdk.assertion_input.column_metric_constants import MetricType, OperatorType
            client.sync_column_metric_assertion(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table,PROD)",
                column_name="user_id",
                metric_type=MetricType.NULL_COUNT,
                operator=OperatorType.GREATER_THAN,
                criteria_parameters=10
            )

            # Using case-insensitive strings (more flexible)
            client.sync_column_metric_assertion(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table,PROD)",
                column_name="price",
                metric_type="mean",
                operator="between",
                criteria_parameters=(100.0, 500.0)
            )

        Args:
            dataset_urn (Union[str, DatasetUrn]): The urn of the dataset to be monitored.
            column_name (Optional[str]): The name of the column to be monitored. Required for creation, optional for updates.
            metric_type (Optional[MetricInputType]): The type of the metric to be monitored. Required for creation, optional for updates. Valid values are:
                - Using MetricType enum: MetricType.NULL_COUNT, MetricType.NULL_PERCENTAGE, MetricType.UNIQUE_COUNT,
                  MetricType.UNIQUE_PERCENTAGE, MetricType.MAX_LENGTH, MetricType.MIN_LENGTH, MetricType.EMPTY_COUNT,
                  MetricType.EMPTY_PERCENTAGE, MetricType.MIN, MetricType.MAX, MetricType.MEAN, MetricType.MEDIAN,
                  MetricType.STDDEV, MetricType.NEGATIVE_COUNT, MetricType.NEGATIVE_PERCENTAGE, MetricType.ZERO_COUNT,
                  MetricType.ZERO_PERCENTAGE
                - Using case-insensitive strings: "null_count", "MEAN", "Max_Length", etc.
                - Using models enum: models.FieldMetricTypeClass.NULL_COUNT, etc. (import with: from datahub.metadata import schema_classes as models)
            operator (Optional[OperatorInputType]): The operator to be used for the assertion. Required for creation, optional for updates. Valid values are:
                - Using OperatorType enum: OperatorType.EQUAL_TO, OperatorType.NOT_EQUAL_TO, OperatorType.GREATER_THAN,
                  OperatorType.GREATER_THAN_OR_EQUAL_TO, OperatorType.LESS_THAN, OperatorType.LESS_THAN_OR_EQUAL_TO,
                  OperatorType.BETWEEN, OperatorType.IN, OperatorType.NOT_IN, OperatorType.NULL, OperatorType.NOT_NULL,
                  OperatorType.IS_TRUE, OperatorType.IS_FALSE, OperatorType.CONTAIN, OperatorType.END_WITH,
                  OperatorType.START_WITH, OperatorType.REGEX_MATCH
                - Using case-insensitive strings: "equal_to", "not_equal_to", "greater_than", "greater_than_or_equal_to",
                  "less_than", "less_than_or_equal_to", "between", "in", "not_in", "null", "not_null", "is_true",
                  "is_false", "contain", "end_with", "start_with", "regex_match"
                - Using models enum: models.AssertionStdOperatorClass.EQUAL_TO, models.AssertionStdOperatorClass.GREATER_THAN, etc.
            criteria_parameters (Optional[ColumnMetricAssertionParameters]): The criteria parameters for the assertion. Required for creation (except for operators that don't need parameters), optional for updates.
                - Single value operators (EQUAL_TO, NOT_EQUAL_TO, GREATER_THAN, GREATER_THAN_OR_EQUAL_TO, LESS_THAN, LESS_THAN_OR_EQUAL_TO, CONTAIN, END_WITH, START_WITH, REGEX_MATCH): pass a single number or string
                - Range operators (BETWEEN): pass a tuple of two numbers (min_value, max_value)
                - List operators (IN, NOT_IN): pass a list of values
                - No parameter operators (NULL, NOT_NULL, IS_TRUE, IS_FALSE): pass None or omit this parameter
            urn (Optional[Union[str, AssertionUrn]]): The urn of the assertion. If not provided, a urn will be generated and the assertion will be created in the DataHub instance.
            display_name (Optional[str]): The display name of the assertion. If not provided, a random display name will be generated.
            enabled (Optional[bool]): Whether the assertion is enabled. If not provided, the existing value will be preserved.
            detection_mechanism (DetectionMechanismInputTypes): The detection mechanism to be used for the assertion. Valid values are (additional_filter is optional):
                - "all_rows_query_datahub_dataset_profile" or DetectionMechanism.ALL_ROWS_QUERY_DATAHUB_DATASET_PROFILE
                - "all_rows_query" or DetectionMechanism.ALL_ROWS_QUERY(), or with additional_filter: {"type": "all_rows_query", "additional_filter": "last_modified > '2021-01-01'"} or DetectionMechanism.ALL_ROWS_QUERY(additional_filter='last_modified > 2021-01-01')
                - {"type": "changed_rows_query", "column_name": "last_modified", "additional_filter": "last_modified > '2021-01-01'"} or DetectionMechanism.CHANGED_ROWS_QUERY(column_name='last_modified', additional_filter='last_modified > 2021-01-01')
            incident_behavior (Optional[Union[str, list[str], AssertionIncidentBehavior, list[AssertionIncidentBehavior]]]): The incident behavior to be applied to the assertion. Valid values are: "raise_on_fail", "resolve_on_pass", or the typed ones (AssertionIncidentBehavior.RAISE_ON_FAIL and AssertionIncidentBehavior.RESOLVE_ON_PASS).
            tags (Optional[TagsInputType]): The tags to be applied to the assertion. Valid values are: a list of strings, TagUrn objects, or TagAssociationClass objects.
            updated_by (Optional[Union[str, CorpUserUrn]]): Optional urn of the user who updated the assertion. The format is "urn:li:corpuser:<username>". The default is the datahub system user.
            schedule (Optional[Union[str, models.CronScheduleClass]]): Optional cron formatted schedule for the assertion. If not provided, a default schedule of every 6 hours will be used. The format is a cron expression, e.g. "0 * * * *" for every hour using UTC timezone. Alternatively, a models.CronScheduleClass object can be provided.

        Returns:
            ColumnMetricAssertion: The created or updated assertion.
        """
        now_utc = datetime.now(timezone.utc)
        gms_criteria_type_info = None

        if updated_by is None:
            logger.warning(
                f"updated_by is not set, using {DEFAULT_CREATED_BY} as a placeholder"
            )
            updated_by = DEFAULT_CREATED_BY

        # 1. If urn is not set, create a new assertion
        if urn is None:
            self._validate_required_column_fields_for_creation(
                column_name, metric_type, operator
            )
            assert (
                column_name is not None
                and metric_type is not None
                and operator is not None
            ), "Fields guaranteed non-None after validation"
            logger.info("URN is not set, creating a new assertion")
            return self._create_column_metric_assertion(
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
                schedule=schedule,
            )

        # 2.1 If urn is set, fetch missing required parameters from backend if needed:
        # NOTE: This is a tactical solution. The problem is we fetch twice (once for validation,
        # once for merge). Strategic solution would be to merge first, then validate after,
        # but that requires heavy refactor and is skipped for now.
        if urn is not None and (
            column_name is None
            or metric_type is None
            or operator is None
            or criteria_parameters is None
        ):
            # Fetch existing assertion to get missing required parameters
            maybe_assertion_entity, _, maybe_monitor_entity = (
                self._retrieve_assertion_and_monitor(
                    {"dataset_urn": dataset_urn, "urn": urn}
                )
            )

            if maybe_assertion_entity is not None:
                assertion_info = maybe_assertion_entity.info
                if (
                    hasattr(assertion_info, "fieldMetricAssertion")
                    and assertion_info.fieldMetricAssertion
                ):
                    field_metric_assertion = assertion_info.fieldMetricAssertion
                    # Use existing values for missing required parameters
                    if (
                        column_name is None
                        and hasattr(field_metric_assertion, "field")
                        and hasattr(field_metric_assertion.field, "path")
                    ):
                        column_name = field_metric_assertion.field.path
                    if metric_type is None and hasattr(
                        field_metric_assertion, "metric"
                    ):
                        metric_type = field_metric_assertion.metric
                    if operator is None and hasattr(field_metric_assertion, "operator"):
                        operator = field_metric_assertion.operator
                    if criteria_parameters is None and hasattr(
                        field_metric_assertion, "parameters"
                    ):
                        # Extract criteria_parameters from existing assertion
                        # This logic should match the parameter extraction in the assertion input class
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
                            criteria_parameters = (
                                params.minValue.value,
                                params.maxValue.value,
                            )

                # Extract gms_criteria_type_info to preserve original parameter types
                gms_criteria_type_info = (
                    _HasColumnMetricFunctionality._get_criteria_parameters_with_type(
                        maybe_assertion_entity
                    )
                )

            self._validate_required_column_fields_for_update(
                column_name, metric_type, operator, urn
            )
            assert (
                column_name is not None
                and metric_type is not None
                and operator is not None
            ), "Fields guaranteed non-None after validation"

        # 2.2 Now validate the input with all required parameters:
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
            created_by=updated_by,  # This will be overridden by the actual created_by
            created_at=now_utc,  # This will be overridden by the actual created_at
            updated_by=updated_by,
            updated_at=now_utc,
            schedule=schedule,
            gms_criteria_type_info=gms_criteria_type_info,
        )

        # 3. Merge the assertion input with the existing assertion and monitor entities or create a new assertion
        # if the assertion does not exist:
        merged_assertion_input_or_created_assertion = (
            self._retrieve_and_merge_column_metric_assertion_and_monitor(
                assertion_input=assertion_input,
                dataset_urn=dataset_urn,
                column_name=column_name,
                metric_type=metric_type,
                operator=operator,
                criteria_parameters=criteria_parameters,
                urn=urn,
                display_name=display_name,
                enabled=enabled,
                detection_mechanism=detection_mechanism,
                incident_behavior=incident_behavior,
                tags=tags,
                updated_by=updated_by,
                now_utc=now_utc,
                schedule=schedule,
            )
        )

        # Return early if we created a new assertion in the merge:
        if isinstance(merged_assertion_input_or_created_assertion, _AssertionPublic):
            # We know this is the correct type because we passed the assertion_class parameter
            assert isinstance(
                merged_assertion_input_or_created_assertion, ColumnMetricAssertion
            )
            return merged_assertion_input_or_created_assertion

        # 4. Upsert the assertion and monitor entities:
        assertion_entity, monitor_entity = (
            merged_assertion_input_or_created_assertion.to_assertion_and_monitor_entities()
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

    def _create_column_metric_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]] = None,
        column_name: str,
        metric_type: MetricInputType,
        operator: OperatorInputType,
        criteria_parameters: Optional[ColumnMetricAssertionParameters] = None,
        display_name: Optional[str] = None,
        enabled: bool = True,
        detection_mechanism: DetectionMechanismInputTypes = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        created_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
    ) -> ColumnMetricAssertion:
        """Create a column metric assertion.

        Note: keyword arguments are required.

        Args:
            dataset_urn: The urn of the dataset to be monitored. (Required)
            column_name: The name of the column to be monitored. (Required)
            metric_type: The type of the metric to be monitored. (Required)
            operator: The operator to be used for the assertion. (Required)
            criteria_parameters: The criteria parameters for the assertion. Required for most operators.
            display_name: The display name of the assertion. If not provided, a random display
                name will be generated.
            enabled: Whether the assertion is enabled. Defaults to True.
            detection_mechanism: The detection mechanism to be used for the assertion.
            incident_behavior: The incident behavior to be applied to the assertion.
            tags: The tags to be applied to the assertion.
            created_by: Optional urn of the user who created the assertion.
            schedule: Optional cron formatted schedule for the assertion.

        Returns:
            ColumnMetricAssertion: The created assertion.
        """
        now_utc = datetime.now(timezone.utc)
        if created_by is None:
            logger.warning(
                f"Created by is not set, using {DEFAULT_CREATED_BY} as a placeholder"
            )
            created_by = DEFAULT_CREATED_BY
        assertion_input = _ColumnMetricAssertionInput(
            urn=urn,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            column_name=column_name,
            metric_type=metric_type,
            operator=operator,
            criteria_parameters=criteria_parameters,
            display_name=display_name,
            enabled=enabled,
            detection_mechanism=detection_mechanism,
            incident_behavior=incident_behavior,
            tags=tags,
            created_by=created_by,
            created_at=now_utc,
            updated_by=created_by,
            updated_at=now_utc,
            schedule=schedule,
            gms_criteria_type_info=None,
        )
        assertion_entity, monitor_entity = (
            assertion_input.to_assertion_and_monitor_entities()
        )
        # If assertion creation fails, we won't try to create the monitor
        self.client.entities.create(assertion_entity)
        # TODO: Wrap monitor creation in a try-except and delete the assertion if monitor creation fails (once delete is implemented https://linear.app/acryl-data/issue/OBS-1350/add-delete-method-to-entity-clientpy)
        # try:
        self.client.entities.create(monitor_entity)
        # except Exception as e:
        #     logger.error(f"Error creating monitor: {e}")
        #     self.client.entities.delete(assertion_entity)
        #     raise e
        return ColumnMetricAssertion._from_entities(assertion_entity, monitor_entity)

    def _retrieve_and_merge_column_metric_assertion_and_monitor(
        self,
        assertion_input: _ColumnMetricAssertionInput,
        dataset_urn: Union[str, DatasetUrn],
        column_name: str,
        metric_type: MetricInputType,
        operator: OperatorInputType,
        criteria_parameters: Optional[ColumnMetricAssertionParameters],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        detection_mechanism: DetectionMechanismInputTypes,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        updated_by: Optional[Union[str, CorpUserUrn]],
        now_utc: datetime,
        schedule: Optional[Union[str, models.CronScheduleClass]],
    ) -> Union[ColumnMetricAssertion, _ColumnMetricAssertionInput]:
        # 1. Retrieve any existing assertion and monitor entities:
        maybe_assertion_entity, monitor_urn, maybe_monitor_entity = (
            self._retrieve_assertion_and_monitor(assertion_input)
        )

        # Extract gms_criteria_type_info from existing assertion if available
        gms_criteria_type_info = None
        if maybe_assertion_entity is not None:
            gms_criteria_type_info = (
                _HasColumnMetricFunctionality._get_criteria_parameters_with_type(
                    maybe_assertion_entity
                )
            )

        # 2.1 If the assertion and monitor entities exist, create an assertion object from them:
        if maybe_assertion_entity and maybe_monitor_entity:
            existing_assertion = ColumnMetricAssertion._from_entities(
                maybe_assertion_entity, maybe_monitor_entity
            )
        # 2.2 If the assertion exists but the monitor does not, create a placeholder monitor entity to be able to create the assertion:
        elif maybe_assertion_entity and not maybe_monitor_entity:
            monitor_mode = (
                "ACTIVE" if enabled else "INACTIVE" if enabled is not None else "ACTIVE"
            )
            existing_assertion = ColumnMetricAssertion._from_entities(
                maybe_assertion_entity,
                Monitor(id=monitor_urn, info=("ASSERTION", monitor_mode)),
            )
        # 2.3 If the assertion does not exist, create a new assertion with a generated urn and return the assertion input:
        elif not maybe_assertion_entity:
            logger.info(
                f"No existing assertion entity found for assertion urn {urn}, creating a new assertion with a generated urn"
            )
            return self._create_column_metric_assertion(
                dataset_urn=dataset_urn,
                urn=urn,
                column_name=column_name,
                metric_type=metric_type,
                operator=operator,
                criteria_parameters=criteria_parameters,
                schedule=schedule,
                display_name=display_name,
                detection_mechanism=detection_mechanism,
                incident_behavior=incident_behavior,
                tags=tags,
                created_by=updated_by,
                enabled=enabled if enabled is not None else True,
            )

        # 3. Check for any issues e.g. different dataset urns
        if (
            existing_assertion
            and hasattr(existing_assertion, "dataset_urn")
            and existing_assertion.dataset_urn != assertion_input.dataset_urn
        ):
            raise SDKUsageError(
                f"Dataset URN mismatch, existing assertion: {existing_assertion.dataset_urn} != new assertion: {dataset_urn}"
            )

        # 4. Merge the existing assertion with the validated input:
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

    def _merge_column_metric_input(  # TODO: Refactor
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
        """Merge the validated assertion input with the existing assertion to create an upsert."""

        # Extract existing values from entities for merging
        existing_display_name = None
        existing_enabled = None
        existing_schedule = None
        existing_detection_mechanism = None
        existing_incident_behavior = None
        existing_tags = None

        if maybe_assertion_entity and maybe_assertion_entity.info:
            if hasattr(maybe_assertion_entity.info, "displayName"):
                existing_display_name = maybe_assertion_entity.info.displayName
            if hasattr(maybe_assertion_entity.info, "tags"):
                existing_tags = maybe_assertion_entity.info.tags

        if maybe_monitor_entity and maybe_monitor_entity.info:
            if (
                hasattr(maybe_monitor_entity.info, "status")
                and maybe_monitor_entity.info.status
            ):
                existing_enabled = maybe_monitor_entity.info.status == "ACTIVE"
            if (
                hasattr(maybe_monitor_entity.info, "config")
                and maybe_monitor_entity.info.config
            ):
                if hasattr(maybe_monitor_entity.info.config, "schedule"):
                    existing_schedule = maybe_monitor_entity.info.config.schedule
                if hasattr(maybe_monitor_entity.info.config, "executorId"):
                    existing_detection_mechanism = (
                        maybe_monitor_entity.info.config.executorId
                    )
                if hasattr(maybe_monitor_entity.info.config, "actions"):
                    existing_incident_behavior = (
                        maybe_monitor_entity.info.config.actions
                    )

        # Merge each field using the merge logic
        merged_display_name = _merge_field(
            display_name,
            "display_name",
            assertion_input,
            existing_assertion,
            existing_display_name,
        )
        merged_enabled = _merge_field(
            enabled, "mode", assertion_input, existing_assertion, existing_enabled
        )
        merged_schedule = _merge_field(
            schedule, "schedule", assertion_input, existing_assertion, existing_schedule
        )
        merged_detection_mechanism = _merge_field(
            detection_mechanism,
            "detection_mechanism",
            assertion_input,
            existing_assertion,
            existing_detection_mechanism,
        )
        merged_incident_behavior = _merge_field(
            incident_behavior,
            "incident_behavior",
            assertion_input,
            existing_assertion,
            existing_incident_behavior,
        )
        merged_tags = _merge_field(
            tags, "tags", assertion_input, existing_assertion, existing_tags
        )

        # Create the merged assertion input
        return _ColumnMetricAssertionInput(
            urn=urn,
            entity_client=assertion_input.entity_client,
            dataset_urn=dataset_urn,
            column_name=column_name,
            metric_type=metric_type,
            operator=operator,
            criteria_parameters=criteria_parameters,
            display_name=merged_display_name,
            enabled=merged_enabled,
            detection_mechanism=merged_detection_mechanism,
            incident_behavior=merged_incident_behavior,
            tags=merged_tags,
            created_by=existing_assertion.created_by
            if existing_assertion.created_by
            else assertion_input.created_by,
            created_at=existing_assertion.created_at
            if existing_assertion.created_at
            else assertion_input.created_at,
            updated_by=assertion_input.updated_by,
            updated_at=now_utc,
            schedule=merged_schedule,
            gms_criteria_type_info=gms_criteria_type_info,
        )
