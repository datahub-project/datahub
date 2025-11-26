from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional, Union

from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    FreshnessAssertion,
    SmartFreshnessAssertion,
    SmartVolumeAssertion,
    SqlAssertion,
    VolumeAssertion,
)
from acryl_datahub_cloud.sdk.assertion.column_metric_assertion import (
    ColumnMetricAssertion,
)
from acryl_datahub_cloud.sdk.assertion.smart_column_metric_assertion import (
    SmartColumnMetricAssertion,
)
from acryl_datahub_cloud.sdk.assertion_client.column_metric import (
    ColumnMetricAssertionClient,
)
from acryl_datahub_cloud.sdk.assertion_client.freshness import (
    FreshnessAssertionClient,
)
from acryl_datahub_cloud.sdk.assertion_client.helpers import (
    _print_experimental_warning,
)
from acryl_datahub_cloud.sdk.assertion_client.smart_column_metric import (
    SmartColumnMetricAssertionClient,
)
from acryl_datahub_cloud.sdk.assertion_client.smart_freshness import (
    SmartFreshnessAssertionClient,
)
from acryl_datahub_cloud.sdk.assertion_client.smart_volume import (
    SmartVolumeAssertionClient,
)
from acryl_datahub_cloud.sdk.assertion_client.sql import (
    SqlAssertionClient,
)
from acryl_datahub_cloud.sdk.assertion_client.volume import (
    VolumeAssertionClient,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    AssertionIncidentBehaviorInputTypes,
    DetectionMechanismInputTypes,
    ExclusionWindowInputTypes,
    InferenceSensitivity,
    TimeWindowSizeInputTypes,
)
from acryl_datahub_cloud.sdk.assertion_input.column_metric_assertion_input import (
    ColumnMetricAssertionParameters,
)
from acryl_datahub_cloud.sdk.assertion_input.column_metric_constants import (
    MetricInputType,
    OperatorInputType,
)
from acryl_datahub_cloud.sdk.assertion_input.freshness_assertion_input import (
    FreshnessAssertionScheduleCheckType,
)
from acryl_datahub_cloud.sdk.assertion_input.sql_assertion_input import (
    SqlAssertionCondition,
)
from acryl_datahub_cloud.sdk.assertion_input.volume_assertion_input import (
    VolumeAssertionCondition,
    VolumeAssertionDefinitionParameters,
)
from acryl_datahub_cloud.sdk.entities.assertion import TagsInputType
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient

logger = logging.getLogger(__name__)

# TODO: Replace __datahub_system with the actual datahub system user https://linear.app/acryl-data/issue/OBS-1351/auditstamp-actor-hydration-pattern-for-sdk-calls
DEFAULT_CREATED_BY = CorpUserUrn.from_string("urn:li:corpuser:__datahub_system")


class AssertionsClient:
    def __init__(self, client: "DataHubClient"):
        self.client = client
        self._freshness_client = FreshnessAssertionClient(client)
        self._volume_client = VolumeAssertionClient(client)
        self._sql_client = SqlAssertionClient(client)
        self._smart_freshness_client = SmartFreshnessAssertionClient(client)
        self._smart_volume_client = SmartVolumeAssertionClient(client)
        self._smart_column_metric_client = SmartColumnMetricAssertionClient(client)
        self._column_metric_client = ColumnMetricAssertionClient(client)
        _print_experimental_warning()

    def sync_smart_freshness_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        sensitivity: Optional[Union[str, InferenceSensitivity]] = None,
        exclusion_windows: Optional[ExclusionWindowInputTypes] = None,
        training_data_lookback_days: Optional[int] = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
    ) -> SmartFreshnessAssertion:
        """Upsert and merge a smart freshness assertion.

        Note:
            Keyword arguments are required.

        Upsert and merge is a combination of create and update. If the assertion does not exist,
        it will be created. If it does exist, it will be updated. Existing assertion fields will
        be updated if the input value is not None. If the input value is None, the existing value
        will be preserved. If the input value can be un-set (e.g. by passing an empty list or
        empty string), it will be unset.

        Schedule behavior:
            - Create case: Uses default hourly schedule ("0 * * * *")
            - Update case: Preserves existing schedule from backend (not modifiable)

        Args:
            dataset_urn (Union[str, DatasetUrn]): The urn of the dataset to be monitored.
            urn (Optional[Union[str, AssertionUrn]]): The urn of the assertion. If not provided, a urn will be generated and the assertion will be created in the DataHub instance.
            display_name (Optional[str]): The display name of the assertion. If not provided, a random display name will be generated.
            enabled (Optional[bool]): Whether the assertion is enabled. If not provided, the existing value will be preserved.
            detection_mechanism (DetectionMechanismInputTypes): The detection mechanism to be used for the assertion. Information schema is recommended. Valid values are:
                - "information_schema" or DetectionMechanism.INFORMATION_SCHEMA
                - "audit_log" or DetectionMechanism.AUDIT_LOG
                - {"type": "last_modified_column", "column_name": "last_modified", "additional_filter": "last_modified > '2021-01-01'"} or DetectionMechanism.LAST_MODIFIED_COLUMN(column_name='last_modified', additional_filter='last_modified > 2021-01-01')
                - "datahub_operation" or DetectionMechanism.DATAHUB_OPERATION
            sensitivity (Optional[Union[str, InferenceSensitivity]]): The sensitivity to be applied to the assertion. Valid values are: "low", "medium", "high".
            exclusion_windows (Optional[ExclusionWindowInputTypes]): The exclusion windows to be applied to the assertion. Only fixed range exclusion windows are supported. Valid values are:
                - {"start": "2025-01-01T00:00:00", "end": "2025-01-02T00:00:00"} (using ISO strings)
                - {"start": datetime(2025, 1, 1, 0, 0, 0), "end": datetime(2025, 1, 2, 0, 0, 0)} (using datetime objects)
                - FixedRangeExclusionWindow(start=datetime(2025, 1, 1, 0, 0, 0), end=datetime(2025, 1, 2, 0, 0, 0)) (using typed object)
                - A list of any of the above formats
            training_data_lookback_days (Optional[int]): The training data lookback days to be applied to the assertion as an integer.
            incident_behavior (Optional[Union[str, list[str], AssertionIncidentBehavior, list[AssertionIncidentBehavior]]]): The incident behavior to be applied to the assertion. Valid values are: "raise_on_fail", "resolve_on_pass" or the typed ones (AssertionIncidentBehavior.RAISE_ON_FAIL and AssertionIncidentBehavior.RESOLVE_ON_PASS).
            tags (Optional[TagsInputType]): The tags to be applied to the assertion. Valid values are: a list of strings, TagUrn objects, or TagAssociationClass objects.
            updated_by (Optional[Union[str, CorpUserUrn]]): Optional urn of the user who updated the assertion. The format is "urn:li:corpuser:<username>". The default is the datahub system user.

        Returns:
            SmartFreshnessAssertion: The created or updated assertion.
        """
        return self._smart_freshness_client.sync_smart_freshness_assertion(
            dataset_urn=dataset_urn,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            detection_mechanism=detection_mechanism,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
            incident_behavior=incident_behavior,
            tags=tags,
            updated_by=updated_by,
        )

    def sync_smart_volume_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        sensitivity: Optional[Union[str, InferenceSensitivity]] = None,
        exclusion_windows: Optional[ExclusionWindowInputTypes] = None,
        training_data_lookback_days: Optional[int] = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
    ) -> SmartVolumeAssertion:
        """Upsert and merge a smart volume assertion.

        Note:
            Keyword arguments are required.

        Upsert and merge is a combination of create and update. If the assertion does not exist,
        it will be created. If it does exist, it will be updated. Existing assertion fields will
        be updated if the input value is not None. If the input value is None, the existing value
        will be preserved. If the input value can be un-set (e.g. by passing an empty list or
        empty string), it will be unset.

        Schedule behavior:
            - Create case: Uses default hourly schedule ("0 * * * *") or provided schedule
            - Update case: Schedule is updated if provided, otherwise existing schedule is preserved.

        Args:
            dataset_urn (Union[str, DatasetUrn]): The urn of the dataset to be monitored.
            urn (Optional[Union[str, AssertionUrn]]): The urn of the assertion. If not provided, a urn will be generated and the assertion will be created in the DataHub instance.
            display_name (Optional[str]): The display name of the assertion. If not provided, a random display name will be generated.
            enabled (Optional[bool]): Whether the assertion is enabled. If not provided, the existing value will be preserved.
            detection_mechanism (DetectionMechanismInputTypes): The detection mechanism to be used for the assertion. Information schema is recommended. Valid values are:
                - "information_schema" or DetectionMechanism.INFORMATION_SCHEMA
                - {"type": "query", "additional_filter": "value > 1000"} or DetectionMechanism.QUERY(additional_filter='value > 1000')
                - "dataset_profile" or DetectionMechanism.DATASET_PROFILE
            sensitivity (Optional[Union[str, InferenceSensitivity]]): The sensitivity to be applied to the assertion. Valid values are: "low", "medium", "high".
            exclusion_windows (Optional[ExclusionWindowInputTypes]): The exclusion windows to be applied to the assertion. Only fixed range exclusion windows are supported. Valid values are:
                - {"start": "2025-01-01T00:00:00", "end": "2025-01-02T00:00:00"} (using ISO strings)
                - {"start": datetime(2025, 1, 1, 0, 0, 0), "end": datetime(2025, 1, 2, 0, 0, 0)} (using datetime objects)
                - FixedRangeExclusionWindow(start=datetime(2025, 1, 1, 0, 0, 0), end=datetime(2025, 1, 2, 0, 0, 0)) (using typed object)
                - A list of any of the above formats
            training_data_lookback_days (Optional[int]): The training data lookback days to be applied to the assertion as an integer.
            incident_behavior (Optional[Union[str, list[str], AssertionIncidentBehavior, list[AssertionIncidentBehavior]]]): The incident behavior to be applied to the assertion. Valid values are: "raise_on_fail", "resolve_on_pass", or the typed ones (AssertionIncidentBehavior.RAISE_ON_FAIL and AssertionIncidentBehavior.RESOLVE_ON_PASS).
            tags (Optional[TagsInputType]): The tags to be applied to the assertion. Valid values are: a list of strings, TagUrn objects, or TagAssociationClass objects.
            updated_by (Optional[Union[str, CorpUserUrn]]): Optional urn of the user who updated the assertion. The format is "urn:li:corpuser:<username>". The default is the datahub system user.
            schedule (Optional[Union[str, models.CronScheduleClass]]): Optional cron formatted schedule for the assertion. If not provided, a default schedule will be used. The format is a cron expression, e.g. "0 * * * *" for every hour using UTC timezone. Alternatively, a models.CronScheduleClass object can be provided.

        Returns:
            SmartVolumeAssertion: The created or updated assertion.
        """
        return self._smart_volume_client.sync_smart_volume_assertion(
            dataset_urn=dataset_urn,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            detection_mechanism=detection_mechanism,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
            incident_behavior=incident_behavior,
            tags=tags,
            updated_by=updated_by,
            schedule=schedule,
        )

    def sync_column_metric_assertion(  # TODO: Refactor
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
        return self._column_metric_client.sync_column_metric_assertion(
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
            schedule=schedule,
        )

    def sync_smart_column_metric_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        column_name: Optional[str] = None,
        metric_type: Optional[MetricInputType] = None,
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        sensitivity: Optional[Union[str, InferenceSensitivity]] = None,
        exclusion_windows: Optional[ExclusionWindowInputTypes] = None,
        training_data_lookback_days: Optional[int] = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
    ) -> SmartColumnMetricAssertion:
        """Upsert and merge a smart column metric assertion.

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
            client.sync_smart_column_metric_assertion(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table,PROD)",
                column_name="user_id",
                metric_type=MetricType.NULL_COUNT
            )

            # Using case-insensitive strings (more flexible)
            client.sync_smart_column_metric_assertion(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table,PROD)",
                column_name="price",
                metric_type="mean"
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
            urn (Optional[Union[str, AssertionUrn]]): The urn of the assertion. If not provided, a urn will be generated and the assertion will be created in the DataHub instance.
            display_name (Optional[str]): The display name of the assertion. If not provided, a random display name will be generated.
            enabled (Optional[bool]): Whether the assertion is enabled. If not provided, the existing value will be preserved.
            detection_mechanism (DetectionMechanismInputTypes): The detection mechanism to be used for the assertion. Valid values are (additional_filter is optional):
                - "all_rows_query_datahub_dataset_profile" or DetectionMechanism.ALL_ROWS_QUERY_DATAHUB_DATASET_PROFILE
                - "all_rows_query" or DetectionMechanism.ALL_ROWS_QUERY(), or with additional_filter: {"type": "all_rows_query", "additional_filter": "last_modified > '2021-01-01'"} or DetectionMechanism.ALL_ROWS_QUERY(additional_filter='last_modified > 2021-01-01')
                - {"type": "changed_rows_query", "column_name": "last_modified", "additional_filter": "last_modified > '2021-01-01'"} or DetectionMechanism.CHANGED_ROWS_QUERY(column_name='last_modified', additional_filter='last_modified > 2021-01-01')
            sensitivity (Optional[Union[str, InferenceSensitivity]]): The sensitivity to be applied to the assertion. Valid values are: "low", "medium", "high".
            exclusion_windows (Optional[ExclusionWindowInputTypes]): The exclusion windows to be applied to the assertion. Only fixed range exclusion windows are supported.
            training_data_lookback_days (Optional[int]): The training data lookback days to be applied to the assertion as an integer.
            incident_behavior (Optional[Union[str, list[str], AssertionIncidentBehavior, list[AssertionIncidentBehavior]]]): The incident behavior to be applied to the assertion. Valid values are: "raise_on_fail", "resolve_on_pass", or the typed ones (AssertionIncidentBehavior.RAISE_ON_FAIL and AssertionIncidentBehavior.RESOLVE_ON_PASS).
            tags (Optional[TagsInputType]): The tags to be applied to the assertion. Valid values are: a list of strings, TagUrn objects, or TagAssociationClass objects.
            updated_by (Optional[Union[str, CorpUserUrn]]): Optional urn of the user who updated the assertion. The format is "urn:li:corpuser:<username>". The default is the datahub system user.
            schedule (Optional[Union[str, models.CronScheduleClass]]): Optional cron formatted schedule for the assertion. If not provided, a default schedule of every 6 hours will be used. The format is a cron expression, e.g. "0 * * * *" for every hour using UTC timezone. Alternatively, a models.CronScheduleClass object can be provided.

        Returns:
            SmartColumnMetricAssertion: The created or updated assertion.
        """
        return self._smart_column_metric_client.sync_smart_column_metric_assertion(
            dataset_urn=dataset_urn,
            column_name=column_name,
            metric_type=metric_type,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            detection_mechanism=detection_mechanism,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
            incident_behavior=incident_behavior,
            tags=tags,
            updated_by=updated_by,
            schedule=schedule,
        )

    def sync_freshness_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
        freshness_schedule_check_type: Optional[
            Union[
                str,
                FreshnessAssertionScheduleCheckType,
                models.FreshnessAssertionScheduleTypeClass,
            ]
        ] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
        lookback_window: Optional[TimeWindowSizeInputTypes] = None,
    ) -> FreshnessAssertion:
        """Upsert and merge a freshness assertion.

        Note:
            Keyword arguments are required.

        Upsert and merge is a combination of create and update. If the assertion does not exist,
        it will be created. If it does exist, it will be updated. Existing assertion fields will
        be updated if the input value is not None. If the input value is None, the existing value
        will be preserved. If the input value can be un-set (e.g. by passing an empty list or
        empty string), it will be unset.

        Schedule behavior:
            - Create case: Uses default daily schedule ("0 0 * * *") or provided schedule
            - Update case: Uses existing schedule or provided schedule.

        Args:
            dataset_urn (Union[str, DatasetUrn]): The urn of the dataset to be monitored.
            urn (Optional[Union[str, AssertionUrn]]): The urn of the assertion. If not provided, a urn will be generated and the assertion will be created in the DataHub instance.
            display_name (Optional[str]): The display name of the assertion. If not provided, a random display name will be generated.
            enabled (Optional[bool]): Whether the assertion is enabled. If not provided, the existing value will be preserved.
            detection_mechanism (DetectionMechanismInputTypes): The detection mechanism to be used for the assertion. Information schema is recommended. Valid values are:
                - "information_schema" or DetectionMechanism.INFORMATION_SCHEMA
                - "audit_log" or DetectionMechanism.AUDIT_LOG
                - {"type": "last_modified_column", "column_name": "last_modified", "additional_filter": "last_modified > '2021-01-01'"} or DetectionMechanism.LAST_MODIFIED_COLUMN(column_name='last_modified', additional_filter='last_modified > 2021-01-01')
                - {"type": "high_watermark_column", "column_name": "id", "additional_filter": "id > 1000"} or DetectionMechanism.HIGH_WATERMARK_COLUMN(column_name='id', additional_filter='id > 1000')
                - "datahub_operation" or DetectionMechanism.DATAHUB_OPERATION
            incident_behavior (Optional[Union[str, list[str], AssertionIncidentBehavior, list[AssertionIncidentBehavior]]]): The incident behavior to be applied to the assertion. Valid values are: "raise_on_fail", "resolve_on_pass", or the typed ones (AssertionIncidentBehavior.RAISE_ON_FAIL and AssertionIncidentBehavior.RESOLVE_ON_PASS).
            tags (Optional[TagsInputType]): The tags to be applied to the assertion. Valid values are: a list of strings, TagUrn objects, or TagAssociationClass objects.
            updated_by (Optional[Union[str, CorpUserUrn]]): Optional urn of the user who updated the assertion. The format is "urn:li:corpuser:<username>". The default is the datahub system user.
            freshness_schedule_check_type (Optional[Union[str, FreshnessAssertionScheduleCheckType, models.FreshnessAssertionScheduleTypeClass]]): The freshness schedule check type to be applied to the assertion. Valid values are: "since_the_last_check", "fixed_interval".
            schedule (Optional[Union[str, models.CronScheduleClass]]): Optional cron formatted schedule for the assertion. If not provided, a default schedule will be used. The format is a cron expression, e.g. "0 * * * *" for every hour using UTC timezone. Alternatively, a models.CronScheduleClass object can be provided.
            lookback_window (Optional[TimeWindowSizeInputTypes]): The lookback window to be applied to the assertion. Valid values are:
                - TimeWindowSize(unit=CalendarInterval.MINUTE, multiple=10) for 10 minutes
                - TimeWindowSize(unit=CalendarInterval.HOUR, multiple=2) for 2 hours
                - TimeWindowSize(unit=CalendarInterval.DAY, multiple=1) for 1 day
                - {"unit": "MINUTE", "multiple": 30} for 30 minutes (using dict)
                - {"unit": "HOUR", "multiple": 6} for 6 hours (using dict)
                - {"unit": "DAY", "multiple": 7} for 7 days (using dict)
                Valid values for CalendarInterval are: "MINUTE", "HOUR", "DAY" and for multiple, the integer number of units.

        Returns:
            FreshnessAssertion: The created or updated assertion.
        """
        return self._freshness_client.sync_freshness_assertion(
            dataset_urn=dataset_urn,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            detection_mechanism=detection_mechanism,
            incident_behavior=incident_behavior,
            tags=tags,
            updated_by=updated_by,
            freshness_schedule_check_type=freshness_schedule_check_type,
            schedule=schedule,
            lookback_window=lookback_window,
        )

    def sync_volume_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
        criteria_condition: Optional[Union[str, VolumeAssertionCondition]] = None,
        criteria_parameters: Optional[VolumeAssertionDefinitionParameters] = None,
    ) -> VolumeAssertion:
        """Upsert and merge a volume assertion.

        Note:
            Keyword arguments are required.

        Upsert and merge is a combination of create and update. If the assertion does not exist,
        it will be created. If it does exist, it will be updated. Existing assertion fields will
        be updated if the input value is not None. If the input value is None, the existing value
        will be preserved. If the input value can be un-set (e.g. by passing an empty list or
        empty string), it will be unset.

        Schedule behavior:
            - Create case: Uses default daily schedule ("0 0 * * *") or provided schedule
            - Update case: Uses existing schedule or provided schedule.

        Args:
            dataset_urn (Union[str, DatasetUrn]): The urn of the dataset to be monitored.
            urn (Optional[Union[str, AssertionUrn]]): The urn of the assertion. If not provided, a urn will be generated and the assertion will be created in the DataHub instance.
            display_name (Optional[str]): The display name of the assertion. If not provided, a random display name will be generated.
            enabled (Optional[bool]): Whether the assertion is enabled. If not provided, the existing value will be preserved.
            detection_mechanism (DetectionMechanismInputTypes): The detection mechanism to be used for the assertion. Information schema is recommended. Valid values are (additional_filter is optional):
                - "information_schema" or DetectionMechanism.INFORMATION_SCHEMA
                - {"type": "query", "additional_filter": "value > 1000"} or DetectionMechanism.QUERY(additional_filter='value > 1000')
                - "dataset_profile" or DetectionMechanism.DATASET_PROFILE
            incident_behavior (Optional[Union[str, list[str], AssertionIncidentBehavior, list[AssertionIncidentBehavior]]]): The incident behavior to be applied to the assertion. Valid values are: "raise_on_fail", "resolve_on_pass", or the typed ones (AssertionIncidentBehavior.RAISE_ON_FAIL and AssertionIncidentBehavior.RESOLVE_ON_PASS).
            tags (Optional[TagsInputType]): The tags to be applied to the assertion. Valid values are: a list of strings, TagUrn objects, or TagAssociationClass objects.
            updated_by (Optional[Union[str, CorpUserUrn]]): Optional urn of the user who updated the assertion. The format is "urn:li:corpuser:<username>". The default is the datahub system user.
            schedule (Optional[Union[str, models.CronScheduleClass]]): Optional cron formatted schedule for the assertion. If not provided, a default schedule will be used. The format is a cron expression, e.g. "0 * * * *" for every hour using UTC timezone. Alternatively, a models.CronScheduleClass object can be provided.
            criteria_condition (Optional[Union[str, VolumeAssertionCondition]]): Optional condition for the volume assertion. Valid values are:
                - "ROW_COUNT_IS_LESS_THAN_OR_EQUAL_TO" -> The row count is less than or equal to the threshold.
                - "ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO" -> The row count is greater than or equal to the threshold.
                - "ROW_COUNT_IS_WITHIN_A_RANGE" -> The row count is within the specified range.
                - "ROW_COUNT_GROWS_BY_AT_MOST_ABSOLUTE" -> The row count growth is at most the threshold (absolute change).
                - "ROW_COUNT_GROWS_BY_AT_LEAST_ABSOLUTE" -> The row count growth is at least the threshold (absolute change).
                - "ROW_COUNT_GROWS_WITHIN_A_RANGE_ABSOLUTE" -> The row count growth is within the specified range (absolute change).
                - "ROW_COUNT_GROWS_BY_AT_MOST_PERCENTAGE" -> The row count growth is at most the threshold (percentage change).
                - "ROW_COUNT_GROWS_BY_AT_LEAST_PERCENTAGE" -> The row count growth is at least the threshold (percentage change).
                - "ROW_COUNT_GROWS_WITHIN_A_RANGE_PERCENTAGE" -> The row count growth is within the specified range (percentage change).
                If not provided, the existing definition from the backend will be preserved (for update operations). Required when creating a new assertion (when urn is None).
            criteria_parameters (Optional[VolumeAssertionDefinitionParameters]): Optional threshold parameters to be used for the assertion. This can be a single threshold value or a tuple range.
                - If the condition is range-based (ROW_COUNT_IS_WITHIN_A_RANGE, ROW_COUNT_GROWS_WITHIN_A_RANGE_ABSOLUTE, ROW_COUNT_GROWS_WITHIN_A_RANGE_PERCENTAGE), the value is a tuple of two threshold values, with format (min, max).
                - For other conditions, the value is a single numeric threshold value.
                If not provided, existing value is preserved for updates. Required when creating a new assertion.

        Returns:
            VolumeAssertion: The created or updated assertion.
        """
        return self._volume_client.sync_volume_assertion(
            dataset_urn=dataset_urn,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            detection_mechanism=detection_mechanism,
            incident_behavior=incident_behavior,
            tags=tags,
            updated_by=updated_by,
            schedule=schedule,
            criteria_condition=criteria_condition,
            criteria_parameters=criteria_parameters,
        )

    def sync_sql_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        statement: Optional[str] = None,
        criteria_condition: Optional[Union[SqlAssertionCondition, str]] = None,
        criteria_parameters: Optional[
            Union[Union[float, int], tuple[Union[float, int], Union[float, int]]]
        ] = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
    ) -> SqlAssertion:
        """Upsert and merge a sql assertion.

        Note:
            Keyword arguments are required.

        Upsert and merge is a combination of create and update. If the assertion does not exist,
        it will be created. If it does exist, it will be updated. Existing assertion fields will
        be updated if the input value is not None. If the input value is None, the existing value
        will be preserved. If the input value can be un-set (e.g. by passing an empty list or
        empty string), it will be unset.

        Schedule behavior:
            - Create case: Uses default daily schedule ("0 0 * * *") or provided schedule
            - Update case: Uses existing schedule or provided schedule.

        Args:
            dataset_urn (Union[str, DatasetUrn]): The urn of the dataset to be monitored.
            urn (Optional[Union[str, AssertionUrn]]): The urn of the assertion. If not provided, a urn will be generated and the assertion will be created in the DataHub instance.
            display_name (Optional[str]): The display name of the assertion. If not provided, a random display name will be generated.
            enabled (Optional[bool]): Whether the assertion is enabled. If not provided, the existing value will be preserved.
            statement (Optional[str]): The SQL statement to be used for the assertion. Required when creating a new assertion (urn=None), optional when updating an existing assertion.
            criteria_condition (Optional[Union[SqlAssertionCondition, str]]): The condition for the sql assertion. Required when creating a new assertion (urn=None), optional when updating an existing assertion. Valid values are:
                - "IS_EQUAL_TO" -> The metric value equals the threshold.
                - "IS_NOT_EQUAL_TO" -> The metric value does not equal the threshold.
                - "IS_GREATER_THAN" -> The metric value is greater than the threshold.
                - "IS_LESS_THAN" -> The metric value is less than the threshold.
                - "IS_WITHIN_A_RANGE" -> The metric value is within the specified range.
                - "GROWS_AT_MOST_ABSOLUTE" -> The metric growth is at most the threshold (absolute change).
                - "GROWS_AT_MOST_PERCENTAGE" -> The metric growth is at most the threshold (percentage change).
                - "GROWS_AT_LEAST_ABSOLUTE" -> The metric growth is at least the threshold (absolute change).
                - "GROWS_AT_LEAST_PERCENTAGE" -> The metric growth is at least the threshold (percentage change).
                - "GROWS_WITHIN_A_RANGE_ABSOLUTE" -> The metric growth is within the specified range (absolute change).
                - "GROWS_WITHIN_A_RANGE_PERCENTAGE" -> The metric growth is within the specified range (percentage change).
            criteria_parameters (Optional[Union[float, int, tuple[float, int]]]): The threshold parameters to be used for the assertion. Required when creating a new assertion (urn=None), optional when updating an existing assertion. This can be a single threshold value or a tuple range.
                - If the condition is range-based (IS_WITHIN_A_RANGE, GROWS_WITHIN_A_RANGE_ABSOLUTE, GROWS_WITHIN_A_RANGE_PERCENTAGE), the value is a tuple of two threshold values, with format (min, max).
                - For other conditions, the value is a single numeric threshold value.
            incident_behavior (Optional[Union[str, list[str], AssertionIncidentBehavior, list[AssertionIncidentBehavior]]]): The incident behavior to be applied to the assertion. Valid values are: "raise_on_fail", "resolve_on_pass", or the typed ones (AssertionIncidentBehavior.RAISE_ON_FAIL and AssertionIncidentBehavior.RESOLVE_ON_PASS).
            tags (Optional[TagsInputType]): The tags to be applied to the assertion. Valid values are: a list of strings, TagUrn objects, or TagAssociationClass objects.
            updated_by (Optional[Union[str, CorpUserUrn]]): Optional urn of the user who updated the assertion. The format is "urn:li:corpuser:<username>". The default is the datahub system user.
            schedule (Optional[Union[str, models.CronScheduleClass]]): Optional cron formatted schedule for the assertion. If not provided, a default schedule will be used. The format is a cron expression, e.g. "0 * * * *" for every hour using UTC timezone. Alternatively, a models.CronScheduleClass object can be provided.

        Returns:
            SqlAssertion: The created or updated assertion.
        """
        return self._sql_client.sync_sql_assertion(
            dataset_urn=dataset_urn,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            statement=statement,
            criteria_condition=criteria_condition,
            criteria_parameters=criteria_parameters,
            incident_behavior=incident_behavior,
            tags=tags,
            updated_by=updated_by,
            schedule=schedule,
        )
