import logging
import time
from typing import List, Optional, Tuple, Union, cast

from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
)

from datahub_executor.common.assertion.engine.evaluator.evaluator import (
    AssertionEvaluator,
)
from datahub_executor.common.assertion.engine.evaluator.utils import (
    get_database_parameters,
)
from datahub_executor.common.assertion.types import (
    AssertionDatabaseParams,
    AssertionState,
    AssertionStateType,
)
from datahub_executor.common.connection.datahub_ingestion_source_connection_provider import (
    DataHubIngestionSourceConnectionProvider,
)
from datahub_executor.common.exceptions import (
    InsufficientDataException,
    InvalidParametersException,
    SourceConnectionErrorException,
)
from datahub_executor.common.source.source import Source
from datahub_executor.common.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionEvaluationResult,
    AssertionResultType,
    AssertionType,
    DatasetFieldAssertionParameters,
    DatasetFieldSourceType,
    DatasetFilter,
    EntityEventType,
    FieldAssertion,
    FieldAssertionType,
    FieldMetricType,
    FieldValuesFailThresholdType,
)

logger = logging.getLogger(__name__)


class FieldAssertionEvaluator(AssertionEvaluator):
    """Evaluator for FIELD assertions."""

    @property
    def type(self) -> AssertionType:
        return AssertionType.FIELD

    @property
    def default_parameters(self) -> AssertionEvaluationParameters:
        return AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_FIELD,
            dataset_field_parameters=DatasetFieldAssertionParameters(
                sourceType=DatasetFieldSourceType.ALL_ROWS_QUERY
            ),
        )

    def _fetch_changed_rows_query_state(
        self,
        entity_urn: str,
        dataset_field_parameters: DatasetFieldAssertionParameters,
        source: Source,
        database_params: AssertionDatabaseParams,
        context: AssertionEvaluationContext,
        filter: Optional[DatasetFilter],
    ) -> Tuple[Optional[AssertionState], Optional[str]]:
        previous_state = None
        prev_high_watermark_value = None

        if (
            dataset_field_parameters.source_type
            == DatasetFieldSourceType.CHANGED_ROWS_QUERY
            and context.monitor_urn
        ):
            assert dataset_field_parameters.changed_rows_field is not None

            # we get the prev state, if available
            previous_state = self.state_provider.get_state(
                context.monitor_urn, AssertionStateType.MONITOR_TIMESERIES_STATE
            )
            # grab the previous high watermark value if we stored one.
            if previous_state:
                prev_high_watermark_value = previous_state.properties.get(
                    "high_watermark_value", None
                )

            # get the current high watermark value for this field
            high_watermark_value, _ = source.get_current_high_watermark_for_column(
                entity_urn,
                EntityEventType.FIELD_UPDATE,
                [0, 0],
                {
                    "path": dataset_field_parameters.changed_rows_field.path,
                    "type": dataset_field_parameters.changed_rows_field.type,
                    "native_type": dataset_field_parameters.changed_rows_field.native_type,
                    "filter": filter.__dict__ if filter else None,
                    "database": database_params,
                },
                prev_high_watermark_value,
            )

            # save state for this assertion/monitor, including the current high watermark value
            # for the next assertion run.
            self.state_provider.save_state(
                context.monitor_urn,
                AssertionState(
                    type=AssertionStateType.MONITOR_TIMESERIES_STATE,
                    timestamp=int(time.time() * 1000),
                    properties={"high_watermark_value": str(high_watermark_value)},
                ),
            )

        return previous_state, prev_high_watermark_value

    def _evaluate_field_values_assertion(
        self,
        entity_urn: str,
        field_assertion: FieldAssertion,
        dataset_field_parameters: DatasetFieldAssertionParameters,
        source: Source,
        database_params: AssertionDatabaseParams,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        assert field_assertion.field_values_assertion is not None
        # TODO - remove this later when we support PERCENTAGE
        assert (
            field_assertion.field_values_assertion.fail_threshold.type
            == FieldValuesFailThresholdType.COUNT
        )

        (
            previous_state,
            prev_high_watermark_value,
        ) = self._fetch_changed_rows_query_state(
            entity_urn,
            dataset_field_parameters,
            source,
            database_params,
            context,
            field_assertion.filter,
        )

        values_count = source.get_field_values_count(
            entity_urn,
            database_params,
            field_assertion.field_values_assertion.field,
            field_assertion.field_values_assertion.operator,
            field_assertion.field_values_assertion.parameters,
            field_assertion.field_values_assertion.exclude_nulls,
            field_assertion.field_values_assertion.transform,
            field_assertion.filter,
            prev_high_watermark_value,
            dataset_field_parameters.changed_rows_field,
        )

        fail_threshold_value = (
            field_assertion.field_values_assertion.fail_threshold.value
        )

        if (
            dataset_field_parameters.source_type
            == DatasetFieldSourceType.CHANGED_ROWS_QUERY
            and previous_state is None
        ):
            return AssertionEvaluationResult(
                AssertionResultType.INIT, parameters={"values_count": str(values_count)}
            )

        return AssertionEvaluationResult(
            (
                AssertionResultType.FAILURE
                if values_count > fail_threshold_value
                else AssertionResultType.SUCCESS
            ),
            parameters={
                "threshold_value": str(fail_threshold_value),
                "values_count": str(values_count),
            },
        )

    def _get_dataset_field_profile(
        self,
        field_profiles: List[DatasetFieldProfileClass],
        field_path: str,
    ) -> Optional[DatasetFieldProfileClass]:
        for field_profile in field_profiles:
            if field_path == field_profile.fieldPath:
                return field_profile
        return None

    def _get_field_metric_value_from_dataset_field_profile(
        self,
        dataset_field_profile: DatasetFieldProfileClass,
        metric: FieldMetricType,
    ) -> Union[None, float]:
        metric_value: Union[None, str, int, float] = None

        if metric == FieldMetricType.UNIQUE_COUNT:
            metric_value = dataset_field_profile.uniqueCount
        if metric == FieldMetricType.UNIQUE_PERCENTAGE:
            metric_value = (
                dataset_field_profile.uniqueProportion * 100.0
                if dataset_field_profile.uniqueProportion is not None
                else None
            )
        if metric == FieldMetricType.NULL_COUNT:
            metric_value = dataset_field_profile.nullCount
        if metric == FieldMetricType.NULL_PERCENTAGE:
            metric_value = (
                dataset_field_profile.nullProportion * 100.0
                if dataset_field_profile.nullProportion is not None
                else None
            )
        if metric == FieldMetricType.MIN:
            metric_value = dataset_field_profile.min
        if metric == FieldMetricType.MAX:
            metric_value = dataset_field_profile.max
        if metric == FieldMetricType.MEAN:
            metric_value = dataset_field_profile.mean
        if metric == FieldMetricType.MEDIAN:
            metric_value = dataset_field_profile.median
        if metric == FieldMetricType.STDDEV:
            metric_value = dataset_field_profile.stdev

        if metric_value is not None:
            return float(metric_value)
        return None

    def _evaluate_datahub_dataset_profile_field_metric_assertion(
        self,
        entity_urn: str,
        field_assertion: FieldAssertion,
    ) -> AssertionEvaluationResult:
        assert field_assertion.field_metric_assertion is not None
        assert isinstance(
            self.connection_provider, DataHubIngestionSourceConnectionProvider
        )
        if field_assertion.field_metric_assertion.metric not in [
            FieldMetricType.UNIQUE_COUNT,
            FieldMetricType.UNIQUE_PERCENTAGE,
            FieldMetricType.NULL_COUNT,
            FieldMetricType.NULL_PERCENTAGE,
            FieldMetricType.MIN,
            FieldMetricType.MAX,
            FieldMetricType.MEAN,
            FieldMetricType.MEDIAN,
            FieldMetricType.STDDEV,
        ]:
            raise InvalidParametersException(
                message=f"Metric {field_assertion.field_metric_assertion.metric.name} is not supported when using DataHub Dataset Profile as the data source.",
                parameters={},
            )

        assertion_params = field_assertion.field_metric_assertion.parameters

        dataset_profile: Optional[DatasetProfileClass] = (
            self.connection_provider.graph.get_latest_timeseries_value(
                entity_urn=entity_urn,
                aspect_type=DatasetProfileClass,
                filter_criteria_map={},
            )
        )

        if dataset_profile is None or dataset_profile.fieldProfiles is None:
            raise InsufficientDataException(
                message=f"Unable to find latest dataset profile for {entity_urn}"
            )

        dataset_field_profile: Optional[DatasetFieldProfileClass] = (
            self._get_dataset_field_profile(
                dataset_profile.fieldProfiles,
                field_assertion.field_metric_assertion.field.path,
            )
        )
        if dataset_field_profile is None:
            raise InsufficientDataException(
                message=f"Unable to find dataset field profile for {entity_urn} {field_assertion.field_metric_assertion.field.path}"
            )

        metric_value = self._get_field_metric_value_from_dataset_field_profile(
            dataset_field_profile,
            field_assertion.field_metric_assertion.metric,
        )
        if metric_value is None:
            raise InsufficientDataException(
                message=f"Unable to find dataset field profile data for for {entity_urn} {field_assertion.field_metric_assertion.metric.name}"
            )

        metric_evaluation = self._evaluate_value(
            metric_value,
            field_assertion.field_metric_assertion.operator,
            assertion_params,
        )

        return AssertionEvaluationResult(
            (
                AssertionResultType.SUCCESS
                if metric_evaluation is True
                else AssertionResultType.FAILURE
            ),
            parameters={
                "metric_value": str(round(metric_value, 2)),
                "value": (
                    str(assertion_params.value.value)
                    if assertion_params.value
                    else None
                ),
                "min_value": (
                    str(assertion_params.min_value.value)
                    if assertion_params.min_value
                    else None
                ),
                "max_value": (
                    str(assertion_params.max_value.value)
                    if assertion_params.max_value
                    else None
                ),
            },
        )

    def _evaluate_field_metric_assertion(
        self,
        entity_urn: str,
        field_assertion: FieldAssertion,
        dataset_field_parameters: DatasetFieldAssertionParameters,
        source: Source,
        database_params: AssertionDatabaseParams,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        assert field_assertion.field_metric_assertion is not None

        (
            previous_state,
            prev_high_watermark_value,
        ) = self._fetch_changed_rows_query_state(
            entity_urn,
            dataset_field_parameters,
            source,
            database_params,
            context,
            field_assertion.filter,
        )

        assertion_params = field_assertion.field_metric_assertion.parameters
        metric_value = source.get_field_metric_value(
            entity_urn,
            database_params,
            field_assertion.field_metric_assertion.field,
            field_assertion.field_metric_assertion.metric,
            field_assertion.filter,
            prev_high_watermark_value,
            dataset_field_parameters.changed_rows_field,
        )
        metric_evaluation = self._evaluate_value(
            metric_value,
            field_assertion.field_metric_assertion.operator,
            assertion_params,
        )

        return AssertionEvaluationResult(
            (
                AssertionResultType.SUCCESS
                if metric_evaluation is True
                else AssertionResultType.FAILURE
            ),
            parameters={
                "metric_value": str(round(metric_value, 2)),
                "value": (
                    str(assertion_params.value.value)
                    if assertion_params.value
                    else None
                ),
                "min_value": (
                    str(assertion_params.min_value.value)
                    if assertion_params.min_value
                    else None
                ),
                "max_value": (
                    str(assertion_params.max_value.value)
                    if assertion_params.max_value
                    else None
                ),
            },
        )

    def _evaluate_internal(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        assert assertion.connection_urn
        assert assertion.field_assertion is not None
        assert parameters.dataset_field_parameters is not None

        entity_urn = assertion.entity.urn
        field_assertion = assertion.field_assertion
        dataset_field_parameters = parameters.dataset_field_parameters

        if (
            field_assertion.type == FieldAssertionType.FIELD_METRIC
            and dataset_field_parameters.source_type
            == DatasetFieldSourceType.DATAHUB_DATASET_PROFILE
        ):
            return self._evaluate_datahub_dataset_profile_field_metric_assertion(
                entity_urn, field_assertion
            )

        connection = self.connection_provider.get_connection(
            cast(str, assertion.entity.urn)
        )
        if connection is None:
            raise SourceConnectionErrorException(
                message=f"Unable to retrieve valid connection for Data Platform with urn {assertion.connection_urn}",
                connection_urn=assertion.connection_urn,
            )

        source = self.source_provider.create_source_from_connection(connection)
        database_params = get_database_parameters(assertion)

        if field_assertion.type == FieldAssertionType.FIELD_VALUES:
            return self._evaluate_field_values_assertion(
                entity_urn,
                field_assertion,
                dataset_field_parameters,
                source,
                database_params,
                context,
            )
        elif field_assertion.type == FieldAssertionType.FIELD_METRIC:
            return self._evaluate_field_metric_assertion(
                entity_urn,
                field_assertion,
                dataset_field_parameters,
                source,
                database_params,
                context,
            )
        else:
            raise InvalidParametersException(
                message=f"Failed to evaluate FIELD Assertion. Unsupported FIELD Assertion Source Type {field_assertion.type} provided.",
                parameters=dataset_field_parameters.__dict__,
            )
