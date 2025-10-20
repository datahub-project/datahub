import logging
import time
from typing import List, Optional, Tuple, Union

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
from datahub_executor.common.assertion.engine.evaluator.utils.field import (
    convert_field_parameters_to_metric_resolver_strategy,
)
from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
    is_training_required,
    make_monitor_metric_cube_urn,
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
from datahub_executor.common.metric.client.client import (
    MetricClient,
)
from datahub_executor.common.metric.resolver.resolver import MetricResolver
from datahub_executor.common.metric.types import (
    Metric,
)
from datahub_executor.common.monitor.client.client import (
    MonitorClient,
)
from datahub_executor.common.source.provider import SourceProvider
from datahub_executor.common.source.source import Source
from datahub_executor.common.state.assertion_state_provider import (
    AssertionStateProvider,
)
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

    metric_resolver: MetricResolver
    metric_client: MetricClient

    def __init__(
        self,
        connection_provider: DataHubIngestionSourceConnectionProvider,
        state_provider: AssertionStateProvider,
        source_provider: SourceProvider,
        metric_resolver: MetricResolver,
        metric_client: MetricClient,
        monitor_client: MonitorClient,
    ) -> None:
        super().__init__(
            connection_provider, state_provider, source_provider, monitor_client
        )
        self.metric_resolver = metric_resolver
        self.metric_client = metric_client

    @property
    def type(self) -> AssertionType:
        return AssertionType.FIELD

    @property
    def default_parameters(self) -> AssertionEvaluationParameters:
        return AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_FIELD,
            dataset_field_parameters=DatasetFieldAssertionParameters(
                source_type=DatasetFieldSourceType.ALL_ROWS_QUERY
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
            assert dataset_field_parameters.changed_rows_field, (
                "Missing required changed rows field for change rows assertion!"
            )

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
                    "filter": filter.model_dump() if filter else None,
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
        assert field_assertion.field_values_assertion is not None, (
            "Missing required field values assertion!"
        )
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

    def _evaluate_internal(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        """Evaluate a FIELD assertion by delegating to sub-methods based on assertion type."""

        # Basic sanity checks.
        assert assertion.field_assertion, "Missing required field assertion!"
        field_assertion = assertion.field_assertion

        if field_assertion.type == FieldAssertionType.FIELD_METRIC:
            return self._evaluate_internal_field_metric(assertion, parameters, context)
        elif field_assertion.type == FieldAssertionType.FIELD_VALUES:
            return self._evaluate_internal_field_values(assertion, parameters, context)
        else:
            # Otherwise, raise an error if a new or unsupported type is encountered.
            raise InvalidParametersException(
                message=(
                    f"Failed to evaluate FIELD Assertion. "
                    f"Unsupported FIELD Assertion Type {field_assertion.type} provided."
                ),
                parameters=parameters.dataset_field_parameters.model_dump(),
            )

    def _evaluate_internal_field_metric(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        """
        Evaluate a FIELD_METRIC assertion. This method branches further based on whether
        we are using DataHub's Dataset Profile or a direct (query-based) source.
        """
        # Determine if this is a smart assertion, requiring stored sampling.
        is_training_required_for_assertion = is_training_required(assertion=assertion)

        logger.debug("About to run metric collection step...")

        # Stage 1: Collect metrics & save to DataHub
        field_metric_value = self._evaluate_field_metric_collection_step(
            assertion=assertion,
            parameters=parameters,
            context=context,
            save=is_training_required_for_assertion,
        )

        logger.debug("About to evaluate field metric assertion...")

        # Stage 2: Perform final evaluation
        result = self._evaluate_field_metric_assertion_step(
            metric_value=field_metric_value.value,
            assertion=assertion,
            context=context,
        )
        # Add the metric context.
        result.metric = field_metric_value
        return result

    def _evaluate_field_metric_collection_step(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
        save: bool,
    ) -> Metric:
        logger.debug(
            "Starting Metric Collection Step for field metric assertion %s",
            assertion.urn or "unknown",
        )
        assert assertion.field_assertion, "Missing required field assertion!"
        assert assertion.field_assertion.field_metric_assertion, (
            "Missing required field metric assertion!"
        )
        assert parameters.dataset_field_parameters, (
            "Missig required dataset field assertion parameters!"
        )

        entity_urn = assertion.entity.urn
        database_params = get_database_parameters(assertion)
        filter_params = (
            assertion.field_assertion.filter if assertion.field_assertion else None
        )

        # Optional Step 1: If there is a previous high watermark value we need to fetch
        # For performing incremental checks, extract that here.
        prev_high_watermark_value = None
        if (
            parameters.dataset_field_parameters.source_type
            == DatasetFieldSourceType.CHANGED_ROWS_QUERY
        ):
            # If its a changed row query, we must be hitting a database directly.
            assert assertion.connection_urn, (
                "Missing required connection urn for changed rows field assertion"
            )

            connection = self.connection_provider.get_connection(entity_urn)
            if connection is None:
                raise SourceConnectionErrorException(
                    message=(
                        f"Unable to retrieve valid connection for Data Platform "
                        f"with urn {assertion.connection_urn}"
                    ),
                    connection_urn=assertion.connection_urn,
                )

            source = self.source_provider.create_source_from_connection(connection)
            database_params = get_database_parameters(assertion)

            (
                _ignored,
                prev_high_watermark_value,
            ) = self._fetch_changed_rows_query_state(
                entity_urn,
                parameters.dataset_field_parameters,
                source,
                database_params,
                context,
                assertion.field_assertion.filter,
            )

        # Step 2: Fetch metric via MetricResolver
        metric = self.metric_resolver.get_field_metric(
            entity_urn,
            assertion.field_assertion.field_metric_assertion.field,
            assertion.field_assertion.field_metric_assertion.metric,
            database_params,
            filter_params,
            parameters.dataset_field_parameters.changed_rows_field,
            prev_high_watermark_value,
            convert_field_parameters_to_metric_resolver_strategy(
                parameters.dataset_field_parameters
            ),
        )

        # Step 3: Optionally save the fetched metric to the metric cube.
        if save and context.monitor_urn:
            metric_cube_urn = make_monitor_metric_cube_urn(context.monitor_urn)
            self.metric_client.save_metric_value(metric_cube_urn, metric)

        # Step 4: Return the metric
        logger.debug("Completed metric collection step with metric=%s", metric)
        return metric

    def _evaluate_field_metric_assertion_step(
        self,
        metric_value: float,
        assertion: Assertion,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        assert assertion.field_assertion, "Missing required field assertion!"
        assert assertion.field_assertion.field_metric_assertion, (
            "Missing required field metric assertion!"
        )

        if context.online_smart_assertions and is_training_required(assertion):
            # If we have smart assertions v2, there might not be an assertion present yet.
            if context.evaluation_spec:
                last_inferred_at = (
                    context.evaluation_spec.context.inference_details.generated_at
                    if context.evaluation_spec.context
                    and context.evaluation_spec.context.inference_details
                    else None
                )

                if not last_inferred_at or last_inferred_at <= 0:
                    return AssertionEvaluationResult(
                        AssertionResultType.INIT,
                        parameters={"metric_value": str(round(metric_value, 2))},
                    )

        assertion_params = assertion.field_assertion.field_metric_assertion.parameters

        metric_evaluation = self._evaluate_value(
            metric_value,
            assertion.field_assertion.field_metric_assertion.operator,
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

    def _evaluate_internal_field_values(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        """
        Evaluate a FIELD_VALUES assertion, which always requires the query-based approach.
        """
        assert assertion.connection_urn, (
            "Missing required connection urn for field values assertion!"
        )
        assert assertion.field_assertion, "Missing required field assertion!"
        assert parameters.dataset_field_parameters, (
            "Missing required dataset field assertion parameters!"
        )

        entity_urn = assertion.entity.urn
        field_assertion = assertion.field_assertion
        dataset_field_parameters = parameters.dataset_field_parameters

        connection = self.connection_provider.get_connection(entity_urn)
        if connection is None:
            raise SourceConnectionErrorException(
                message=(
                    f"Unable to retrieve valid connection for Data Platform "
                    f"with urn {assertion.connection_urn}"
                ),
                connection_urn=assertion.connection_urn,
            )

        source = self.source_provider.create_source_from_connection(connection)
        database_params = get_database_parameters(assertion)

        return self._evaluate_field_values_assertion(
            entity_urn,
            field_assertion,
            dataset_field_parameters,
            source,
            database_params,
            context,
        )
