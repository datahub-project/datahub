import logging
import time
from typing import List, Optional, cast

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import OperationClass

from datahub_executor.common.assertion.engine.evaluator.evaluator import (
    AssertionEvaluator,
)
from datahub_executor.common.assertion.engine.evaluator.utils.freshness import (
    get_event_type_parameters_from_parameters,
)
from datahub_executor.common.assertion.engine.evaluator.utils.time import (
    get_fixed_interval_start,
    get_next_cron_schedule_time,
    get_prev_cron_schedule_time,
)
from datahub_executor.common.assertion.types import AssertionState, AssertionStateType
from datahub_executor.common.connection.datahub_ingestion_source_connection_provider import (
    DataHubIngestionSourceConnectionProvider,
)
from datahub_executor.common.exceptions import (
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
    CronSchedule,
    DatasetFreshnessAssertionParameters,
    DatasetFreshnessSourceType,
    EntityEvent,
    EntityEventType,
    FixedIntervalSchedule,
    FreshnessAssertion,
    FreshnessAssertionScheduleType,
    FreshnessCronSchedule,
    FreshnessFieldKind,
)

logger = logging.getLogger(__name__)


STATEFUL_ASSERTION_EVALUATION_BUFFER = 5 * 60 * 1000


class InternalAssertionEvaluationResult:
    result: AssertionEvaluationResult
    next_assertion_state: Optional[AssertionState]

    def __init__(
        self,
        result: AssertionEvaluationResult,
        next_assertion_state: Optional[AssertionState] = None,
    ):
        self.result = result
        self.next_assertion_state = next_assertion_state


class FreshnessAssertionEvaluator(AssertionEvaluator):
    """Evaluator for FRESHNESS assertions."""

    @property
    def type(self) -> AssertionType:
        return AssertionType.FRESHNESS

    @property
    def default_parameters(self) -> AssertionEvaluationParameters:
        return AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_FRESHNESS,
            dataset_freshness_parameters=DatasetFreshnessAssertionParameters(
                sourceType=DatasetFreshnessSourceType.INFORMATION_SCHEMA
            ),
        )

    def _hydrate_window_event_result_with_window_parameters(
        self, internal_result: InternalAssertionEvaluationResult, window: List[int]
    ) -> InternalAssertionEvaluationResult:
        if internal_result.result is None:
            return internal_result
        if len(window) == 0:
            return internal_result
        if internal_result.result.parameters is None:
            internal_result.result.parameters = {}
        internal_result.result.parameters["window_start_time"] = str(window[0])
        if len(window) == 2:
            internal_result.result.parameters["window_end_time"] = str(window[1])
        return internal_result

    def _evaluate_internal_window_event(
        self,
        window: List[int],
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
        maybe_previous_state: Optional[AssertionState] = None,
    ) -> InternalAssertionEvaluationResult:
        assert assertion.connection_urn
        entity_urn = assertion.entity.urn

        # Check whether any matching events have fallen into the bucket
        event_type, source_params = get_event_type_parameters_from_parameters(
            assertion, parameters
        )

        if event_type == EntityEventType.DATAHUB_OPERATION:
            return self._hydrate_window_event_result_with_window_parameters(
                self._evaluate_datahub_operation_assertion(
                    entity_urn,
                    window,
                    source_params,
                ),
                window,
            )

        connection = self.connection_provider.get_connection(
            cast(str, assertion.entity.urn)
        )

        if connection is None:
            raise SourceConnectionErrorException(
                message=f"Unable to retrieve valid connection for Data Platform with urn {assertion.connection_urn}",
                connection_urn=assertion.connection_urn,
            )

        # This is where we drop into system-specific bits --> Need a way to do this for Snowflake first.
        # TODO: Consider what it would take to batch queries. Maybe the client aggregates?
        source = self.source_provider.create_source_from_connection(connection)

        if (
            parameters
            and parameters.dataset_freshness_parameters
            and parameters.dataset_freshness_parameters.field
            and parameters.dataset_freshness_parameters.field.kind
            == FreshnessFieldKind.HIGH_WATERMARK
        ):
            return self._hydrate_window_event_result_with_window_parameters(
                self._evaluate_high_watermark_assertion(
                    source,
                    assertion,
                    entity_urn,
                    event_type,
                    window,
                    source_params,
                    context,
                    maybe_previous_state,
                ),
                window,
            )

        return self._hydrate_window_event_result_with_window_parameters(
            self._evaluate_assertion(
                source,
                assertion,
                entity_urn,
                event_type,
                window,
                source_params,
            ),
            window,
        )

    def _evaluate_assertion(
        self,
        source: Source,
        assertion: Assertion,
        entity_urn: str,
        event_type: EntityEventType,
        window: List[int],
        source_params: dict,
    ) -> InternalAssertionEvaluationResult:
        maybe_events = source.get_entity_events(
            entity_urn,
            event_type,
            window,
            source_params,
        )

        # Now verify whether there are any events in the window
        if maybe_events is not None and len(maybe_events) > 0:
            # We have some events within the expected window. That means the assertion has passed! Make sure we establish WHY the assertion has passed.
            logger.debug(
                "Found matching events within the provided window! Assertion is passing."
            )
            return InternalAssertionEvaluationResult(
                result=AssertionEvaluationResult(
                    AssertionResultType.SUCCESS, {"events": maybe_events}
                )
            )
        else:
            # No events are found. The assertion is failing!
            logger.info(
                "No matching events found within the provided window! Assertion is failing."
            )
            return InternalAssertionEvaluationResult(
                result=AssertionEvaluationResult(
                    AssertionResultType.FAILURE, parameters=None
                )
            )

    def _evaluate_high_watermark_freshness(
        self,
        assertion_urn: str,
        prev_field_value: str,
        prev_row_count: int,
        curr_field_value: str,
        curr_row_count: int,
    ) -> bool:
        # blank value means no data returned from when checking for new high watermark
        # field value, return False
        if curr_field_value == "":
            logger.debug(
                "New value is blank - marking assertion({assertion_urn}) FAILURE"
            )
            return False

        # row count is zero when checking for the new high watermark field value, data
        # has not changed as expected, return False
        if curr_row_count == 0:
            logger.debug(
                "New row count is zero - marking assertion({assertion_urn}) FAILURE"
            )
            return False

        # high watermark field has a new value, data has changed as expected, return True
        if curr_field_value != prev_field_value and prev_field_value != "":
            logger.debug(
                f"New value {curr_field_value} is different than previous value {prev_field_value} - marking assertion({assertion_urn}) SUCCESS"
            )
            return True

        # high watermark field value has not changed, so we check the row count
        # a new row count means data has changed as expected, return True
        if curr_row_count != prev_row_count and prev_row_count != 0:
            logger.debug(
                f"New row count {curr_row_count} is different than previous row count {prev_row_count} - marking assertion({assertion_urn}) SUCCESS"
            )
            return True

        logger.debug("Default - marking assertion({assertion_urn}) FAILURE")
        return False

    def _evaluate_high_watermark_assertion(
        self,
        source: Source,
        assertion: Assertion,
        entity_urn: str,
        event_type: EntityEventType,
        window: List[int],
        source_params: dict,
        context: AssertionEvaluationContext,
        maybe_previous_state: Optional[AssertionState],
    ) -> InternalAssertionEvaluationResult:
        [start_time, _] = window
        start_time = start_time - STATEFUL_ASSERTION_EVALUATION_BUFFER

        previous_state: AssertionState | None
        if maybe_previous_state is not None:
            previous_state = maybe_previous_state
        else:
            previous_state = (
                self.state_provider.get_state(
                    context.monitor_urn, AssertionStateType.MONITOR_TIMESERIES_STATE
                )
                if context.monitor_urn
                else None
            )

        if (
            previous_state
            and previous_state.timestamp
            and previous_state.timestamp < start_time
        ):
            # we last saved state for this assertion before our time window
            # we don't want to trigger false assertions, let's treat this like there is no previous state
            logger.debug(
                f"_evaluate_high_watermark_assertion for {assertion.urn} - no previous state found"
            )
            previous_state = None

        (
            current_field_value,
            current_row_count,
        ) = source.get_current_high_watermark_for_column(
            entity_urn,
            event_type,
            window,
            source_params,
            previous_state.properties.get("field_value") if previous_state else None,
        )

        last_state_change = (
            int(previous_state.properties.get("last_state_change", "0"))
            if previous_state
            else None
        )

        current_evaluation_freshness = (
            self._evaluate_high_watermark_freshness(
                assertion.urn,
                previous_state.properties.get("field_value", ""),
                int(previous_state.properties.get("row_count", "0")),
                current_field_value,
                current_row_count,
            )
            if previous_state
            else False
        )

        """
        here we evaluate the assertion, which can return one of three results
            INIT - we return init if we don't have the data to decide on SUCCESS or FAILURE
                this is either because we have no previous state or the previous state is too old
                ie. previous_state.timestamp < start_time (above)

            SUCCESS - we return success if we evaluate the prev/curr state to be fresh
                OR if we have a state change that falls within our evaluation window.
                eg. if the assertion says "dataset is updated every 10 mins" but our cron checks every minute
                    the current_evaluation_freshness may be false, but we still could have a valid change within
                    the past 10 mins.

            FAILURE - this is the default case if none of the above conditions are met
        """
        if previous_state is None:
            assertion_evaluation_result = AssertionEvaluationResult(
                AssertionResultType.INIT, parameters={"row_count": current_row_count}
            )
        elif (
            last_state_change and last_state_change > start_time
        ) or current_evaluation_freshness is True:
            assertion_evaluation_result = AssertionEvaluationResult(
                AssertionResultType.SUCCESS,
                parameters={
                    "row_count": current_row_count,
                    "prev_row_count": (
                        previous_state.properties.get("row_count", "0")
                        if previous_state
                        else "0"
                    ),
                },
            )
        else:
            assertion_evaluation_result = AssertionEvaluationResult(
                AssertionResultType.FAILURE,
                parameters={
                    "row_count": current_row_count,
                    "prev_row_count": (
                        previous_state.properties.get("row_count", "0")
                        if previous_state
                        else 0
                    ),
                },
            )

        # we store the current state regardless of assertion success/failure
        time_now = int(time.time() * 1000)

        if current_evaluation_freshness is True:
            last_state_change_str = str(time_now)
        else:
            last_state_change_str = (
                previous_state.properties.get("last_state_change", "0")
                if previous_state
                else "0"
            )

        next_assertion_state = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=time_now,
            properties={
                "field_value": current_field_value,
                "row_count": str(current_row_count),
                "last_state_change": last_state_change_str,
            },
        )

        return InternalAssertionEvaluationResult(
            result=assertion_evaluation_result,
            next_assertion_state=next_assertion_state,
        )

    def _evaluate_datahub_operation_assertion(
        self,
        entity_urn: str,
        window: List[int],
        source_params: dict,
    ) -> InternalAssertionEvaluationResult:
        assert isinstance(
            self.connection_provider, DataHubIngestionSourceConnectionProvider
        )
        assert isinstance(self.connection_provider.graph, DataHubGraph)

        [start_timestamp, end_timestamp] = window
        operation_aspects = self.connection_provider.graph.get_timeseries_values(
            entity_urn=entity_urn,
            aspect_type=OperationClass,
            filter={
                "or": [
                    {
                        "and": [
                            {
                                "field": "lastUpdatedTimestamp",
                                "condition": "GREATER_THAN_OR_EQUAL_TO",
                                "value": str(start_timestamp),
                            },
                            {
                                "field": "lastUpdatedTimestamp",
                                "condition": "LESS_THAN_OR_EQUAL_TO",
                                "value": str(end_timestamp),
                            },
                        ]
                    }
                ]
            },
            limit=1,
        )

        if operation_aspects is not None and len(operation_aspects) > 0:
            entity_events = [
                EntityEvent(
                    EntityEventType.DATAHUB_OPERATION, operation.timestampMillis
                )
                for operation in operation_aspects
            ]
            return InternalAssertionEvaluationResult(
                result=AssertionEvaluationResult(
                    AssertionResultType.SUCCESS, {"events": entity_events}
                )
            )

        return InternalAssertionEvaluationResult(
            result=AssertionEvaluationResult(
                AssertionResultType.FAILURE, parameters=None
            )
        )

    def _evaluate_internal_cron(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> InternalAssertionEvaluationResult:
        assert assertion.freshness_assertion is not None
        assert assertion.freshness_assertion.schedule.cron is not None

        # These fields are required for
        cast(FreshnessAssertion, assertion.freshness_assertion)
        cron_schedule = cast(
            FreshnessCronSchedule, assertion.freshness_assertion.schedule.cron
        )
        cron_schedule_start_offset = cron_schedule.window_start_offset_ms

        # Get the last executed time of the assertion, to compute the bounds of the cron window.
        basic_cron_schedule = CronSchedule(
            cron=cron_schedule.cron, timezone=cron_schedule.timezone
        )
        next_cron_schedule_time = get_next_cron_schedule_time(basic_cron_schedule)
        prev_cron_schedule_time = get_prev_cron_schedule_time(basic_cron_schedule)

        # If a window start offset was explicitly provided, use that to generate the start time boundary.
        # If none was specified, simply use the previous cron schedule evaluation time as the window start time boundary.
        start_time_ms = (
            next_cron_schedule_time - cron_schedule_start_offset
            if cron_schedule_start_offset is not None
            else prev_cron_schedule_time
        )
        end_time_ms = next_cron_schedule_time

        validation_window = [start_time_ms, end_time_ms]

        # Now we have the window to validate on, so let's try to see if any events fall into the window!
        return self._evaluate_internal_window_event(
            validation_window, assertion, parameters, context
        )

    def _evaluate_internal_fixed_interval(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> InternalAssertionEvaluationResult:
        assert assertion.freshness_assertion is not None
        assert assertion.freshness_assertion.schedule.fixed_interval is not None

        freshness_assertion = cast(FreshnessAssertion, assertion.freshness_assertion)
        fixed_interval_schedule = cast(
            FixedIntervalSchedule, freshness_assertion.schedule.fixed_interval
        )

        end_time = int(time.time() * 1000)
        start_time = get_fixed_interval_start(end_time, fixed_interval_schedule)

        validation_window = [start_time, end_time]

        logger.info(f"Evaluating assertion against window {start_time} {end_time}")

        # Now we have the window to validate on, so let's try to see if any events fall into the window!
        return self._evaluate_internal_window_event(
            validation_window, assertion, parameters, context
        )

    def _evaluate_internal_since_last_check(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> InternalAssertionEvaluationResult:
        assert assertion.freshness_assertion is not None

        # 1. Get previous monitor state
        previous_state = (
            self.state_provider.get_state(
                context.monitor_urn, AssertionStateType.MONITOR_TIMESERIES_STATE
            )
            if context.monitor_urn
            else None
        )

        # 2. Initialize variables
        # 2.1 default result to an INIT run event (ie. if no previous_state)
        assertion_evaluation_result = InternalAssertionEvaluationResult(
            result=AssertionEvaluationResult(AssertionResultType.INIT)
        )
        # 2.2 get the current ts in millis
        now_millis = int(time.time() * 1000)

        # 3. If there's a valid previous state, evaluate freshness from last monitor run time to now
        if previous_state and previous_state.timestamp is not None:
            # check for freshness within window
            start_time = previous_state.timestamp
            end_time = now_millis
            validation_window = [start_time, end_time]
            assertion_evaluation_result = self._evaluate_internal_window_event(
                validation_window, assertion, parameters, context, previous_state
            )

        # 4. Ensure we store the timestamp of this run in the monitorTimeseriesState aspect
        if assertion_evaluation_result.next_assertion_state is None:
            assertion_evaluation_result.next_assertion_state = AssertionState(
                type=AssertionStateType.MONITOR_TIMESERIES_STATE,
                timestamp=now_millis,
                properties={},
            )

        # 5. Return the result
        return assertion_evaluation_result

    def _evaluate_internal(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        # Here's how we evaluate an Assertion.
        # 1. Fetch the "dataset FRESHNESS assertion config"
        # 2. Based on the type, we take different paths.
        assert assertion.freshness_assertion is not None

        freshness_assertion = cast(FreshnessAssertion, assertion.freshness_assertion)
        if freshness_assertion.schedule.type == FreshnessAssertionScheduleType.CRON:
            internal_result = self._evaluate_internal_cron(
                assertion, parameters, context
            )
        elif (
            freshness_assertion.schedule.type
            == FreshnessAssertionScheduleType.FIXED_INTERVAL
        ):
            internal_result = self._evaluate_internal_fixed_interval(
                assertion, parameters, context
            )
        elif (
            freshness_assertion.schedule.type
            == FreshnessAssertionScheduleType.SINCE_THE_LAST_CHECK
        ):
            internal_result = self._evaluate_internal_since_last_check(
                assertion, parameters, context
            )
        else:
            raise InvalidParametersException(
                message=f"Failed to evaluate FRESHNESS Assertion. Unsupported FRESHNESS Schedule Type {assertion.freshness_assertion.schedule.type} provided.",
                parameters=parameters.__dict__,
            )
        if internal_result.next_assertion_state and context.monitor_urn:
            self.state_provider.save_state(
                context.monitor_urn, internal_result.next_assertion_state
            )
        return internal_result.result
