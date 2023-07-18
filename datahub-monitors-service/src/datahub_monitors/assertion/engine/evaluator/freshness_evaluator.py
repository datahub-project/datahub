import logging
import time
from typing import List, Optional, cast

from datahub_monitors.assertion.engine.evaluator.error_utils import (
    extract_assertion_evaluation_result_error,
)
from datahub_monitors.assertion.engine.evaluator.evaluator import AssertionEvaluator
from datahub_monitors.assertion.engine.evaluator.freshness_utils import (
    get_event_type_parameters_from_parameters,
)
from datahub_monitors.assertion.engine.evaluator.time_utils import (
    get_fixed_interval_start,
    get_next_cron_schedule_time,
    get_prev_cron_schedule_time,
)
from datahub_monitors.connection.connection import Connection
from datahub_monitors.connection.provider import ConnectionProvider
from datahub_monitors.exceptions import (
    AssertionResultException,
    InvalidParametersException,
    SourceConnectionErrorException,
)
from datahub_monitors.source.provider import SourceProvider
from datahub_monitors.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionEvaluationResult,
    AssertionEvaluationResultError,
    AssertionResultErrorType,
    AssertionResultType,
    AssertionType,
    CronSchedule,
    DatasetFreshnessAssertionParameters,
    DatasetFreshnessSourceType,
    FixedIntervalSchedule,
    FreshnessAssertion,
    FreshnessAssertionScheduleType,
    FreshnessCronSchedule,
)

logger = logging.getLogger(__name__)


DEFAULT_FRESHNESS_PARAMETERS = AssertionEvaluationParameters(
    type=AssertionEvaluationParametersType.DATASET_FRESHNESS,
    dataset_freshness_parameters=DatasetFreshnessAssertionParameters(
        sourceType=DatasetFreshnessSourceType.INFORMATION_SCHEMA
    ),
)


class FreshnessAssertionEvaluator(AssertionEvaluator):
    """Evaluator for FRESHNESS assertions."""

    @property
    def type(self) -> AssertionType:
        return AssertionType.FRESHNESS

    def __init__(self, connection_provider: ConnectionProvider):
        self.connection_provider = connection_provider
        self.source_provider = SourceProvider()

    def _evaluate_internal_window_event(
        self,
        window: List[int],
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        connection: Connection,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        entity_urn = assertion.entity.urn

        # Check whether any matching events have fallen into the bucket
        event_type, source_params = get_event_type_parameters_from_parameters(
            parameters
        )
        # This is where we drop into system-specific bits --> Need a way to do this for Snowflake first.
        # TODO: Consider what it would take to batch queries. Maybe the client aggregates?
        source = self.source_provider.create_source_from_connection(connection)

        maybe_events = source.get_entity_events(
            entity_urn, event_type, window, source_params
        )

        # Now verify whether there are any events in the window
        if maybe_events is not None and len(maybe_events) > 0:
            # We have some events within the expected window. That means the assertion has passed! Make sure we establish WHY the assertion has passed.
            logger.error(
                "Found matching events within the provided window! Assertion is passing."
            )
            return AssertionEvaluationResult(
                AssertionResultType.SUCCESS, parameters={"events": maybe_events}
            )
        else:
            # No events are found. The assertion is failing!
            logger.error(
                "No matching events found within the provided window! Assertion is failing."
            )
            return AssertionEvaluationResult(
                AssertionResultType.FAILURE, parameters=None
            )

    def _evaluate_internal_cron(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        connection: Connection,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
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
            validation_window, assertion, parameters, connection, context
        )

    def _evaluate_internal_fixed_interval(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        connection: Connection,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
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
            validation_window, assertion, parameters, connection, context
        )

    def _evaluate_internal(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        connection: Connection,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        # Here's how we evaluate an Assertion.
        # 1. Fetch the "dataset FRESHNESS assertion config"
        # 2. Based on the type, we take different paths.
        assert assertion.freshness_assertion is not None

        freshness_assertion = cast(FreshnessAssertion, assertion.freshness_assertion)
        if freshness_assertion.schedule.type == FreshnessAssertionScheduleType.CRON:
            return self._evaluate_internal_cron(
                assertion, parameters, connection, context
            )
        elif (
            freshness_assertion.schedule.type
            == FreshnessAssertionScheduleType.FIXED_INTERVAL
        ):
            return self._evaluate_internal_fixed_interval(
                assertion, parameters, connection, context
            )
        else:
            raise InvalidParametersException(
                message=f"Failed to evaluate FRESHNESS Assertion. Unsupported FRESHNESS Schedule Type {assertion.freshness_assertion.schedule.type} provided.",
                parameters=parameters.__dict__,
            )

    def evaluate(
        self,
        assertion: Assertion,
        parameters: Optional[AssertionEvaluationParameters],
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        try:
            assert assertion.connection_urn
            connection = self.connection_provider.get_connection(
                cast(str, assertion.connection_urn)
            )

            if connection is None:
                raise SourceConnectionErrorException(
                    message=f"Unable to retrieve valid connection for Data Platform with urn {assertion.connection_urn}",
                    connection_urn=assertion.connection_urn,
                )

            return self._evaluate_internal(
                assertion,
                parameters if parameters is not None else DEFAULT_FRESHNESS_PARAMETERS,
                connection,
                context,
            )
        except AssertionResultException as e:
            error = extract_assertion_evaluation_result_error(e)
            result = AssertionEvaluationResult(
                AssertionResultType.ERROR,
                error=error,
            )
            logger.exception(
                f"Caught error of type {error.type} when attempting to evaluate assertion with urn {assertion.urn} and properties {error.properties}. Original message: {e}"
            )
            return result
        except Exception as e:
            logger.exception(
                f"An unknown error occurred when attempting to evaluate assertion with urn {assertion.urn} and parameters {parameters}. Could not produce an assertion evaluation result. Original message: {e}"
            )
            return AssertionEvaluationResult(
                AssertionResultType.ERROR,
                error=AssertionEvaluationResultError(
                    type=AssertionResultErrorType.UNKNOWN_ERROR,
                    properties={
                        "assertion_urn": assertion.urn,
                        "parameters": parameters,
                    },
                ),
            )
