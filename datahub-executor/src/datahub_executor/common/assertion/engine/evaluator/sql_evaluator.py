import logging
import re
import time
from typing import cast

from datahub_executor.common.assertion.engine.evaluator.evaluator import (
    AssertionEvaluator,
)
from datahub_executor.common.assertion.engine.evaluator.utils import (
    get_database_parameters,
)
from datahub_executor.common.assertion.types import AssertionState, AssertionStateType
from datahub_executor.common.exceptions import (
    CustomSQLErrorException,
    InvalidParametersException,
    SourceConnectionErrorException,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionEvaluationResult,
    AssertionResultType,
    AssertionType,
    SQLAssertion,
    SQLAssertionType,
)

logger = logging.getLogger(__name__)


class SQLAssertionEvaluator(AssertionEvaluator):
    """Evaluator for SQL assertions."""

    @property
    def type(self) -> AssertionType:
        return AssertionType.SQL

    @property
    def default_parameters(self) -> AssertionEvaluationParameters:
        return AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_SQL
        )

    def _evaluate_metric_assertion(
        self, sql_assertion: SQLAssertion, metric_value: float
    ) -> AssertionEvaluationResult:
        metric_evaluation = self._evaluate_value(
            metric_value, sql_assertion.operator, sql_assertion.parameters
        )
        if metric_evaluation is True:
            return AssertionEvaluationResult(
                AssertionResultType.SUCCESS,
                parameters={"metric_value": str(metric_value)},
            )
        else:
            return AssertionEvaluationResult(
                AssertionResultType.FAILURE,
                parameters={"metric_value": str(metric_value)},
            )

    def _evaluate_metric_change_assertion_with_previous_state(
        self,
        sql_assertion: SQLAssertion,
        previous_state: AssertionState,
        metric_value: float,
    ) -> AssertionEvaluationResult:
        assert sql_assertion.change_type is not None

        prev_metric_value = previous_state.properties.get("metric_value", None)
        if prev_metric_value is None:
            return AssertionEvaluationResult(
                AssertionResultType.INIT,
                parameters={"metric_value": str(metric_value)},
            )

        metric_evaluation = self._evaluate_value_change(
            sql_assertion.change_type,
            float(prev_metric_value),
            metric_value,
            sql_assertion.operator,
            sql_assertion.parameters,
        )
        if metric_evaluation is True:
            return AssertionEvaluationResult(
                AssertionResultType.SUCCESS,
                parameters={
                    "metric_value": str(metric_value),
                    "prev_metric_value": prev_metric_value,
                },
            )

        return AssertionEvaluationResult(
            AssertionResultType.FAILURE,
            parameters={
                "metric_value": str(metric_value),
                "prev_metric_value": prev_metric_value,
            },
        )

    def _evaluate_metric_change_assertion(
        self,
        entity_urn: str,
        sql_assertion: SQLAssertion,
        metric_value: float,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        previous_state = (
            self.state_provider.get_state(
                context.monitor_urn, AssertionStateType.MONITOR_TIMESERIES_STATE
            )
            if context.monitor_urn
            else None
        )

        if previous_state:
            assertion_evaluation_result = (
                self._evaluate_metric_change_assertion_with_previous_state(
                    sql_assertion,
                    previous_state,
                    metric_value,
                )
            )
        else:
            assertion_evaluation_result = AssertionEvaluationResult(
                AssertionResultType.INIT, parameters={"metric_value": str(metric_value)}
            )

        if context.monitor_urn:
            self.state_provider.save_state(
                context.monitor_urn,
                AssertionState(
                    type=AssertionStateType.MONITOR_TIMESERIES_STATE,
                    timestamp=int(time.time() * 1000),
                    properties={
                        "metric_value": str(metric_value),
                    },
                ),
            )

        return assertion_evaluation_result

    def _validate_custom_sql(
        self,
        sql_statement: str,
    ) -> None:
        INVALID_STATEMENTS = [
            r"INSERT INTO",
            r"UPDATE .*? SET",
            r"DELETE FROM",
            r"CREATE TABLE",
            r"ALTER TABLE",
            r"DROP TABLE",
            r"CREATE DATABASE",
            r"DROP DATABASE",
        ]
        if any(
            re.search(invalid_statement, sql_statement, re.IGNORECASE)
            for invalid_statement in INVALID_STATEMENTS
        ):
            raise CustomSQLErrorException(
                message="Custom SQL cannot alter tables or databases"
            )

    def _evaluate_internal(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        assert assertion.sql_assertion is not None
        assert assertion.connection_urn
        connection = self.connection_provider.get_connection(
            cast(str, assertion.entity.urn)
        )

        if connection is None:
            raise SourceConnectionErrorException(
                message=f"Unable to retrieve valid connection for Data Platform with urn {assertion.connection_urn}",
                connection_urn=assertion.connection_urn,
            )

        entity_urn = assertion.entity.urn
        sql_assertion = assertion.sql_assertion

        source = self.source_provider.create_source_from_connection(connection)
        database_params = get_database_parameters(assertion)

        self._validate_custom_sql(sql_assertion.statement)
        metric_value = source.execute_custom_sql(
            entity_urn,
            database_params,
            sql_assertion.statement,
        )

        if sql_assertion.type == SQLAssertionType.METRIC:
            return self._evaluate_metric_assertion(
                sql_assertion,
                metric_value,
            )
        elif sql_assertion.type == SQLAssertionType.METRIC_CHANGE:
            return self._evaluate_metric_change_assertion(
                entity_urn,
                sql_assertion,
                metric_value,
                context,
            )
        else:
            raise InvalidParametersException(
                message=f"Failed to evaluate SQL Assertion. Unsupported SQL Assertion Type {assertion.sql_assertion.type} provided.",
                parameters=sql_assertion.__dict__,
            )
