import logging
import time
from typing import Optional, cast

from datahub.metadata.schema_classes import DatasetProfileClass

from datahub_executor.common.assertion.engine.evaluator.evaluator import (
    AssertionEvaluator,
)
from datahub_executor.common.assertion.engine.evaluator.utils import (
    get_database_parameters,
)
from datahub_executor.common.assertion.engine.evaluator.utils.volume import (
    get_filter_parameters,
)
from datahub_executor.common.assertion.types import AssertionState, AssertionStateType
from datahub_executor.common.connection.datahub_ingestion_source_connection_provider import (
    DataHubIngestionSourceConnectionProvider,
)
from datahub_executor.common.exceptions import (
    InsufficientDataException,
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
    DatasetVolumeAssertionParameters,
    DatasetVolumeSourceType,
    VolumeAssertion,
    VolumeAssertionType,
)

logger = logging.getLogger(__name__)


class VolumeAssertionEvaluator(AssertionEvaluator):
    """Evaluator for VOLUME assertions."""

    @property
    def type(self) -> AssertionType:
        return AssertionType.VOLUME

    @property
    def default_parameters(self) -> AssertionEvaluationParameters:
        return AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_VOLUME,
            dataset_volume_parameters=DatasetVolumeAssertionParameters(
                sourceType=DatasetVolumeSourceType.INFORMATION_SCHEMA
            ),
        )

    def _evaluate_row_count_total_assertion(
        self,
        entity_urn: str,
        volume_assertion: VolumeAssertion,
        row_count: int,
    ) -> AssertionEvaluationResult:
        assert volume_assertion.row_count_total is not None
        row_count_evaluation = self._evaluate_value(
            row_count,
            volume_assertion.row_count_total.operator,
            volume_assertion.row_count_total.parameters,
        )
        if row_count_evaluation is True:
            return AssertionEvaluationResult(
                AssertionResultType.SUCCESS, parameters={"row_count": row_count}
            )
        else:
            return AssertionEvaluationResult(
                AssertionResultType.FAILURE, parameters={"row_count": row_count}
            )

    def _evaluate_row_count_change_assertion(
        self,
        entity_urn: str,
        volume_assertion: VolumeAssertion,
        row_count: int,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        assert volume_assertion.row_count_change is not None

        previous_state = (
            self.state_provider.get_state(
                context.monitor_urn, AssertionStateType.MONITOR_TIMESERIES_STATE
            )
            if context.monitor_urn
            else None
        )

        if previous_state:
            prev_row_count = previous_state.properties.get("row_count", None)
            if prev_row_count is None:
                assertion_evaluation_result = AssertionEvaluationResult(
                    AssertionResultType.INIT, parameters={"row_count": row_count}
                )
            else:
                row_count_evaluation = self._evaluate_value_change(
                    volume_assertion.row_count_change.type,
                    int(prev_row_count),
                    row_count,
                    volume_assertion.row_count_change.operator,
                    volume_assertion.row_count_change.parameters,
                )
                if row_count_evaluation is True:
                    assertion_evaluation_result = AssertionEvaluationResult(
                        AssertionResultType.SUCCESS,
                        parameters={
                            "row_count": row_count,
                            "prev_row_count": prev_row_count,
                        },
                    )
                else:
                    assertion_evaluation_result = AssertionEvaluationResult(
                        AssertionResultType.FAILURE,
                        parameters={
                            "row_count": row_count,
                            "prev_row_count": prev_row_count,
                        },
                    )
        else:
            assertion_evaluation_result = AssertionEvaluationResult(
                AssertionResultType.INIT, parameters={"row_count": row_count}
            )

        if context.monitor_urn:
            self.state_provider.save_state(
                context.monitor_urn,
                AssertionState(
                    type=AssertionStateType.MONITOR_TIMESERIES_STATE,
                    timestamp=int(time.time() * 1000),
                    properties={
                        "row_count": str(row_count),
                    },
                ),
            )

        return assertion_evaluation_result

    def _evaluate_datahub_dataset_profile_assertion(
        self,
        entity_urn: str,
        volume_assertion: VolumeAssertion,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        assert isinstance(
            self.connection_provider, DataHubIngestionSourceConnectionProvider
        )
        dataset_profile: Optional[DatasetProfileClass] = (
            self.connection_provider.graph.get_latest_timeseries_value(
                entity_urn=entity_urn,
                aspect_type=DatasetProfileClass,
                filter_criteria_map={},
            )
        )

        if dataset_profile is None or dataset_profile.rowCount is None:
            raise InsufficientDataException(
                message=f"Unable to find latest dataset profile for {entity_urn}"
            )

        row_count = dataset_profile.rowCount
        if volume_assertion.type == VolumeAssertionType.ROW_COUNT_TOTAL:
            return self._evaluate_row_count_total_assertion(
                entity_urn,
                volume_assertion,
                row_count,
            )
        elif volume_assertion.type == VolumeAssertionType.ROW_COUNT_CHANGE:
            return self._evaluate_row_count_change_assertion(
                entity_urn, volume_assertion, row_count, context
            )
        raise InvalidParametersException(
            message=f"Unsupported Volume Assertion Type {volume_assertion.type} provided.",
            parameters=volume_assertion.__dict__,
        )

    def _evaluate_internal(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        assert assertion.volume_assertion is not None
        assert parameters.dataset_volume_parameters is not None
        assert assertion.connection_urn

        entity_urn = assertion.entity.urn
        volume_assertion = assertion.volume_assertion
        dataset_volume_parameters = parameters.dataset_volume_parameters
        filter_params = get_filter_parameters(assertion)

        if (
            dataset_volume_parameters.source_type
            == DatasetVolumeSourceType.DATAHUB_DATASET_PROFILE
        ):
            return self._evaluate_datahub_dataset_profile_assertion(
                entity_urn, volume_assertion, context
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
        row_count = source.get_row_count(
            entity_urn, database_params, dataset_volume_parameters, filter_params
        )

        if volume_assertion.type == VolumeAssertionType.ROW_COUNT_TOTAL:
            return self._evaluate_row_count_total_assertion(
                entity_urn,
                volume_assertion,
                row_count,
            )
        elif volume_assertion.type == VolumeAssertionType.ROW_COUNT_CHANGE:
            return self._evaluate_row_count_change_assertion(
                entity_urn,
                volume_assertion,
                row_count,
                context,
            )
        else:
            raise InvalidParametersException(
                message=f"Failed to evaluate VOLUME Assertion. Unsupported VOLUME Assertion Type {assertion.volume_assertion.type} provided.",
                parameters=volume_assertion.__dict__,
            )
