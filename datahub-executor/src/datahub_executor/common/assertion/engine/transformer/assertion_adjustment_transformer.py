import logging
from typing import Tuple

from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.aspect_builder import (
    get_assertion_info,
    get_assertion_std_parameters,
    to_raw_aspect,
)
from datahub_executor.common.assertion.engine.transformer.assertion_adjustment.algorithm import (
    AdjustmentAlgorithm,
    IQRAdjustmentAlgorithm,
    StdDevAdjustmentAlgorithm,
)
from datahub_executor.common.assertion.engine.transformer.transformer import (
    AssertionTransformer,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
)
from datahub_executor.config import ONLINE_SMART_ASSERTIONS_ENABLED

logger = logging.getLogger(__name__)


# This is a legacy transformer which would apply buffer to adjust the range boundaries
# on inferred volume and inferred freshness assertions.
# As of Smart Assertions V2, it is no longer used.
#
# There is no longer support for adjusting the settings using an AssertionAdjustmentSettings in here,
# and it was never used in the first place.
#
# This should be removed once Smart Assertions V2 are the "default" smart assertions used.
class AssertionAdjustmentTransformer(AssertionTransformer):
    """
    This transformer is only applicable for INFERRED Assertions that are of type
    1. VOLUME Assertion with ROW_COUNT_TOTAL type
    2. FIELD Assertion with FIELD_METRIC type
    """

    @classmethod
    def create(cls, graph: DataHubGraph) -> "AssertionTransformer":
        return AssertionAdjustmentTransformer(graph)

    def __init__(self, graph: DataHubGraph) -> None:
        self.graph = graph

    def transform(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> Tuple[Assertion, AssertionEvaluationParameters, AssertionEvaluationContext]:
        if (
            not ONLINE_SMART_ASSERTIONS_ENABLED
            and assertion.is_inferred
            and (
                assertion.is_volume_row_count_total_assertion
                or assertion.is_field_metric_assertion
            )
        ):
            logger.debug("Inside Assertion Adjustment Transformer")
            try:
                context.base_assertion_info = assertion.raw_info_aspect

                algorithm = get_adjustment_algorithm_from_settings(context)
                new_assertion = algorithm.adjust_assertion_info(assertion)
                self._update_raw_info_aspect_parameters(new_assertion)
                return new_assertion, parameters, context
            except Exception as e:
                logger.error(
                    f"Unable to adjust smart assertion due to error {e},"
                    "will continue to execute existing assertion without adjustments."
                )

        return assertion, parameters, context

    def _update_raw_info_aspect_parameters(self, assertion: Assertion) -> None:
        """
        This keeps `assertion.raw_info_aspect` in sync with parameter adjustments

        Ideally, we can write a complete mapper from "Assertion" type here to "AssertionInfo" aspect
        that covers all existing assertion types (freshness, volume, custom, field, etc)
        in aspect_builder.py and this will not be needed, however at this point, that seems like more
        maintenance-heavy as compared to this workaround. In future, we may revisit this.
        """
        # First, read existing aspect class from raw_aspect_info
        info_aspect = get_assertion_info(assertion.raw_info_aspect)

        # Then, update aspect parameters in aspect class
        if (
            info_aspect
            and info_aspect.volumeAssertion
            and info_aspect.volumeAssertion.rowCountTotal
            and assertion.volume_assertion
            and assertion.volume_assertion.row_count_total
        ):
            info_aspect.volumeAssertion.rowCountTotal.parameters = (
                get_assertion_std_parameters(
                    assertion.volume_assertion.row_count_total.parameters
                )
            )
        elif (
            info_aspect
            and info_aspect.fieldAssertion
            and info_aspect.fieldAssertion.fieldMetricAssertion
            and assertion.field_assertion
            and assertion.field_assertion.field_metric_assertion
        ):
            info_aspect.fieldAssertion.fieldMetricAssertion.parameters = (
                get_assertion_std_parameters(
                    assertion.field_assertion.field_metric_assertion.parameters
                )
            )

        # Finally, write the aspect back as raw_aspect_info
        if info_aspect:
            assertion.raw_info_aspect = to_raw_aspect(info_aspect)


def get_adjustment_algorithm_from_settings(
    context: AssertionEvaluationContext,
) -> AdjustmentAlgorithm:
    """
    Adjustment Algorithm Registry
    """
    if (
        context.evaluation_spec is not None
        and context.evaluation_spec.context is not None
        and context.evaluation_spec.context.std_dev is not None
    ):
        # In this case, use the new Std Deviation adjustment algo by default!
        # TODO: In the future, we can allow users to configure this themselves using the adjustment_settings flow.
        std_dev = context.evaluation_spec.context.std_dev
        return StdDevAdjustmentAlgorithm(std_dev=std_dev)
    else:
        # Else fall back to legacy IQR.
        return IQRAdjustmentAlgorithm()
