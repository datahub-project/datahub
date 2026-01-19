import logging
from typing import TYPE_CHECKING, Dict, Optional, Union

from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
    is_smart_assertion,
)
from datahub_executor.common.metric.client.client import MetricClient
from datahub_executor.common.monitor.client.client import MonitorClient
from datahub_executor.common.monitor.inference.base_assertion_trainer import (
    BaseAssertionTrainer,
)
from datahub_executor.common.monitor.inference.field_assertion_trainer import (
    FieldAssertionTrainer,
)
from datahub_executor.common.monitor.inference.freshness_assertion_trainer import (
    FreshnessAssertionTrainer,
)
from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricPredictor,
)
from datahub_executor.common.monitor.inference.sql_assertion_trainer import (
    SqlAssertionTrainer,
)
from datahub_executor.common.monitor.inference.volume_assertion_trainer import (
    VolumeAssertionTrainer,
)
from datahub_executor.common.monitor.inference_v2.config import USE_OBSERVE_MODELS
from datahub_executor.common.types import AssertionType, Monitor

# Type hints for V2 trainers (optional dependency on datahub_observe)
if TYPE_CHECKING:
    from datahub_executor.common.monitor.inference_v2.base_trainer_v2 import (
        BaseTrainerV2,
    )

# Conditionally import V2 trainers only when observe-models is available.
# This prevents import errors in slim builds without datahub_observe.
if USE_OBSERVE_MODELS:
    from datahub_executor.common.monitor.inference_v2.base_trainer_v2 import (
        BaseTrainerV2,
    )
    from datahub_executor.common.monitor.inference_v2.field_trainer_v2 import (
        FieldTrainerV2,
    )
    from datahub_executor.common.monitor.inference_v2.sql_trainer_v2 import (
        SqlTrainerV2,
    )
    from datahub_executor.common.monitor.inference_v2.volume_trainer_v2 import (
        VolumeTrainerV2,
    )

logger = logging.getLogger(__name__)


class MonitorTrainingEngine:
    """
    Engine responsible for training monitors by delegating to specialized trainers.
    Uses composition to delegate to the appropriate trainer based on assertion type.

    When DATAHUB_USE_OBSERVE_MODELS=true, metric-based assertions (VOLUME, FIELD, SQL)
    are routed to V2 trainers using observe-models. FRESHNESS assertions use V1 as a
    temporary fallback until V2 freshness support is implemented.
    """

    def __init__(
        self,
        graph: DataHubGraph,
        metrics_client: MetricClient,
        metrics_predictor: MetricPredictor,
        monitor_client: MonitorClient,
    ) -> None:
        self.graph = graph
        self.metrics_client = metrics_client
        self.metrics_predictor = metrics_predictor
        self.monitor_client = monitor_client

        # Initialize trainers based on feature flag
        if USE_OBSERVE_MODELS:
            self._trainers = self._initialize_v2_trainers()
        else:
            self._trainers = self._initialize_v1_trainers()

    def _initialize_v1_trainers(
        self,
    ) -> Dict[AssertionType, Union[BaseAssertionTrainer, "BaseTrainerV2"]]:
        """Initialize V1 assertion trainers."""
        return {
            AssertionType.VOLUME: VolumeAssertionTrainer(
                self.graph,
                self.metrics_client,
                self.metrics_predictor,
                self.monitor_client,
            ),
            AssertionType.FRESHNESS: FreshnessAssertionTrainer(
                self.graph,
                self.metrics_client,
                self.metrics_predictor,
                self.monitor_client,
            ),
            AssertionType.FIELD: FieldAssertionTrainer(
                self.graph,
                self.metrics_client,
                self.metrics_predictor,
                self.monitor_client,
            ),
            AssertionType.SQL: SqlAssertionTrainer(
                self.graph,
                self.metrics_client,
                self.metrics_predictor,
                self.monitor_client,
            ),
        }

    def _initialize_v2_trainers(
        self,
    ) -> Dict[AssertionType, Union[BaseAssertionTrainer, "BaseTrainerV2"]]:
        """Initialize V2 trainers for supported types, V1 for others."""
        return {
            # V2 trainers for metric-based assertions
            AssertionType.VOLUME: VolumeTrainerV2(
                self.graph, self.metrics_client, self.monitor_client
            ),
            AssertionType.FIELD: FieldTrainerV2(
                self.graph, self.metrics_client, self.monitor_client
            ),
            AssertionType.SQL: SqlTrainerV2(
                self.graph, self.metrics_client, self.monitor_client
            ),
            # V1 trainer for freshness (not supported by V2)
            AssertionType.FRESHNESS: FreshnessAssertionTrainer(
                self.graph,
                self.metrics_client,
                self.metrics_predictor,
                self.monitor_client,
            ),
        }

    def train(self, monitor: Monitor) -> None:
        """Train a monitor's assertions by delegating to the appropriate trainers."""
        logger.debug(f"Starting training run for monitor {monitor.urn}")

        if not monitor.assertion_monitor or not monitor.assertion_monitor.assertions:
            logger.warning(
                f"Monitor {monitor.urn} has no assertions to train. "
                "This may indicate incomplete monitor configuration."
            )
            return

        for assertion_evaluation_spec in monitor.assertion_monitor.assertions:
            assertion = assertion_evaluation_spec.assertion

            if not is_smart_assertion(assertion):
                logger.debug(
                    f"Skipping training for assertion {assertion.urn}, not marked for inference."
                )
                continue

            try:
                self._train_assertion(monitor, assertion, assertion_evaluation_spec)
            except Exception as e:
                logger.exception(
                    f"Error training assertion {assertion.urn} of type {assertion.type}: {e}"
                )

        logger.info(f"Completed training run for monitor {monitor.urn}!")

    def _train_assertion(
        self,
        monitor: Monitor,
        assertion,
        assertion_evaluation_spec,
    ) -> None:
        """Train a single assertion using the appropriate trainer."""
        trainer = self._trainers.get(assertion.type)
        if trainer is None:
            raise ValueError(
                f"Training is not supported for assertion of type {assertion.type}"
            )

        trainer.train(monitor, assertion, assertion_evaluation_spec)

    def get_trainer(
        self, assertion_type: AssertionType
    ) -> Optional[Union[BaseAssertionTrainer, "BaseTrainerV2"]]:
        """Get a trainer for the given assertion type."""
        return self._trainers.get(assertion_type)
