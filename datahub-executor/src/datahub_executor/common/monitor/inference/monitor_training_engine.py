import logging
from typing import Dict, Optional

from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
    is_training_required,
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
from datahub_executor.common.monitor.inference.volume_assertion_trainer import (
    VolumeAssertionTrainer,
)
from datahub_executor.common.types import AssertionType, Monitor

logger = logging.getLogger(__name__)


class MonitorTrainingEngine:
    """
    Engine responsible for training monitors by delegating to specialized trainers.
    Uses composition to delegate to the appropriate trainer based on assertion type.
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

        # Initialize trainers
        self._trainers = self._initialize_trainers()

    def _initialize_trainers(self) -> Dict[AssertionType, BaseAssertionTrainer]:
        """
        Initialize all assertion trainers.
        """
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
        }

    def train(self, monitor: Monitor) -> None:
        """
        Train a monitor's assertions by delegating to the appropriate trainers.
        """
        logger.info(f"Starting training run for monitor {monitor.urn}")

        # Validate monitor has assertions
        if not monitor.assertion_monitor or not len(
            monitor.assertion_monitor.assertions
        ):
            logger.info("Training Monitor is missing assertions! Skipping training...")
            return None

        # Iterate over all of the assertions in the monitor
        for assertion_evaluation_spec in monitor.assertion_monitor.assertions:
            assertion = assertion_evaluation_spec.assertion

            # Check if assertion is set for inference
            if not is_training_required(assertion):
                logger.info(
                    f"Skipping training for assertion {assertion.urn}, not marked for inference."
                )
                continue

            # Check if we have a trainer for this assertion type
            if assertion.type not in self._trainers:
                logger.info(
                    f"Training is not supported for assertion of type {assertion.type}"
                )
                continue

            # Delegate to the appropriate trainer
            try:
                trainer = self._trainers[assertion.type]
                trainer.train(monitor, assertion, assertion_evaluation_spec)
            except Exception as e:
                logger.exception(
                    f"Error training assertion {assertion.urn} of type {assertion.type}: {e}"
                )

        logger.info(f"Completed training run for monitor {monitor.urn}!")

    def get_trainer(
        self, assertion_type: AssertionType
    ) -> Optional[BaseAssertionTrainer]:
        """
        Get a trainer for the given assertion type.
        """
        return self._trainers.get(assertion_type)
