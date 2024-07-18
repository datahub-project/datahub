import datetime
import logging

from datahub_executor.common.graph import DataHubExecutorGraph
from datahub_executor.config import DATAHUB_EXECUTOR_SWEEPER_ABORT_THRESHOLD

logger = logging.getLogger(__name__)


class SweeperJob:
    """Class for cleaning up stale and duplicate execution requests."""

    def __init__(self) -> None:
        """
        Initialize the sweeper
        """
        self.graph = DataHubExecutorGraph()

    def run(self) -> None:
        logger.info("Sweeper: running stale job cleanup")

        try:
            running_ingestions = self.graph.get_running_ingestions()
            if len(running_ingestions) == 0:
                logger.info("Sweeper: no running ingestions found.")

            for ingestion in running_ingestions:
                time_diff = (
                    datetime.datetime.now(tz=datetime.timezone.utc)
                    - ingestion.last_observed
                ).seconds
                if time_diff > DATAHUB_EXECUTOR_SWEEPER_ABORT_THRESHOLD:
                    logger.info(
                        f"Sweeper: ingestion {ingestion.execution_request_id} is stale for {time_diff} seconds, going to abort."
                    )

                    # Mark the job as ABORTED + submit KILL signal. This is necessary, because it is possible that
                    # the worker is still alive, and keeps processing the job but it's not visible to the coordinator,
                    # because of e.g. an interim network issue or kafka topic lag. Cancellation request ensures that
                    # job either no longer exists or get killed as soon as we mark the job as ABORTED.

                    self.graph.abort_execution_request(
                        ingestion.execution_request_id,
                        "Ingestion was aborted due to worker pod eviction, crash, or restart.",
                    )
                    self.graph.cancel_ingestion_execution(
                        ingestion.ingestion_source_urn, ingestion.execution_request_urn
                    )
        except Exception as e:
            logger.error(f"Sweeper: error while cancelling stale jobs: {e}")
