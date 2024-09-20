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
            ingestions_processed = 0

            for ingestion in running_ingestions:
                if ingestion.last_observed == 0:
                    logger.warning(
                        f"Sweeper: unable to process ingestion {ingestion.execution_request_id}, last_observed timestamp is 0."
                    )
                    continue

                time_diff = (
                    datetime.datetime.now(tz=datetime.timezone.utc)
                    - datetime.datetime.fromtimestamp(
                        ingestion.last_observed / 1000, tz=datetime.timezone.utc
                    )
                ).total_seconds()

                if time_diff > DATAHUB_EXECUTOR_SWEEPER_ABORT_THRESHOLD:
                    ingestions_processed = ingestions_processed + 1
                    logger.info(
                        f"Sweeper: ingestion {ingestion.execution_request_id} is stale for {time_diff} seconds, going to abort."
                    )

                    # Mark the job as ABORTED + submit KILL signal. This is necessary, because it is possible that
                    # the worker is still alive, and keeps processing the job but it's not visible to the coordinator,
                    # because of e.g. an interim network issue or kafka topic lag. Cancellation request ensures that
                    # job either no longer exists or get killed as soon as we mark the job as ABORTED.

                    try:
                        current_time = datetime.datetime.now()
                        self.graph.abort_execution_request(
                            ingestion.execution_request_id,
                            f"{current_time} Ingestion was aborted due to worker pod eviction, crash, or restart.\n\n---\n{ingestion.report}",
                            ingestion.start_time,
                        )
                        self.graph.cancel_ingestion_execution(
                            ingestion.ingestion_source_urn,
                            ingestion.execution_request_urn,
                        )
                    except Exception as e:
                        logger.error(f"Sweeper: error while cancelling stale jobs: {e}")

            logger.info(
                f"Sweeper: done processing ingestions. Total ingestions processed: {ingestions_processed}."
            )
        except Exception as e:
            logger.error(f"Sweeper: error while processing stale jobs: {e}")
