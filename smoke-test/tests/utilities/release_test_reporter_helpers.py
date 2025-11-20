"""
Helper functions for integrating release test reporter with pytest test execution.

This module provides high-level functions for test tracking and metadata emission.
"""

import logging
import time
from typing import Optional

from datahub.api.entities.dataprocess.dataprocess_instance import DataProcessInstance
from tests.utilities import env_vars, release_test_reporter

logger = logging.getLogger(__name__)


def initialize_test_tracking(
    test_identifier: Optional[str] = None,
) -> tuple[Optional[str], Optional[str], Optional[DataProcessInstance], Optional[int]]:
    """
    Initialize test tracking by emitting pipeline/task URNs and starting process instance.

    Args:
        test_identifier: Optional test identifier (defaults to env var)

    Returns:
        Tuple of (pipeline_urn, task_urn, process_instance, start_timestamp_millis)
        Returns (None, None, None, None) if reporting fails
    """
    try:
        # Determine task name based on test identifier or frontend URL
        task_name = test_identifier or env_vars.get_test_identifier() or "release-tests"
        task_name = task_name.lower().replace(" ", "-")

        logger.info(f"📊 Initializing release test tracking for: {task_name}")

        # Emit pipeline and task URNs
        pipeline_urn, task_urn = release_test_reporter.emit_pipeline_and_task_urns(
            task_name
        )

        # Emit process start event
        if task_urn:
            result = release_test_reporter.emit_process_start(task_urn)
            if result is not None:
                process_instance, start_timestamp_millis = result
                logger.info(
                    f"Emitted process start to reporting DataHub: "
                    f"task_urn={task_urn}, instance_urn={process_instance.urn}"
                )
                return (
                    pipeline_urn,
                    task_urn,
                    process_instance,
                    start_timestamp_millis,
                )

        return None, None, None, None

    except Exception as e:
        logger.warning(f"Failed to initialize test tracking: {e}")
        return None, None, None, None


def finalize_test_tracking(
    task_urn: Optional[str],
    pipeline_urn: Optional[str],
    process_instance: Optional[DataProcessInstance],
    start_timestamp_millis: Optional[int],
    failed_count: int,
    passed_count: int,
    skipped_count: int,
    failed_tests: set,
    message: str,
) -> None:
    """
    Finalize test tracking by emitting process end and raising incidents on failure.

    Args:
        task_urn: URN of the DataJob
        pipeline_urn: URN of the DataFlow
        process_instance: DataProcessInstance from process start
        start_timestamp_millis: Start timestamp in milliseconds
        failed_count: Number of failed tests
        passed_count: Number of passed tests
        skipped_count: Number of skipped tests
        failed_tests: Set of tuples (nodeid, description) for failed tests
        message: Message to include in the incident
    """
    try:
        if not task_urn or not process_instance:
            logger.info(
                "⏭️  Skipping test tracking finalization - tracking not initialized"
            )
            return

        logger.info(
            f"🏁 Finalizing release test tracking: "
            f"passed={passed_count}, failed={failed_count}, skipped={skipped_count}"
        )

        # Calculate duration
        duration_millis = None
        if start_timestamp_millis:
            duration_millis = int(time.time() * 1000) - start_timestamp_millis

        # Determine success based on test failures
        success = failed_count == 0

        # Emit process end
        release_test_reporter.emit_process_end(
            process_instance=process_instance,
            success=success,
            duration_millis=duration_millis,
            start_timestamp_millis=start_timestamp_millis,
        )
        logger.info(
            f"Emitted process end to reporting DataHub: success={success}, "
            f"duration={duration_millis}ms"
        )

        # Raise incidents on failure
        if not success:
            incident_message = (
                f"Release tests failed on {env_vars.get_frontend_url()}\n\n"
                f"{message}\n\n"
            )
            incident_title = f"Release test failures on {env_vars.get_frontend_url()}"

            # Raise incident on task
            release_test_reporter.raise_incident_on_task(
                task_urn=task_urn,
                message=incident_message,
                title=incident_title,
            )

            logger.info("✅ Raised incidents on task")
        else:
            logger.info("✅ Test tracking finalized successfully - all tests passed!")

    except Exception as e:
        logger.warning(f"❌ Failed to finalize test tracking or raise incidents: {e}")
