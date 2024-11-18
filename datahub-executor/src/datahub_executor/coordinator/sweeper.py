import datetime
import logging
import time
from typing import Dict, List, Optional

from datahub.metadata.schema_classes import (
    ExecutionRequestInputClass,
    ExecutionRequestSourceClass,
)

from datahub_executor.common.constants import (
    DATAHUB_EXECUTION_REQUEST_STATUS_PENDING,
    DATAHUB_EXECUTION_REQUEST_STATUS_RUNNING,
)
from datahub_executor.common.graph import DataHubExecutorGraph
from datahub_executor.common.ingestion.helpers import emit_execution_request_input
from datahub_executor.common.monitoring.metrics import (
    STATS_SWEEPER_ACTIONS_EXECUTED,
    STATS_SWEEPER_ACTIONS_PLANNED,
    STATS_SWEEPER_EXECUTION_REQUESTS_COUNT,
)
from datahub_executor.common.types import ExecutionRequestStatus, SweeperAction
from datahub_executor.config import (
    DATAHUB_EXECUTOR_SWEEPER_ABORT_THRESHOLD,
    DATAHUB_EXECUTOR_SWEEPER_DISABLED,
    DATAHUB_EXECUTOR_SWEEPER_PENDING_THRESHOLD,
    DATAHUB_EXECUTOR_SWEEPER_RESTARTS_DISABLED,
)

logger = logging.getLogger(__name__)


class SweeperJob:
    """Class for cleaning up stale and duplicate execution requests."""

    def __init__(self) -> None:
        """
        Initialize the sweeper
        """
        self.graph = DataHubExecutorGraph()
        self.is_locked = False
        self.is_disabled = DATAHUB_EXECUTOR_SWEEPER_DISABLED
        self.current_time: str
        self.run_id: str

    def _lock(self) -> bool:
        if self.is_locked:
            logger.warning("Another instance of sweeper job is already running.")
            return False

        self.is_locked = True

        ts = time.time()
        self.current_time = datetime.datetime.fromtimestamp(
            ts, datetime.timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        self.run_id = str(int(ts))

        return True

    def _unlock(self) -> None:
        self.is_locked = False
        self.current_time = "not-set"
        self.run_id = "not-set"

    def _get_elapsed_seconds(self, timestamp_ms: int) -> float:
        return (
            datetime.datetime.now(tz=datetime.timezone.utc)
            - datetime.datetime.fromtimestamp(
                timestamp_ms / 1000, tz=datetime.timezone.utc
            )
        ).total_seconds()

    def _is_pending_too_long(self, ingestion: ExecutionRequestStatus) -> bool:
        elapsed = self._get_elapsed_seconds(ingestion.request_time)
        return (ingestion.status == DATAHUB_EXECUTION_REQUEST_STATUS_PENDING) and (
            elapsed > DATAHUB_EXECUTOR_SWEEPER_PENDING_THRESHOLD
        )

    def _is_stale_ingestion(self, ingestion: ExecutionRequestStatus) -> bool:
        elapsed = self._get_elapsed_seconds(ingestion.last_observed)
        return elapsed > DATAHUB_EXECUTOR_SWEEPER_ABORT_THRESHOLD

    def _repopulate_cancel_action_reports(
        self,
        actions: Dict[str, SweeperAction],
        running: Dict[str, ExecutionRequestStatus],
        pending: Dict[str, ExecutionRequestStatus],
    ) -> List[SweeperAction]:
        # For all requests cancelled as duplicate, update their report block to include reference to the
        # execution requests they are being preemted with. This is done purely to facilitate investigations
        # and reduce the amount of customer questions.
        for exec_id, action in actions.items():
            if action.action != "DUPLICATE":
                continue
            ingestion = action.ingestion
            if ingestion.ingestion_source_urn in running:
                actions[exec_id] = self._build_duplicate_action(
                    ingestion,
                    running[ingestion.ingestion_source_urn].execution_request_urn,
                )
            elif action.ingestion.ingestion_source_urn in pending:
                actions[exec_id] = self._build_duplicate_action(
                    ingestion,
                    pending[ingestion.ingestion_source_urn].execution_request_urn,
                )

        return list(actions.values())

    def _build_aborted_action(self, ingestion: ExecutionRequestStatus) -> SweeperAction:
        elapsed = self._get_elapsed_seconds(ingestion.last_observed)
        return SweeperAction.parse_obj(
            {
                "action": "ABORTED",
                "description": f"ingestion has been inactive for {elapsed} seconds",
                "report": f"{self.current_time} Ingestion was aborted due to worker pod eviction, crash, or restart.\n\n---\n{ingestion.report}",
                "ingestion": ingestion,
            }
        )

    def _build_duplicate_action(
        self, ingestion: ExecutionRequestStatus, dup_of: Optional[str] = None
    ) -> SweeperAction:
        action_message = "Ingestion was cancelled as a duplicate"
        if dup_of is not None:
            action_message = f"{action_message} of {dup_of}"

        return SweeperAction.parse_obj(
            {
                "action": "DUPLICATE",
                "description": action_message,
                "report": f"{self.current_time} {action_message}.\n\n---\n{ingestion.report}",
                "ingestion": ingestion,
            }
        )

    def _build_cancelled_action(
        self, ingestion: ExecutionRequestStatus
    ) -> SweeperAction:
        self._get_elapsed_seconds(ingestion.request_time)
        return SweeperAction.parse_obj(
            {
                "action": "CANCELLED",
                "description": "Pending ingestion has exceeded a timeout",
                "report": f"{self.current_time} Pending ingestion was cancelled due to a timeout.\n\n---\n{ingestion.report}",
                "ingestion": ingestion,
            }
        )

    def _build_restart_action(self, ingestion: ExecutionRequestStatus) -> SweeperAction:
        self._get_elapsed_seconds(ingestion.last_observed)
        return SweeperAction.parse_obj(
            {
                "action": "RESTART",
                "description": "presiously aborted ingestion has been restarted",
                "report": f"{self.current_time} Ingestion was restarted due to a previously aborted attempt.\n\n---\n{ingestion.report}",
                "ingestion": ingestion,
                "errors_fatal": True,
            }
        )

    def _get_restart_actions(
        self, ingestions: List[ExecutionRequestStatus]
    ) -> List[SweeperAction]:
        # The goal here is to find recoverable failed ingestion requests and schedule their restart:
        # - Currently, we only treat ABORTED requests as recoverable:
        #   * Pending is tricky, because there's no way to tell whether it's lost or waiting for remote executor pick up.
        #   * Failed is tricky, because there's no way to tell whether the failure is permaent (e.g. bad credentials).
        # - If there are any jobs currently RUNNING and are *not stale*, no ABORTED requests are eligible for restart.
        # - If there're no RUNNING jobs, and the most recent request in any given ingestion source is ABORTED, it's scheduled for a restart
        #   unless it's followed by any other request.

        actions: List[SweeperAction] = []
        running_requests: Dict[str, bool] = {}
        aborted_requests: Dict[str, ExecutionRequestStatus] = {}
        affected_sources: Dict[str, ExecutionRequestStatus] = {}
        recent_requests_params: List = []

        for ingestion in ingestions:
            if ingestion.status == DATAHUB_EXECUTION_REQUEST_STATUS_RUNNING:
                # Do not consider RUNNING jobs with unreliable staleness
                if ingestion.last_observed == 0:
                    continue
                is_stale = self._is_stale_ingestion(ingestion)
                if is_stale:
                    aborted_requests[ingestion.ingestion_source_urn] = ingestion
                else:
                    running_requests[ingestion.ingestion_source_urn] = True
            elif ingestion.ingestion_source_urn in running_requests:
                # Do not consider the request if it has any successors
                aborted_requests.pop(ingestion.ingestion_source_urn, None)

        # Filter out sources that have RUNNING non-stale requests
        for source, ingestion in aborted_requests.items():
            if source not in running_requests:
                affected_sources[source] = ingestion
                recent_requests_params.append([source, ingestion.request_time])

        # Finally, check if the most recent observed ABORTED request has any successorts
        recent_requests = self.graph.get_recent_requests_for_sources(
            recent_requests_params
        )
        for source, ingestion in affected_sources.items():
            if (
                source in recent_requests
                and (len(recent_requests[source]) == 1)
                and (
                    ingestion.execution_request_urn
                    == recent_requests[source][0].execution_request_urn
                )
            ):
                actions.append(self._build_restart_action(ingestion))

        return actions

    def _get_cancel_actions(
        self, ingestions: List[ExecutionRequestStatus]
    ) -> List[SweeperAction]:
        # The goal is to keep at most one running or pending ingestion, and cancel all the stale ingestions:
        # - Any stale running ingestions are marked as ABORTED.
        # - The oldest running non-stale ingestion is always preserved since it's alive and made most progress.
        # - Most recent pending ingestion is also preserved unless it has been pending for too long.
        # - If both pending and running ingestions are present after first pass, only the running one is kept.
        # - All remaining running/pending ingestions are marked as DUPLICATE.

        actions: Dict[str, SweeperAction] = {}

        # Oldest running and latest pending are computed per ingestion source
        oldest_running: Dict[str, ExecutionRequestStatus] = {}
        latest_pending: Dict[str, ExecutionRequestStatus] = {}

        for ingestion in ingestions:
            # Request status may change while paginating over results and ES does not provide stable view of the resultset.
            # Do a secondary filtering of irrelevant requests here to suppress unnecessary exceptions.
            if ingestion.status not in [
                DATAHUB_EXECUTION_REQUEST_STATUS_PENDING,
                DATAHUB_EXECUTION_REQUEST_STATUS_RUNNING,
            ]:
                continue

            is_stale = self._is_stale_ingestion(ingestion)

            # Save oldest non-stale running and latest pending ingestions for the filter pass
            if ingestion.status == DATAHUB_EXECUTION_REQUEST_STATUS_PENDING:
                latest_pending[ingestion.ingestion_source_urn] = ingestion
            elif (
                ingestion.ingestion_source_urn not in oldest_running
                and ingestion.status == DATAHUB_EXECUTION_REQUEST_STATUS_RUNNING
                and not is_stale
            ):
                oldest_running[ingestion.ingestion_source_urn] = ingestion

            if (
                ingestion.status == DATAHUB_EXECUTION_REQUEST_STATUS_RUNNING
                and is_stale
            ):
                # Skip if staleness information is not reliable
                if ingestion.last_observed == 0:
                    logger.warning(
                        f"Sweeper({self.run_id}): skipping malformed record {ingestion.execution_request_urn} with last_observed = 0."
                    )
                    continue
                action = self._build_aborted_action(ingestion)
            else:
                action = self._build_duplicate_action(ingestion)
            actions[ingestion.execution_request_id] = action

        # The oldest non-stale running ingestion is always preserved.
        for source, ingestion in oldest_running.items():
            if (
                ingestion.execution_request_id in actions
                and actions[ingestion.execution_request_id].action != "ABORTED"
            ):
                logger.info(
                    f"Sweeper({self.run_id}): will skip RUNNING ingestion {ingestion.execution_request_id} in source {source}."
                )
                actions.pop(ingestion.execution_request_id, None)

        for source, ingestion in latest_pending.items():
            # Cancel pending ingestion if it has been pending for too long
            if self._is_pending_too_long(ingestion):
                logger.info(
                    f"Sweeper({self.run_id}): will CANCEL ingestion {ingestion.execution_request_id} in source {source} since it exceeded pending timeout."
                )
                actions[ingestion.execution_request_id] = self._build_cancelled_action(
                    ingestion
                )
            elif source not in oldest_running:
                # Delete "latest pending" if a non-stale running ingestions is also present.
                logger.info(
                    f"Sweeper({self.run_id}): will skip PENDING ingestion {ingestion.execution_request_id} in source {source}."
                )
                actions.pop(ingestion.execution_request_id, None)

        return self._repopulate_cancel_action_reports(actions, oldest_running, latest_pending)  # type: ignore

    def _update_execution_request_metrics(
        self, ingestions: List[ExecutionRequestStatus]
    ) -> None:
        stats: Dict[str, int] = {}

        for ingestion in ingestions:
            stats[ingestion.status] = stats.get(ingestion.status, 0) + 1
        for status, count in stats.items():
            STATS_SWEEPER_EXECUTION_REQUESTS_COUNT.labels(status).set(count)

    def _get_execution_plan(self) -> List[SweeperAction]:
        actions: List[SweeperAction] = []
        ingestions = self.graph.get_running_and_pending_ingestions()

        self._update_execution_request_metrics(ingestions)

        if not DATAHUB_EXECUTOR_SWEEPER_RESTARTS_DISABLED:
            actions = actions + self._get_restart_actions(ingestions)

        actions = actions + self._get_cancel_actions(ingestions)

        return actions

    def _execute_cancel_action(self, action: SweeperAction) -> None:
        # Set our status first to provide proper reason for cancellation. Once "final" state is set on execution request
        # further updates are rejected by GMS.
        self.graph.set_execution_request_status(
            action.ingestion.execution_request_id,
            action.report,
            action.action,
            action.ingestion.start_time,
        )
        # We always cancel execution and update request status to better handle situations when the worker is still alive,
        # and keeps processing the job but it's not visible to the coordinator because of a network partition or kafka lag.
        self.graph.cancel_ingestion_execution(
            action.ingestion.ingestion_source_urn,
            action.ingestion.execution_request_urn,
        )

    def _execute_restart_action(self, action: SweeperAction) -> None:
        old_input = action.ingestion.raw_input_aspect
        source = old_input.get("source", None)
        args = old_input.get("args", None)

        if isinstance(source, dict) and isinstance(args, dict):
            new_input = ExecutionRequestInputClass(
                task=str(old_input.get("task")),
                args=args,
                executorId=str(old_input.get("executorId")),
                requestedAt=int(time.time() * 1000),
                source=ExecutionRequestSourceClass(
                    type=str(source.get("type")),
                    ingestionSource=source.get("ingestionSource"),
                ),
            )
            if not emit_execution_request_input(new_input):
                raise ValueError("Failed to emit restart action")
        else:
            logger.error(
                f"Sweeper({self.run_id}): failed to emit restart MCP -- required arguments are missing or incorrect: {old_input}"
            )

    def _execute_action(self, action: SweeperAction) -> bool:
        try:
            logger.info(
                f"Sweeper({self.run_id}): executing {action.action} action for {action.ingestion.execution_request_id}: {action.description}"
            )
            if action.action == "RESTART":
                self._execute_restart_action(action)
            else:
                self._execute_cancel_action(action)

            return True
        except Exception as e:
            logger.error(f"Sweeper({self.run_id}): error executing an action: {e}")

            # If action's errors are fatal, do not proceed
            if action.errors_fatal:
                raise e

            return False

    def _execute_plan(self, plan: List[SweeperAction]) -> int:
        executed_total: int = 0
        stats: Dict[str, Dict] = {
            "planned": {},
            "executed": {},
        }
        for action in plan:
            stats["planned"][action.action] = stats["planned"].get(action.action, 0) + 1
            if self._execute_action(action):
                stats["executed"][action.action] = (
                    stats["executed"].get(action.action, 0) + 1
                )
                executed_total += 1

        for action, count in stats["planned"].items():
            STATS_SWEEPER_ACTIONS_PLANNED.labels(action).set(count)
        for action, count in stats["executed"].items():
            STATS_SWEEPER_ACTIONS_EXECUTED.labels(action).set(count)

        return executed_total

    def run(self) -> None:
        if self.is_disabled:
            logger.error("Sweeper is disabled.")
            return

        if self._lock():
            logger.info(
                f"Sweeper({self.run_id}): performing stale/duplicate job cleanup; run_id = {self.run_id}"
            )
            try:
                plan = self._get_execution_plan()
                executed = self._execute_plan(plan)

                logger.info(
                    f"Sweeper({self.run_id}): plan execution finished: {executed} of {len(plan)} actions succeeded."
                )
            except Exception as e:
                logger.error(
                    f"Sweeper({self.run_id}): error while running sweeper job: {e}",
                    exc_info=True,
                )
            self._unlock()
