import datetime
import logging
import time
from typing import Any, Dict, List, Optional, cast

from datahub.metadata.schema_classes import (
    ExecutionRequestInputClass,
    ExecutionRequestSourceClass,
    RemoteExecutorStatusClass,
)

from datahub_executor.common.constants import (
    DATAHUB_EXECUTION_REQUEST_STATUS_PENDING,
    DATAHUB_EXECUTION_REQUEST_STATUS_RUNNING,
)
from datahub_executor.common.discovery.utils import send_remote_executor_status
from datahub_executor.common.graph import DataHubExecutorGraph
from datahub_executor.common.identity.utils import get_remote_executor_id_from_urn
from datahub_executor.common.ingestion.helpers import emit_execution_request_input
from datahub_executor.common.monitoring.base import METRIC
from datahub_executor.common.types import (
    ExecutionRequestStatus,
    SweeperAction,
)
from datahub_executor.config import (
    DATAHUB_EXECUTOR_DISCOVERY_EXPIRE_THRESHOLD,
    DATAHUB_EXECUTOR_DISCOVERY_PURGE_AFTER,
    DATAHUB_EXECUTOR_DISCOVERY_PURGE_THRESHOLD,
    DATAHUB_EXECUTOR_SWEEPER_ABORT_THRESHOLD,
    DATAHUB_EXECUTOR_SWEEPER_CANCEL_THRESHOLD,
    DATAHUB_EXECUTOR_SWEEPER_DISABLED,
    DATAHUB_EXECUTOR_SWEEPER_PENDING_THRESHOLD,
    DATAHUB_EXECUTOR_SWEEPER_RESTART_MAX_ATTEMPTS,
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
        self.state: Dict[str, Any] = {}
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

    def _is_cancelled_ingestion(self, ingestion: ExecutionRequestStatus) -> bool:
        signal = ingestion.raw_signal_aspect.get("signal", "UNSET")
        elapsed = self._get_elapsed_seconds(
            ingestion.raw_signal_aspect.get("createdAt", {}).get("time", 0)
        )
        return (signal == "KILL") and (
            elapsed > DATAHUB_EXECUTOR_SWEEPER_CANCEL_THRESHOLD
        )

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
            ingestion = cast(ExecutionRequestStatus, action.args.get("ingestion"))
            if ingestion.ingestion_source_urn in running:
                actions[exec_id] = self._build_duplicate_action(
                    ingestion,
                    running[ingestion.ingestion_source_urn].execution_request_urn,
                )
            elif ingestion.ingestion_source_urn in pending:
                actions[exec_id] = self._build_duplicate_action(
                    ingestion,
                    pending[ingestion.ingestion_source_urn].execution_request_urn,
                )

        return list(actions.values())

    def _build_executor_action(
        self,
        urn: str,
        status: RemoteExecutorStatusClass,
        action: str,
    ) -> SweeperAction:
        return SweeperAction.parse_obj(
            {
                "action": action,
                "description": f"Processing stale remote executor status record: {urn}; executorId = {status.executorPoolId}",
                "args": {
                    "urn": urn,
                    "status": status,
                },
            }
        )

    def _build_aborted_action(self, ingestion: ExecutionRequestStatus) -> SweeperAction:
        elapsed = self._get_elapsed_seconds(ingestion.last_observed)
        return SweeperAction.parse_obj(
            {
                "action": "ABORTED",
                "description": f"Ingestion {ingestion.execution_request_id} has been inactive for {elapsed} seconds",
                "args": {
                    "report": f"{self.current_time} Ingestion was aborted due to worker pod eviction, crash, or restart.\n\n---\n{ingestion.report}",
                    "ingestion": ingestion,
                },
            }
        )

    def _build_duplicate_action(
        self, ingestion: ExecutionRequestStatus, dup_of: Optional[str] = None
    ) -> SweeperAction:
        action_message = (
            "Ingestion {ingestion.execution_request_id} was cancelled as a duplicate"
        )
        if dup_of is not None:
            action_message = f"{action_message} of {dup_of}"

        return SweeperAction.parse_obj(
            {
                "action": "DUPLICATE",
                "description": action_message,
                "args": {
                    "report": f"{self.current_time} {action_message}.\n\n---\n{ingestion.report}",
                    "ingestion": ingestion,
                },
            }
        )

    def _build_cancelled_action(
        self, ingestion: ExecutionRequestStatus, reason: str
    ) -> SweeperAction:
        return SweeperAction.parse_obj(
            {
                "action": "CANCELLED",
                "description": f"Ingestion {ingestion.execution_request_id} cancelled due to {reason}",
                "args": {
                    "report": f"{self.current_time} Ingestion was cancelled due to {reason}.\n\n---\n{ingestion.report}",
                    "ingestion": ingestion,
                },
            }
        )

    def _build_restart_action(self, ingestion: ExecutionRequestStatus) -> SweeperAction:
        self._get_elapsed_seconds(ingestion.last_observed)
        return SweeperAction.parse_obj(
            {
                "action": "RESTART",
                "description": "Presiously aborted ingestion {ingestion.execution_request_id} has been restarted",
                "args": {
                    "report": f"{self.current_time} Ingestion was restarted due to a previously aborted attempt.\n\n---\n{ingestion.report}",
                    "ingestion": ingestion,
                },
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
            attempts = ingestion.raw_input_aspect.get("attempts", 0)
            attempts = attempts + 1 if attempts is not None else 1
            if (
                source in recent_requests
                and (len(recent_requests[source]) == 1)
                and (
                    ingestion.execution_request_urn
                    == recent_requests[source][0].execution_request_urn
                )
            ):
                if attempts > DATAHUB_EXECUTOR_SWEEPER_RESTART_MAX_ATTEMPTS:
                    logger.warning(
                        f"Sweeper: task {ingestion.execution_request_urn} reached maximum restart attempts, will not restart."
                    )
                    continue
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
                if self._is_cancelled_ingestion(ingestion):
                    actions[ingestion.execution_request_id] = (
                        self._build_cancelled_action(ingestion, "user request")
                    )
                    logger.info(
                        f"Sweeper({self.run_id}): will FORCE CANCEL ingestion {ingestion.execution_request_id} in source {source}."
                    )
                else:
                    actions.pop(ingestion.execution_request_id, None)
                    logger.info(
                        f"Sweeper({self.run_id}): will skip RUNNING ingestion {ingestion.execution_request_id} in source {source}."
                    )

        for source, ingestion in latest_pending.items():
            # Cancel pending ingestion if it has been pending for too long
            if self._is_pending_too_long(ingestion):
                logger.info(
                    f"Sweeper({self.run_id}): will CANCEL ingestion {ingestion.execution_request_id} in source {source} since it exceeded pending timeout."
                )
                actions[ingestion.execution_request_id] = self._build_cancelled_action(
                    ingestion, "pending timeout"
                )
            elif source not in oldest_running:
                # Delete "latest pending" if a non-stale running ingestions is also present.
                logger.info(
                    f"Sweeper({self.run_id}): will skip PENDING ingestion {ingestion.execution_request_id} in source {source}."
                )
                actions.pop(ingestion.execution_request_id, None)

        return self._repopulate_cancel_action_reports(
            actions, oldest_running, latest_pending
        )  # type: ignore

    def _get_remote_executor_actions(
        self,
        executors: Dict[str, RemoteExecutorStatusClass],
    ) -> List[SweeperAction]:
        actions: Dict[str, SweeperAction] = {}
        keep: Dict[str, List] = {}

        now = int(time.time())
        expire_threshold = now - DATAHUB_EXECUTOR_DISCOVERY_EXPIRE_THRESHOLD
        purge_threshold = now - DATAHUB_EXECUTOR_DISCOVERY_PURGE_THRESHOLD

        for urn, status in executors.items():
            # Save urn of the most recent status update for each executor id
            if (
                status.executorPoolId not in keep
                or keep[status.executorPoolId][1] < status.reportedAt
            ):
                keep[status.executorPoolId] = [urn, status.reportedAt]
            # Delete status records updated over a month ago
            if status.reportedAt < purge_threshold:
                logger.info(
                    f"Sweeper: going to DELETE executor status record for {status.executorPoolId}: {urn}; reportedAt = {status.reportedAt}"
                )
                actions[urn] = self._build_executor_action(
                    urn, status, "EXECUTOR_DELETE"
                )
            # Expire status records last updated over an hour ago, and not already expired
            elif (
                (status.reportedAt < expire_threshold) or status.executorStopped
            ) and not status.executorExpired:
                logger.info(
                    f"Sweeper: going to EXPIRE executor status record for {status.executorPoolId}: {urn}; reportedAt = {status.reportedAt}"
                )
                actions[urn] = self._build_executor_action(
                    urn, status, "EXECUTOR_EXPIRE"
                )

        # Do not delete most recent status for each executorId
        for info in keep.values():
            logger.info(
                f"Sweeper: going to SKIP expiring the most recent executor status record: {info[0]}"
            )
            actions.pop(info[0], None)

        return list(actions.values())

    def _update_execution_request_metrics(
        self, ingestions: List[ExecutionRequestStatus]
    ) -> None:
        stats: Dict[str, int] = {}

        for ingestion in ingestions:
            stats[ingestion.status] = stats.get(ingestion.status, 0) + 1
        for status, count in stats.items():
            METRIC("SWEEPER_EXECUTION_REQUESTS", status=status).set(count)

    def _get_execution_plan(self) -> List[SweeperAction]:
        actions: List[SweeperAction] = []
        ingestions = self.graph.get_running_and_pending_ingestions()

        self._update_execution_request_metrics(ingestions)

        if not DATAHUB_EXECUTOR_SWEEPER_RESTARTS_DISABLED:
            actions = actions + self._get_restart_actions(ingestions)

        actions = actions + self._get_cancel_actions(ingestions)

        # To keep the amount of data being pulled from GMS every minute under control, pull
        # status records that need to be expired every minute, pull records that need to be
        # deleted once a day.
        now = time.time()
        if (
            now - self.state.get("executors_last_purged", 0)
        ) > DATAHUB_EXECUTOR_DISCOVERY_PURGE_AFTER:
            logger.info("Sweeper: doing a full swipe of remote executor statuses")

            executors = self.graph.get_remote_executors(query="*")
            self.state["executors_last_purged"] = now
        else:
            logger.info("Sweeper: doing a partial swipe of remote executor statuses")
            executors = self.graph.get_remote_executors(query="executorExpired:false")

        actions = actions + self._get_remote_executor_actions(executors)

        return actions

    def _execute_cancel_action(self, action: SweeperAction) -> None:
        # Set our status first to provide proper reason for cancellation. Once "final" state is set on execution request
        # further updates are rejected by GMS.
        ingestion = cast(ExecutionRequestStatus, action.args.get("ingestion"))
        self.graph.set_execution_request_status(
            ingestion.execution_request_id,
            cast(str, action.args.get("report")),
            action.action,
            cast(ExecutionRequestStatus, action.args.get("ingestion")).start_time,
        )
        # We always cancel execution and update request status to better handle situations when the worker is still alive,
        # and keeps processing the job but it's not visible to the coordinator because of a network partition or kafka lag.
        self.graph.cancel_ingestion_execution(
            ingestion.ingestion_source_urn,
            ingestion.execution_request_urn,
        )

    def _execute_restart_action(self, action: SweeperAction) -> None:
        old_input = cast(
            ExecutionRequestStatus, action.args.get("ingestion")
        ).raw_input_aspect
        source = old_input.get("source", None)
        args = old_input.get("args", None)

        attempts = old_input.get("attempts", 0)
        attempts = attempts + 1 if attempts is not None else 1

        if isinstance(source, dict) and isinstance(args, dict):
            new_input = ExecutionRequestInputClass(
                attempts=attempts,
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

    def _execute_executor_action(self, action: SweeperAction) -> None:
        urn = cast(str, action.args.get("urn"))
        status = cast(RemoteExecutorStatusClass, action.args.get("status"))

        if action.action == "EXECUTOR_EXPIRE":
            status.executorExpired = True
            instance_id = get_remote_executor_id_from_urn(urn)
            send_remote_executor_status(self.graph, instance_id, status)
        else:
            self.graph.delete_entity(urn, True)

    def _execute_action(self, action: SweeperAction) -> bool:
        try:
            logger.info(
                f"Sweeper({self.run_id}): executing {action.action} action: {action.description}"
            )
            if action.action == "RESTART":
                self._execute_restart_action(action)
            elif action.action in ["EXECUTOR_EXPIRE", "EXECUTOR_DELETE"]:
                self._execute_executor_action(action)
            elif action.action in ["CANCELLED", "DUPLICATE", "ABORTED"]:
                self._execute_cancel_action(action)
            else:
                raise ValueError(f"Unrecognized action: {action.action}")
            return True
        except Exception as e:
            logger.error(
                f"Sweeper({self.run_id}): error executing an action {action}",
                exc_info=True,
            )

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
            METRIC("SWEEPER_ACTIONS_PLANNED", action=action).set(count)
        for action, count in stats["executed"].items():
            METRIC("SWEEPER_ACTIONS_EXECUTED", action=action).set(count)

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
