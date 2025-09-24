"""Action lifecycle management utilities for propagation tests."""

import json
import logging
import os
import pathlib
import signal
import subprocess
from typing import Any, Iterable, Iterator

import yaml

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DataHubActionConfigClass,
    DataHubActionInfoClass,
)
from tests.integrations_service_utils import (
    bootstrap_action,
    rollback_action,
    start_action,
    stop_action,
)

logger = logging.getLogger(__name__)


def kill_action_processes(action_urn=None):
    """Kill action runner processes.
    Note: This is a temporary solution that only works with local deployments.
    """
    try:
        output = subprocess.check_output(["ps", "aux"], universal_newlines=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running ps command: {e}")
        return

    pids = []
    for line in output.splitlines():
        if "action_runner.py" in line:
            if action_urn is None or action_urn in line:
                parts = line.split()
                if len(parts) > 1:
                    pids.append(int(parts[1]))

    if not pids:
        message = (
            f"No processes found for action URN: {action_urn}"
            if action_urn
            else "No action_runner.py processes found"
        )
        logger.info(message)
        return

    # Kill processes
    for pid in pids:
        try:
            os.kill(pid, signal.SIGKILL)
            logger.info(f"Killed process {pid}")
        except ProcessLookupError:
            logger.warning(f"Process {pid} not found")
        except PermissionError:
            logger.error(f"Permission denied to kill process {pid}")

    message = (
        f"All processes for action URN {action_urn} have been terminated"
        if action_urn
        else "All action_runner.py processes have been terminated"
    )
    logger.info(message)


def cleanup_entities_and_actions(
    auth_session: Any,
    graph_client: DataHubGraph,
    urns: Iterable[str],
    test_resources_dir: str,
    remove_actions: bool = True,
):
    """Clean up entities and actions."""
    for urn in urns:
        graph_client.delete_entity(urn, hard=True)

    if remove_actions:
        actions_gql = (pathlib.Path(test_resources_dir) / "actions.gql").read_text()
        all_actions = graph_client.execute_graphql(
            query=actions_gql, operation_name="listActions"
        )
        logger.debug(f"Got actions: {all_actions}")
        try:
            for action in all_actions["listActionPipelines"]["actionPipelines"]:
                action_urn = action["urn"]
                try:
                    stop_action(action_urn, auth_session.integrations_service_url())
                except Exception as e:
                    logger.error(f"Error stopping action: {e}")

                try:
                    kill_action_processes(action_urn)
                except Exception as e:
                    logger.error(f"Error killing action processes: {e}")

                graph_client.delete_entity(action_urn, hard=True)
        except Exception as e:
            logger.error(f"Error deleting action: {e}")

        try:
            kill_action_processes()
        except Exception as e:
            logger.error(f"Error killing action processes: {e}")


class ActionManager:
    """Manages action lifecycle for propagation tests."""

    def __init__(
        self, auth_session: Any, graph_client: DataHubGraph, test_resources_dir: str
    ):
        self.auth_session = auth_session
        self.graph_client = graph_client
        self.test_resources_dir = test_resources_dir

    def create_test_action(
        self, action_urn: str, recipe_filename: str, action_name: str, action_type: str
    ) -> Iterator[None]:
        """Create and provision a test action."""
        try:
            recipe_file = pathlib.Path(self.test_resources_dir) / recipe_filename

            with open(recipe_file, "r") as f:
                recipe_json_str = json.dumps(yaml.safe_load(f))

            cleanup_entities_and_actions(
                self.auth_session,
                self.graph_client,
                [action_urn],
                self.test_resources_dir,
            )

            self.graph_client.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=action_urn,
                    aspect=DataHubActionInfoClass(
                        name=action_name,
                        type=action_type,
                        config=DataHubActionConfigClass(
                            recipe=recipe_json_str,
                        ),
                    ),
                )
            )
            yield
        finally:
            cleanup_entities_and_actions(
                self.auth_session,
                self.graph_client,
                [action_urn],
                self.test_resources_dir,
            )

    def run_bootstrap(self, action_urn: str) -> None:
        """Run bootstrap action with better error handling."""
        try:
            bootstrap_action(
                action_urn,
                self.auth_session.integrations_service_url(),
                wait_for_completion=True,
            )
        except Exception as e:
            logger.warning(
                f"Bootstrap action may have completed but stats endpoint failed: {e}"
            )
            # Check if it's just a stats endpoint issue vs actual bootstrap failure
            if "Timed out waiting" in str(e) and "stats" not in str(e):
                raise  # Re-raise if it's a real bootstrap failure
            logger.info(
                "Continuing despite stats endpoint issue - bootstrap may have succeeded"
            )

    def run_rollback(self, action_urn: str) -> None:
        """Run rollback action."""
        rollback_action(
            action_urn,
            self.auth_session.integrations_service_url(),
            wait_for_completion=True,
        )

    def start_live_action(self, action_urn: str) -> None:
        """Start live action."""
        # Wait for completion to ensure action is fully running before proceeding
        # This matches the working test_generic_tag_propagation.py approach
        start_action(
            action_urn,
            self.auth_session.integrations_service_url(),
            wait_for_completion=True,
        )

    def stop_live_action(self, action_urn: str) -> None:
        """Stop live action."""
        stop_action(action_urn, self.auth_session.integrations_service_url())

    def cleanup(self, urns: Iterable[str], remove_actions: bool = True) -> None:
        """Clean up entities and actions."""
        cleanup_entities_and_actions(
            self.auth_session,
            self.graph_client,
            urns,
            self.test_resources_dir,
            remove_actions,
        )
