"""
Runs an action remotely, but spawning a new managed system ingestion source. 
Risks: Note that the risk with the approach inside this file is that the secondary index doesn't yet know about the running source (e.g. large kafka delay)
This could in worst case lead to duplicate messages, violation of exactly once guarantees. 
TODO: Support handling of cancellation in the process. 
"""

import base64
import json
import logging
import signal
import threading
import time
from typing import Any, Dict, List, Optional

from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.telemetry.telemetry import telemetry_instance
from datahub_actions.pipeline.pipeline_config import PipelineConfig
from tenacity import before_log, retry, stop_after_attempt, wait_exponential

from datahub_integrations import __version__
from datahub_integrations.actions.constants import (
    MAX_REMOTE_ACTION_REFRESH_INTERVAL_SECONDS,
)
from datahub_integrations.actions.stats_util import Stage
from datahub_integrations.app import DATAHUB_FRONTEND_URL

# We force load the telemetry client because it has a side-effect of loading Sentry.
assert telemetry_instance

DEFAULT_EXECUTOR_ID = "default"
REMOTE_ACTION_SOURCE_NAME = "datahub_integrations.sources.remote_actions.remote_action_source.RemoteActionSource"
MAX_REQUEST_RETRIES = 3

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

should_continue_running = True


def run_action_remotely(
    action_urn: str, action_recipe: dict, executor_id: str, stage: Stage
) -> None:

    # Initialize graph
    graph = get_default_graph()
    # TODO: Support stages remotely. We'll need to disable through the UI
    if stage != Stage.LIVE:
        raise Exception(
            f"Running stages aside from Live actions are not currently supported remotely! {action_urn}"
        )

    remote_action_runner = RemoteActionRunner(graph, DATAHUB_FRONTEND_URL)

    # Register signal handlers to stop the pipeline gracefully.
    def stop_handler(signum: int, frame: Optional[Any]) -> None:
        logger.info(
            f"Received signal {signum}. Stopping remote action {action_urn} gracefully..."
        )
        remote_action_runner.stop_action(action_urn)

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    # Start running the remote action!
    remote_action_runner.run_action(action_urn, action_recipe, executor_id)


class RemoteActionRunner:
    def __init__(self, graph: DataHubGraph, base_url: str):
        self.should_continue_running = threading.Event()
        self.graph = graph
        self.base_url = base_url

    def run_action(
        self,
        action_urn: str,
        action_recipe: dict,
        executor_id: str,
    ) -> None:
        # How this should work is:
        try:
            # Step 1: First, check whether there is an ingestion source created for this action.
            # If not, provision one.
            ingestion_source_urn = self.try_create_remote_action_ingestion_source(
                action_urn, action_recipe, executor_id
            )

            # Step 2: Then, check whether the ingestion source is already running (has open execution requests)
            # If the ingestion source is not currently running, run it.
            self.start_running_ingestion_source(ingestion_source_urn)

        except Exception as e:
            logger.exception(
                f"Caught an unexpected exception while attempting to run remote action with urn {action_urn}: {e}"
            )

    def stop_action(self, action_urn: str) -> None:
        self.should_continue_running.clear()  # Use event to stop running
        self.clean_up_remote_action_ingestion_source(
            self.create_remote_ingestion_source_urn(action_urn)
        )

    def try_create_remote_action_ingestion_source(
        self,
        action_urn: str,
        action_recipe: dict,
        executor_id: str,
    ) -> str:
        """
        Creates a remove action ingestion source for the automation if it does not already exist.

        To do this, it uses the action_urn to encode a unique identifier for the ingestion source.
        Then it provisions it if it doesn't already exist.
        """
        logger.info(
            f"Trying to create remote ingestion source for action with urn {action_urn}"
        )

        ingestion_source_urn = self.create_remote_ingestion_source_urn(action_urn)

        # Step 1: Build the ingestion recipe from config.
        ingestion_recipe = self.build_remote_ingestion_source_recipe_json(
            action_urn, action_recipe
        )

        # Step 2: Provision the ingestion source
        self.upsert_remote_ingestion_source(
            ingestion_source_urn, ingestion_recipe, executor_id
        )

        logger.info(f"Upserted remote ingestion source: {ingestion_source_urn}")

        # Return a deterministic URN for the ingestion source managed by this remote action.
        return ingestion_source_urn

    def start_running_ingestion_source(self, ingestion_source_urn: str) -> None:
        """
        Checks if the ingestion source is running; if not, starts it. Repeats every 30 minutes.
        """
        self.should_continue_running.set()  # Set the event to continue running

        while self.should_continue_running.is_set():
            if not self.is_running(ingestion_source_urn):
                self.start_ingestion(ingestion_source_urn)
                logger.info(f"Started ingestion source: {ingestion_source_urn}")
            else:
                logger.info(f"Ingestion source already running: {ingestion_source_urn}")

            time.sleep(MAX_REMOTE_ACTION_REFRESH_INTERVAL_SECONDS)

    def upsert_remote_ingestion_source(
        self,
        ingestion_source_urn: str,
        ingestion_recipe_json: str,
        executor_id: str,
    ) -> None:
        mutation = """
            mutation updateIngestionSource($urn: String!, $input: UpdateIngestionSourceInput!) {
                updateIngestionSource(urn: $urn, input: $input)
            }
        """
        input_value = {
            "name": ingestion_source_urn,
            "type": REMOTE_ACTION_SOURCE_NAME,
            "config": {
                "recipe": ingestion_recipe_json,
                "executorId": executor_id,
                "debugMode": False,
                "extraArgs": [
                    {
                        "key": "extra_pip_requirements",
                        "value": f'["{self.base_url}/integrations/dist/integrations/acryl_datahub_integrations-{__version__}-py3-none-any.whl"]',
                    }
                ],  # Assuming no extra arguments are required,
                "version": f"{self.base_url}/integrations/dist/datahub/acryl_datahub-{__version__}-py3-none-any.whl",
            },
            "source": {
                "type": "SYSTEM"  # System source, so it is hidden from the UI by default.
            },
        }
        self.execute_graphql(
            mutation, {"urn": ingestion_source_urn, "input": input_value}
        )

    def is_running(self, ingestion_source_urn: str) -> bool:
        """
        Checks if an ingestion source is currently running using a GraphQL query.
        """
        query = """
            query($urn: String!) {
                ingestionSource(urn: $urn) {
                    executions(start: 0, count: 1) {
                        executionRequests {
                            result {
                                status
                            }
                        }
                    }
                }
            }
        """
        result = self.execute_graphql(query, {"urn": ingestion_source_urn})

        if (
            "ingestionSource" in result
            and len(result["ingestionSource"]["executions"]["executionRequests"]) > 0
        ):
            # Extract the latest execution
            latest_execution = result["ingestionSource"]["executions"][
                "executionRequests"
            ][0]
            # If the latest result is not there yet, OR it is there in running state, it is considered running.
            # TODO: Add a zombie check here. If there is a PENDING run that has not been picked up in some time, we can force it to run again.
            return (
                "result" not in latest_execution
                or latest_execution["result"] is None
                or latest_execution["result"]["status"] == "RUNNING"
            )
        return False

    def get_running_execution_request_urns(
        self, ingestion_source_urn: str
    ) -> List[str]:
        """
        Gets any running execution request URNs for a given ingestion source.
        By default, only checks the last 10 runs.
        """
        query = """
            query($urn: String!) {
                ingestionSource(urn: $urn) {
                    executions(start: 0, count: 10) {
                        executionRequests {
                            urn
                            result {
                                status
                            }
                        }
                    }
                }
            }
        """

        result = self.execute_graphql(query, {"urn": ingestion_source_urn})

        if (
            "ingestionSource" in result
            and len(result["ingestionSource"]["executions"]["executionRequests"]) > 0
        ):
            # Filter for any execution requests that are running
            running_requests = filter(
                lambda request: (
                    "result" in request
                    and request["result"] is not None
                    and request["result"]["status"] == "RUNNING"
                ),
                result["ingestionSource"]["executions"]["executionRequests"],
            )

            # Map the URNs of the running requests
            urns = map(lambda request: request["urn"], running_requests)

            return list(urns)

        return []

    def start_ingestion(self, ingestion_source_urn: str) -> None:
        mutation = """
            mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {
                createIngestionExecutionRequest(input: $input)
            }
        """
        input_value = {"ingestionSourceUrn": ingestion_source_urn}
        self.execute_graphql(mutation, {"input": input_value})

    def clean_up_remote_action_ingestion_source(
        self, ingestion_source_urn: str
    ) -> None:
        """
        Cancel existing runs and delete the remote action ingestion source.
        """
        running_execution_requests = self.get_running_execution_request_urns(
            ingestion_source_urn
        )

        # Cancel each running URN
        for running_execution_request in running_execution_requests:
            self.cancel_remote_action_ingestion_source(
                ingestion_source_urn, running_execution_request
            )

        self.delete_remote_action_ingestion_source(ingestion_source_urn)

    def cancel_remote_action_ingestion_source(
        self, ingestion_source_urn: str, execution_request_urn: str
    ) -> None:
        mutation = """
            mutation cancelIngestionExecutionRequest($ingestionSourceUrn: String!, $executionRequestUrn: String!) {
                cancelIngestionExecutionRequest(input: { ingestionSourceUrn: $ingestionSourceUrn, executionRequestUrn: $executionRequestUrn })
            }
        """
        self.execute_graphql(
            mutation,
            {
                "ingestionSourceUrn": ingestion_source_urn,
                "executionRequestUrn": execution_request_urn,
            },
        )

    def delete_remote_action_ingestion_source(self, ingestion_source_urn: str) -> None:
        mutation = """
            mutation deleteIngestionSource($urn: String!) {
                deleteIngestionSource(urn: $urn)
            }
        """
        self.execute_graphql(mutation, {"urn": ingestion_source_urn})

    # Define a retry strategy using tenacity
    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_exponential(
            multiplier=1, min=4, max=10
        ),  # Exponential backoff between 4 and 10 seconds
        before=before_log(logger, logging.INFO),  # Log before each retry
    )
    def execute_graphql(self, query: str, variables: Dict[str, Any]) -> Any:
        """
        Execute GraphQL queries or mutations with retry logic using tenacity.
        """
        return self.graph.execute_graphql(query=query, variables=variables)

    def build_remote_ingestion_source_recipe_json(
        self, action_urn: str, action_recipe: dict
    ) -> str:
        """
        Generates a remote ingestion source recipe from an action recipe.
        To do this, we simply build a recipe like this:
        source:
            type: datahub_integrations.sources.remote_actions.remote_action_source.RemoteActionSource
            config:
                action_urn: 'urn:li:dataHubAction:remote_hello_world'
                action_spec:
                    type: hello_world
                    config: {}
        """
        config = PipelineConfig.parse_obj(
            action_recipe
        )  # Converts action_recipe to Pydantic model
        action_config = (
            config.action
        )  # Access the action configuration from the Pydantic model

        # Now create a dict with this embedded within
        ingestion_recipe = {
            "source": {
                "type": REMOTE_ACTION_SOURCE_NAME,
                "config": {
                    "action_urn": action_urn,
                    "action_spec": action_config.dict(),
                },
            }
        }

        # Convert the dictionary to JSON string
        return json.dumps(ingestion_recipe)

    def create_remote_ingestion_source_urn(self, action_urn: str) -> str:
        """
        Base64 encodes the action_urn to generate a unique ingestion source URN.
        """
        encoded_bytes = base64.urlsafe_b64encode(action_urn.encode("utf-8"))
        return f"urn:li:dataHubIngestionSource:{encoded_bytes.decode('utf-8')}"
