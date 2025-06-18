#!/usr/bin/env python3

import logging
import os
import sys
import time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import (
    DataHubGraph,
    get_default_graph,
)
from datahub.metadata.schema_classes import (
    RemoteExecutorPoolInfoClass,
    RemoteExecutorPoolKeyClass,
    RemoteExecutorPoolStateClass,
    RemoteExecutorPoolStatusClass,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_executor_pool(graph: DataHubGraph, name: str) -> None:
    logger.info(f"Creating executor pool with name: {name}")

    aspect = RemoteExecutorPoolInfoClass(
        createdAt=int(time.time() * 1000),
        isEmbedded=False,
        state=RemoteExecutorPoolStateClass(
            status=RemoteExecutorPoolStatusClass.PROVISIONING_PENDING
        ),
    )

    logger.debug("Created aspect with status: PROVISIONING_PENDING")

    mcpw = MetadataChangeProposalWrapper(
        entityKeyAspect=RemoteExecutorPoolKeyClass(id=name),
        entityUrn=f"urn:li:dataHubRemoteExecutorPool:{name}",
        entityType="dataHubRemoteExecutorPool",
        aspectName="dataHubRemoteExecutorPoolInfo",
        aspect=aspect,
        changeType="UPSERT",
    )

    logger.info(
        f"Emitting MCP for executor pool URN: urn:li:dataHubRemoteExecutorPool:{name}"
    )
    try:
        graph.emit_mcp(mcpw)
        logger.info(f"Successfully emitted MCP for executor pool: {name}")
    except Exception as e:
        logger.error(
            f"Failed to emit MCP for executor pool {name}: {str(e)}", exc_info=True
        )
        raise


def get_pool_status(graph: DataHubGraph, name: str) -> RemoteExecutorPoolInfoClass:
    request_urn = f"urn:li:dataHubRemoteExecutorPool:{name}"
    logger.debug(f"Getting pool status for URN: {request_urn}")

    try:
        result = graph.get_aspect(request_urn, RemoteExecutorPoolInfoClass)
        if result is None:
            logger.debug(f"No aspect found for executor pool: {name}")
        else:
            logger.debug(
                f"Retrieved pool status for {name}: {result.state.status if result.state else 'No state'}"
            )
        return result
    except Exception as e:
        logger.error(f"Error getting pool status for {name}: {str(e)}", exc_info=True)
        raise


def wait_for_pool_creation(graph: DataHubGraph, name: str, timeout=60) -> bool:
    logger.info(f"Waiting for pool creation: {name} (timeout: {timeout}s)")
    start_time = time.time()
    last_status = None

    while True:
        elapsed_time = time.time() - start_time

        try:
            res = get_pool_status(graph, name)

            if res is not None:
                current_status = res.state.status if res.state else None

                # Log status changes
                if current_status != last_status:
                    logger.info(
                        f"Pool {name} status changed: {last_status} -> {current_status}"
                    )
                    last_status = current_status

                if current_status == "READY":
                    logger.info(
                        f"Pool {name} is READY after {elapsed_time:.1f} seconds"
                    )
                    return True
                elif current_status in ["FAILED"]:
                    logger.error(
                        f"Pool {name} creation FAILED after {elapsed_time:.1f} seconds"
                    )
                    return False
            else:
                if elapsed_time % 10 == 0:  # Log every 10 seconds
                    logger.debug(
                        f"Still waiting for pool {name} to be created... ({elapsed_time:.0f}s elapsed)"
                    )

            if elapsed_time > timeout:
                logger.error(f"Pool {name} creation timed out after {timeout} seconds")
                return False

        except Exception as e:
            logger.error(
                f"Error while waiting for pool creation: {str(e)}", exc_info=True
            )
            if elapsed_time > timeout:
                return False

        time.sleep(1.0)


def delete_executor_pool(graph: DataHubGraph, name: str) -> None:
    logger.info(f"Deleting executor pool: {name}")
    urn = f"urn:li:dataHubRemoteExecutorPool:{name}"

    try:
        graph.hard_delete_entity(urn)
        logger.info(f"Successfully deleted executor pool: {name}")
    except Exception as e:
        logger.error(f"Failed to delete executor pool {name}: {str(e)}", exc_info=True)
        raise


# Main execution
if __name__ == "__main__":
    logger.info("Starting remote executor pool setup")

    executor_id = os.environ.get("DATAHUB_SMOKETEST_EXECUTOR_ID")

    if executor_id is None or executor_id == "":
        logger.error("DATAHUB_SMOKETEST_EXECUTOR_ID is not set. Skipping setup")
        sys.exit(0)

    logger.info(f"Using executor ID: {executor_id}")

    try:
        logger.info("Getting DataHub graph client")
        graph = get_default_graph()
        logger.info("Successfully connected to DataHub graph")

        # Create the pool
        create_executor_pool(graph, executor_id)

        # Wait for it to be ready
        success = wait_for_pool_creation(graph, executor_id)

        if success:
            logger.info(f"Remote executor pool {executor_id} is ready for use")
            sys.exit(0)
        else:
            logger.error(f"Failed to create remote executor pool {executor_id}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Unexpected error in main execution: {str(e)}", exc_info=True)
        sys.exit(1)
