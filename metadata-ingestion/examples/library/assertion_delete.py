import logging

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

log = logging.getLogger(__name__)

graph = DataHubGraph(
    config=DatahubClientConfig(
        server="http://localhost:8080",
    )
)

assertion_urn = "urn:li:assertion:my-assertion"

# Delete the Assertion
graph.delete_entity(urn=assertion_urn, hard=True)

log.info(f"Deleted assertion {assertion_urn}")
