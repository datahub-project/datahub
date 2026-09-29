from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

graph = DataHubGraph(
    config=DatahubClientConfig(
        server="http://localhost:8080",
    )
)

assertion_urn = "urn:li:assertion:my-assertion"

# Delete the Assertion
graph.delete_entity(urn=assertion_urn, hard=True)

print(f"Deleted assertion {assertion_urn}")
