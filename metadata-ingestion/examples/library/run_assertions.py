import logging

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

log = logging.getLogger(__name__)

graph = DataHubGraph(
    config=DatahubClientConfig(
        server="http://localhost:8080",
    )
)

assertion_urns = [
    "urn:li:assertion:6e3f9e09-1483-40f9-b9cd-30e5f182694a",
    "urn:li:assertion:9e3f9e09-1483-40f9-b9cd-30e5f182694g",
]

# Run the assertions
assertion_results = graph.run_assertions(urns=assertion_urns, save_result=True).get(
    "results"
)

if assertion_results is not None:
    assertion_result_1 = assertion_results.get(
        "urn:li:assertion:6e3f9e09-1483-40f9-b9cd-30e5f182694a"
    )
    assertion_result_2 = assertion_results.get(
        "urn:li:assertion:9e3f9e09-1483-40f9-b9cd-30e5f182694g"
    )

    log.info(f"Assertion results: {assertion_results}")
    log.info(
        f"Assertion result 1 (SUCCESS / FAILURE / ERROR): {assertion_result_1.get('type')}"
    )
    log.info(
        f"Assertion result 2 (SUCCESS / FAILURE / ERROR): {assertion_result_2.get('type')}"
    )
