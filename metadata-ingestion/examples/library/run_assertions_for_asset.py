import logging

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

log = logging.getLogger(__name__)

graph = DataHubGraph(
    config=DatahubClientConfig(
        server="http://localhost:8080",
    )
)

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_snowflake_table,PROD)"

# Run all native assertions for the dataset
assertion_results = graph.run_assertions_for_asset(urn=dataset_urn).get("results")

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

# Run a subset of native assertions having a specific tag
important_assertion_tag = "urn:li:tag:my-important-assertion-tag"
assertion_results = graph.run_assertions_for_asset(
    urn=dataset_urn, tag_urns=[important_assertion_tag]
).get("results")
