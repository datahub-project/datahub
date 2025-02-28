import logging

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

log = logging.getLogger(__name__)

graph = DataHubGraph(
    config=DatahubClientConfig(
        server="http://localhost:8080",
    )
)

new_assertion_urn = "urn:li:assertion:my-unique-assertion-id"

# Upsert the assertion
res = graph.upsert_custom_assertion(
    urn=new_assertion_urn,  # If the assertion already exists, provide the URN
    entity_urn="<urn of entity being monitored>",
    type="My Custom Category",  # This categorizes your assertion in DataHub
    description="The description of my external assertion for my dataset",
    platform_urn="urn:li:dataPlatform:great-expectations",  # OR you can provide 'platformName="My Custom Platform"'
    field_path="field_foo",  # Optional: if you want to associate it with a specific field
    external_url="https://my-monitoring-tool.com/result-for-this-assertion",  # Optional: link to monitoring tool
    logic="SELECT * FROM X WHERE Y",  # Optional: custom SQL for the assertion, rendered in the UI
)

if res is not None:
    log.info(f"Upserted assertion with urn: {new_assertion_urn}")
