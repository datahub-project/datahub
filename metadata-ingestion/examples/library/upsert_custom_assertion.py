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
new_assertion_urn = graph.upsert_custom_assertion(
    urn=new_assertion_urn,  # If the assertion already exists, provide the URN
    entityUrn="<urn of entity being monitored>",
    type="My Custom Category",  # This categorizes your assertion in DataHub
    description="The description of my external assertion for my dataset",
    platformUrn="urn:li:dataPlatform:great-expectations",  # OR you can provide 'platformName="My Custom Platform"'
    fieldPath="field_foo",  # Optional: if you want to associate it with a specific field
    externalUrl="https://my-monitoring-tool.com/result-for-this-assertion",  # Optional: link to monitoring tool
    logic="SELECT * FROM X WHERE Y",  # Optional: custom SQL for the assertion, rendered in the UI
).get("urn")

if new_assertion_urn is not None:
    log.info(f"Upserted assertion URN: {new_assertion_urn}")
