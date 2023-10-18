from datahub.api.entities.dataproduct.dataproduct import DataProduct
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

data_product = DataProduct(
    id="pet_of_the_week",
    display_name="Pet of the Week Campagin",
    domain="urn:li:domain:ef39e99a-9d61-406d-b4a8-c70b16380206",
    description="This campaign includes Pet of the Week data.",
    assets=[
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD)",
        "urn:li:dashboard:(looker,baz)",
        "urn:li:dataFlow:(airflow,dag_abc,PROD)",
    ],
    owners=[{"id": "urn:li:corpuser:jdoe", "type": "BUSINESS_OWNER"}],
    terms=["urn:li:glossaryTerm:ClientsAndAccounts.AccountBalance"],
    tags=["urn:li:tag:adoption"],
    properties={"lifecycle": "production", "sla": "7am every day"},
    external_url="https://en.wikipedia.org/wiki/Sloth",
)

for mcp in data_product.generate_mcp(upsert=False):
    graph.emit(mcp)
