from datahub.api.entities.dataproduct.dataproduct import DataProduct
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

data_product = DataProduct(
    id="customer_360",
    display_name="Customer 360",
    domain="urn:li:domain:marketing",
    description="A comprehensive view of customer data including profiles, transactions, and behaviors.",
    assets=[
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_db.public.customer_profile,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_db.public.customer_transactions,PROD)",
        "urn:li:dashboard:(looker,customer_overview)",
    ],
    output_ports=[
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_db.public.customer_profile,PROD)"
    ],
    owners=[
        {"id": "urn:li:corpuser:datahub", "type": "BUSINESS_OWNER"},
        {"id": "urn:li:corpuser:jdoe", "type": "TECHNICAL_OWNER"},
    ],
    terms=["urn:li:glossaryTerm:CustomerData"],
    tags=["urn:li:tag:production"],
    properties={"tier": "gold", "sla": "99.9%"},
    external_url="https://wiki.company.com/customer-360",
)

for mcp in data_product.generate_mcp(upsert=True):
    graph.emit(mcp)

print(f"Created Data Product: urn:li:dataProduct:{data_product.id}")
