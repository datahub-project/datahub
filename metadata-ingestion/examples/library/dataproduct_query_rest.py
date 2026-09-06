from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

data_product_urn = "urn:li:dataProduct:customer_360"

data_product = graph.get_entity_raw(
    entity_urn=data_product_urn,
    aspects=[
        "dataProductKey",
        "dataProductProperties",
        "ownership",
        "domains",
        "globalTags",
        "glossaryTerms",
    ],
)

if not data_product:
    raise SystemExit(f"Data Product not found: {data_product_urn}")

print(f"Successfully retrieved Data Product: {data_product_urn}")

properties = data_product.get("dataProductProperties")
if properties:
    print(f"Name: {properties.get('name')}")
    print(f"Description: {properties.get('description')}")

    assets = properties.get("assets", [])
    print(f"Number of assets: {len(assets)}")
    for asset in assets:
        asset_urn = asset.get("destinationUrn")
        is_output_port = asset.get("outputPort", False)
        print(f"  - Asset: {asset_urn} (Output Port: {is_output_port})")

domains = data_product.get("domains")
if domains:
    domain_urns = domains.get("domains", [])
    print(f"Domain: {domain_urns}")

ownership = data_product.get("ownership")
if ownership:
    owners = ownership.get("owners", [])
    print(f"Number of owners: {len(owners)}")
    for owner in owners:
        print(f"  - Owner: {owner.get('owner')} (Type: {owner.get('type')})")

tags = data_product.get("globalTags")
if tags:
    tag_list = tags.get("tags", [])
    print(f"Tags: {[t.get('tag') for t in tag_list]}")

terms = data_product.get("glossaryTerms")
if terms:
    term_list = terms.get("terms", [])
    print(f"Glossary Terms: {[t.get('urn') for t in term_list]}")
