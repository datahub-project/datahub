import logging

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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

if data_product:
    log.info(f"Successfully retrieved Data Product: {data_product_urn}")

    properties = data_product.get("dataProductProperties")
    if properties:
        log.info(f"Name: {properties.get('name')}")
        log.info(f"Description: {properties.get('description')}")

        assets = properties.get("assets", [])
        log.info(f"Number of assets: {len(assets)}")
        for asset in assets:
            asset_urn = asset.get("destinationUrn")
            is_output_port = asset.get("outputPort", False)
            log.info(f"  - Asset: {asset_urn} (Output Port: {is_output_port})")

    domains = data_product.get("domains")
    if domains:
        domain_urns = domains.get("domains", [])
        log.info(f"Domain: {domain_urns}")

    ownership = data_product.get("ownership")
    if ownership:
        owners = ownership.get("owners", [])
        log.info(f"Number of owners: {len(owners)}")
        for owner in owners:
            log.info(f"  - Owner: {owner.get('owner')} (Type: {owner.get('type')})")

    tags = data_product.get("globalTags")
    if tags:
        tag_list = tags.get("tags", [])
        log.info(f"Tags: {[t.get('tag') for t in tag_list]}")

    terms = data_product.get("glossaryTerms")
    if terms:
        term_list = terms.get("terms", [])
        log.info(f"Glossary Terms: {[t.get('urn') for t in term_list]}")
else:
    log.error(f"Data Product not found: {data_product_urn}")
