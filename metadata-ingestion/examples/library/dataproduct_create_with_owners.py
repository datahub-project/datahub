"""
Example showing how to create a Data Product with multiple owners and custom ownership types.

This example demonstrates the enhanced gen_data_product functionality that supports:
- Multiple owners (owner_urns parameter)
- Custom ownership types (owner_type parameter supports both string constants and custom URNs)
- Assets included in the initial creation (assets parameter)

The function is backward compatible - existing code will continue to work with defaults.

Examples of owner_type values:
- "DATAOWNER" (default)
- "TECHNICAL_OWNER"
- "BUSINESS_OWNER"
- "DATA_STEWARD"
- "urn:li:ownershipType:producer" (custom URN)
"""

import logging

from datahub.emitter.mce_builder import make_dataset_urn, make_user_urn
from datahub.emitter.mcp_builder import DataProductKey, gen_data_product
from datahub.emitter.rest_emitter import DatahubRestEmitter

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

# Create a data product key for deterministic URN generation
data_product_key = DataProductKey(
    platform="my-platform",
    name="customer_analytics_suite",
    env="PROD",
    instance=None,
)

# Define assets
assets = [
    make_dataset_urn(
        platform="snowflake", name="analytics.public.customers", env="PROD"
    ),
    make_dataset_urn(
        platform="snowflake", name="analytics.public.transactions", env="PROD"
    ),
]

# Define multiple owners
owners = [
    make_user_urn("data-product-owner"),
    make_user_urn("analytics-lead"),
    "urn:li:corpGroup:data-platform-team",
]

# Create the data product with all metadata in one call
for wu in gen_data_product(
    data_product_key=data_product_key,
    name="Customer Analytics Suite",
    description="A comprehensive analytics data product for customer insights",
    external_url="https://wiki.company.com/customer-analytics",
    custom_properties={
        "tier": "gold",
        "sla": "99.9%",
        "cost_center": "analytics",
    },
    domain_urn="urn:li:domain:analytics",
    owner_urns=owners,
    owner_type="BUSINESS_OWNER",  # All owners get this type (defaults to DATAOWNER)
    assets=assets,  # Assets included in initial creation
    tags=["production", "pii"],
):
    # gen_data_product yields MetadataWorkUnit, extract the metadata for emission
    rest_emitter.emit(wu.metadata)  # type: ignore[arg-type]

log.info(f"Created Data Product: {data_product_key.as_urn()}")
log.info(f"  - With {len(owners)} owners (BUSINESS_OWNER)")
log.info(f"  - With {len(assets)} assets")
