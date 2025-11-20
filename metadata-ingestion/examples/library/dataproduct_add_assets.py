import logging

from datahub.emitter.mce_builder import make_data_product_urn, make_dataset_urn
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.specific.dataproduct import DataProductPatchBuilder

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

data_product_urn = make_data_product_urn("customer_360")

new_assets = [
    make_dataset_urn(
        platform="snowflake",
        name="customer_db.public.customer_orders",
        env="PROD",
    ),
    make_dataset_urn(
        platform="snowflake",
        name="customer_db.public.customer_support_tickets",
        env="PROD",
    ),
]

for mcp in (
    DataProductPatchBuilder(data_product_urn)
    .add_asset(new_assets[0])
    .add_asset(new_assets[1])
    .build()
):
    rest_emitter.emit(mcp)
    log.info(f"Added assets to Data Product {data_product_urn}")
