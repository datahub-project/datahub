# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
