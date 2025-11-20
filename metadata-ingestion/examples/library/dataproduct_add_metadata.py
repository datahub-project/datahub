import logging

from datahub.emitter.mce_builder import (
    make_data_product_urn,
    make_tag_urn,
    make_term_urn,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    GlossaryTermAssociationClass,
    TagAssociationClass,
)
from datahub.specific.dataproduct import DataProductPatchBuilder

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

data_product_urn = make_data_product_urn("customer_360")

for mcp in (
    DataProductPatchBuilder(data_product_urn)
    .add_tag(TagAssociationClass(tag=make_tag_urn("production")))
    .add_tag(TagAssociationClass(tag=make_tag_urn("pii")))
    .add_term(
        GlossaryTermAssociationClass(urn=make_term_urn("CustomerData.PersonalInfo"))
    )
    .build()
):
    rest_emitter.emit(mcp)
    log.info(f"Added metadata to Data Product {data_product_urn}")
