import logging

from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    BusinessAttributeAssociationClass,
    BusinessAttributesClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

dataset_urn = make_dataset_urn(platform="hive", name="realestate_db.sales", env="PROD")
schema_field_urn = make_schema_field_urn(
    parent_urn=dataset_urn, field_path="customer_id"
)

business_attribute_urn = "urn:li:businessAttribute:customer_id"

business_attributes = BusinessAttributesClass(
    businessAttribute=BusinessAttributeAssociationClass(
        businessAttributeUrn=business_attribute_urn
    )
)

event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=schema_field_urn,
    aspect=business_attributes,
)

rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)
log.info(
    f"Applied business attribute {business_attribute_urn} to field {schema_field_urn}"
)
