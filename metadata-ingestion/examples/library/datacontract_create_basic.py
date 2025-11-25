# metadata-ingestion/examples/library/datacontract_create_basic.py
import logging
import os

from datahub.emitter.mce_builder import make_assertion_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DataContractPropertiesClass,
    DataContractStateClass,
    DataContractStatusClass,
    DataQualityContractClass,
    FreshnessContractClass,
    SchemaContractClass,
    StatusClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

dataset_urn = make_dataset_urn(platform="snowflake", name="purchases", env="PROD")

schema_assertion_urn = make_assertion_urn("schema-assertion-for-purchases")
freshness_assertion_urn = make_assertion_urn("freshness-assertion-for-purchases")
quality_assertion_urn_1 = make_assertion_urn("quality-assertion-for-purchases-1")
quality_assertion_urn_2 = make_assertion_urn("quality-assertion-for-purchases-2")

contract_urn = "urn:li:dataContract:purchases-contract"

properties_aspect = DataContractPropertiesClass(
    entity=dataset_urn,
    schema=[SchemaContractClass(assertion=schema_assertion_urn)],
    freshness=[FreshnessContractClass(assertion=freshness_assertion_urn)],
    dataQuality=[
        DataQualityContractClass(assertion=quality_assertion_urn_1),
        DataQualityContractClass(assertion=quality_assertion_urn_2),
    ],
)

status_aspect = StatusClass(removed=False)

contract_status_aspect = DataContractStatusClass(state=DataContractStateClass.ACTIVE)

rest_emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)

for event in MetadataChangeProposalWrapper.construct_many(
    entityUrn=contract_urn,
    aspects=[
        properties_aspect,
        status_aspect,
        contract_status_aspect,
    ],
):
    rest_emitter.emit(event)

log.info(f"Created data contract {contract_urn}")
