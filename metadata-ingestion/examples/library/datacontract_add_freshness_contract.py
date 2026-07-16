# metadata-ingestion/examples/library/datacontract_add_freshness_contract.py
import logging

from datahub.emitter.mce_builder import make_assertion_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    DataContractPropertiesClass,
    FreshnessContractClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

contract_urn = "urn:li:dataContract:purchases-contract"

graph = DataHubGraph(config=DatahubClientConfig(server="http://localhost:8080"))

contract_properties = graph.get_aspect(
    entity_urn=contract_urn,
    aspect_type=DataContractPropertiesClass,
)

if not contract_properties:
    log.error(f"Contract {contract_urn} not found")
    exit(1)

new_freshness_assertion_urn = make_assertion_urn("new-freshness-assertion")

existing_freshness = contract_properties.freshness or []
existing_freshness.append(FreshnessContractClass(assertion=new_freshness_assertion_urn))

contract_properties.freshness = existing_freshness

event = MetadataChangeProposalWrapper(
    entityUrn=contract_urn,
    aspect=contract_properties,
)

rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)

log.info(f"Added freshness contract to {contract_urn}")
