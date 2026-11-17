# metadata-ingestion/examples/library/datacontract_update_status.py

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DataContractStateClass,
    DataContractStatusClass,
)

contract_urn = "urn:li:dataContract:purchases-contract"

contract_status_aspect = DataContractStatusClass(state=DataContractStateClass.ACTIVE)

event = MetadataChangeProposalWrapper(
    entityUrn=contract_urn,
    aspect=contract_status_aspect,
)

rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)

print(f"Updated status of data contract {contract_urn} to ACTIVE")
