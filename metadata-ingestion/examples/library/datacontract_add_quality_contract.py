# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/datacontract_add_quality_contract.py
import logging

from datahub.emitter.mce_builder import make_assertion_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    DataContractPropertiesClass,
    DataQualityContractClass,
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

new_quality_assertion_urn_1 = make_assertion_urn("completeness-check-user-id")
new_quality_assertion_urn_2 = make_assertion_urn("validity-check-email-format")

existing_quality = contract_properties.dataQuality or []
existing_quality.extend(
    [
        DataQualityContractClass(assertion=new_quality_assertion_urn_1),
        DataQualityContractClass(assertion=new_quality_assertion_urn_2),
    ]
)

contract_properties.dataQuality = existing_quality

event = MetadataChangeProposalWrapper(
    entityUrn=contract_urn,
    aspect=contract_properties,
)

rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)

log.info(f"Added data quality contracts to {contract_urn}")
