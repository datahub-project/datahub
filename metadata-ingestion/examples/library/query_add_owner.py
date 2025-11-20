# metadata-ingestion/examples/library/query_add_owner.py
import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from datahub.metadata.urns import CorpUserUrn, QueryUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

query_urn = QueryUrn("my-unique-query-id")

graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

existing_ownership = graph.get_aspect(
    entity_urn=query_urn.urn(),
    aspect_type=OwnershipClass,
)

owners = existing_ownership.owners if existing_ownership else []

new_owner = OwnerClass(
    owner=CorpUserUrn("jdoe").urn(),
    type=OwnershipTypeClass.TECHNICAL_OWNER,
)

if new_owner not in owners:
    owners.append(new_owner)

ownership_aspect = OwnershipClass(
    owners=owners,
)

event = MetadataChangeProposalWrapper(
    entityUrn=query_urn.urn(),
    aspect=ownership_aspect,
)

emitter.emit(event)
log.info(f"Added owner to query {query_urn}")
