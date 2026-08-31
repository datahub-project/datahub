# metadata-ingestion/examples/library/query_update_properties.py
import logging
import time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import (
    AuditStampClass,
    QueryPropertiesClass,
)
from datahub.metadata.urns import CorpUserUrn, QueryUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

query_urn = QueryUrn("my-unique-query-id")

graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

existing_properties = graph.get_aspect(
    entity_urn=query_urn.urn(),
    aspect_type=QueryPropertiesClass,
)

if not existing_properties:
    log.error(f"Query {query_urn} does not exist or has no properties")
    exit(1)

current_timestamp = int(time.time() * 1000)
actor_urn = CorpUserUrn("datahub")

existing_properties.name = "Updated Query Name"
existing_properties.description = "This query has been updated with new documentation"
existing_properties.lastModified = AuditStampClass(
    time=current_timestamp, actor=actor_urn.urn()
)

event = MetadataChangeProposalWrapper(
    entityUrn=query_urn.urn(),
    aspect=existing_properties,
)

emitter.emit(event)
log.info(f"Updated properties for query {query_urn}")
