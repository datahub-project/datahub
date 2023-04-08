import logging
import time
from pprint import pprint

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper

# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetPropertiesClass,

)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

dataset_urn = make_dataset_urn(platform="hive", name="realestate_db.sales", env="PROD")

# Some helpful variables to fill out objects later
now = int(time.time() * 1000)  # milliseconds since epoch
current_timestamp = AuditStampClass(time=now, actor="urn:li:corpuser:ingestion")
custom_properties_to_add = {"key": "value"}

gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(config=DatahubClientConfig(server=gms_endpoint))

# check if there are existing custom properties. If you want to overwrite the current ones, you can comment out this part.
if graph.get_aspect(entity_urn=dataset_urn, aspect_type=DatasetPropertiesClass):
    existing_custom_properties = graph.get_aspect(entity_urn=dataset_urn, aspect_type=DatasetPropertiesClass).customProperties
    custom_properties_to_add.update(existing_custom_properties)

custom_properties_class = DatasetPropertiesClass(
    customProperties=custom_properties_to_add,
)
event = MetadataChangeProposalWrapper(
    entityUrn=dataset_urn,
    aspect=custom_properties_class,
)
graph.emit(event)
log.info(f"Custom Properties {custom_properties_to_add} added to dataset {dataset_urn}")
