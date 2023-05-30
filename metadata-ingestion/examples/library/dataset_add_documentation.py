import logging
import time

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper

# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EditableDatasetPropertiesClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# Inputs -> owner, ownership_type, dataset
documentation_to_add = "## The Real Estate Sales Dataset\nThis is a really important Dataset that contains all the relevant information about sales that have happened organized by address.\n"
link_to_add = "https://wikipedia.com/real_estate"
link_description = "This is the definition of what real estate means"
dataset_urn = make_dataset_urn(platform="hive", name="realestate_db.sales", env="PROD")

# Some helpful variables to fill out objects later
now = int(time.time() * 1000)  # milliseconds since epoch
current_timestamp = AuditStampClass(time=now, actor="urn:li:corpuser:ingestion")
institutional_memory_element = InstitutionalMemoryMetadataClass(
    url=link_to_add,
    description=link_description,
    createStamp=current_timestamp,
)


# First we get the current owners
gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(config=DatahubClientConfig(server=gms_endpoint))

current_editable_properties = graph.get_aspect(
    entity_urn=dataset_urn, aspect_type=EditableDatasetPropertiesClass
)

need_write = False
if current_editable_properties:
    if documentation_to_add != current_editable_properties.description:
        current_editable_properties.description = documentation_to_add
        need_write = True
else:
    # create a brand new editable dataset properties aspect
    current_editable_properties = EditableDatasetPropertiesClass(
        created=current_timestamp, description=documentation_to_add
    )
    need_write = True

if need_write:
    event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=current_editable_properties,
    )
    graph.emit(event)
    log.info(f"Documentation added to dataset {dataset_urn}")

else:
    log.info("Documentation already exists and is identical, omitting write")


current_institutional_memory = graph.get_aspect(
    entity_urn=dataset_urn, aspect_type=InstitutionalMemoryClass
)

need_write = False

if current_institutional_memory:
    if link_to_add not in [x.url for x in current_institutional_memory.elements]:
        current_institutional_memory.elements.append(institutional_memory_element)
        need_write = True
else:
    # create a brand new institutional memory aspect
    current_institutional_memory = InstitutionalMemoryClass(
        elements=[institutional_memory_element]
    )
    need_write = True

if need_write:
    event = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=current_institutional_memory,
    )
    graph.emit(event)
    log.info(f"Link {link_to_add} added to dataset {dataset_urn}")

else:
    log.info(f"Link {link_to_add} already exists and is identical, omitting write")
