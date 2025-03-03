import time

from datahub.metadata.schema_classes import (
    AuditStampClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
)
from datahub.sdk import DataHubClient, DatasetUrn

client = DataHubClient.from_env()

dataset = client.entities.get(DatasetUrn(platform="hive", name="realestate_db.sales"))

# Add dataset documentation
documentation = """## The Real Estate Sales Dataset
This is a really important Dataset that contains all the relevant information about sales that have happened organized by address.
"""
dataset.set_description(documentation)

# Add link to institutional memory
current_timestamp = AuditStampClass(
    time=int(time.time() * 1000),  # milliseconds since epoch
    actor="urn:li:corpuser:ingestion",
)

memory_metadata = InstitutionalMemoryMetadataClass(
    url="https://wikipedia.com/real_estate",
    description="This is the definition of what real estate means",
    createStamp=current_timestamp,
)
dataset._set_aspect(InstitutionalMemoryClass(elements=[memory_metadata]))

client.entities.update(dataset)
