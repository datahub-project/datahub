import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import FormsClass
from datahub.metadata.urns import DatasetUrn, FormUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Form to remove
form_urn = FormUrn("metadata_initiative_2024")

# Entities to remove the form from
dataset_urns = [
    DatasetUrn(platform="snowflake", name="prod.analytics.customer_data", env="PROD"),
    DatasetUrn(platform="snowflake", name="prod.analytics.sales_data", env="PROD"),
]

# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

# Remove the form from each entity by setting empty forms aspect
for dataset_urn in dataset_urns:
    # Create an empty forms aspect
    # This will remove all form assignments from the entity
    forms_aspect = FormsClass(
        incompleteForms=[],
        completedForms=[],
        verifications=[],
    )

    # Emit the forms aspect for the entity
    event = MetadataChangeProposalWrapper(
        entityUrn=str(dataset_urn),
        aspect=forms_aspect,
    )
    rest_emitter.emit(event)
    log.info(f"Removed forms from entity {dataset_urn}")

log.info(f"Successfully removed forms from {len(dataset_urns)} entities")
