import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import FormAssociationClass, FormsClass
from datahub.metadata.urns import DatasetUrn, FormUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Form to assign
form_urn = FormUrn("metadata_initiative_2024")

# Entities to assign the form to
dataset_urns = [
    DatasetUrn(platform="snowflake", name="prod.analytics.customer_data", env="PROD"),
    DatasetUrn(platform="snowflake", name="prod.analytics.sales_data", env="PROD"),
]

# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

# Assign the form to each entity by updating the forms aspect
for dataset_urn in dataset_urns:
    # Create a forms aspect with the form marked as incomplete
    forms_aspect = FormsClass(
        incompleteForms=[FormAssociationClass(urn=str(form_urn))],
        completedForms=[],
        verifications=[],
    )

    # Emit the forms aspect for the entity
    event = MetadataChangeProposalWrapper(
        entityUrn=str(dataset_urn),
        aspect=forms_aspect,
    )
    rest_emitter.emit(event)
    log.info(f"Assigned form {form_urn} to entity {dataset_urn}")

log.info(f"Successfully assigned form to {len(dataset_urns)} entities")
