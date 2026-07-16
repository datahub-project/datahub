import logging
import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    FormActorAssignmentClass,
    FormInfoClass,
    FormPromptClass,
    FormPromptTypeClass,
    FormTypeClass,
    StructuredPropertyParamsClass,
)
from datahub.metadata.urns import FormUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Use the structured property created by structured_property_create_basic.py
retention_property_urn = "urn:li:structuredProperty:io.acryl.privacy.retentionTime"

# define the prompts for our form
prompt_1 = FormPromptClass(
    id="1",  # ensure IDs are globally unique
    title="Data Retention Policy",
    type=FormPromptTypeClass.STRUCTURED_PROPERTY,  # structured property type prompt
    structuredPropertyParams=StructuredPropertyParamsClass(urn=retention_property_urn),
    required=True,
)
prompt_2 = FormPromptClass(
    id="2",  # ensure IDs are globally unique
    title="Field-Level Retention",
    type=FormPromptTypeClass.FIELDS_STRUCTURED_PROPERTY,  # structured property prompt on dataset schema fields
    structuredPropertyParams=StructuredPropertyParamsClass(urn=retention_property_urn),
    required=False,  # dataset schema fields prompts should not be required
)

form_urn = FormUrn("metadata_initiative_1")
form_info_aspect = FormInfoClass(
    name="Metadata Initiative 2024",
    description="Please respond to this form for metadata compliance purposes",
    type=FormTypeClass.VERIFICATION,
    actors=FormActorAssignmentClass(owners=True),
    prompts=[prompt_1, prompt_2],
)

event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=str(form_urn),
    aspect=form_info_aspect,
)

# Create rest emitter
rest_emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)
rest_emitter.emit_mcp(event)
print(f"Created form: {form_urn}")
