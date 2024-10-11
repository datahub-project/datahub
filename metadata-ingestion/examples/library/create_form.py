import logging

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

# define the prompts for our form
prompt_1 = FormPromptClass(
    id="1",  # ensure IDs are globally unique
    title="First Prompt",
    type=FormPromptTypeClass.STRUCTURED_PROPERTY,  # structured property type prompt
    structuredPropertyParams=StructuredPropertyParamsClass(
        urn="urn:li:structuredProperty:property1"
    ),  # reference existing structured property
    required=True,
)
prompt_2 = FormPromptClass(
    id="2",  # ensure IDs are globally unique
    title="Second Prompt",
    type=FormPromptTypeClass.FIELDS_STRUCTURED_PROPERTY,  # structured property prompt on dataset schema fields
    structuredPropertyParams=StructuredPropertyParamsClass(
        urn="urn:li:structuredProperty:property1"
    ),
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
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)
