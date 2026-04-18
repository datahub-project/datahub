import logging
import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    CriterionClass,
    DynamicFormAssignmentClass,
    FilterClass,
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

# Create a form that will be dynamically assigned to all Snowflake datasets
# in the "Finance" domain

# Define the form metadata
form_urn = FormUrn("snowflake_finance_compliance")
form_info = FormInfoClass(
    name="Snowflake Finance Data Compliance",
    description="Compliance form for all Snowflake datasets in the Finance domain",
    type=FormTypeClass.VERIFICATION,
    actors=FormActorAssignmentClass(owners=True),
    prompts=[
        FormPromptClass(
            id="retention_time_prompt",
            title="Data Retention Period",
            description="Specify how long this data should be retained",
            type=FormPromptTypeClass.STRUCTURED_PROPERTY,
            structuredPropertyParams=StructuredPropertyParamsClass(
                urn="urn:li:structuredProperty:io.acryl.dataRetentionTime"
            ),
            required=True,
        ),
        FormPromptClass(
            id="pii_classification_prompt",
            title="PII Classification",
            description="Classify whether this dataset contains PII",
            type=FormPromptTypeClass.STRUCTURED_PROPERTY,
            structuredPropertyParams=StructuredPropertyParamsClass(
                urn="urn:li:structuredProperty:io.acryl.piiClassification"
            ),
            required=True,
        ),
    ],
)

# Define dynamic assignment filter
# This form will be assigned to all entities matching these criteria
dynamic_assignment = DynamicFormAssignmentClass(
    filter=FilterClass(
        criteria=[
            CriterionClass(
                field="platform",
                value="urn:li:dataPlatform:snowflake",
                condition="EQUAL",
            ),
            CriterionClass(
                field="domains",
                value="urn:li:domain:finance",
                condition="EQUAL",
            ),
            CriterionClass(
                field="_entityType",
                value="urn:li:entityType:dataset",
                condition="EQUAL",
            ),
        ]
    )
)

# Create rest emitter
rest_emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)

# Emit the form info
form_info_event = MetadataChangeProposalWrapper(
    entityUrn=str(form_urn),
    aspect=form_info,
)
rest_emitter.emit(form_info_event)

# Emit the dynamic assignment
dynamic_assignment_event = MetadataChangeProposalWrapper(
    entityUrn=str(form_urn),
    aspect=dynamic_assignment,
)
rest_emitter.emit(dynamic_assignment_event)

log.info(
    f"Created form {form_urn} with dynamic assignment to Snowflake datasets in Finance domain"
)
