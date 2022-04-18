# Imports for urn construction utility methods

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn,
    make_tag_urn,
    make_term_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TagAssociationClass,
)

event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityType="dataset",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=make_dataset_urn(platform="hive", name="foodb.barTable", env="PROD"),
    aspectName="schemaMetadata",
    aspect=SchemaMetadataClass(
        schemaName="customer",  # not used
        platform=make_data_platform_urn("hive"),  # important <- platform must be an urn
        version=0,  # when the source system has a notion of versioning of schemas, insert this in, otherwise leave as 0
        hash="",  # when the source system has a notion of unique schemas identified via hash, include a hash, else leave it as empty string
        platformSchema=OtherSchemaClass(rawSchema="__insert raw schema here__"),
        fields=[
            SchemaFieldClass(
                fieldPath="address.zipcode",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="VARCHAR(100)",  # use this to provide the type of the field in the source system's vernacular
                jsonPath="",  # Unused field, can omit
                nullable=True,
                description="This is the zipcode of the address. Specified using extended form and limited to addresses in the United States",
                recursive=False,  # Unused field, can omit
                # It is rare to attach tags to fields as part of the technical schema unless you are purely reflecting state that exists in the source system.
                # For an editable (in UI) version of this, use the editableSchemaMetadata aspect
                globalTags=GlobalTagsClass(
                    tags=[TagAssociationClass(tag=make_tag_urn("location"))]
                ),
                # It is rare to attach glossary terms to fields as part of the technical schema unless you are purely reflecting state that exists in the source system.
                # For an editable (in UI) version of this, use the editableSchemaMetadata aspect
                glossaryTerms=GlossaryTermsClass(
                    terms=[
                        GlossaryTermAssociationClass(
                            urn=make_term_urn("Classification.PII")
                        )
                    ],
                    auditStamp=AuditStampClass(  # represents the time when this term was attached to this field?
                        time=0,  # time in milliseconds, leave as 0 if no time of association is known
                        actor="urn:li:corpuser:ingestion",  # if this is a system provided tag, use a bot user id like ingestion
                    ),
                ),
            )
        ],
    ),
)

# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)
