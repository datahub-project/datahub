# Imports for urn construction utility methods
from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DateTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)

event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityType="dataset",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=make_dataset_urn(platform="hive", name="realestate_db.sales", env="PROD"),
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
                nativeDataType="VARCHAR(50)",  # use this to provide the type of the field in the source system's vernacular
                description="This is the zipcode of the address. Specified using extended form and limited to addresses in the United States",
            ),
            SchemaFieldClass(
                fieldPath="address.street",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="VARCHAR(100)",
                description="Street corresponding to the address",
            ),
            SchemaFieldClass(
                fieldPath="last_sold_date",
                type=SchemaFieldDataTypeClass(type=DateTypeClass()),
                nativeDataType="Date",
                description="Date of the last sale date for this property",
            ),
        ],
    ),
)

# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)
