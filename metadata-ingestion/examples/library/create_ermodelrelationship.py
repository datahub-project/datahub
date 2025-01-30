import time

from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ERModelRelationshipCardinalityClass,
    ERModelRelationshipKeyClass,
    ERModelRelationshipPropertiesClass,
    NumberTypeClass,
    OtherSchemaClass,
    RelationshipFieldMappingClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)

# Configuration
GMS_ENDPOINT = "http://localhost:8080"
PLATFORM = "mysql"
ENV = "PROD"

e = DatahubRestEmitter(gms_server=GMS_ENDPOINT, extra_headers={})


def get_schema_field(
    name: str, dtype: str, type: SchemaFieldDataTypeClass
) -> SchemaFieldClass:
    """Creates a schema field for MySQL columns."""

    field = SchemaFieldClass(
        fieldPath=name,
        type=type,
        nativeDataType=dtype,
        description=name,
        lastModified=AuditStampClass(
            time=int(time.time() * 1000),
            actor="urn:li:corpuser:ingestion",
        ),
    )
    if name == "id":
        field.isPartitioningKey = True
    return field


# Define Employee Table
dataset_employee = make_dataset_urn(PLATFORM, "Employee", ENV)
employee_fields = [
    get_schema_field("id", "int", SchemaFieldDataTypeClass(type=NumberTypeClass())),
    get_schema_field(
        "name", "varchar", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
    get_schema_field("age", "int", SchemaFieldDataTypeClass(type=NumberTypeClass())),
    get_schema_field(
        "company_id", "int", SchemaFieldDataTypeClass(type=NumberTypeClass())
    ),
]

e.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=dataset_employee,
        aspect=SchemaMetadataClass(
            schemaName="Employee",
            platform=make_data_platform_urn(PLATFORM),
            fields=employee_fields,
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
        ),
    )
)

# Define Company Table
dataset_company = make_dataset_urn(PLATFORM, "Company", ENV)
company_fields = [
    get_schema_field("id", "int", SchemaFieldDataTypeClass(type=NumberTypeClass())),
    get_schema_field(
        "name", "varchar", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
]

e.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=dataset_company,
        aspect=SchemaMetadataClass(
            schemaName="Company",
            platform=make_data_platform_urn(PLATFORM),
            fields=company_fields,
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
        ),
    )
)

# Establish Relationship (Foreign Key: Employee.company_id → Company.id)
relationship_key = ERModelRelationshipKeyClass(id="employee_to_company")
relationship_properties = ERModelRelationshipPropertiesClass(
    name="Employee to Company Relationship",
    source=dataset_employee,
    destination=dataset_company,
    relationshipFieldMappings=[
        RelationshipFieldMappingClass(sourceField="company_id", destinationField="id")
    ],
    cardinality=ERModelRelationshipCardinalityClass.N_ONE,
    customProperties={"constraint": "Foreign Key", "index": "company_id"},
)

relationship_urn = f"urn:li:erModelRelationship:{relationship_key.id}"

e.emit_mcp(
    MetadataChangeProposalWrapper(
        entityType="erModelRelationship",
        changeType="UPSERT",
        entityKeyAspect=relationship_key,
        aspectName=relationship_key.ASPECT_NAME,
        aspect=relationship_key,
    )
)

e.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=relationship_urn,
        entityType="erModelRelationship",
        changeType="UPSERT",
        aspectName=relationship_properties.ASPECT_NAME,
        aspect=relationship_properties,
    )
)

print("relationship_urn", relationship_urn)
print("Employee and Company tables created with ERModelRelationship linking them.")
