# metadata-ingestion/examples/library/ermodelrelationship_create_basic.py
import os
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

GMS_ENDPOINT = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
GMS_TOKEN = os.getenv("DATAHUB_GMS_TOKEN")
PLATFORM = "mysql"
ENV = "PROD"

emitter = DatahubRestEmitter(gms_server=GMS_ENDPOINT, token=GMS_TOKEN)


def create_dataset_with_schema(
    dataset_name: str, fields: list[SchemaFieldClass]
) -> str:
    """Helper function to create a dataset with schema."""
    dataset_urn = make_dataset_urn(PLATFORM, dataset_name, ENV)

    schema_metadata = SchemaMetadataClass(
        schemaName=dataset_name,
        platform=make_data_platform_urn(PLATFORM),
        fields=fields,
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
    )

    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata,
        )
    )

    return dataset_urn


def create_schema_field(
    field_path: str, native_type: str, data_type: SchemaFieldDataTypeClass
) -> SchemaFieldClass:
    """Helper function to create a schema field."""
    return SchemaFieldClass(
        fieldPath=field_path,
        type=data_type,
        nativeDataType=native_type,
        description=f"Field: {field_path}",
        lastModified=AuditStampClass(
            time=int(time.time() * 1000),
            actor="urn:li:corpuser:datahub",
        ),
    )


# Create Employee table
employee_fields = [
    create_schema_field("id", "int", SchemaFieldDataTypeClass(type=NumberTypeClass())),
    create_schema_field(
        "name", "varchar(100)", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
    create_schema_field(
        "email", "varchar(255)", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
    create_schema_field(
        "company_id", "int", SchemaFieldDataTypeClass(type=NumberTypeClass())
    ),
]
employee_urn = create_dataset_with_schema("Employee", employee_fields)
print(f"Created Employee dataset: {employee_urn}")

# Create Company table
company_fields = [
    create_schema_field("id", "int", SchemaFieldDataTypeClass(type=NumberTypeClass())),
    create_schema_field(
        "name", "varchar(200)", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
    create_schema_field(
        "industry", "varchar(100)", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
]
company_urn = create_dataset_with_schema("Company", company_fields)
print(f"Created Company dataset: {company_urn}")

# Create ER Model Relationship
relationship_id = "employee_to_company"
relationship_urn = f"urn:li:erModelRelationship:{relationship_id}"

# Emit the key aspect
relationship_key = ERModelRelationshipKeyClass(id=relationship_id)
emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=relationship_urn,
        aspect=relationship_key,
    )
)

# Emit the properties aspect
relationship_properties = ERModelRelationshipPropertiesClass(
    name="Employee to Company Relationship",
    source=employee_urn,
    destination=company_urn,
    relationshipFieldMappings=[
        RelationshipFieldMappingClass(
            sourceField="company_id",
            destinationField="id",
        )
    ],
    cardinality=ERModelRelationshipCardinalityClass.N_ONE,
    customProperties={
        "constraint_type": "FOREIGN_KEY",
        "on_delete": "CASCADE",
        "on_update": "CASCADE",
    },
    created=AuditStampClass(
        time=int(time.time() * 1000),
        actor="urn:li:corpuser:datahub",
    ),
)

emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=relationship_urn,
        aspect=relationship_properties,
    )
)

print(f"Created ER Model Relationship: {relationship_urn}")
print(
    "This N:1 relationship connects Employee.company_id to Company.id, "
    "representing that many employees belong to one company."
)
