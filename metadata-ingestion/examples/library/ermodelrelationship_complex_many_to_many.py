# metadata-ingestion/examples/library/ermodelrelationship_complex_many_to_many.py
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

GMS_ENDPOINT = "http://localhost:8080"
PLATFORM = "postgres"
ENV = "PROD"

emitter = DatahubRestEmitter(gms_server=GMS_ENDPOINT, extra_headers={})


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


# Create Student table
student_fields = [
    create_schema_field("id", "int", SchemaFieldDataTypeClass(type=NumberTypeClass())),
    create_schema_field(
        "name", "varchar(100)", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
    create_schema_field(
        "email", "varchar(255)", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
]
student_urn = create_dataset_with_schema("Student", student_fields)
print(f"Created Student dataset: {student_urn}")

# Create Course table
course_fields = [
    create_schema_field("id", "int", SchemaFieldDataTypeClass(type=NumberTypeClass())),
    create_schema_field(
        "code", "varchar(20)", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
    create_schema_field(
        "title", "varchar(200)", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
]
course_urn = create_dataset_with_schema("Course", course_fields)
print(f"Created Course dataset: {course_urn}")

# Create StudentCourse junction table with composite key
student_course_fields = [
    create_schema_field(
        "student_id", "int", SchemaFieldDataTypeClass(type=NumberTypeClass())
    ),
    create_schema_field(
        "course_id", "int", SchemaFieldDataTypeClass(type=NumberTypeClass())
    ),
    create_schema_field(
        "enrollment_date", "date", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
    create_schema_field(
        "grade", "varchar(2)", SchemaFieldDataTypeClass(type=StringTypeClass())
    ),
]
student_course_urn = create_dataset_with_schema("StudentCourse", student_course_fields)
print(f"Created StudentCourse junction table: {student_course_urn}")

# Create relationship: StudentCourse -> Student (many-to-one)
student_relationship_id = "student_course_to_student"
student_relationship_urn = f"urn:li:erModelRelationship:{student_relationship_id}"

student_relationship_key = ERModelRelationshipKeyClass(id=student_relationship_id)
emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=student_relationship_urn,
        aspect=student_relationship_key,
    )
)

student_relationship_properties = ERModelRelationshipPropertiesClass(
    name="StudentCourse to Student Relationship",
    source=student_course_urn,
    destination=student_urn,
    relationshipFieldMappings=[
        RelationshipFieldMappingClass(
            sourceField="student_id",
            destinationField="id",
        )
    ],
    cardinality=ERModelRelationshipCardinalityClass.N_ONE,
    customProperties={
        "constraint_type": "FOREIGN_KEY",
        "part_of_composite_key": "true",
    },
    created=AuditStampClass(
        time=int(time.time() * 1000),
        actor="urn:li:corpuser:datahub",
    ),
)

emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=student_relationship_urn,
        aspect=student_relationship_properties,
    )
)

print(f"Created relationship: {student_relationship_urn}")

# Create relationship: StudentCourse -> Course (many-to-one)
course_relationship_id = "student_course_to_course"
course_relationship_urn = f"urn:li:erModelRelationship:{course_relationship_id}"

course_relationship_key = ERModelRelationshipKeyClass(id=course_relationship_id)
emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=course_relationship_urn,
        aspect=course_relationship_key,
    )
)

course_relationship_properties = ERModelRelationshipPropertiesClass(
    name="StudentCourse to Course Relationship",
    source=student_course_urn,
    destination=course_urn,
    relationshipFieldMappings=[
        RelationshipFieldMappingClass(
            sourceField="course_id",
            destinationField="id",
        )
    ],
    cardinality=ERModelRelationshipCardinalityClass.N_ONE,
    customProperties={
        "constraint_type": "FOREIGN_KEY",
        "part_of_composite_key": "true",
    },
    created=AuditStampClass(
        time=int(time.time() * 1000),
        actor="urn:li:corpuser:datahub",
    ),
)

emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=course_relationship_urn,
        aspect=course_relationship_properties,
    )
)

print(f"Created relationship: {course_relationship_urn}")

print("\nMany-to-many relationship established through junction table:")
print("- Student N:N Course (via StudentCourse junction table)")
print("- StudentCourse has composite primary key (student_id, course_id)")
print("- Each component of the composite key is a foreign key to its respective table")
