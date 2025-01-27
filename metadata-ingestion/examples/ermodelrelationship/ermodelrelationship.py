import time
from typing import Any, Dict, Union

from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BooleanTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    ERModelRelationshipCardinalityClass,
    ERModelRelationshipKeyClass,
    ERModelRelationshipPropertiesClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    RelationshipFieldMappingClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    UnionTypeClass,
)

gms_endpoint = "http://localhost:8080"

# Set up the DataHub emitter
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})


# Define constants for node and relationship types
NODE = "node"
RELATIONSHIP = "relationship"
PLATFORM = "neo4j"
ENV = "PROD"

# Define type mapping for Neo4j property types
_type_mapping: Dict[Union[type, str], type] = {
    "list": UnionTypeClass,
    "boolean": BooleanTypeClass,
    "integer": NumberTypeClass,
    "local_date_time": DateTypeClass,
    "float": NumberTypeClass,
    "string": StringTypeClass,
    "date": DateTypeClass,
    NODE: StringTypeClass,
    RELATIONSHIP: StringTypeClass,
}


def get_field_type(attribute_type: Union[type, str]) -> SchemaFieldDataTypeClass:
    """
    Map attribute types to SchemaFieldDataTypeClass.
    """
    type_class: type = _type_mapping.get(attribute_type, NullTypeClass)
    return SchemaFieldDataTypeClass(type=type_class())


def get_schema_field_class(
    col_name: str, col_type: str, **kwargs: Any
) -> SchemaFieldClass:
    """
    Create a SchemaFieldClass for a Neo4j property or relationship.
    """
    if kwargs.get("obj_type") == NODE and col_type == RELATIONSHIP:
        col_type = NODE  # Adjust type if it's a relationship within a node
    return SchemaFieldClass(
        fieldPath=col_name,
        type=get_field_type(col_type),
        nativeDataType=col_type,
        description=col_type.upper() if col_type in (NODE, RELATIONSHIP) else col_type,
        lastModified=AuditStampClass(
            time=round(time.time() * 1000),
            actor="urn:li:corpuser:ingestion",
        ),
    )


def create_dataset_for_neo4j_node(node_name: str, properties: dict) -> str:
    dataset_urn = make_dataset_urn(platform=PLATFORM, name=node_name, env=ENV)

    schema_fields = [
        get_schema_field_class(col_name=key, col_type=col_type, obj_type=NODE)
        for key, col_type in properties.items()
    ]

    # Create schema metadata for the node
    schema_metadata = SchemaMetadataClass(
        schemaName=node_name,
        platform=make_data_platform_urn(PLATFORM),
        fields=schema_fields,
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=schema_metadata,
        changeType="UPSERT",
        aspectName=schema_metadata.ASPECT_NAME,
    )

    emitter.emit_mcp(mcp)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=SubTypesClass(typeNames=[DatasetSubTypes.NEO4J_NODE]),
    )
    emitter.emit_mcp(mcp)

    dataset_properties = DatasetPropertiesClass(
        description=node_name,
        customProperties={"node_name": node_name},
    )
    mcp = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=dataset_properties,
    )
    emitter.emit_mcp(mcp)
    return dataset_urn


def create_dataset_for_neo4j_relationship(
    relationship_name: str,
    properties: dict,
    source_node: str,
    target_node: str,
) -> str:
    dataset_urn = make_dataset_urn(platform=PLATFORM, name=relationship_name, env=ENV)

    schema_fields = [
        get_schema_field_class(col_name=key, col_type=col_type, obj_type=RELATIONSHIP)
        for key, col_type in properties.items()
    ]

    schema_metadata = SchemaMetadataClass(
        schemaName=relationship_name,
        platform=make_data_platform_urn(PLATFORM),
        fields=schema_fields,
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
    )

    relationship_properties = DatasetPropertiesClass(
        description=f"Relationship '{relationship_name}' connects '{source_node}' to '{target_node}' in Neo4j.",
        customProperties={"source_node": source_node, "target_node": target_node},
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=schema_metadata,
    )
    emitter.emit_mcp(mcp)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        changeType="UPSERT",
        aspect=relationship_properties,
        aspectName=relationship_properties.ASPECT_NAME,
    )
    emitter.emit_mcp(mcp)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=SubTypesClass(typeNames=[DatasetSubTypes.NEO4J_RELATIONSHIP]),
    )
    emitter.emit_mcp(mcp)

    return dataset_urn


# Example usage
if __name__ == "__main__":
    # Define example nodes
    person_node_urn = create_dataset_for_neo4j_node(
        node_name="Person",
        properties={"name": "string", "age": "integer"},
    )
    company_node_urn = create_dataset_for_neo4j_node(
        node_name="Company",
        properties={"name": "string", "industry": "string"},
    )

    # Define example relationship
    works_at_relationship = create_dataset_for_neo4j_relationship(
        relationship_name="WORKS_AT",
        properties={"since": "date"},
        source_node="Person",
        target_node="Company",
    )

    # Print the MetadataChangeEvents
    print("Person Node URN:")
    print(person_node_urn)
    print("\nCompany Node URN:")
    print(company_node_urn)
    print("\nWORKS_AT Relationship URN:")
    print(works_at_relationship)

    field_mappings = [
        RelationshipFieldMappingClass(
            sourceField="name",
            destinationField="name",
        )
    ]

    relationship_key = ERModelRelationshipKeyClass(id="person_to_company")
    relationship_properties = ERModelRelationshipPropertiesClass(
        name="Person to Company Relationship",
        source=person_node_urn,
        destination=company_node_urn,
        relationshipFieldMappings=field_mappings,
        cardinality=ERModelRelationshipCardinalityClass.ONE_ONE,
        customProperties={"business_unit": "Sales", "priority": "High"},
    )

    mcp_key = MetadataChangeProposalWrapper(
        entityType="erModelRelationship",
        changeType="UPSERT",
        entityKeyAspect=relationship_key,
        aspectName=relationship_key.ASPECT_NAME,
        aspect=relationship_key,
    )

    # Emit the relationship key
    emitter.emit_mcp(mcp_key)

    # Create a Metadata Change Proposal for relationship properties
    urn = f"urn:li:erModelRelationship:{relationship_key.id}"
    mcp_properties = MetadataChangeProposalWrapper(
        entityUrn=urn,
        entityType="erModelRelationship",
        changeType="UPSERT",
        aspectName=relationship_properties.ASPECT_NAME,
        aspect=relationship_properties,
    )

    # Emit the relationship properties
    emitter.emit_mcp(mcp_properties)
    print("\nER Model Relationship URN:")
    print(urn)
