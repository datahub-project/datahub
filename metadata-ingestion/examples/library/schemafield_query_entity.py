import datahub.emitter.mce_builder as builder
from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import (
    DocumentationClass,
    StructuredPropertiesClass,
)

graph = get_default_graph()

dataset_urn = builder.make_dataset_urn(
    platform="postgres", name="public.customers", env="PROD"
)

field_urn = builder.make_schema_field_urn(
    parent_urn=dataset_urn, field_path="email_address"
)

# get_entity_semityped() cannot be used to test existence -- it always returns a
# non-empty aspect bag, even for an entity that does not exist. graph.exists() is
# the real probe.
if not graph.exists(field_urn):
    raise SystemExit(f"Schema field not found: {field_urn}")

print(f"Schema Field URN: {field_urn}")

documentation = graph.get_aspect(entity_urn=field_urn, aspect_type=DocumentationClass)
if documentation is not None:
    for doc in documentation.documentations:
        print(f"Documentation: {doc.documentation[:100]}...")

structured_properties = graph.get_aspect(
    entity_urn=field_urn, aspect_type=StructuredPropertiesClass
)
if structured_properties is not None:
    for prop in structured_properties.properties:
        print(f"Property {prop.propertyUrn}: {prop.values}")
