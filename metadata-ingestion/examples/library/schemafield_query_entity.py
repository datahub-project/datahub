from typing import Any, cast

import datahub.emitter.mce_builder as builder
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

dataset_urn = builder.make_dataset_urn(
    platform="postgres", name="public.customers", env="PROD"
)

field_urn = builder.make_schema_field_urn(
    parent_urn=dataset_urn, field_path="email_address"
)

entity = graph.get_entity_semityped(entity_urn=field_urn)

if entity:
    print(f"Schema Field URN: {field_urn}")
    print(f"Entity Type: {entity.get('entityType')}")

    aspects = cast(dict[str, Any], entity.get("aspects", {}))

    if "globalTags" in aspects:
        tags = aspects["globalTags"]["tags"]
        print(f"Tags: {[tag['tag'] for tag in tags]}")

    if "glossaryTerms" in aspects:
        terms = aspects["glossaryTerms"]["terms"]
        print(f"Glossary Terms: {[term['urn'] for term in terms]}")

    if "documentation" in aspects:
        docs = aspects["documentation"]["documentations"]
        for doc in docs:
            print(f"Documentation: {doc['documentation'][:100]}...")

    if "structuredProperties" in aspects:
        props = aspects["structuredProperties"]["properties"]
        for prop in props:
            print(f"Property {prop['propertyUrn']}: {prop['values']}")
else:
    print(f"Schema field {field_urn} not found")
