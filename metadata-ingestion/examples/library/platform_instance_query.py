# metadata-ingestion/examples/library/platform_instance_query.py
import json
import os

import datahub.emitter.mce_builder as builder
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Create a DataHub graph client
gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
config = DatahubClientConfig(server=gms_server, token=token)
graph = DataHubGraph(config)

# Create the platform instance URN
platform_instance_urn = builder.make_dataplatform_instance_urn(
    platform="mysql", instance="production-mysql-cluster"
)

# Check if the platform instance exists
if graph.exists(platform_instance_urn):
    print(f"Platform instance exists: {platform_instance_urn}\n")

    # Get the full entity with all aspects
    entity = graph.get_entity_semityped(platform_instance_urn)

    # Access the key aspect to get platform and instance ID
    if "dataPlatformInstanceKey" in entity:
        key_aspect = entity["dataPlatformInstanceKey"]
        print(f"Platform: {key_aspect.platform}")
        print(f"Instance ID: {key_aspect.instance}\n")

    # Access properties
    if "dataPlatformInstanceProperties" in entity:
        props = entity["dataPlatformInstanceProperties"]
        print(f"Name: {props.name}")
        print(f"Description: {props.description}")
        if props.customProperties:
            print("Custom Properties:")
            for key, value in props.customProperties.items():
                print(f"  {key}: {value}")
        if props.externalUrl:
            print(f"External URL: {props.externalUrl}")
        print()

    # Access ownership
    if "ownership" in entity:
        ownership = entity["ownership"]
        print("Owners:")
        for owner in ownership.owners:
            print(f"  - {owner.owner} ({owner.type})")
        print()

    # Access tags
    if "globalTags" in entity:
        global_tags = entity["globalTags"]
        print("Tags:")
        for tag_association in global_tags.tags:
            print(f"  - {tag_association.tag}")
        print()

    # Access institutional memory (links)
    if "institutionalMemory" in entity:
        institutional_memory = entity["institutionalMemory"]
        print("Links:")
        for element in institutional_memory.elements:
            print(f"  - {element.description}: {element.url}")
        print()

    # Get raw aspects using REST API for complete data
    raw_entity = graph.get_entity_raw(
        entity_urn=platform_instance_urn,
        aspects=[
            "dataPlatformInstanceKey",
            "dataPlatformInstanceProperties",
            "ownership",
            "globalTags",
            "institutionalMemory",
            "deprecation",
            "status",
        ],
    )

    print("Raw entity data:")
    print(json.dumps(raw_entity, indent=2))

else:
    print(f"Platform instance does not exist: {platform_instance_urn}")
