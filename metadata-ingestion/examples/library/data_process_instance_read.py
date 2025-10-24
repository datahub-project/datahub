from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Create a graph client to query DataHub
graph = DataHubGraph(config=DatahubClientConfig(server="http://localhost:8080"))

# Query for process instances of a specific DataJob
datajob_urn = (
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,sales_pipeline,prod),process_sales_data)"
)

print(f"Querying process instances for DataJob: {datajob_urn}")

# Get incoming relationships of type "InstanceOf"
relationships = graph.get_related_entities(
    entity_urn=datajob_urn,
    relationship_types=["InstanceOf"],
    direction=DataHubGraph.RelationshipDirection.INCOMING,
)

relationships_list = list(relationships)
print(f"\nFound {len(relationships_list)} process instances:")

# Fetch details for each process instance
for rel in relationships_list[:5]:  # Show first 5 instances
    instance_urn = rel.urn
    print(f"\n  Instance URN: {instance_urn}")

    # Get the entity details
    entity_raw = graph.get_entity_raw(
        entity_urn=instance_urn, aspects=["dataProcessInstanceProperties"]
    )

    if entity_raw:
        properties = entity_raw.get("aspects", {}).get(
            "dataProcessInstanceProperties", {}
        )
        if properties and "value" in properties:
            prop_value = properties["value"]
            print(f"    Name: {prop_value.get('name')}")
            print(f"    Type: {prop_value.get('type')}")
            if prop_value.get("created"):
                print(f"    Created: {prop_value.get('created', {}).get('time')}")
            if prop_value.get("externalUrl"):
                print(f"    URL: {prop_value.get('externalUrl')}")
            if prop_value.get("customProperties"):
                print(f"    Custom Properties: {prop_value.get('customProperties')}")

# Query for all instances across the platform (with pagination)
print("\n\nQuerying all DataProcessInstance entities (first 10):")

search_results = list(
    graph.get_urns_by_filter(
        entity_types=["dataProcessInstance"],
        query="*",
    )
)

print(f"Total instances found: {len(search_results)}")
for urn in search_results[:10]:
    print(f"  - {urn}")
