# Inlined from /metadata-ingestion/examples/library/ownership_type_list.py

from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig

# Create DataHub client
graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Search for all ownership type entities
# Note: The GraphQL API provides a listOwnershipTypes query, but we can also
# use the search API to find all ownership types
search_query = """
query listOwnershipTypes($input: ListOwnershipTypesInput!) {
  listOwnershipTypes(input: $input) {
    start
    count
    total
    ownershipTypes {
      urn
      type
      info {
        name
        description
      }
    }
  }
}
"""

variables = {
    "input": {
        "start": 0,
        "count": 100,  # Adjust as needed
    }
}

# Execute the GraphQL query
result = graph.execute_graphql(query=search_query, variables=variables)

# Process and display the results
if result and "listOwnershipTypes" in result:
    ownership_types = result["listOwnershipTypes"]["ownershipTypes"]
    total = result["listOwnershipTypes"]["total"]

    print(f"Found {total} ownership types:")
    print("-" * 80)

    for ownership_type in ownership_types:
        urn = ownership_type["urn"]
        name = ownership_type["info"]["name"]
        description = ownership_type["info"].get("description", "No description")

        print(f"URN: {urn}")
        print(f"Name: {name}")
        print(f"Description: {description}")
        print("-" * 80)
else:
    print("No ownership types found or query failed")
