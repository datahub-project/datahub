# metadata-ingestion/examples/library/dataflow_search.py
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.sdk import DataHubClient
from datahub.sdk.search_filters import FilterDsl as F

graph = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))

# Search for DataFlows by name using get_urns_by_filter
results = list(
    graph.get_urns_by_filter(
        entity_types=["dataFlow"],
        query="sales",
    )
)

print(f"Found {len(results)} DataFlows matching 'sales':\n")
for urn in results[:10]:  # Show first 10
    print(f"URN: {urn}")
    print("-" * 80)

# Search with filters using FilterDsl

client = DataHubClient.from_env()

# Search for DataFlows with specific platform
filtered_results = list(
    client.search.get_urns(
        filter=F.and_(
            F.entity_type("dataFlow"),
            F.platform("airflow"),
        )
    )
)

print(f"\nFound {len(filtered_results)} Airflow DataFlows")
