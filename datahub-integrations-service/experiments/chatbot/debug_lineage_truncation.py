"""
Debug script to test get_lineage entity-level truncation and offset.
"""

import json
import os

from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
from datahub.sdk.main_client import DataHubClient

from datahub_integrations.mcp.mcp_server import (
    get_lineage,
    with_datahub_client,
)


def main():
    # Set up DataHub client using longtailcompanions demo instance
    import pathlib
    creds_file = pathlib.Path("../graph_credentials.json")

    if creds_file.exists():
        creds = json.load(open(creds_file))
        instance_creds = creds.get("longtailcompanions", {})
        gms_url = instance_creds.get("server")
        token = instance_creds.get("token")
    else:
        gms_url = os.getenv("DATAHUB_GMS_URL")
        token = os.getenv("DATAHUB_GMS_TOKEN")

    if not gms_url or not token:
        print("⚠️  Could not find credentials.")
        return

    print(f"Connecting to: {gms_url}\n")
    config = DatahubClientConfig(server=gms_url, token=token)
    graph = DataHubGraph(config)
    client = DataHubClient(graph=graph)

    # Table with known lineage
    table_urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,banking.public.account_transactions,PROD)"
    another_table_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,smoke_test_db.public.base_table,PROD)"
    print(f"=== Testing get_lineage Entity-Level Truncation ===")
    print(f"Table: {table_urn}")
    print(f"\n" + "="*80 + "\n")

    # Test 1: Request with max_results=50, offset=0
    print("Test 1: max_results=50, offset=0")
    print("-" * 80)
    
    with with_datahub_client(client):
        result = get_lineage(
            urn=another_table_urn,
            column="revenue", #"transaction_type",
            upstream=False,  # False = downstream lineage
            max_hops=3,
            max_results=50,
            offset=0,
        )

    if "downstreams" in result:
        ds = result["downstreams"]
        print(f"  Total available: {len(ds.get('searchResults', []))}")
        print(f"  Offset: {ds.get('offset')}")
        print(f"  Requested: {ds.get('requested')}")
        print(f"  Returned: {ds.get('returned')}")
        print(f"  HasMore: {ds.get('hasMore')}")
        print(f"  Truncated due to budget: {ds.get('truncatedDueToTokenBudget', False)}")
        
        # Show first 3 entities
        for idx, item in enumerate(ds.get('searchResults', [])[:3], 1):
            entity = item.get('entity', {})
            print(f"  Entity {idx}: {entity.get('name')} (degree={item.get('degree')})")
    
    print(f"\n" + "="*80 + "\n")

    # Test 2: Pagination - offset=10
    print("Test 2: max_results=50, offset=10 (pagination)")
    print("-" * 80)
    
    with with_datahub_client(client):
        result2 = get_lineage(
            urn=table_urn,
            column=None,
            upstream=False,  # False = downstream lineage
            max_hops=3,
            max_results=50,
            offset=10,
        )

    if "downstreams" in result2:
        ds = result2["downstreams"]
        print(f"  Total available: {len(ds.get('searchResults', []))}")
        print(f"  Offset: {ds.get('offset')}")
        print(f"  Requested: {ds.get('requested')}")
        print(f"  Returned: {ds.get('returned')}")
        print(f"  HasMore: {ds.get('hasMore')}")
        print(f"  Truncated due to budget: {ds.get('truncatedDueToTokenBudget', False)}")
        
        # Show first 3 entities
        for idx, item in enumerate(ds.get('searchResults', [])[:3], 1):
            entity = item.get('entity', {})
            print(f"  Entity {idx}: {entity.get('name')} (degree={item.get('degree')})")
    
    print(f"\n" + "="*80 + "\n")

    # Test 3: Small max_results to see immediate truncation
    print("Test 3: max_results=5, offset=0 (small request)")
    print("-" * 80)
    
    with with_datahub_client(client):
        result3 = get_lineage(
            urn=table_urn,
            column=None,
            upstream=False,  # False = downstream lineage
            max_hops=2,
            max_results=5,
            offset=0,
        )

    if "downstreams" in result3:
        ds = result3["downstreams"]
        print(f"  Total available: {len(ds.get('searchResults', []))} entities")
        print(f"  Offset: {ds.get('offset')}")
        print(f"  Requested: {ds.get('requested')}")
        print(f"  Returned: {ds.get('returned')}")
        print(f"  HasMore: {ds.get('hasMore')}")
        print(f"  Truncated due to budget: {ds.get('truncatedDueToTokenBudget', False)}")
        
        print(f"\n  All entities returned:")
        for idx, item in enumerate(ds.get('searchResults', []), 1):
            entity = item.get('entity', {})
            print(f"    {idx}. {entity.get('name')} (degree={item.get('degree')})")
    
    print(f"\n" + "="*80 + "\n")
    print("✅ Entity-level truncation tests complete!")


if __name__ == "__main__":
    main()


