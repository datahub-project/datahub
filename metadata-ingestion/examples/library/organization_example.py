#!/usr/bin/env python3
"""
Example script demonstrating how to use the DataHub Python SDK to manage organizations.

This script shows how to:
1. Create organizations
2. Add entities to organizations  
3. Add users to organizations
4. Delete organizations

Prerequisites:
    pip install acryl-datahub

Usage:
    python organization_example.py
"""


from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.specific.organization import (
    add_entity_to_organizations,
    add_user_to_organizations,
    create_organization,
)


def main():
    """Main example function demonstrating organization management."""
    
    # Initialize the emitter to connect to DataHub
    emitter = DatahubRestEmitter("http://localhost:8080")
    
    print("=" * 60)
    print("DataHub Python SDK - Organization Management Example")
    print("=" * 60)
    print()
    
    # Example 1: Create a new organization
    print("1. Creating 'engineering' organization...")
    org_mcp = create_organization(
        organization_id="engineering",
        name="Engineering Team",
        description="Core engineering organization responsible for building products",
        external_url="https://example.com/engineering",
    )
    emitter.emit_mcp(org_mcp)
    print("   ✓ Created organization: urn:li:organization:engineering")
    print()
    
    # Example 2: Create another organization
    print("2. Creating 'data-platform' organization...")
    data_org_mcp = create_organization(
        organization_id="data-platform",
        name="Data Platform Team",
        description="Team managing data infrastructure and platforms",
    )
    emitter.emit_mcp(data_org_mcp)
    print("   ✓ Created organization: urn:li:organization:data-platform")
    print()
    
    # Example 3: Create a third organization
    print("3. Creating 'analytics' organization...")
    analytics_org_mcp = create_organization(
        organization_id="analytics",
        name="Analytics Team",
        description="Business analytics and data science team",
    )
    emitter.emit_mcp(analytics_org_mcp)
    print("   ✓ Created organization: urn:li:organization:analytics")
    print()
    
    # Example 4: Add a dataset to organizations
    print("4. Adding dataset to 'engineering' and 'data-platform' organizations...")
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"
    dataset_org_mcp = add_entity_to_organizations(
        entity_urn=dataset_urn,
        organization_ids=["engineering", "data-platform"],
    )
    emitter.emit_mcp(dataset_org_mcp)
    print(f"   ✓ Added {dataset_urn} to organizations")
    print()
    
    # Example 5: Add a chart to an organization
    print("5. Adding chart to 'analytics' organization...")
    chart_urn = "urn:li:chart:(looker,baz1)"
    chart_org_mcp = add_entity_to_organizations(
        entity_urn=chart_urn,
        organization_ids=["analytics"],
    )
    emitter.emit_mcp(chart_org_mcp)
    print(f"   ✓ Added {chart_urn} to organization")
    print()
    
    # Example 6: Add a user to an organization
    print("6. Adding user to 'engineering' organization...")
    user_urn = "urn:li:corpuser:datahub"
    user_org_mcp = add_user_to_organizations(
        user_urn=user_urn,
        organization_ids=["engineering"],
    )
    emitter.emit_mcp(user_org_mcp)
    print(f"   ✓ Added {user_urn} to organization")
    print()
    
    # Example 7: Add multiple entities
    print("7. Adding multiple entities to organizations...")
    entities_to_add = [
        ("urn:li:dataset:(urn:li:dataPlatform:hive,sample_table,PROD)", ["engineering"]),
        ("urn:li:dashboard:(looker,dashboard1)", ["analytics", "engineering"]),
    ]
    
    for entity_urn, org_ids in entities_to_add:
        mcp = add_entity_to_organizations(entity_urn, org_ids)
        emitter.emit_mcp(mcp)
        print(f"   ✓ Added {entity_urn} to {', '.join(org_ids)}")
    print()
    
    print("=" * 60)
    print("✓ All operations completed successfully!")
    print("=" * 60)
    print()
    print("Next steps:")
    print("  1. View organizations in DataHub UI: http://localhost:9002/organizations")
    print("  2. Query entities by organization using GraphQL:")
    print()
    print("     query {")
    print("       getEntitiesByOrganization(")
    print("         organizationUrn: \"urn:li:organization:engineering\"")
    print("         entityTypes: [DATASET, CHART, DASHBOARD]")
    print("         start: 0")
    print("         count: 10")
    print("       ) {")
    print("         total")
    print("         entities { urn }")
    print("       }")
    print("     }")
    print()
    
    # Note: Uncommenting the following will delete the organization
    # print("8. Deleting 'analytics' organization...")
    # delete_mcp = delete_organization("analytics")
    # emitter.emit_mcp(delete_mcp)
    # print("   ✓ Deleted organization")


if __name__ == "__main__":
    main()
