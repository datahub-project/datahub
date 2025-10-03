#!/usr/bin/env python3
"""
Test script for DataHub Dataset Governance Validator using Python SDK
"""
import os
import sys
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,
    GlobalTagsClass,
    TagAssociationClass,
    DomainsClass,
)

def main():
    # Configuration
    token = os.getenv("DATAHUB_TOKEN")
    if not token:
        print("❌ ERROR: Please set DATAHUB_TOKEN environment variable")
        sys.exit(1)
    
    datahub_url = os.getenv("DATAHUB_URL", "http://localhost:8080")
    
    print("🧪 Testing DataHub Dataset Governance Validator")
    print("=" * 50)
    
    # Configure emitter
    emitter = DatahubRestEmitter(gms_server=datahub_url, token=token)
    
    # Test 1: MySQL dataset WITHOUT governance metadata (should pass - no validation)
    print("\n📋 Test 1: MySQL dataset WITHOUT governance metadata (should pass)...")
    
    mysql_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,test_db.mysql_table,PROD)"
    
    mysql_mcp = MetadataChangeProposalWrapper(
        entityUrn=mysql_dataset_urn,
        aspect=DatasetPropertiesClass(
            name="MySQL Test Table",
            description="A MySQL table without governance metadata - should pass"
        )
    )
    
    test1_passed = False
    try:
        emitter.emit(mysql_mcp)
        print("✅ EXPECTED: MySQL dataset ingestion succeeded (no validation required)")
        test1_passed = True
    except Exception as e:
        print(f"❌ UNEXPECTED: MySQL dataset validation failed - {e}")
    
    # Test 2: Logical dataset WITHOUT governance metadata (should fail)
    print("\n📋 Test 2: Logical dataset WITHOUT governance metadata (should fail)...")
    
    bad_logical_urn = "urn:li:dataset:(urn:li:dataPlatform:logical,test_db.bad_logical_table,PROD)"
    
    bad_logical_mcp = MetadataChangeProposalWrapper(
        entityUrn=bad_logical_urn,
        aspect=DatasetPropertiesClass(
            name="Bad Logical Table",
            description="A logical table without governance metadata - should fail"
        )
    )
    
    test2_passed = False
    try:
        emitter.emit(bad_logical_mcp)
        print("❌ UNEXPECTED: Logical dataset without governance succeeded")
    except Exception as e:
        print(f"✅ EXPECTED: Logical dataset validation failed - {e}")
        test2_passed = True
    
    # Test 3: Logical dataset WITH governance metadata (should succeed)
    print("\n📋 Test 3: Logical dataset WITH governance metadata (should succeed)...")
    
    good_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:logical,test_db.good_logical_table,PROD)"
    
    # First add ownership
    print("  📝 Adding ownership...")
    ownership_mcp = MetadataChangeProposalWrapper(
        entityUrn=good_dataset_urn,
        aspect=OwnershipClass(
            owners=[
                OwnerClass(
                    owner="urn:li:corpuser:datahub",
                    type=OwnershipTypeClass.DATAOWNER
                )
            ]
        )
    )
    
    # Then add tags
    print("  🏷️  Adding tags...")
    tags_mcp = MetadataChangeProposalWrapper(
        entityUrn=good_dataset_urn,
        aspect=GlobalTagsClass(
            tags=[
                TagAssociationClass(
                    tag="urn:li:tag:test-tag"
                )
            ]
        )
    )
    
    # Then add domain
    print("  🏢 Adding domain...")
    domains_mcp = MetadataChangeProposalWrapper(
        entityUrn=good_dataset_urn,
        aspect=DomainsClass(
            domains=["urn:li:domain:test-domain"]
        )
    )
    
    # Finally add dataset properties
    print("  📊 Adding dataset properties...")
    good_properties_mcp = MetadataChangeProposalWrapper(
        entityUrn=good_dataset_urn,
        aspect=DatasetPropertiesClass(
            name="Good Test Table",
            description="A test table with proper governance metadata"
        )
    )
    
    test3_passed = False
    try:
        # Emit all aspects for the good dataset as a batch (tests transaction-based validation)
        emitter.emit_mcps([ownership_mcp, tags_mcp, domains_mcp, good_properties_mcp])
        print("✅ EXPECTED: Logical dataset with governance succeeded")
        test3_passed = True
    except Exception as e:
        print(f"⚠️  UNEXPECTED: Logical dataset with governance failed")
        print(f"   Error: {e}")
    
    # Summary
    print("\n🏁 Test Summary:")
    print("=" * 16)
    print(f"Test 1 (MySQL without governance): {'✅ PASS' if test1_passed else '❌ FAIL'}")
    print(f"Test 2 (Logical without governance): {'✅ PASS' if test2_passed else '❌ FAIL'}")
    print(f"Test 3 (Logical with governance): {'✅ PASS' if test3_passed else '❌ FAIL'}")
    
    if test1_passed and test2_passed and test3_passed:
        print("\n🎉 SUCCESS: Validator is working correctly!")
        print("   - MySQL datasets bypass validation")
        print("   - Logical datasets without governance are blocked")
        print("   - Logical datasets with governance are allowed")
    elif not test2_passed:
        print("\n⚠️  VALIDATOR NOT ACTIVE: Logical datasets should be validated")
        print("   - The validator may not be loaded or configured correctly")
        print("   - Check GMS logs for plugin loading messages")
    else:
        print("\n❓ MIXED RESULTS: Check responses above for details")

if __name__ == "__main__":
    main()