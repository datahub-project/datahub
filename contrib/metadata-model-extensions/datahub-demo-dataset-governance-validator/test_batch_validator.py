#!/usr/bin/env python3

"""
Test script for the DataHub dataset governance validator using ASYNC_BATCH mode.
This script tests both valid and invalid datasets to prove the validator works.
"""

import json
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,
    GlobalTagsClass,
    TagAssociationClass,
    DomainsClass
)
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.utilities.urns.urn import Urn

# DataHub connection config
GMS_SERVER = "http://localhost:8080"
DATAHUB_TOKEN = "<omitted>"

def create_emitter():
    """Create DataHub REST emitter."""
    return DataHubRestEmitter(gms_server=GMS_SERVER, token=DATAHUB_TOKEN)

def test_invalid_logical_dataset():
    """Test ingestion with invalid logical dataset (missing governance) - should FAIL."""
    print("🔥 Testing INVALID LOGICAL dataset (missing governance metadata)...")
    
    emitter = create_emitter()
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:logical,invalid_logical_dataset,PROD)"
    
    # Create MCP with only dataset properties (missing ownership, tags, domain)
    mcp = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=DatasetPropertiesClass(
            name="invalid_logical_dataset",
            description="A logical dataset without governance metadata - should be blocked"
        )
    )
    
    try:
        # This should fail with validation error
        emitter.emit_mcps([mcp])
        print("❌ ERROR: Expected validation failure but ingestion succeeded!")
        return False
    except Exception as e:
        print(f"✅ SUCCESS: Validator blocked invalid logical dataset as expected")
        print(f"   Error: {str(e)}")
        return True

def test_valid_logical_dataset_batch():
    """Test batch ingestion with valid logical dataset (has all governance) - should PASS."""
    print("\\n🟢 Testing VALID LOGICAL dataset (has all governance metadata)...")
    
    emitter = create_emitter()
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:logical,valid_logical_dataset,PROD)"
    
    # Create audit stamp
    audit_stamp = AuditStamp(time=1640995200000, actor="urn:li:corpuser:datahub")
    
    # Create MCPs with all required governance aspects
    mcps = [
        # Dataset properties
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                name="valid_logical_dataset",
                description="A logical dataset with all governance metadata - should succeed"
            )
        ),
        
        # Ownership (required)
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=OwnershipClass(
                owners=[
                    OwnerClass(
                        owner="urn:li:corpuser:datahub",
                        type=OwnershipTypeClass.DATAOWNER
                    )
                ],
                lastModified=audit_stamp
            )
        ),
        
        # Global tags (required)
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=GlobalTagsClass(
                tags=[
                    TagAssociationClass(tag="urn:li:tag:Production")
                ]
            )
        ),
        
        # Domains (required)
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DomainsClass(
                domains=["urn:li:domain:engineering"]
            )
        )
    ]
    
    try:
        # This should succeed because all governance aspects are present
        emitter.emit_mcps(mcps)
        print("✅ SUCCESS: Valid logical dataset with governance metadata was accepted")
        return True
    except Exception as e:
        print(f"❌ ERROR: Expected success but validation failed: {str(e)}")
        return False

def test_physical_dataset_single():
    """Test ingestion with physical dataset (missing governance) - should PASS (ignored by validator)."""
    print("\\n🔧 Testing PHYSICAL dataset (missing governance, should be ignored)...")
    
    emitter = create_emitter()
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.physical_dataset,PROD)"
    
    # Create MCP with only dataset properties (missing governance)
    mcp = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=DatasetPropertiesClass(
            name="physical_dataset",
            description="A physical dataset without governance metadata - should be ignored by validator"
        )
    )
    
    try:
        # This should succeed because physical datasets are not validated
        emitter.emit_mcps([mcp])
        print("✅ SUCCESS: Physical dataset was accepted (validator ignored it)")
        return True
    except Exception as e:
        print(f"❌ ERROR: Physical dataset was unexpectedly blocked: {str(e)}")
        return False

def test_physical_dataset_batch():
    """Test batch ingestion with physical dataset (has governance) - should PASS (ignored by validator)."""
    print("\\n🔧 Testing PHYSICAL dataset batch (with governance, should be ignored)...")
    
    emitter = create_emitter()
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,db.physical_batch_dataset,PROD)"
    
    # Create audit stamp
    audit_stamp = AuditStamp(time=1640995200000, actor="urn:li:corpuser:datahub")
    
    # Create MCPs with governance aspects (validator should ignore all of these)
    mcps = [
        # Dataset properties
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                name="physical_batch_dataset",
                description="A physical dataset with governance metadata - should be ignored by validator"
            )
        ),
        
        # Ownership
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=OwnershipClass(
                owners=[
                    OwnerClass(
                        owner="urn:li:corpuser:datahub",
                        type=OwnershipTypeClass.DATAOWNER
                    )
                ],
                lastModified=audit_stamp
            )
        )
    ]
    
    try:
        # This should succeed because physical datasets are not validated
        emitter.emit_mcps(mcps)
        print("✅ SUCCESS: Physical dataset batch was accepted (validator ignored it)")
        return True
    except Exception as e:
        print(f"❌ ERROR: Physical dataset batch was unexpectedly blocked: {str(e)}")
        return False

def main():
    """Run validator tests."""
    print("🧪 Testing DataHub Dataset Governance Validator (logical datasets only)\\n")
    
    # Test 1: Invalid logical dataset should be blocked
    invalid_logical_test = test_invalid_logical_dataset()
    
    # Test 2: Valid logical dataset should succeed  
    valid_logical_test = test_valid_logical_dataset_batch()
    
    # Test 3: Physical dataset (missing governance) should be ignored
    physical_single_test = test_physical_dataset_single()
    
    # Test 4: Physical dataset batch should be ignored
    physical_batch_test = test_physical_dataset_batch()
    
    print("\\n" + "="*70)
    print("📊 TEST RESULTS:")
    print(f"   Invalid logical dataset blocked:  {'✅ PASS' if invalid_logical_test else '❌ FAIL'}")
    print(f"   Valid logical dataset accepted:   {'✅ PASS' if valid_logical_test else '❌ FAIL'}")
    print(f"   Physical dataset (single) ignored: {'✅ PASS' if physical_single_test else '❌ FAIL'}")
    print(f"   Physical dataset (batch) ignored:  {'✅ PASS' if physical_batch_test else '❌ FAIL'}")
    
    all_tests_passed = all([invalid_logical_test, valid_logical_test, physical_single_test, physical_batch_test])
    
    if all_tests_passed:
        print("\\n🎉 ALL TESTS PASSED - Validator is working correctly!")
        print("   ✓ Logical datasets are properly validated")
        print("   ✓ Physical datasets are ignored as expected")
        return 0
    else:
        print("\\n💥 SOME TESTS FAILED - Validator may not be working properly!")
        return 1

if __name__ == "__main__":
    exit(main())
