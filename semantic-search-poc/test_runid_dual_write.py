#!/usr/bin/env python3
"""Test runId dual-write to verify semantic index synchronization."""

import time
import requests
from datahub.sdk.document import Document
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.metadata.com.linkedin.pegasus2avro.mxe import SystemMetadata

def create_test_document_with_runid(run_id: str):
    """Create a test document with a specific runId."""
    
    # Create unique document ID
    timestamp = int(time.time())
    doc_id = f"test-runid-dual-write-{timestamp}"
    
    print(f"📄 Creating test document: {doc_id}")
    print(f"🏃 RunId: {run_id}")
    
    # Create document using SDK
    doc = Document.create(
        id=doc_id,
        title=f"Test Document for RunId Dual-Write ({timestamp})",
        text="This is a test document created to verify that runId updates are dual-written to both v2 and semantic indices.",
        source_url=f"test://runid-test/{doc_id}",
        custom_properties={
            "test_type": "runid_dual_write",
            "timestamp": str(timestamp)
        }
    )
    
    # Emit to DataHub with runId in SystemMetadata
    emitter = DatahubRestEmitter("http://localhost:8080")
    
    # Get MCPs and add systemMetadata with runId
    for mcp in doc.as_mcps():
        # Add systemMetadata with runId
        mcp.systemMetadata = SystemMetadata(runId=run_id)
        
        # Emit the MCP directly
        emitter.emit_mcp(mcp)
    
    emitter.flush()
    
    print(f"✅ Document created and emitted")
    return str(doc.urn)


def verify_dual_write(doc_urn: str, expected_run_id: str):
    """Verify that runId was written to both v2 and semantic indices."""
    
    # URL encode the URN
    import urllib.parse
    doc_id = urllib.parse.quote(doc_urn, safe='')
    
    print(f"\n⏳ Waiting for indexing (5 seconds)...")
    time.sleep(5)
    
    print(f"\n🔍 Verifying dual-write for: {doc_urn}")
    print(f"   Encoded ID: {doc_id}")
    
    # Check v2 index
    v2_response = requests.get(f"http://localhost:9200/documentindex_v2/_doc/{doc_id}")
    v2_doc = v2_response.json()
    
    # Check semantic index
    sem_response = requests.get(f"http://localhost:9200/documentindex_v2_semantic/_doc/{doc_id}")
    sem_doc = sem_response.json()
    
    print(f"\n📊 Results:")
    print(f"   V2 Index found: {v2_doc.get('found', False)}")
    print(f"   Semantic Index found: {sem_doc.get('found', False)}")
    
    if v2_doc.get('found'):
        v2_runid = v2_doc['_source'].get('runId')
        print(f"\n   V2 RunId: {v2_runid}")
    else:
        v2_runid = None
        print(f"\n   ❌ Document not found in V2 index")
    
    if sem_doc.get('found'):
        sem_runid = sem_doc['_source'].get('runId')
        print(f"   Semantic RunId: {sem_runid}")
    else:
        sem_runid = None
        print(f"   ❌ Document not found in Semantic index")
    
    # Verify sync
    print(f"\n🎯 Verification:")
    if v2_doc.get('found') and sem_doc.get('found'):
        if v2_runid == sem_runid and v2_runid and expected_run_id in str(v2_runid):
            print(f"   ✅ RunIds match and contain expected runId!")
            print(f"   ✅ V2: {v2_runid}")
            print(f"   ✅ Semantic: {sem_runid}")
            return True
        elif v2_runid == sem_runid:
            print(f"   ⚠️  RunIds match but don't contain expected runId")
            print(f"   V2: {v2_runid}")
            print(f"   Semantic: {sem_runid}")
            print(f"   Expected: {expected_run_id}")
            return False
        else:
            print(f"   ❌ RunIds DO NOT MATCH!")
            print(f"   V2: {v2_runid}")
            print(f"   Semantic: {sem_runid}")
            return False
    else:
        print(f"   ❌ Document not found in one or both indices")
        return False


if __name__ == "__main__":
    # Generate unique runId for this test
    test_run_id = f"test-dual-write-{int(time.time())}"
    
    print("=" * 60)
    print("RunId Dual-Write Test")
    print("=" * 60)
    
    try:
        # Create document
        doc_urn = create_test_document_with_runid(test_run_id)
        
        # Verify dual-write
        success = verify_dual_write(doc_urn, test_run_id)
        
        print("\n" + "=" * 60)
        if success:
            print("✅ TEST PASSED: RunId dual-write is working!")
        else:
            print("❌ TEST FAILED: RunId dual-write has issues")
        print("=" * 60)
        
        exit(0 if success else 1)
        
    except Exception as e:
        print(f"\n❌ Error during test: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

