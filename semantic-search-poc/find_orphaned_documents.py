#!/usr/bin/env python3
"""Find and delete documents in OpenSearch that don't exist in MySQL."""

import sys
import argparse
from opensearchpy import OpenSearch
import pymysql
from typing import Set, List

def get_mysql_document_urns() -> Set[str]:
    """Get all document URNs from MySQL."""
    print("📊 Querying MySQL for document URNs...")
    
    connection = pymysql.connect(
        host='localhost',
        user='datahub',
        password='datahub',
        database='datahub',
        port=3306
    )
    
    try:
        with connection.cursor() as cursor:
            # Get all document URNs with documentInfo aspect
            query = """
                SELECT DISTINCT urn 
                FROM metadata_aspect_v2 
                WHERE urn LIKE 'urn:li:document:%' 
                AND aspect = 'documentInfo'
            """
            cursor.execute(query)
            urns = {row[0] for row in cursor.fetchall()}
            print(f"✓ Found {len(urns)} document URNs in MySQL")
            return urns
    finally:
        connection.close()

def get_opensearch_document_urns(index_name: str) -> Set[str]:
    """Get all document URNs from OpenSearch."""
    print(f"📊 Querying OpenSearch {index_name} for document URNs...")
    
    client = OpenSearch(
        hosts=[{"host": "localhost", "port": 9200}],
        use_ssl=False,
        verify_certs=False,
    )
    
    urns = set()
    
    # Scroll through all documents
    query = {
        "_source": ["urn"],
        "query": {"match_all": {}},
        "size": 1000
    }
    
    response = client.search(index=index_name, body=query, scroll='2m')
    scroll_id = response['_scroll_id']
    hits = response['hits']['hits']
    
    while hits:
        for hit in hits:
            urn = hit['_source'].get('urn')
            if urn:
                urns.add(urn)
        
        # Get next batch
        response = client.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']
    
    # Clear scroll
    client.clear_scroll(scroll_id=scroll_id)
    
    print(f"✓ Found {len(urns)} document URNs in OpenSearch {index_name}")
    return urns

def delete_orphaned_documents(index_name: str, orphaned_urns: List[str], dry_run: bool = True):
    """Delete orphaned documents from OpenSearch."""
    if not orphaned_urns:
        print("✓ No orphaned documents to delete")
        return
    
    if dry_run:
        print(f"\n🔍 DRY RUN - Would delete {len(orphaned_urns)} orphaned documents from {index_name}:")
        for i, urn in enumerate(orphaned_urns[:10], 1):
            print(f"  {i}. {urn}")
        if len(orphaned_urns) > 10:
            print(f"  ... and {len(orphaned_urns) - 10} more")
        return
    
    print(f"\n🗑️  Deleting {len(orphaned_urns)} orphaned documents from {index_name}...")
    
    client = OpenSearch(
        hosts=[{"host": "localhost", "port": 9200}],
        use_ssl=False,
        verify_certs=False,
    )
    
    # Delete by query
    delete_query = {
        "query": {
            "terms": {
                "urn.keyword": orphaned_urns
            }
        }
    }
    
    response = client.delete_by_query(index=index_name, body=delete_query)
    deleted = response.get('deleted', 0)
    print(f"✓ Deleted {deleted} documents from {index_name}")

def main():
    parser = argparse.ArgumentParser(description='Find and delete orphaned documents in OpenSearch')
    parser.add_argument('--execute', action='store_true', help='Actually delete orphaned documents (default is dry-run)')
    parser.add_argument('--index', default='documentindex_v2,documentindex_v2_semantic', 
                       help='Comma-separated OpenSearch indices to check (default: both document indices)')
    args = parser.parse_args()
    
    dry_run = not args.execute
    indices = [idx.strip() for idx in args.index.split(',')]
    
    print("=" * 80)
    print("Finding Orphaned Documents in OpenSearch")
    print("=" * 80)
    print()
    
    # Step 1: Get MySQL URNs
    try:
        mysql_urns = get_mysql_document_urns()
    except Exception as e:
        print(f"❌ Error querying MySQL: {e}")
        sys.exit(1)
    
    print()
    
    # Step 2: Check each index
    for index_name in indices:
        print(f"\n{'=' * 80}")
        print(f"Checking index: {index_name}")
        print('=' * 80)
        
        try:
            opensearch_urns = get_opensearch_document_urns(index_name)
        except Exception as e:
            print(f"❌ Error querying OpenSearch {index_name}: {e}")
            continue
        
        # Step 3: Find orphaned URNs
        orphaned_urns = list(opensearch_urns - mysql_urns)
        orphaned_urns.sort()
        
        print()
        print(f"📈 Summary for {index_name}:")
        print(f"  - MySQL documents: {len(mysql_urns)}")
        print(f"  - OpenSearch documents: {len(opensearch_urns)}")
        print(f"  - Orphaned (in OpenSearch but not MySQL): {len(orphaned_urns)}")
        print(f"  - Missing (in MySQL but not OpenSearch): {len(mysql_urns - opensearch_urns)}")
        
        # Step 4: Delete orphaned documents
        if orphaned_urns:
            delete_orphaned_documents(index_name, orphaned_urns, dry_run=dry_run)
    
    print()
    if dry_run:
        print("=" * 80)
        print("DRY RUN MODE - No changes were made")
        print("Run with --execute to actually delete orphaned documents")
        print("=" * 80)

if __name__ == '__main__':
    main()
