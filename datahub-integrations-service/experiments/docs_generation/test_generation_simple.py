#!/usr/bin/env python3
"""
Simple test script for documentation generation with hardcoded URNs.

This is the quickest way to test documentation generation:
    python test_generation_simple.py
"""

import os
from pathlib import Path
from unittest.mock import patch

# Load environment variables from .env file
from dotenv import load_dotenv
from datahub_integrations.experimentation.docs_generation.eval_common import (
    docs_generation_experiments_dir,
)

# Load .env from the datahub-integrations-service root
env_path = docs_generation_experiments_dir.parent.parent / '.env'
load_dotenv(env_path, override=True)

from datahub.ingestion.graph.client import get_default_graph
from datahub.utilities.perf_timer import PerfTimer
from datahub_integrations.gen_ai.description_v3 import (
    generate_entity_descriptions_for_urn,
    get_extra_documentation_instructions,
)

# Configuration - EDIT THESE FOR YOUR TEST
TEST_URN = "urn:li:dataset:(urn:li:dataPlatform:databricks,dbc-a2674c21-c551.acryl_metastore.main.information_schema.catalog_privileges,PROD)"  # Databricks catalog_privileges table
MOCK_INSTRUCTIONS = "Always mention data quality and compliance considerations. Use business-friendly language."  # Optional: set to None to use real instructions
USE_MOCK_INSTRUCTIONS = False  # Set to False to fetch from GraphQL


def test_generation():
    """Test documentation generation with a hardcoded URN."""
    
    print("=" * 80)
    print("DOCUMENTATION GENERATION TEST")
    print("=" * 80)
    
    # Show AWS configuration
    print("\nAWS Configuration:")
    print(f"  AWS_PROFILE: {os.environ.get('AWS_PROFILE', 'Not set')}")
    print(f"  AWS_REGION: {os.environ.get('AWS_REGION', 'Not set')}")
    print(f"  BEDROCK_MODEL: {os.environ.get('BEDROCK_MODEL', 'Not set')}")
    
    # Connect to DataHub
    try:
        graph = get_default_graph()
        print("\n✓ Connected to DataHub")
    except Exception as e:
        print(f"\n✗ Failed to connect to DataHub: {e}")
        print("  Make sure ~/.datahubenv is configured")
        return
    
    # Test instructions fetch
    print("\n" + "-" * 40)
    print("TESTING EXTRA INSTRUCTIONS")
    print("-" * 40)
    
    if USE_MOCK_INSTRUCTIONS and MOCK_INSTRUCTIONS:
        print(f"Using mock instructions: {MOCK_INSTRUCTIONS[:50]}...")
        
        # Mock the get_extra_documentation_instructions function
        with patch('datahub_integrations.gen_ai.description_v3.get_extra_documentation_instructions') as mock_get:
            mock_get.return_value = MOCK_INSTRUCTIONS
            run_generation(graph)
    else:
        # Fetch real instructions
        try:
            instructions = get_extra_documentation_instructions(graph)
            if instructions:
                print(f"✓ Fetched instructions from GraphQL:")
                print(f"  {instructions[:100]}...")
            else:
                print("✗ No instructions configured in DataHub")
        except Exception as e:
            print(f"✗ Error fetching instructions: {e}")
        
        run_generation(graph)


def run_generation(graph):
    """Run the actual generation."""
    
    print("\n" + "-" * 40)
    print(f"GENERATING DOCUMENTATION FOR: {TEST_URN}")
    print("-" * 40)
    
    with PerfTimer() as timer:
        try:
            result = generate_entity_descriptions_for_urn(graph, TEST_URN)
            print(f"✓ Generation completed in {timer.elapsed_seconds():.2f}s")
        except Exception as e:
            print(f"✗ Generation failed: {e}")
            import traceback
            traceback.print_exc()
            return
    
    # Display results
    print("\n" + "-" * 40)
    print("RESULTS")
    print("-" * 40)
    
    if result.failure_reason:
        print(f"⚠ Failure reason: {result.failure_reason}")
    
    if result.table_description:
        print(f"\n✓ Table Description ({len(result.table_description)} chars):")
        print("-" * 20)
        print(result.table_description[:500])
        if len(result.table_description) > 500:
            print("... [truncated]")
    else:
        print("\n✗ No table description generated")
    
    if result.column_descriptions:
        print(f"\n✓ Column Descriptions ({len(result.column_descriptions)} columns):")
        print("-" * 20)
        
        # Show first 5 columns
        for i, (col, desc) in enumerate(list(result.column_descriptions.items())[:5]):
            print(f"  {i+1}. {col}:")
            print(f"     {desc[:100]}..." if len(desc) > 100 else f"     {desc}")
        
        if len(result.column_descriptions) > 5:
            print(f"  ... and {len(result.column_descriptions) - 5} more columns")
    else:
        print("\n✗ No column descriptions generated")
    
    # Show metadata statistics
    if result.extracted_entity_info:
        print(f"\n📊 Entity Statistics:")
        print(f"  - URN: {result.extracted_entity_info.urn}")
        print(f"  - Total columns: {len(result.extracted_entity_info.column_names)}")
        print(f"  - Table name: {result.extracted_entity_info.table_name or 'Unknown'}")
        if result.extracted_entity_info.table_tags:
            print(f"  - Tags: {', '.join([t.tag_name for t in result.extracted_entity_info.table_tags[:5]])}")
        if result.extracted_entity_info.table_glossary_terms:
            print(f"  - Glossary Terms: {', '.join([t.tag_name for t in result.extracted_entity_info.table_glossary_terms[:5]])}")


if __name__ == "__main__":
    # Quick example URNs for different platforms (uncomment one to use)
    
    # Snowflake example
    # TEST_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.public.orders,PROD)"
    
    # BigQuery example
    # TEST_URN = "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"
    
    # Postgres example
    # TEST_URN = "urn:li:dataset:(urn:li:dataPlatform:postgres,database.schema.table,PROD)"
    
    # Query example
    # TEST_URN = "urn:li:query:61705a7013ab63277266251565750876"
    
    test_generation()
