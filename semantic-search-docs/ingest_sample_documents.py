#!/usr/bin/env python3
"""
Sample Document Ingestion Tool for DataHub Semantic Search

This script ingests sample document entities into DataHub using the GraphQL API.
It's designed for testing and demonstrating semantic search functionality.

Features:
- Generate synthetic sample documents from templates
- Load documents from a JSON file
- Optionally generate embeddings BEFORE ingestion (--embed-first)
  This demonstrates/tests the race condition where embeddings arrive first

Usage:
    # Basic ingestion (10 sample documents)
    uv run python ingest_sample_documents.py --count 10 --server http://localhost:8080

    # Ingest with embeddings written first (tests race condition)
    AWS_PROFILE=your-profile uv run python ingest_sample_documents.py --count 5 --embed-first

    # Load from JSON file
    uv run python ingest_sample_documents.py --from-file sample_documents.json

    # Dry run (preview without making changes)
    uv run python ingest_sample_documents.py --count 5 --dry-run

Environment variables:
    DATAHUB_TOKEN: Personal access token for authentication
    DATAHUB_SERVER: Server URL (default: http://localhost:8080)
    AWS_PROFILE: AWS profile for embeddings (when using --embed-first)
"""

import argparse
import json
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from urllib.parse import quote

import requests

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv():
        pass


@dataclass
class DocumentInput:
    """Represents a document to be ingested."""
    id: str
    title: str
    text: str
    sub_type: str = "guide"
    state: str = "PUBLISHED"
    parent_document: Optional[str] = None


@dataclass
class IngestConfig:
    """Configuration for the ingestion process."""
    server_url: str
    token: Optional[str]
    dry_run: bool = False
    embed_first: bool = False
    opensearch_host: str = "localhost"
    opensearch_port: int = 9200
    embedding_field: str = "cohere_embed_v3"
    max_chunk_tokens: int = 400


SAMPLE_DOCUMENTS = [
    {
        "title": "Getting Started with DataHub",
        "text": """# Getting Started with DataHub

## Introduction
DataHub is a modern data catalog designed to help you discover, understand, and trust your data.

## Quick Start
1. Navigate to the DataHub UI
2. Use the search bar to find datasets
3. Explore metadata, lineage, and documentation

## Key Features
- **Search**: Find data assets across your organization
- **Lineage**: Understand data dependencies and flow
- **Documentation**: Add and view rich documentation
- **Governance**: Manage access and policies
""",
        "sub_type": "guide",
    },
    {
        "title": "Data Access Request Process",
        "text": """# Data Access Request Process

## Overview
This document describes how to request access to data assets in our data platform.

## Access Levels

### Read Access
- View data in dashboards and reports
- Run queries on approved datasets

### Write Access
- Modify existing records
- Requires additional approval

## Request Steps
1. Identify the dataset you need access to
2. Submit a request through the data portal
3. Wait for owner approval (typically 24-48 hours)
4. Access will be granted automatically upon approval
""",
        "sub_type": "process",
    },
    {
        "title": "Machine Learning Model: Churn Prediction",
        "text": """# Churn Prediction Model

## Model Overview
This model predicts the probability of customer churn within the next 30 days.

## Features
- Days since last login
- Purchase frequency
- Support ticket count

## Performance Metrics
- AUC-ROC: 0.87
- Precision: 0.82
- F1 Score: 0.80
""",
        "sub_type": "model",
    },
    {
        "title": "ETL Pipeline Documentation: Sales Data",
        "text": """# Sales Data ETL Pipeline

## Pipeline Overview
This pipeline extracts sales data from our CRM, transforms it, and loads into the data warehouse.

## Schedule
- Runs daily at 2:00 AM UTC
- Full refresh on Sundays

## Data Sources
1. Salesforce CRM (primary)
2. Payment gateway logs
3. Product catalog database

## Output Tables
- sales.transactions - Individual sale records
- sales.daily_summary - Aggregated daily metrics
""",
        "sub_type": "pipeline",
    },
    {
        "title": "Customer Data Platform Overview",
        "text": """# Customer Data Platform (CDP)

## What is a CDP?
A Customer Data Platform is a unified database that collects customer data from multiple sources.

## Our CDP Architecture
- Website tracking
- Mobile app events
- CRM integrations

## Key Metrics
- 50M+ unified customer profiles
- 200+ data sources integrated
""",
        "sub_type": "overview",
    },
]


def generate_document_id(prefix: str = "sample-doc") -> str:
    """Generate a unique document ID."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def generate_sample_documents(count: int) -> list[DocumentInput]:
    """Generate sample documents from templates."""
    documents = []
    num_templates = len(SAMPLE_DOCUMENTS)

    for i in range(count):
        template = SAMPLE_DOCUMENTS[i % num_templates]
        doc_id = generate_document_id()
        title = template["title"]
        if i >= num_templates:
            title = f"{title} (v{i // num_templates + 1})"

        documents.append(DocumentInput(
            id=doc_id,
            title=title,
            text=template["text"],
            sub_type=template["sub_type"],
            state="PUBLISHED",
        ))

    return documents


def load_documents_from_file(filepath: str) -> list[DocumentInput]:
    """Load documents from a JSON file."""
    with open(filepath, "r") as f:
        data = json.load(f)

    documents = []
    for item in data:
        documents.append(DocumentInput(
            id=item.get("id", generate_document_id()),
            title=item["title"],
            text=item["text"],
            sub_type=item.get("subType", item.get("sub_type", "guide")),
            state=item.get("state", "PUBLISHED"),
            parent_document=item.get("parentDocument"),
        ))
    return documents


def get_document_urn(doc_id: str) -> str:
    """Get the URN for a document ID."""
    return f"urn:li:document:{doc_id}"


def get_opensearch_doc_id(doc_id: str) -> str:
    """Get the OpenSearch document ID (URL-encoded URN)."""
    return quote(get_document_urn(doc_id), safe='')


def chunk_text(text: str, max_chunk_tokens: int = 400, chars_per_token: int = 4) -> list[str]:
    """Chunk text into segments of approximately max_chunk_tokens."""
    if not text or not text.strip():
        return []

    max_chars = max_chunk_tokens * chars_per_token
    if len(text) <= max_chars:
        return [text]

    chunks = []
    current_chunk = ""
    sentences = text.replace(". ", ".|").replace(".\n", ".|").split("|")

    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue

        if current_chunk and len(current_chunk) + len(sentence) + 1 > max_chars:
            chunks.append(current_chunk)
            current_chunk = sentence
        else:
            current_chunk = f"{current_chunk} {sentence}".strip() if current_chunk else sentence

        while len(current_chunk) > max_chars:
            chunks.append(current_chunk[:max_chars])
            current_chunk = current_chunk[max_chars:]

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def write_embeddings_to_opensearch(config: IngestConfig, doc_id: str, title: str, text: str) -> bool:
    """
    Write embeddings directly to OpenSearch BEFORE document ingestion.
    
    This demonstrates/tests the scenario where embeddings arrive before
    the document data (race condition handling).
    """
    from opensearchpy import OpenSearch
    from embedding_utils import create_embeddings

    opensearch_doc_id = get_opensearch_doc_id(doc_id)
    index_name = "documentindex_v2_semantic"

    print(f"  📝 Generating embeddings for: {title[:40]}...")

    if config.dry_run:
        chunks = chunk_text(text, config.max_chunk_tokens)
        print(f"  [DRY RUN] Would write {len(chunks)} chunks to {index_name}")
        print(f"  [DRY RUN] OpenSearch doc ID: {opensearch_doc_id}")
        return True

    try:
        opensearch = OpenSearch(
            hosts=[{"host": config.opensearch_host, "port": config.opensearch_port}],
            use_ssl=False,
            verify_certs=False,
        )
        embeddings = create_embeddings(use_cache=True)

        chunks = chunk_text(text, config.max_chunk_tokens)
        if not chunks:
            print(f"  [WARN] No text to embed")
            return True

        print(f"  📝 Chunked into {len(chunks)} chunks, generating vectors...")
        vectors = embeddings.embed_documents(chunks)

        chunk_data = []
        current_offset = 0
        for i, (chunk_text_item, vector) in enumerate(zip(chunks, vectors)):
            chunk_data.append({
                "position": i,
                "text": chunk_text_item,
                "character_offset": current_offset,
                "character_length": len(chunk_text_item),
                "token_count": len(chunk_text_item.split()),
                "vector": vector,
            })
            current_offset += len(chunk_text_item)

        doc_body = {
            "urn": get_document_urn(doc_id),
            "embeddings": {
                config.embedding_field: {
                    "model_version": embeddings.model_name,
                    "generated_at": datetime.utcnow().isoformat() + "Z",
                    "chunking_strategy": f"sentence_boundary_{config.max_chunk_tokens}t",
                    "total_chunks": len(chunks),
                    "total_tokens": sum(c["token_count"] for c in chunk_data),
                    "chunks": chunk_data,
                }
            }
        }

        print(f"  📝 Writing embeddings to {index_name}...")
        response = opensearch.index(index=index_name, id=opensearch_doc_id, body=doc_body, refresh=True)
        print(f"  ✅ Embeddings written ({response.get('result')}): {len(chunks)} chunks, {len(vectors[0])} dims")
        return True

    except Exception as e:
        print(f"  [ERROR] Failed to write embeddings: {e}")
        return False


def create_document_via_graphql(session: requests.Session, config: IngestConfig, document: DocumentInput) -> str | None:
    """Create a document using the DataHub GraphQL API."""
    mutation = """
        mutation CreateDocument($input: CreateDocumentInput!) {
            createDocument(input: $input)
        }
    """
    variables = {
        "input": {
            "id": document.id,
            "subType": document.sub_type,
            "title": document.title,
            "contents": {"text": document.text},
            "state": document.state,
        }
    }
    if document.parent_document:
        variables["input"]["parentDocument"] = document.parent_document

    if config.dry_run:
        print(f"  [DRY RUN] Would create document: {document.id}")
        print(f"    Title: {document.title}")
        return f"urn:li:document:{document.id}"

    try:
        response = session.post(
            f"{config.server_url}/api/graphql",
            json={"query": mutation, "variables": variables},
            timeout=30,
        )
        response.raise_for_status()
        result = response.json()
        if "errors" in result:
            print(f"  [ERROR] GraphQL error: {result['errors']}")
            return None
        return result["data"]["createDocument"]
    except requests.RequestException as e:
        print(f"  [ERROR] Request failed: {e}")
        return None


def ingest_documents(config: IngestConfig, documents: list[DocumentInput]) -> dict:
    """Ingest documents with optional embed-first behavior."""
    stats = {"total": len(documents), "success": 0, "failed": 0, "embeddings_written": 0, "urns": []}

    session = requests.Session()
    if config.token:
        session.headers["Authorization"] = f"Bearer {config.token}"

    print(f"\n{'='*60}")
    print(f"Document Ingestion")
    print(f"{'='*60}")
    print(f"  Server: {config.server_url}")
    print(f"  Documents: {len(documents)}")
    print(f"  Embed first: {config.embed_first}")
    print(f"  Dry run: {config.dry_run}")
    print(f"{'='*60}\n")

    for i, doc in enumerate(documents, 1):
        print(f"[{i}/{len(documents)}] {doc.title[:50]}...")

        # Step 1: Write embeddings FIRST if enabled
        if config.embed_first:
            print(f"  Step 1: Writing embeddings FIRST (before ingestion)...")
            if write_embeddings_to_opensearch(config, doc.id, doc.title, doc.text):
                stats["embeddings_written"] += 1
            else:
                print(f"  [WARN] Embeddings write failed, continuing...")

        # Step 2: Create document via GraphQL
        step_num = "2" if config.embed_first else "1"
        print(f"  Step {step_num}: Creating document via GraphQL...")
        urn = create_document_via_graphql(session, config, doc)

        if urn:
            stats["success"] += 1
            stats["urns"].append(urn)
            print(f"  ✅ Created: {urn}")
        else:
            stats["failed"] += 1
            print(f"  ❌ Failed to create document")

        print()

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Ingest sample documents into DataHub",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--count", type=int, default=10, help="Number of documents to generate (default: 10)")
    parser.add_argument("--from-file", type=str, help="Load documents from JSON file instead of generating")
    parser.add_argument("--server", type=str, default=os.environ.get("DATAHUB_SERVER", "http://localhost:8080"),
                        help="DataHub server URL")
    parser.add_argument("--token", type=str, default=os.environ.get("DATAHUB_TOKEN"),
                        help="Authentication token")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without making them")
    parser.add_argument("--embed-first", action="store_true",
                        help="Write embeddings to OpenSearch BEFORE ingesting documents (tests race condition)")
    parser.add_argument("--opensearch-host", type=str, default="localhost", help="OpenSearch host")
    parser.add_argument("--opensearch-port", type=int, default=9200, help="OpenSearch port")

    args = parser.parse_args()
    load_dotenv()

    if not args.token:
        args.token = os.environ.get("DATAHUB_TOKEN")

    config = IngestConfig(
        server_url=args.server,
        token=args.token,
        dry_run=args.dry_run,
        embed_first=args.embed_first,
        opensearch_host=args.opensearch_host,
        opensearch_port=args.opensearch_port,
    )

    # Generate or load documents
    if args.from_file:
        print(f"[INFO] Loading documents from {args.from_file}...")
        documents = load_documents_from_file(args.from_file)
    else:
        print(f"[INFO] Generating {args.count} sample documents...")
        documents = generate_sample_documents(args.count)

    # Ingest
    stats = ingest_documents(config, documents)

    # Summary
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Total documents: {stats['total']}")
    print(f"  Successful: {stats['success']}")
    print(f"  Failed: {stats['failed']}")
    if config.embed_first:
        print(f"  Embeddings written first: {stats['embeddings_written']}")

    if stats["urns"]:
        print(f"\nCreated URNs:")
        for urn in stats["urns"]:
            print(f"  - {urn}")

    if stats["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
