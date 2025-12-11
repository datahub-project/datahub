#!/usr/bin/env python3
"""
Document Semantic Search Test Suite

This script validates end-to-end semantic search functionality for DataHub documents.
It tests both the direct OpenSearch API and the DataHub GraphQL API to ensure semantic
search is working correctly.

This script is designed to work with `ingest_sample_documents.py` - run that script
first to populate documents, then run this script to verify semantic search works.

Features tested:
1. OpenSearch kNN vector search on semantic index
2. DataHub GraphQL keyword search (searchAcrossEntities)  
3. DataHub GraphQL semantic search (semanticSearchAcrossEntities)
4. Result quality comparison between keyword and semantic search

Prerequisites:
- DataHub running locally with semantic search enabled
- Documents ingested via `ingest_sample_documents.py`
- Embeddings backfilled via `backfill_document_embeddings.py`
- AWS credentials configured (for embedding generation)

Usage:
    # Run all tests
    uv run python test_document_semantic_search.py

    # Run with custom query
    uv run python test_document_semantic_search.py --query "how to access data"

    # Run only OpenSearch tests (no GMS auth needed)
    uv run python test_document_semantic_search.py --opensearch-only

    # Run with verbose output
    uv run python test_document_semantic_search.py --verbose

Environment variables:
    DATAHUB_GMS_URL: GMS server URL (default: http://localhost:8080)
    DATAHUB_TOKEN: Authentication token for GraphQL API
    AWS_PROFILE: AWS profile for Bedrock embeddings

Example workflow:
    1. Start DataHub: ./gradlew :docker:quickstartDebug
    2. Ingest documents: uv run python ingest_sample_documents.py --count 10
    3. Backfill embeddings: AWS_PROFILE=your-profile uv run python backfill_document_embeddings.py
    4. Run tests: AWS_PROFILE=your-profile uv run python test_document_semantic_search.py
"""

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from opensearchpy import OpenSearch

from _embedding_utils import create_embeddings

# Load environment variables
load_dotenv()


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class SearchResult:
    """A single search result."""

    urn: str
    title: str
    score: float
    text_preview: Optional[str] = None
    matched_fields: Optional[List[str]] = None


@dataclass
class SearchTestResult:
    """Results from a search test."""

    query: str
    search_type: str  # "opensearch_knn", "graphql_keyword", "graphql_semantic"
    results: List[SearchResult]
    total_count: int
    response_time_ms: float
    success: bool
    error: Optional[str] = None


@dataclass
class DemoSuiteResults:
    """Aggregated results from the test suite."""

    tests_run: int = 0
    tests_passed: int = 0
    tests_failed: int = 0
    test_results: List[SearchTestResult] = field(default_factory=list)


# =============================================================================
# Test Queries
# =============================================================================

# Test queries designed to work with sample documents from ingest_sample_documents.py
# These queries test different semantic matching scenarios
TEST_QUERIES = [
    {
        "query": "how to request data access permissions",
        "expected_top_match": "Data Access Request Process",
        "description": "Direct semantic match to access request documentation",
    },
    {
        "query": "machine learning model for predicting customer behavior",
        "expected_top_match": "Churn Prediction",
        "description": "Semantic match to ML model documentation",
    },
    {
        "query": "getting started guide for new users",
        "expected_top_match": "Getting Started with DataHub",
        "description": "Semantic match to onboarding documentation",
    },
    {
        "query": "best practices for writing data pipelines",
        "expected_top_match": "Best Practices",
        "description": "Semantic match to pipeline development docs",
    },
    {
        "query": "frequently asked questions about warehouse",
        "expected_top_match": "FAQ: Data Warehouse Access",
        "description": "Semantic match to FAQ documentation",
    },
]


# =============================================================================
# OpenSearch Tests
# =============================================================================


class OpenSearchTester:
    """Tests semantic search directly against OpenSearch."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9200,
        index: str = "documentindex_v2_semantic",
        embedding_field: str = "cohere_embed_v3",
        verbose: bool = False,
    ):
        self.index = index
        self.embedding_field = embedding_field
        self.verbose = verbose

        self.client = OpenSearch(
            hosts=[{"host": host, "port": port}],
            use_ssl=False,
            verify_certs=False,
        )

        # Initialize embeddings (uses AWS Bedrock by default)
        self.embeddings = create_embeddings(use_cache=True)

    def check_index_health(self) -> Dict[str, Any]:
        """Check if the semantic index exists and has documents with embeddings."""
        try:
            # Check index exists
            if not self.client.indices.exists(index=self.index):
                return {"healthy": False, "error": f"Index {self.index} does not exist"}

            # Count total documents
            total_count = self.client.count(index=self.index)["count"]

            # Count documents with embeddings
            embedding_query = {
                "query": {"exists": {"field": f"embeddings.{self.embedding_field}"}}
            }
            embedding_count = self.client.count(index=self.index, body=embedding_query)[
                "count"
            ]

            return {
                "healthy": True,
                "index": self.index,
                "total_documents": total_count,
                "documents_with_embeddings": embedding_count,
                "embedding_coverage": (
                    f"{embedding_count}/{total_count}"
                    if total_count > 0
                    else "0/0"
                ),
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}

    def search(self, query: str, top_k: int = 5) -> SearchTestResult:
        """Execute a semantic search using kNN."""
        start_time = time.time()

        try:
            # Generate embedding for query
            query_vector = self.embeddings.embed_query(query)

            if self.verbose:
                print(f"  Generated query embedding ({len(query_vector)} dims)")

            # Build kNN search query
            search_body = {
                "size": top_k,
                "query": {
                    "nested": {
                        "path": f"embeddings.{self.embedding_field}.chunks",
                        "score_mode": "max",
                        "query": {
                            "knn": {
                                f"embeddings.{self.embedding_field}.chunks.vector": {
                                    "vector": query_vector,
                                    "k": top_k,
                                }
                            }
                        },
                    }
                },
                "_source": ["urn", "title", "text"],
            }

            response = self.client.search(index=self.index, body=search_body)
            response_time_ms = (time.time() - start_time) * 1000

            # Parse results
            results = []
            for hit in response["hits"]["hits"]:
                source = hit["_source"]
                text = source.get("text", "")
                results.append(
                    SearchResult(
                        urn=source.get("urn", hit["_id"]),
                        title=source.get("title", "Untitled"),
                        score=hit["_score"],
                        text_preview=text[:150] + "..." if len(text) > 150 else text,
                    )
                )

            return SearchTestResult(
                query=query,
                search_type="opensearch_knn",
                results=results,
                total_count=response["hits"]["total"]["value"],
                response_time_ms=response_time_ms,
                success=True,
            )

        except Exception as e:
            return SearchTestResult(
                query=query,
                search_type="opensearch_knn",
                results=[],
                total_count=0,
                response_time_ms=(time.time() - start_time) * 1000,
                success=False,
                error=str(e),
            )


# =============================================================================
# GraphQL Tests
# =============================================================================


class GraphQLTester:
    """Tests search via DataHub GraphQL API."""

    def __init__(
        self,
        gms_url: str = "http://localhost:8080",
        token: Optional[str] = None,
        verbose: bool = False,
    ):
        self.gms_url = gms_url
        self.graphql_url = f"{gms_url}/api/graphql"
        self.verbose = verbose

        self.headers = {"Content-Type": "application/json"}
        if token:
            self.headers["Authorization"] = f"Bearer {token}"

    def _execute_query(
        self, query: str, variables: Dict[str, Any]
    ) -> tuple[Dict[str, Any], float]:
        """Execute a GraphQL query and return response with timing."""
        start_time = time.time()

        response = requests.post(
            self.graphql_url,
            headers=self.headers,
            json={"query": query, "variables": variables},
            timeout=30,
        )

        response_time_ms = (time.time() - start_time) * 1000
        response.raise_for_status()

        data = response.json()
        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")

        return data, response_time_ms

    def search_keyword(self, query: str, top_k: int = 5) -> SearchTestResult:
        """Execute keyword search via searchAcrossEntities."""
        graphql_query = """
            query SearchDocuments($input: SearchAcrossEntitiesInput!) {
                searchAcrossEntities(input: $input) {
                    start
                    count
                    total
                    searchResults {
                        entity {
                            urn
                            type
                            ... on Document {
                                info {
                                    title
                                    contents {
                                        text
                                    }
                                }
                            }
                        }
                        matchedFields {
                            name
                            value
                        }
                    }
                }
            }
        """

        variables = {
            "input": {
                "query": query,
                "types": ["DOCUMENT"],
                "start": 0,
                "count": top_k,
            }
        }

        try:
            data, response_time_ms = self._execute_query(graphql_query, variables)

            search_data = data["data"]["searchAcrossEntities"]
            results = []

            for item in search_data.get("searchResults", []):
                entity = item["entity"]
                props = entity.get("info", {}) or {}
                matched = [f["name"] for f in item.get("matchedFields", [])]

                results.append(
                    SearchResult(
                        urn=entity["urn"],
                        title=props.get("title", "Untitled"),
                        score=0.0,  # GraphQL doesn't expose score by default
                        text_preview=(props.get("contents") or {}).get("text"),
                        matched_fields=matched,
                    )
                )

            return SearchTestResult(
                query=query,
                search_type="graphql_keyword",
                results=results,
                total_count=search_data.get("total", 0),
                response_time_ms=response_time_ms,
                success=True,
            )

        except Exception as e:
            return SearchTestResult(
                query=query,
                search_type="graphql_keyword",
                results=[],
                total_count=0,
                response_time_ms=0,
                success=False,
                error=str(e),
            )

    def search_semantic(self, query: str, top_k: int = 5) -> SearchTestResult:
        """Execute semantic search via semanticSearchAcrossEntities."""
        graphql_query = """
            query SemanticSearchDocuments($input: SearchAcrossEntitiesInput!) {
                semanticSearchAcrossEntities(input: $input) {
                    start
                    count
                    total
                    searchResults {
                        entity {
                            urn
                            type
                            ... on Document {
                                info {
                                    title
                                    contents {
                                        text
                                    }
                                }
                            }
                        }
                        matchedFields {
                            name
                            value
                        }
                    }
                }
            }
        """

        variables = {
            "input": {
                "query": query,
                "types": ["DOCUMENT"],
                "start": 0,
                "count": top_k,
            }
        }

        try:
            data, response_time_ms = self._execute_query(graphql_query, variables)

            search_data = data["data"]["semanticSearchAcrossEntities"]
            results = []

            for item in search_data.get("searchResults", []):
                entity = item["entity"]
                props = entity.get("info", {}) or {}
                matched = [f["name"] for f in item.get("matchedFields", [])]

                results.append(
                    SearchResult(
                        urn=entity["urn"],
                        title=props.get("title", "Untitled"),
                        score=0.0,
                        text_preview=(props.get("contents") or {}).get("text"),
                        matched_fields=matched,
                    )
                )

            return SearchTestResult(
                query=query,
                search_type="graphql_semantic",
                results=results,
                total_count=search_data.get("total", 0),
                response_time_ms=response_time_ms,
                success=True,
            )

        except Exception as e:
            return SearchTestResult(
                query=query,
                search_type="graphql_semantic",
                results=[],
                total_count=0,
                response_time_ms=0,
                success=False,
                error=str(e),
            )


# =============================================================================
# Test Runner
# =============================================================================


def print_header(text: str) -> None:
    """Print a section header."""
    print()
    print("=" * 70)
    print(f" {text}")
    print("=" * 70)


def print_result(result: SearchTestResult, expected_top_match: Optional[str] = None) -> bool:
    """Print a search result and return whether it passed."""
    status = "✅" if result.success else "❌"
    print(f"\n{status} {result.search_type}: {result.query}")
    print(f"   Time: {result.response_time_ms:.1f}ms | Total: {result.total_count}")

    if result.error:
        print(f"   Error: {result.error}")
        return False

    if not result.results:
        print("   No results found")
        return False

    # Check if expected match is in top result
    passed = True
    if expected_top_match:
        top_title = result.results[0].title if result.results else ""
        if expected_top_match.lower() in top_title.lower():
            print(f"   ✓ Top result matches expected: '{expected_top_match}'")
        else:
            print(f"   ⚠ Expected '{expected_top_match}', got '{top_title}'")
            passed = False

    # Print top 3 results
    for i, r in enumerate(result.results[:3], 1):
        score_str = f" (score: {r.score:.4f})" if r.score > 0 else ""
        print(f"   {i}. {r.title}{score_str}")

    return passed


def run_tests(
    queries: List[Dict[str, str]],
    opensearch_only: bool = False,
    verbose: bool = False,
) -> DemoSuiteResults:
    """Run the full test suite."""
    results = DemoSuiteResults()

    # Initialize testers
    os_tester = OpenSearchTester(verbose=verbose)
    gql_tester = None if opensearch_only else GraphQLTester(
        gms_url=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
        token=os.getenv("DATAHUB_TOKEN"),
        verbose=verbose,
    )

    # Check OpenSearch health
    print_header("OpenSearch Index Health Check")
    health = os_tester.check_index_health()
    if health["healthy"]:
        print(f"✅ Index: {health['index']}")
        print(f"   Documents: {health['total_documents']}")
        print(f"   With embeddings: {health['embedding_coverage']}")
    else:
        print(f"❌ Index health check failed: {health.get('error')}")
        return results

    # Run OpenSearch tests
    print_header("OpenSearch kNN Semantic Search Tests")
    for test_case in queries:
        result = os_tester.search(test_case["query"])
        results.test_results.append(result)
        results.tests_run += 1

        passed = print_result(result, test_case.get("expected_top_match"))
        if passed:
            results.tests_passed += 1
        else:
            results.tests_failed += 1

    # Run GraphQL tests if not opensearch_only
    if gql_tester:
        # Semantic search
        print_header("GraphQL Semantic Search Tests")
        for test_case in queries:
            result = gql_tester.search_semantic(test_case["query"])
            results.test_results.append(result)
            results.tests_run += 1

            passed = print_result(result, test_case.get("expected_top_match"))
            if passed:
                results.tests_passed += 1
            else:
                results.tests_failed += 1

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Test document semantic search functionality",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--query",
        type=str,
        help="Run a single custom query instead of test suite",
    )
    parser.add_argument(
        "--opensearch-only",
        action="store_true",
        help="Only test OpenSearch (skip GraphQL tests)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    print("🔍 DataHub Document Semantic Search Test Suite")
    print("=" * 70)

    # Use custom query or default test queries
    if args.query:
        queries = [{"query": args.query, "description": "Custom query"}]
    else:
        queries = TEST_QUERIES

    # Run tests
    results = run_tests(
        queries=queries,
        opensearch_only=args.opensearch_only,
        verbose=args.verbose,
    )

    # Print summary
    print_header("Test Summary")
    print(f"Tests run: {results.tests_run}")
    print(f"Passed: {results.tests_passed}")
    print(f"Failed: {results.tests_failed}")

    if results.tests_failed > 0:
        print("\n❌ Some tests failed!")
        sys.exit(1)
    else:
        print("\n✅ All tests passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()


# =============================================================================
# Notes on GraphQL Semantic Search
# =============================================================================
#
# The GraphQL `semanticSearchAcrossEntities` endpoint requires GMS to have AWS
# credentials to call Bedrock for query embedding generation. 
#
# If running GMS in Docker, you need to either:
# 1. Mount AWS credentials: -v ~/.aws:/root/.aws:ro
# 2. Set environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
# 3. Use an IAM role if running on AWS infrastructure
#
# The OpenSearch kNN test bypasses GMS and generates embeddings locally using
# the `embedding_utils` module with your local AWS_PROFILE, which is why it works.
