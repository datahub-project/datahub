#!/usr/bin/env python3
"""
Test GraphQL semantic search functionality using dedicated endpoints.

This test validates that the new dedicated semantic search endpoints can be invoked
through the DataHub GraphQL API and returns meaningful results. Uses:
- semanticSearchAcrossEntities() for semantic search
- searchAcrossEntities() for keyword search

Prerequisites:
- DataHub instance running locally (default: http://localhost:8080)
- DataHub fork with dedicated semantic search endpoints
- Semantic search enabled in DataHub configuration
- OpenSearch semantic indices created and populated with embeddings
- Valid authentication token (if required)

Usage:
    cd semantic-search-poc
    python test_graphql_semantic_search.py

Environment Variables:
    DATAHUB_GMS_URL: DataHub GMS URL (default: http://localhost:8080)
    DATAHUB_TOKEN: Authentication token (optional, for secured instances)
"""

import json
import os
import sys
from typing import Dict, List, Optional, Any
import requests
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# Load environment variables from .env file
load_dotenv()

# Initialize Rich console for beautiful output
console = Console()


class GraphQLSemanticSearchTester:
    """
    Tests semantic search functionality through DataHub's GraphQL API.
    """
    
    def __init__(self, gms_url: str = None, token: str = None):
        """
        Initialize the tester with DataHub connection details.
        
        Args:
            gms_url: DataHub GMS URL (defaults to env var or localhost:8080)
            token: Authentication token (optional)
        """
        self.gms_url = gms_url or os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
        self.token = token or os.getenv("DATAHUB_TOKEN")
        self.graphql_endpoint = f"{self.gms_url}/api/graphql"
        
        # Headers for GraphQL requests
        self.headers = {
            "Content-Type": "application/json",
        }
        if self.token:
            self.headers["Authorization"] = f"Bearer {self.token}"
    
    def _execute_graphql_query(self, query: str, variables: Optional[Dict] = None) -> Dict:
        """
        Execute a GraphQL query against the DataHub API.
        
        Args:
            query: GraphQL query string
            variables: Optional variables for the query
            
        Returns:
            Response data from the GraphQL API
            
        Raises:
            requests.HTTPError: If the request fails
        """
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        
        response = requests.post(
            self.graphql_endpoint,
            headers=self.headers,
            json=payload,
            timeout=30
        )
        
        # Check for HTTP errors
        response.raise_for_status()
        
        # Parse and return JSON response
        return response.json()
    
    def test_keyword_search(self, query: str, entity_types: Optional[List[str]] = None) -> Dict:
        """
        Test traditional keyword search (baseline for comparison).
        
        Args:
            query: Search query string
            entity_types: Optional list of entity types to search
            
        Returns:
            Search results from keyword search
        """
        graphql_query = """
        query searchAcrossEntities(
            $query: String!
            $types: [EntityType!]
            $start: Int!
            $count: Int!
        ) {
            searchAcrossEntities(
                input: {
                    query: $query
                    types: $types
                    start: $start
                    count: $count
                    searchFlags: {
                        skipHighlighting: true
                    }
                }
            ) {
                start
                count
                total
                searchResults {
                    entity {
                        urn
                        type
                        ... on Dataset {
                            name
                            properties {
                                name
                                description
                            }
                            platform {
                                name
                            }
                        }
                        ... on Chart {
                            properties {
                                name
                                description
                            }
                        }
                        ... on Dashboard {
                            properties {
                                name
                                description
                            }
                        }
                    }
                }
            }
        }
        """
        
        variables = {
            "query": query,
            "types": entity_types or ["DATASET", "CHART", "DASHBOARD"],
            "start": 0,
            "count": 10
        }
        
        return self._execute_graphql_query(graphql_query, variables)
    
    def test_semantic_search(self, query: str, entity_types: Optional[List[str]] = None) -> Dict:
        """
        Test semantic search functionality using the dedicated semantic search endpoint.
        
        Args:
            query: Search query string
            entity_types: Optional list of entity types to search
            
        Returns:
            Search results from semantic search
        """
        graphql_query = """
        query semanticSearchAcrossEntities(
            $query: String!
            $types: [EntityType!]
            $start: Int!
            $count: Int!
        ) {
            semanticSearchAcrossEntities(
                input: {
                    query: $query
                    types: $types
                    start: $start
                    count: $count
                    searchFlags: {
                        skipHighlighting: true
                    }
                }
            ) {
                start
                count
                total
                searchResults {
                    entity {
                        urn
                        type
                        ... on Dataset {
                            name
                            properties {
                                name
                                description
                            }
                            platform {
                                name
                            }
                        }
                        ... on Chart {
                            properties {
                                name
                                description
                            }
                        }
                        ... on Dashboard {
                            properties {
                                name
                                description
                            }
                        }
                    }
                }
            }
        }
        """
        
        variables = {
            "query": query,
            "types": entity_types or ["DATASET", "CHART", "DASHBOARD"],
            "start": 0,
            "count": 10
        }
        
        result = self._execute_graphql_query(graphql_query, variables)
        
        # If there's an error, print the full response for debugging
        if "errors" in result:
            console.print(f"[yellow]Full semantic search response: {json.dumps(result, indent=2)}[/yellow]")
        
        return result
    
    def _format_search_results(self, results: Dict, search_type: str) -> None:
        """
        Format and display search results in a beautiful table.
        
        Args:
            results: Search results from GraphQL
            search_type: Type of search ("Keyword" or "Semantic")
        """
        # Extract search results - handle both keyword and semantic endpoints
        search_data = results.get("data", {})
        # Try semantic endpoint first, then fall back to keyword endpoint
        search_results_data = (search_data.get("semanticSearchAcrossEntities") or 
                              search_data.get("searchAcrossEntities") or {})
        total = search_results_data.get("total", 0)
        search_results = search_results_data.get("searchResults", [])
        
        # Create a results panel
        console.print(Panel(
            f"[bold]{search_type} Search Results[/bold]\n"
            f"Total Results: {total}\n"
            f"Showing: {len(search_results)}",
            style="cyan"
        ))
        
        if not search_results:
            console.print("[yellow]No results found.[/yellow]")
            return
        
        # Create a table for results
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("No.", style="dim", width=4)
        table.add_column("Type", width=12)
        table.add_column("Name", width=40)
        table.add_column("Platform", width=12)
        table.add_column("Description", width=50)
        
        for idx, result in enumerate(search_results, 1):
            entity = result.get("entity", {})
            entity_type = entity.get("type", "Unknown")
            urn = entity.get("urn", "")
            
            # Extract name and description based on entity type
            name = "Unknown"
            description = "No description"
            platform = "N/A"
            
            if entity_type == "DATASET":
                name = entity.get("name", "") or entity.get("properties", {}).get("name", "Unknown")
                description = entity.get("properties", {}).get("description", "No description") or "No description"
                platform = entity.get("platform", {}).get("name", "Unknown") if entity.get("platform") else "Unknown"
            elif entity_type in ["CHART", "DASHBOARD"]:
                props = entity.get("properties", {})
                name = props.get("name", "Unknown")
                description = props.get("description", "No description") or "No description"
            
            # Truncate long descriptions
            if len(description) > 47:
                description = description[:47] + "..."
            
            table.add_row(
                str(idx),
                entity_type,
                name,
                platform,
                description
            )
        
        console.print(table)
    
    def run_comparison_test(self, test_queries: List[str]) -> None:
        """
        Run comparison tests between keyword and semantic search.
        
        Args:
            test_queries: List of test queries to run
        """
        console.print("\n[bold cyan]🔍 Testing DataHub GraphQL Semantic Search[/bold cyan]\n")
        console.print(f"📍 Endpoint: {self.graphql_endpoint}")
        console.print(f"🔐 Authentication: {'Enabled' if self.token else 'Disabled'}\n")
        
        for query in test_queries:
            console.print(f"\n[bold yellow]Query: '{query}'[/bold yellow]\n")
            
            # Test keyword search
            try:
                console.print("[dim]Running keyword search...[/dim]")
                keyword_results = self.test_keyword_search(query)
                self._format_search_results(keyword_results, "Keyword")
            except Exception as e:
                console.print(f"[red]❌ Keyword search failed: {e}[/red]")
            
            console.print()  # Add spacing
            
            # Test semantic search
            try:
                console.print("[dim]Running semantic search...[/dim]")
                semantic_results = self.test_semantic_search(query)
                # Check for GraphQL errors
                if "errors" in semantic_results:
                    console.print(f"[red]GraphQL errors: {json.dumps(semantic_results['errors'], indent=2)}[/red]")
                self._format_search_results(semantic_results, "Semantic")
            except Exception as e:
                console.print(f"[red]❌ Semantic search failed: {e}[/red]")
                # Check if it's an authorization or feature disabled error
                if "errors" in str(e).lower() or "not enabled" in str(e).lower():
                    console.print("[yellow]ℹ️  Semantic search may not be enabled or authorized.[/yellow]")
                    console.print("[yellow]   Check SEMANTIC_SEARCH_ENABLED and SEMANTIC_SEARCH_ALLOWED_USERS config.[/yellow]")
            
            console.print("\n" + "="*80 + "\n")
    
    def validate_semantic_search_availability(self) -> bool:
        """
        Validate if semantic search is available and working.
        
        Returns:
            True if semantic search is available, False otherwise
        """
        console.print("[bold]🧪 Validating Semantic Search Availability[/bold]\n")
        
        test_query = "test query for validation"
        
        try:
            # Try a simple semantic search
            result = self.test_semantic_search(test_query, ["DATASET"])
            
            # Check if we got a valid response from either endpoint
            if ("data" in result and 
                ("semanticSearchAcrossEntities" in result["data"] or 
                 "searchAcrossEntities" in result["data"])):
                console.print("[green]✅ Semantic search is available and responding![/green]")
                
                # Check if there are any errors in the response
                if "errors" in result:
                    console.print("[yellow]⚠️  Response contains errors:[/yellow]")
                    for error in result["errors"]:
                        console.print(f"   - {error.get('message', 'Unknown error')}")
                    return False
                
                return True
            else:
                console.print("[red]❌ Unexpected response structure[/red]")
                console.print(f"Response: {json.dumps(result, indent=2)}")
                return False
                
        except requests.HTTPError as e:
            console.print(f"[red]❌ HTTP Error: {e}[/red]")
            console.print(f"[red]   Status Code: {e.response.status_code}[/red]")
            console.print(f"[red]   Response: {e.response.text}[/red]")
            return False
        except Exception as e:
            console.print(f"[red]❌ Error: {e}[/red]")
            return False


def main():
    """
    Main function to run the semantic search tests.
    """
    # Initialize the tester
    tester = GraphQLSemanticSearchTester()
    
    # First, validate that semantic search is available
    if not tester.validate_semantic_search_availability():
        console.print("\n[yellow]⚠️  Semantic search validation failed. Some tests may not work.[/yellow]")
        console.print("[yellow]   Continuing with tests anyway...[/yellow]\n")
    
    # Define test queries - these should be meaningful for your data
    test_queries = [
        "customer data and user information",
        "financial metrics and revenue analysis",
        "product analytics dashboards",
        "machine learning models and predictions",
        "sales performance reports"
    ]
    
    # Run comparison tests
    tester.run_comparison_test(test_queries)
    
    console.print("[bold green]✨ Test completed![/bold green]")


if __name__ == "__main__":
    main()
