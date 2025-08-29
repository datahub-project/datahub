#!/usr/bin/env python3
"""
Integration test for GraphQL semantic search with performance metrics.

This comprehensive test validates the new dedicated semantic search endpoints, 
compares them with keyword search, and provides performance metrics and result 
quality analysis.

Uses the new dedicated endpoints:
- semanticSearchAcrossEntities() for semantic search
- searchAcrossEntities() for keyword search

Prerequisites:
- DataHub instance running locally (default: http://localhost:8080)
- Semantic search enabled with proper configuration
- OpenSearch semantic indices populated
- Test datasets with embeddings
- DataHub fork with dedicated semantic search endpoints

Usage:
    cd semantic-search-poc
    python test_graphql_semantic_search_integration.py
"""

import json
import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
import requests
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

# Load environment variables
load_dotenv()

# Initialize console
console = Console()


@dataclass
class SearchResult:
    """Container for search result data."""
    urn: str
    entity_type: str
    name: str
    description: Optional[str]
    platform: Optional[str]
    score: Optional[float] = None
    matched_fields: Optional[List[Dict]] = None
    extra_properties: Optional[Dict[str, str]] = None


@dataclass
class SearchMetrics:
    """Container for search performance metrics."""
    query: str
    search_type: str
    total_results: int
    returned_results: int
    response_time_ms: float
    errors: Optional[List[str]] = None


class SemanticSearchIntegrationTest:
    """
    Comprehensive integration test for semantic search in DataHub GraphQL API.
    """
    
    def __init__(self, gms_url: str = None, token: str = None, verbose: bool = False):
        """
        Initialize the integration test.
        
        Args:
            gms_url: DataHub GMS URL
            token: Authentication token
            verbose: Enable verbose output
        """
        self.gms_url = gms_url or os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
        self.token = token or os.getenv("DATAHUB_TOKEN")
        self.graphql_endpoint = f"{self.gms_url}/api/graphql"
        self.verbose = verbose
        
        # Headers for requests
        self.headers = {"Content-Type": "application/json"}
        if self.token:
            self.headers["Authorization"] = f"Bearer {self.token}"
        
        # Metrics storage
        self.metrics: List[SearchMetrics] = []
    
    def _execute_graphql(self, query: str, variables: Dict) -> Tuple[Dict, float]:
        """
        Execute a GraphQL query and measure response time.
        
        Returns:
            Tuple of (response_data, response_time_ms)
        """
        payload = {"query": query, "variables": variables}
        
        if self.verbose:
            console.print(f"[dim]Sending query with variables: {variables}[/dim]")
            console.print(f"[dim]Query (first 500 chars): {query[:500]}...[/dim]")
        
        start_time = time.time()
        response = requests.post(
            self.graphql_endpoint,
            headers=self.headers,
            json=payload,
            timeout=30
        )
        response_time_ms = (time.time() - start_time) * 1000
        
        if self.verbose and response.status_code != 200:
            console.print(f"[red]HTTP {response.status_code}: {response.text[:500]}[/red]")
        
        response.raise_for_status()
        return response.json(), response_time_ms
    
    def _build_search_query(self, use_semantic_endpoint: bool = False, fetch_extra_fields: Optional[List[str]] = None) -> str:
        """
        Build the GraphQL search query.
        
        Args:
            use_semantic_endpoint: Whether to use the dedicated semantic search endpoint
            fetch_extra_fields: Optional list of extra fields to fetch from OpenSearch
            
        Returns:
            GraphQL query string
        """
        # Choose the appropriate endpoint and parameters
        if use_semantic_endpoint:
            # Use dedicated semanticSearchAcrossEntities endpoint
            endpoint_name = "semanticSearchAcrossEntities"
            query_name = "semanticSearchAcrossEntities"
        else:
            # Use regular searchAcrossEntities endpoint for keyword search
            endpoint_name = "searchAcrossEntities"
            query_name = "searchAcrossEntities"
        
        # Build search flags with fetchExtraFields if provided
        if fetch_extra_fields:
            extra_fields_str = json.dumps(fetch_extra_fields)
            search_flags = f"""
                    searchFlags: {{
                        skipHighlighting: true
                        fetchExtraFields: {extra_fields_str}
                    }}"""
        else:
            search_flags = """
                    searchFlags: {
                        skipHighlighting: true
                    }"""
        
        return f"""
        query {query_name}(
            $query: String!
            $types: [EntityType!]
            $start: Int!
            $count: Int!
        ) {{
            {endpoint_name}(
                input: {{
                    query: $query
                    types: $types
                    start: $start
                    count: $count{search_flags}
                }}
            ) {{
                start
                count
                total
                searchResults {{
                    entity {{
                        urn
                        type
                        ... on Dataset {{
                            name
                            properties {{
                                name
                                description
                                qualifiedName
                            }}
                            platform {{
                                name
                                properties {{
                                    displayName
                                }}
                            }}
                        }}
                        ... on Chart {{
                            properties {{
                                name
                                description
                            }}
                            platform {{
                                name
                            }}
                        }}
                        ... on Dashboard {{
                            properties {{
                                name
                                description
                            }}
                            platform {{
                                name
                            }}
                        }}
                        ... on Container {{
                            properties {{
                                name
                                description
                            }}
                            platform {{
                                name
                            }}
                        }}
                    }}
                    matchedFields {{
                        name
                        value
                    }}
                    extraProperties {{
                        name
                        value
                    }}
                }}
                facets {{
                    field
                    aggregations {{
                        value
                        count
                    }}
                }}
            }}
        }}
        """
    
    def perform_search(
        self, 
        query: str, 
        search_mode: str = "KEYWORD",
        entity_types: Optional[List[str]] = None,
        count: int = 10,
        fetch_extra_fields: Optional[List[str]] = None
    ) -> Tuple[List[SearchResult], SearchMetrics]:
        """
        Perform a search and return parsed results with metrics.
        
        Args:
            query: Search query
            search_mode: "KEYWORD" or "SEMANTIC"
            entity_types: Entity types to search
            count: Number of results to return
            fetch_extra_fields: Optional list of extra fields to fetch from OpenSearch
            
        Returns:
            Tuple of (search_results, metrics)
        """
        # Build query based on mode - use dedicated semantic endpoint for semantic search
        use_semantic_endpoint = (search_mode == "SEMANTIC")
        graphql_query = self._build_search_query(use_semantic_endpoint, fetch_extra_fields)
        
        variables = {
            "query": query,
            "types": entity_types or ["DATASET", "CHART", "DASHBOARD", "CONTAINER"],
            "start": 0,
            "count": count
        }
        
        try:
            # Execute search
            response, response_time = self._execute_graphql(graphql_query, variables)
            
            # Check for errors in response
            if "errors" in response:
                error_msg = response["errors"][0].get("message", "Unknown error")
                if self.verbose:
                    print(f"GraphQL Error: {error_msg}")
                if "disabled" in error_msg.lower():
                    raise Exception(f"Semantic search is disabled: {error_msg}")
            
            # Parse results - handle both endpoints
            data = response.get("data", {})
            # Try semantic endpoint first, then regular endpoint
            search_data = (data.get("semanticSearchAcrossEntities") or 
                          data.get("searchAcrossEntities") or {})
            total = search_data.get("total", 0)
            results_data = search_data.get("searchResults", [])
            
            # Convert to SearchResult objects
            results = []
            for item in results_data:
                entity = item.get("entity", {})
                entity_type = entity.get("type", "UNKNOWN")
                
                # Extract properties based on entity type
                name = "Unknown"
                description = None
                platform = None
                
                if entity_type == "DATASET":
                    props = entity.get("properties", {})
                    name = entity.get("name") or props.get("name") or props.get("qualifiedName", "Unknown")
                    description = props.get("description")
                    platform_data = entity.get("platform", {})
                    platform = platform_data.get("name") or platform_data.get("properties", {}).get("displayName")
                elif entity_type in ["CHART", "DASHBOARD", "CONTAINER"]:
                    props = entity.get("properties", {})
                    name = props.get("name", "Unknown")
                    description = props.get("description")
                    platform = entity.get("platform", {}).get("name") if entity.get("platform") else None
                
                # Get semantic similarity score if available
                score = item.get("semanticSimilarity") if use_semantic_endpoint else None
                
                # Parse extra properties if available
                extra_props = {}
                for prop in item.get("extraProperties", []):
                    extra_props[prop["name"]] = prop["value"]
                
                results.append(SearchResult(
                    urn=entity.get("urn", ""),
                    entity_type=entity_type,
                    name=name,
                    description=description,
                    platform=platform,
                    score=score,
                    matched_fields=item.get("matchedFields"),
                    extra_properties=extra_props if extra_props else None
                ))
            
            # Create metrics
            metrics = SearchMetrics(
                query=query,
                search_type=search_mode,
                total_results=total,
                returned_results=len(results),
                response_time_ms=response_time
            )
            
            # Store metrics
            self.metrics.append(metrics)
            
            return results, metrics
            
        except Exception as e:
            # Create error metrics
            if self.verbose:
                console.print(f"[red]Error in {search_mode} search: {e}[/red]")
                import traceback
                console.print(f"[dim]{traceback.format_exc()}[/dim]")
            metrics = SearchMetrics(
                query=query,
                search_type=search_mode,
                total_results=0,
                returned_results=0,
                response_time_ms=0,
                errors=[str(e)]
            )
            self.metrics.append(metrics)
            return [], metrics
    
    def compare_search_modes(self, query: str) -> Dict[str, Any]:
        """
        Compare keyword and semantic search for the same query.
        
        Args:
            query: Search query to test
            
        Returns:
            Comparison results dictionary
        """
        console.print(f"\n[bold]Query: '{query}'[/bold]")
        
        # Perform keyword search
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True
        ) as progress:
            task = progress.add_task("Running keyword search...", total=None)
            keyword_results, keyword_metrics = self.perform_search(query, "KEYWORD")
            progress.update(task, completed=True)
        
        # Perform semantic search
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True
        ) as progress:
            task = progress.add_task("Running semantic search...", total=None)
            semantic_results, semantic_metrics = self.perform_search(query, "SEMANTIC")
            progress.update(task, completed=True)
        
        # Calculate overlap
        keyword_urns = {r.urn for r in keyword_results}
        semantic_urns = {r.urn for r in semantic_results}
        overlap = keyword_urns & semantic_urns
        
        # Create comparison
        comparison = {
            "query": query,
            "keyword": {
                "total": keyword_metrics.total_results,
                "returned": keyword_metrics.returned_results,
                "response_time_ms": keyword_metrics.response_time_ms,
                "results": keyword_results[:5],  # Top 5 for display
                "errors": keyword_metrics.errors
            },
            "semantic": {
                "total": semantic_metrics.total_results,
                "returned": semantic_metrics.returned_results,
                "response_time_ms": semantic_metrics.response_time_ms,
                "results": semantic_results[:5],  # Top 5 for display
                "errors": semantic_metrics.errors
            },
            "overlap": {
                "count": len(overlap),
                "percentage": (len(overlap) / min(len(keyword_urns), len(semantic_urns)) * 100) 
                              if keyword_urns and semantic_urns else 0
            }
        }
        
        return comparison
    
    def display_comparison(self, comparison: Dict[str, Any]) -> None:
        """
        Display search comparison results in a formatted table.
        
        Args:
            comparison: Comparison results from compare_search_modes
        """
        # Create metrics comparison table
        metrics_table = Table(title="Search Metrics Comparison", show_header=True)
        metrics_table.add_column("Metric", style="cyan")
        metrics_table.add_column("Keyword Search", style="yellow")
        metrics_table.add_column("Semantic Search", style="green")
        
        # Add metrics rows
        metrics_table.add_row(
            "Total Results",
            str(comparison["keyword"]["total"]),
            str(comparison["semantic"]["total"])
        )
        metrics_table.add_row(
            "Response Time (ms)",
            f"{comparison['keyword']['response_time_ms']:.2f}",
            f"{comparison['semantic']['response_time_ms']:.2f}"
        )
        metrics_table.add_row(
            "Result Overlap",
            f"{comparison['overlap']['count']} results",
            f"{comparison['overlap']['percentage']:.1f}%"
        )
        
        console.print(metrics_table)
        
        # Display top results comparison
        if self.verbose or (comparison["keyword"]["results"] or comparison["semantic"]["results"]):
            results_table = Table(title="Top 5 Results Comparison", show_header=True)
            results_table.add_column("#", width=3)
            results_table.add_column("Keyword Results", width=50)
            results_table.add_column("Semantic Results", width=50)
            
            for i in range(5):
                keyword_text = ""
                semantic_text = ""
                
                if i < len(comparison["keyword"]["results"]):
                    kr = comparison["keyword"]["results"][i]
                    keyword_text = f"[yellow]{kr.name}[/yellow]\n[dim]{kr.entity_type}"
                    if kr.platform:
                        keyword_text += f" • {kr.platform}[/dim]"
                
                if i < len(comparison["semantic"]["results"]):
                    sr = comparison["semantic"]["results"][i]
                    semantic_text = f"[green]{sr.name}[/green]\n[dim]{sr.entity_type}"
                    if sr.platform:
                        semantic_text += f" • {sr.platform}"
                    if sr.score is not None:
                        semantic_text += f" • Score: {sr.score:.3f}[/dim]"
                
                results_table.add_row(str(i + 1), keyword_text, semantic_text)
            
            console.print(results_table)
        
        # Display errors if any
        if comparison["keyword"]["errors"]:
            console.print(f"[red]Keyword search errors: {comparison['keyword']['errors']}[/red]")
        if comparison["semantic"]["errors"]:
            console.print(f"[red]Semantic search errors: {comparison['semantic']['errors']}[/red]")
    
    def test_fetch_extra_fields(self, query: str = "customer analytics") -> Dict[str, Any]:
        """
        Test the fetchExtraFields functionality for field selection optimization.
        
        Args:
            query: Search query to test
            
        Returns:
            Test results dictionary
        """
        console.print(f"\n[bold]Testing fetchExtraFields with query: '{query}'[/bold]")
        
        # Define field sets to test
        field_sets = {
            "minimal": None,  # No extra fields
            "basic": ["name", "platform", "browsePaths"],
            "comprehensive": [
                "name", "platform", "browsePaths", "browsePathV2",
                "owners", "tags", "glossaryTerms", "description",
                "customProperties", "container", "created", 
                "lastModified", "usageCountLast30Days"
            ]
        }
        
        results = {}
        for field_set_name, fields in field_sets.items():
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
                transient=True
            ) as progress:
                task = progress.add_task(f"Testing {field_set_name} field set...", total=None)
                
                # Run semantic search with fetchExtraFields
                search_results, metrics = self.perform_search(
                    query, 
                    "SEMANTIC", 
                    fetch_extra_fields=fields
                )
                
                progress.update(task, completed=True)
            
            # Analyze extra properties
            extra_field_counts = []
            for result in search_results[:5]:  # Check top 5 results
                if result.extra_properties:
                    extra_field_counts.append(len(result.extra_properties))
            
            results[field_set_name] = {
                "fields_requested": len(fields) if fields else 0,
                "response_time_ms": metrics.response_time_ms,
                "avg_extra_fields": sum(extra_field_counts) / len(extra_field_counts) if extra_field_counts else 0,
                "sample_result": search_results[0] if search_results else None
            }
        
        # Display results
        self.display_fetch_extra_fields_results(results)
        return results
    
    def display_fetch_extra_fields_results(self, results: Dict[str, Any]) -> None:
        """
        Display fetchExtraFields test results in a formatted table.
        
        Args:
            results: Test results from test_fetch_extra_fields
        """
        table = Table(title="fetchExtraFields Performance Impact", show_header=True)
        table.add_column("Field Set", style="cyan")
        table.add_column("Fields Requested", style="yellow")
        table.add_column("Avg Extra Fields", style="green")
        table.add_column("Response Time (ms)", style="red")
        table.add_column("Performance Impact", style="magenta")
        
        baseline_time = results.get("minimal", {}).get("response_time_ms", 0)
        
        for field_set_name, data in results.items():
            perf_impact = ""
            if baseline_time > 0 and field_set_name != "minimal":
                impact_pct = ((data["response_time_ms"] - baseline_time) / baseline_time) * 100
                perf_impact = f"+{impact_pct:.1f}%" if impact_pct > 0 else f"{impact_pct:.1f}%"
            
            table.add_row(
                field_set_name.capitalize(),
                str(data["fields_requested"]),
                f"{data['avg_extra_fields']:.1f}",
                f"{data['response_time_ms']:.2f}",
                perf_impact if field_set_name != "minimal" else "Baseline"
            )
        
        console.print(table)
        
        # Show sample extra properties
        if results.get("comprehensive", {}).get("sample_result"):
            sample = results["comprehensive"]["sample_result"]
            if sample.extra_properties:
                console.print("\n[bold]Sample Extra Properties (Comprehensive):[/bold]")
                for key, value in list(sample.extra_properties.items())[:5]:
                    # Truncate long values
                    display_value = value[:100] + "..." if len(value) > 100 else value
                    console.print(f"  • {key}: {display_value}")
    
    def run_test_suite(self) -> None:
        """
        Run a comprehensive test suite with multiple query types.
        """
        console.print(Panel.fit(
            "[bold cyan]DataHub GraphQL Semantic Search Integration Test[/bold cyan]\n"
            f"Endpoint: {self.graphql_endpoint}\n"
            f"Authentication: {'Enabled' if self.token else 'Disabled'}",
            border_style="cyan"
        ))
        
        # Test queries covering different search intents
        test_queries = [
            # Conceptual queries (should benefit from semantic search)
            "customer data privacy and compliance",
            "financial reporting and analytics",
            "user behavior tracking metrics",
            
            # Technical queries
            "kafka streaming pipeline",
            "snowflake warehouse tables",
            
            # Mixed queries
            "sales revenue dashboard Q4",
            "machine learning feature store"
        ]
        
        # Run comparisons
        all_comparisons = []
        for query in test_queries:
            comparison = self.compare_search_modes(query)
            self.display_comparison(comparison)
            all_comparisons.append(comparison)
            console.print("─" * 80)
        
        # Test fetchExtraFields functionality
        console.print("\n" + "="*80)
        console.print(Panel.fit("[bold]Testing fetchExtraFields Functionality[/bold]", border_style="green"))
        self.test_fetch_extra_fields("customer sales analytics")
        console.print("="*80 + "\n")
        
        # Display summary statistics
        self.display_summary_statistics(all_comparisons)
    
    def display_summary_statistics(self, comparisons: List[Dict[str, Any]]) -> None:
        """
        Display summary statistics across all test queries.
        
        Args:
            comparisons: List of comparison results
        """
        console.print("\n")
        console.print(Panel.fit("[bold]Test Summary Statistics[/bold]", border_style="green"))
        
        # Calculate averages
        keyword_times = [c["keyword"]["response_time_ms"] for c in comparisons if not c["keyword"]["errors"]]
        semantic_times = [c["semantic"]["response_time_ms"] for c in comparisons if not c["semantic"]["errors"]]
        overlaps = [c["overlap"]["percentage"] for c in comparisons]
        
        summary_table = Table(show_header=True)
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="white")
        
        # Add summary rows
        summary_table.add_row(
            "Total Queries Tested",
            str(len(comparisons))
        )
        summary_table.add_row(
            "Avg Keyword Response Time",
            f"{sum(keyword_times) / len(keyword_times):.2f} ms" if keyword_times else "N/A"
        )
        summary_table.add_row(
            "Avg Semantic Response Time",
            f"{sum(semantic_times) / len(semantic_times):.2f} ms" if semantic_times else "N/A"
        )
        summary_table.add_row(
            "Avg Result Overlap",
            f"{sum(overlaps) / len(overlaps):.1f}%" if overlaps else "N/A"
        )
        
        # Count successes/failures
        keyword_success = sum(1 for c in comparisons if not c["keyword"]["errors"])
        semantic_success = sum(1 for c in comparisons if not c["semantic"]["errors"])
        
        summary_table.add_row(
            "Keyword Search Success Rate",
            f"{keyword_success}/{len(comparisons)} ({keyword_success/len(comparisons)*100:.0f}%)"
        )
        summary_table.add_row(
            "Semantic Search Success Rate",
            f"{semantic_success}/{len(comparisons)} ({semantic_success/len(comparisons)*100:.0f}%)"
        )
        
        console.print(summary_table)
        
        # Semantic search status
        if semantic_success == 0:
            console.print("\n[red]⚠️  Semantic search appears to be unavailable or misconfigured.[/red]")
            console.print("[yellow]Check the following:[/yellow]")
            console.print("  • SEMANTIC_SEARCH_ENABLED environment variable")
            console.print("  • SEMANTIC_SEARCH_ALLOWED_USERS configuration")
            console.print("  • OpenSearch semantic indices are created and populated")
            console.print("  • Embedding service is configured and accessible")
        elif semantic_success < len(comparisons):
            console.print("\n[yellow]⚠️  Some semantic searches failed. Check logs for details.[/yellow]")
        else:
            console.print("\n[green]✅ All semantic searches completed successfully![/green]")


def main():
    """
    Main function to run the integration test.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Test GraphQL semantic search integration")
    parser.add_argument("--url", help="DataHub GMS URL", default=None)
    parser.add_argument("--token", help="Authentication token", default=None)
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--query", help="Single query to test", default=None)
    parser.add_argument("--test-fetch-fields", action="store_true", 
                       help="Test only fetchExtraFields functionality")
    parser.add_argument("--fetch-fields", nargs="+", 
                       help="Specific fields to fetch (for single query test)")
    
    args = parser.parse_args()
    
    # Initialize test
    test = SemanticSearchIntegrationTest(
        gms_url=args.url,
        token=args.token,
        verbose=args.verbose
    )
    
    # Run appropriate test based on arguments
    if args.test_fetch_fields:
        # Test fetchExtraFields functionality only
        test.test_fetch_extra_fields(args.query or "customer analytics")
    elif args.query:
        # Test single query with optional fetchExtraFields
        if args.fetch_fields:
            console.print(f"[bold]Testing with fetchExtraFields: {args.fetch_fields}[/bold]")
            results, metrics = test.perform_search(
                args.query, "SEMANTIC", fetch_extra_fields=args.fetch_fields
            )
            console.print(f"Response time: {metrics.response_time_ms:.2f}ms")
            console.print(f"Results: {len(results)}")
            for i, result in enumerate(results[:5]):
                console.print(f"{i+1}. {result.name} ({result.entity_type})")
                if result.extra_properties:
                    console.print(f"   Extra fields: {len(result.extra_properties)}")
        else:
            comparison = test.compare_search_modes(args.query)
            test.display_comparison(comparison)
    else:
        test.run_test_suite()
    
    console.print("\n[bold green]✨ Integration test completed![/bold green]")


if __name__ == "__main__":
    main()
