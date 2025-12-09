#!/usr/bin/env python3
"""
Generate a comprehensive report comparing all three approaches for up to 10 entities
"""
import os
import sys
from typing import Dict, Any, List
import numpy as np
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import print
from dotenv import load_dotenv

from opensearch_client import OpenSearchClient
from embeddings_base import BaseEmbeddings
from bedrock_llm import create_bedrock_llm
from embedding_text_generator_v2 import EmbeddingTextGeneratorV2
from embedding_text_generator_v3 import EmbeddingTextGeneratorV3
from pipe_concatenator import PipeConcatenator
from embedding_utils import create_embeddings
from embedding_cache import CachedEmbeddings


class ComprehensiveReportGenerator:
    def __init__(self, embeddings_instance: BaseEmbeddings) -> None:
        self.console = Console()
        self.natural_generator = EmbeddingTextGeneratorV2()
        self.pipe_generator = PipeConcatenator()
        
        # V3 generator - single hybrid approach
        self.v3_generator = EmbeddingTextGeneratorV3()
        
        self.embeddings = embeddings_instance
        self.console.print(f"[dim]Using embeddings model: {self.embeddings.model_name}[/dim]")
        # Bedrock LLM (Sonnet 4) for description rewriting
        # Hardcode on-demand-friendly model ID per request
        self.llm = create_bedrock_llm(
            model_id=os.environ.get("BEDROCK_LLM_MODEL", "anthropic.claude-3-5-sonnet-20240620-v1:0"),
            region_name=os.environ.get("AWS_REGION", "us-west-2"),
            temperature=0,
        )
        self.results = []
        
        # Store LLM-generated descriptions (will be populated as we process)
        self.llm_descriptions = {}
    
    def generate_llm_description(self, entity: Dict[str, Any], natural_text: str) -> str:
        """
        Use a Bedrock-hosted Sonnet model to rewrite the natural text into a
        concise, human-friendly description optimized for semantic embeddings.
        """
        # Always use the V2 natural generator output as the source
        source_text = natural_text

        system = {
            "role": "system",
            "content": (
                "You are a helpful data documentation assistant. Rewrite technical dataset summaries "
                "into a single natural paragraph that is unambiguous and embedding-friendly. "
                "Keep 1-3 sentences, avoid IDs, file paths, and low-signal metadata, however try "
                "including keywords important to identify the data usage. Emphasize what the "
                "dataset contains and how it is used. Use plain language."
            ),
        }
        user = {
            "role": "user",
            "content": (
                "Convert the following into more natural language so it works better with text embeddings:\n\n"
                f"{source_text}"
            ),
        }
        response = self.llm.invoke([system, user])
        text = response.content
        return text.strip()
    
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors"""
        vec1 = np.array(vec1)
        vec2 = np.array(vec2)
        return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))
    
    def _get_test_queries(self, entity: Dict[str, Any]) -> List[str]:
        """Generate test queries based on entity content"""
        name = entity.get('name', 'unknown').lower()
        description = entity.get('description', '').lower()
        
        queries = []
        
        # Pet/Animal related
        if any(word in name or word in description for word in ['pet', 'animal', 'dog', 'cat', 'adoption']):
            queries.extend([
                "Find pet adoption records and status",
                "Show animal health and medical history",
                "Where are pet profiles and characteristics stored?",
                "Pet availability and adoption metrics",
                "Track pet status changes over time",
                "Historical pet adoption data"
            ])
        # Customer related
        elif any(word in name or word in description for word in ['customer', 'user', 'buyer', 'client']):
            queries.extend([
                "Show customer purchase history",
                "Find customer transaction data",
                "Where is customer buying behavior tracked?",
                "Customer engagement and retention metrics",
                "Customer lifetime value analysis",
                "User demographics and segmentation"
            ])
        # Order/Transaction related
        elif any(word in name or word in description for word in ['order', 'transaction', 'payment', 'sale']):
            queries.extend([
                "Order processing and fulfillment data",
                "Transaction history and payment records",
                "Sales performance metrics",
                "Revenue tracking and analysis",
                "Order status and delivery information",
                "Payment processing statistics"
            ])
        # Product/Inventory related
        elif any(word in name or word in description for word in ['product', 'item', 'inventory', 'stock']):
            queries.extend([
                "Product catalog and specifications",
                "Inventory levels and stock management",
                "Product categories and classifications",
                "Item availability and supply data",
                "Product pricing and cost analysis",
                "Stock movement and turnover metrics"
            ])
        else:
            # Generic queries
            queries.extend([
                f"Find information about {name.replace('_', ' ')}",
                f"Show {name.replace('_', ' ')} data",
                f"Where is {name.replace('_', ' ')} stored?",
                "Business metrics and KPIs",
                "Operational data and analytics",
                "Performance tracking information"
            ])
        
        return queries[:6]  # Return 6 queries
    
    def _get_unrelated_queries(self) -> List[str]:
        """Get completely unrelated queries"""
        return [
            "Weather forecast for tomorrow in San Francisco",
            "How to cook pasta carbonara recipe",
            "Latest NBA basketball game scores",
            "Python programming tutorial for beginners"
        ]
    
    def process_entity(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single entity with all approaches including V3"""
        entity_name = entity.get('name', 'Unknown')
        
        # Generate texts with all approaches
        natural_text = self.natural_generator.generate(entity, include_unknown=True)
        pipe_text = self.pipe_generator.generate(entity)
        v3_text = self.v3_generator.generate(entity)
        llm_text = self.generate_llm_description(entity, natural_text)
        
        # Store for later reference
        self.llm_descriptions[entity_name] = llm_text
        
        # Generate embeddings for all texts
        texts_to_embed = [pipe_text, natural_text, v3_text, llm_text]
        embeddings = self.embeddings.embed_documents(texts_to_embed)
        pipe_embedding = embeddings[0]
        natural_embedding = embeddings[1]
        v3_embedding = embeddings[2]
        llm_embedding = embeddings[3]
        
        # Test with queries
        queries = self._get_test_queries(entity)
        query_results = []
        
        for query in queries:
            query_embedding = self.embeddings.embed_query(query)
            
            pipe_score = self._cosine_similarity(query_embedding, pipe_embedding)
            natural_score = self._cosine_similarity(query_embedding, natural_embedding)
            v3_score = self._cosine_similarity(query_embedding, v3_embedding)
            llm_score = self._cosine_similarity(query_embedding, llm_embedding)
            
            scores = {
                "pipe": pipe_score, 
                "natural": natural_score,
                "v3": v3_score,
                "llm": llm_score
            }
            winner = max(scores, key=scores.get)
            
            query_results.append({
                "query": query,
                "pipe_score": pipe_score,
                "natural_score": natural_score,
                "v3_score": v3_score,
                "llm_score": llm_score,
                "winner": winner
            })
        
        # Test unrelated queries
        unrelated_queries = self._get_unrelated_queries()
        unrelated_results = []
        
        for query in unrelated_queries:
            query_embedding = self.embeddings.embed_query(query)
            
            pipe_score = self._cosine_similarity(query_embedding, pipe_embedding)
            natural_score = self._cosine_similarity(query_embedding, natural_embedding)
            v3_score = self._cosine_similarity(query_embedding, v3_embedding)
            llm_score = self._cosine_similarity(query_embedding, llm_embedding)
            
            unrelated_results.append({
                "query": query,
                "pipe_score": pipe_score,
                "natural_score": natural_score,
                "v3_score": v3_score,
                "llm_score": llm_score,
                "max_score": max(pipe_score, natural_score, v3_score, llm_score)
            })
        
        # Calculate averages
        avg_pipe = np.mean([q["pipe_score"] for q in query_results])
        avg_natural = np.mean([q["natural_score"] for q in query_results])
        avg_v3 = np.mean([q["v3_score"] for q in query_results])
        avg_llm = np.mean([q["llm_score"] for q in query_results])
        avg_unrelated = np.mean([q["max_score"] for q in unrelated_results])
        
        return {
            "name": entity_name,
            "description": entity.get("description", ""),
            "pipe_text": pipe_text,
            "natural_text": natural_text,
            "v3_text": v3_text,
            "llm_text": llm_text,
            "pipe_length": len(pipe_text),
            "natural_length": len(natural_text),
            "v3_length": len(v3_text),
            "llm_length": len(llm_text),
            "pipe_words": len(pipe_text.split()),
            "natural_words": len(natural_text.split()),
            "v3_words": len(v3_text.split()),
            "llm_words": len(llm_text.split()),
            "query_results": query_results,
            "unrelated_results": unrelated_results,
            "avg_pipe": avg_pipe,
            "avg_natural": avg_natural,
            "avg_v3": avg_v3,
            "avg_llm": avg_llm,
            "avg_unrelated": avg_unrelated,
            "overall_winner": max({
                "pipe": avg_pipe, 
                "natural": avg_natural,
                "v3": avg_v3,
                "llm": avg_llm
            }, key=lambda k: {
                "pipe": avg_pipe, 
                "natural": avg_natural,
                "v3": avg_v3,
                "llm": avg_llm
            }[k])
        }
    
    def display_entity_report(self, result: Dict[str, Any], index: int) -> None:
        """Display detailed report for one entity"""
        self.console.print(f"\n{'='*100}")
        self.console.print(f"[bold cyan]ENTITY {index}: {result['name'].upper()}[/bold cyan]")
        if result['description']:
            self.console.print(f"[dim]{result['description'][:100]}{'...' if len(result['description']) > 100 else ''}[/dim]")
        self.console.print(f"{'='*100}\n")
        
        # Show all generated texts
        self.console.print("[bold]1. GENERATED TEXTS:[/bold]\n")
        
        # Pipe text
        self.console.print(f"[yellow]A) Pipe Concatenation ({result['pipe_words']} words, {result['pipe_length']} chars):[/yellow]")
        self.console.print(Panel(Text(result['pipe_text'][:1000] + "..." if len(result['pipe_text']) > 1000 else result['pipe_text'], 
                                      style="dim"), border_style="yellow"))
        
        # Natural text V2
        self.console.print(f"\n[green]B) Natural Language V2 ({result['natural_words']} words, {result['natural_length']} chars):[/green]")
        self.console.print(Panel(Text(result['natural_text'][:1000] + "..." if len(result['natural_text']) > 1000 else result['natural_text'], 
                                      style="dim"), border_style="green"))
        
        # V3 Hybrid (NEW)
        self.console.print(f"\n[cyan]C) V3 Hybrid [NEW] ({result['v3_words']} words, {result['v3_length']} chars):[/cyan]")
        self.console.print(Panel(Text(result['v3_text'][:1000] + "..." if len(result['v3_text']) > 1000 else result['v3_text'], 
                                      style="dim"), border_style="cyan"))
        
        # LLM text
        self.console.print(f"\n[bright_magenta]D) LLM-Style Description ({result['llm_words']} words, {result['llm_length']} chars):[/bright_magenta]")
        self.console.print(Panel(Text(result['llm_text'][:1000] + "..." if len(result['llm_text']) > 1000 else result['llm_text'], 
                                      style="dim"), border_style="magenta"))
        
        # Query results
        self.console.print("\n[bold]2. QUERY MATCHING SCORES:[/bold]")
        query_table = Table(show_header=True, header_style="bold cyan")
        query_table.add_column("Query", width=40)
        query_table.add_column("Pipe", justify="center", style="yellow", width=8)
        query_table.add_column("V2", justify="center", style="green", width=8)
        query_table.add_column("V3 Hybrid", justify="center", style="cyan", width=10)
        query_table.add_column("LLM", justify="center", style="bright_magenta", width=8)
        query_table.add_column("Winner", justify="center", width=12)
        
        winner_colors = {
            "pipe": "yellow", 
            "natural": "green",
            "v3": "cyan",
            "llm": "bright_magenta"
        }
        
        for q in result['query_results']:
            winner_color = winner_colors.get(q['winner'], "white")
            query_table.add_row(
                q['query'][:37] + "..." if len(q['query']) > 40 else q['query'],
                f"{q['pipe_score']:.3f}",
                f"{q['natural_score']:.3f}",
                f"{q['v3_score']:.3f}",
                f"{q['llm_score']:.3f}",
                f"[{winner_color}]{q['winner'].replace('_', ' ').upper()}[/{winner_color}]"
            )
        
        self.console.print(query_table)
        
        # Average scores
        self.console.print(f"\n[bold]Average Relevant Scores:[/bold]")
        self.console.print(f"  Pipe: {result['avg_pipe']:.4f}")
        self.console.print(f"  Natural V2: {result['avg_natural']:.4f} ({(result['avg_natural']-result['avg_pipe'])/result['avg_pipe']*100:+.1f}% vs pipe)")
        self.console.print(f"  V3 Hybrid [NEW]: {result['avg_v3']:.4f} ({(result['avg_v3']-result['avg_pipe'])/result['avg_pipe']*100:+.1f}% vs pipe)")
        self.console.print(f"  LLM: {result['avg_llm']:.4f} ({(result['avg_llm']-result['avg_pipe'])/result['avg_pipe']*100:+.1f}% vs pipe)")
        
        # Unrelated queries
        self.console.print("\n[bold]3. UNRELATED QUERY SCORES (should be low):[/bold]")
        unrelated_table = Table(show_header=True, header_style="bold red")
        unrelated_table.add_column("Unrelated Query", width=35)
        unrelated_table.add_column("Pipe", justify="center", width=8)
        unrelated_table.add_column("V2", justify="center", width=8)
        unrelated_table.add_column("V3", justify="center", width=8)
        unrelated_table.add_column("LLM", justify="center", width=8)
        unrelated_table.add_column("Max", justify="center", width=10)
        
        for u in result['unrelated_results']:
            unrelated_table.add_row(
                u['query'][:32] + "..." if len(u['query']) > 35 else u['query'],
                f"{u['pipe_score']:.3f}",
                f"{u['natural_score']:.3f}",
                f"{u['v3_score']:.3f}",
                f"{u['llm_score']:.3f}",
                f"{u['max_score']:.3f} {'✓' if u['max_score'] < 0.2 else '⚠'}"
            )
        
        self.console.print(unrelated_table)
        
        # Summary
        winner_color = {"pipe": "yellow", "natural": "green", "v3": "cyan", "llm": "magenta"}.get(result['overall_winner'], "white")
        self.console.print(f"\n[bold]WINNER:[/bold] [{winner_color}]{result['overall_winner'].upper()}[/{winner_color}]")
        self.console.print(f"Discrimination Gap: {result['avg_' + result['overall_winner']] - result['avg_unrelated']:.4f}")


def main() -> None:
    # Load .env from this script directory first, then fall back to CWD
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
    load_dotenv()
    console = Console()
    
    console.print("[bold cyan]📊 COMPREHENSIVE SEMANTIC SEARCH REPORT[/bold cyan]")
    console.print("[dim]Comparing Pipe Concatenation vs Natural Language vs LLM Descriptions[/dim]\n")
    
    # Connect to OpenSearch
    try:
        client = OpenSearchClient()
        console.print("✅ Connected to OpenSearch\n")
    except Exception as e:
        console.print(f"[red]❌ Failed to connect: {e}[/red]")
        sys.exit(1)
    
    # Initialize embeddings via shared factory (provider: bedrock | cohere)
    embeddings = create_embeddings(use_cache=True)
    console.print("✅ Ready\n")
    
    # Initialize report generator
    generator = ComprehensiveReportGenerator(embeddings)
    
    # Get up to 10 diverse entities
    console.print("[bold]Selecting up to 10 diverse entities...[/bold]")
    search_terms = [
        "pet_status_history",
        "customer_last_purchase",
        "order",
        "product",
        "pet_profiles"
    ]
    
    entities = []
    for term in search_terms:
        results = client.search_entities(term, limit=1)
        if results:
            entities.append(results[0])
            console.print(f"  ✓ Found: {results[0].get('name', 'Unknown')}")
    
    # If we need more, get rich entities
    target_count = 5
    if len(entities) < target_count:
        additional = client.get_rich_entities(limit=target_count - len(entities))
        entities.extend(additional)
        for e in additional:
            console.print(f"  ✓ Added: {e.get('name', 'Unknown')}")
    
    console.print(f"\n[bold]Processing {len(entities)} entities...[/bold]\n")
    
    # Process each entity
    results = []
    for i, entity in enumerate(entities, 1):
        console.print(f"Processing entity {i}/{len(entities)}: {entity.get('name', 'Unknown')}...")
        result = generator.process_entity(entity)
        results.append(result)
        generator.display_entity_report(result, i)
    
    # Overall summary
    console.print(f"\n{'='*100}")
    console.print("[bold yellow]OVERALL SUMMARY[/bold yellow]")
    console.print(f"{'='*100}\n")
    
    # Win counts
    wins = {"pipe": 0, "natural": 0, "v3": 0, "llm": 0}
    for r in results:
        winner = r['overall_winner']
        if winner in wins:
            wins[winner] += 1
    
    console.print("[bold]Entity-Level Winners:[/bold]")
    for approach, count in wins.items():
        display_name = approach.replace('_', ' ').upper()
        console.print(f"  {display_name}: {count}/{len(results)} ({count/len(results)*100:.0f}%)")
    
    # Average scores across all entities
    avg_scores = {
        "pipe": np.mean([r['avg_pipe'] for r in results]),
        "natural": np.mean([r['avg_natural'] for r in results]),
        "v3": np.mean([r['avg_v3'] for r in results]),
        "llm": np.mean([r['avg_llm'] for r in results])
    }
    
    console.print("\n[bold]Average Scores Across All Entities:[/bold]")
    for approach, score in avg_scores.items():
        improvement = ((score - avg_scores['pipe']) / avg_scores['pipe'] * 100) if approach != 'pipe' else 0
        display_name = approach.replace('_', ' ').upper()
        console.print(f"  {display_name}: {score:.4f} {f'(+{improvement:.1f}% vs pipe)' if improvement > 0 else ''}")
    
    # Best approach
    best = max(avg_scores, key=avg_scores.get)
    best_display = best.replace('_', ' ').upper()
    console.print(f"\n[bold green]BEST OVERALL APPROACH: {best_display} (score: {avg_scores[best]:.4f})[/bold green]")
    
    # Cache stats
    match embeddings:
        case CachedEmbeddings() as cached:
            stats = cached.get_cache_stats()
            console.print(f"\n[dim]Cache: {stats['cache_hits']} hits, {stats['cache_misses']} misses[/dim]")
        case _:
            pass


if __name__ == "__main__":
    main()
