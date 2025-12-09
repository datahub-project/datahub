#!/usr/bin/env python3
"""
Test Bedrock connection and embedding generation with AWS SSO
"""
import os
from dotenv import load_dotenv
from rich.console import Console

from embedding_utils import create_bedrock_embeddings


def test_bedrock_connection() -> bool:
    """Test that we can connect to Bedrock and generate embeddings"""
    console = Console()
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment
    profile = os.environ.get('AWS_PROFILE', 'AcrylData-legacy-Developer_SSO')
    region = os.environ.get('AWS_REGION', 'us-east-1')
    model_id = os.environ.get('BEDROCK_MODEL', 'amazon.titan-embed-text-v1')
    
    console.print("[bold cyan]🔌 Testing Bedrock Connection[/bold cyan]\n")
    console.print(f"Profile: [yellow]{profile}[/yellow]")
    console.print(f"Region: [yellow]{region}[/yellow]")
    console.print(f"Model: [yellow]{model_id}[/yellow]")
    console.print()
    
    try:
        # Create embeddings instance
        console.print("Creating embeddings instance...")
        embeddings = create_bedrock_embeddings(
            model_id=model_id,
            region=region,
        )
        console.print("✅ Embeddings instance created\n")
        
        # Test with a simple text
        test_text = "This is a test of the DataHub semantic search embedding system"
        console.print(f"Test text: [dim]{test_text}[/dim]\n")
        
        # Generate embedding for single text
        console.print("Generating single embedding...")
        single_vector = embeddings.embed_query(test_text)
        console.print(f"✅ Generated embedding with {len(single_vector)} dimensions\n")
        
        # Generate embeddings for multiple texts
        test_texts = [
            "Customer orders table with order information",
            "User profile data including preferences",
            "Product catalog with pricing details"
        ]
        
        console.print("Generating batch embeddings...")
        batch_vectors = embeddings.embed_documents(test_texts)
        console.print(f"✅ Generated {len(batch_vectors)} embeddings\n")
        
        # Show sample of embedding values
        console.print("[bold]Sample embedding values (first 10):[/bold]")
        console.print(f"{single_vector[:10]}\n")
        
        # Calculate similarity between query and documents
        import numpy as np
        
        def cosine_similarity(a, b):
            return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))
        
        query = "Find customer order data"
        query_vec = embeddings.embed_query(query)
        
        console.print(f"[bold]Query:[/bold] {query}\n")
        console.print("[bold]Similarities:[/bold]")
        for text, vec in zip(test_texts, batch_vectors):
            similarity = cosine_similarity(query_vec, vec)
            console.print(f"  {similarity:.3f} - {text[:50]}...")
        
        console.print("\n[bold green]✅ All tests passed![/bold green]")
        
    except Exception as e:
        console.print(f"[bold red]❌ Error:[/bold red] {e}")
        console.print("\n[yellow]Troubleshooting:[/yellow]")
        console.print("1. Make sure you're logged in with SSO:")
        console.print(f"   aws sso login --profile {profile}")
        console.print("2. Check that your profile has Bedrock access")
        console.print("3. Verify the region has Bedrock available")
        return False

    return True


if __name__ == "__main__":
    test_bedrock_connection()
