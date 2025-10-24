"""
Smart search - Advanced keyword search with AI-powered reranking.

This module provides enhanced search capabilities without vector embeddings by:
1. Taking keywords extracted by the LLM from natural language queries
2. Running two-pass keyword search (anchors+phrases, then synonyms)
3. Reranking results using Cohere Rerank AI for semantic relevance

The tool is called "smart_search" to reflect that it's keyword-based search
enhanced with AI reranking, not true semantic/embedding-based search.

Internal use only - not exposed to customers via MCP.
"""
