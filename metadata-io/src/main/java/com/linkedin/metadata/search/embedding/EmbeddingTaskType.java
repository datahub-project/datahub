package com.linkedin.metadata.search.embedding;

/**
 * Distinguishes whether an embedding is for indexing a document or for querying. Asymmetric
 * embedding models (e.g. Vertex AI Gemini Embedding) produce higher-quality results when the
 * embedding is generated with the correct task type.
 */
public enum EmbeddingTaskType {
  /** Embed a document to be stored in an index. */
  DOCUMENT,

  /** Embed a user query for nearest-neighbor retrieval. */
  QUERY
}
