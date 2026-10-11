package com.linkedin.metadata.search;

/**
 * Thrown when a request asks for SEMANTIC search mode but semantic search is disabled for the
 * current environment.
 */
public final class SemanticSearchDisabledException extends RuntimeException {
  public SemanticSearchDisabledException() {
    super("Semantic search is disabled in this environment");
  }
}
