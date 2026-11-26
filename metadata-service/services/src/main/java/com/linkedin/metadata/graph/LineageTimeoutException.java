package com.linkedin.metadata.graph;

/**
 * Thrown when a lineage graph traversal exceeds its configured wall-clock budget
 * (elasticsearch.search.graph.timeoutSeconds). A distinct type so API layers can classify it: the
 * GraphQL handler maps it to DEADLINE_EXCEEDED (504) and Rest.li to HTTP 504, instead of the
 * generic server error a plain RuntimeException would produce.
 */
public class LineageTimeoutException extends RuntimeException {
  public LineageTimeoutException(String message) {
    super(message);
  }

  public LineageTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
