package com.linkedin.metadata.graph;

/**
 * Thrown when a lineage graph traversal exceeds its configured wall-clock budget
 * (elasticsearch.search.graph.timeoutSeconds). Distinct type so the GraphQL layer can surface a
 * DEADLINE_EXCEEDED code (not a generic server error) and clients can prompt the user to narrow the
 * scope (time range / hops) rather than showing an opaque failure.
 *
 * <p>Extends {@link IllegalStateException} (the type the timeout previously threw) so existing
 * callers that catch {@code IllegalStateException} keep working; the GraphQL exception handler
 * checks this subtype first so a timeout still maps to DEADLINE_EXCEEDED rather than SERVER_ERROR.
 */
public class LineageTimeoutException extends IllegalStateException {
  public LineageTimeoutException(String message) {
    super(message);
  }

  public LineageTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
