package com.linkedin.datahub.graphql.context;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/**
 * Request-scoped context for tracking visited URNs during GraphQL relationship resolution. Used to
 * detect cycles and prevent unbounded recursion when the same entity is reached again via
 * relationships. One instance per GraphQL request, provided via {@link
 * com.linkedin.datahub.graphql.QueryContext#getRelationshipTraversalContext()}.
 *
 * <p>Enforces a maximum number of distinct URNs per request so that even with a very large or
 * cyclic graph we short-circuit and avoid OOM. The limit is configurable via {@link
 * com.linkedin.metadata.config.graphql.GraphQLQueryConfiguration#getMaxVisitedUrns()}.
 */
public class RelationshipTraversalContext {

  private final int _maxVisitedUrns;
  private final Set<String> _visitedUrns = ConcurrentHashMap.newKeySet();

  /**
   * Creates a traversal context with the given cap on distinct URNs per request.
   *
   * @param maxVisitedUrns max distinct URNs to visit; beyond this we short-circuit to prevent OOM
   */
  public RelationshipTraversalContext(int maxVisitedUrns) {
    this._maxVisitedUrns = maxVisitedUrns;
  }

  /**
   * Marks the given URN as visited for this request. Call before resolving relationships for that
   * entity.
   *
   * @param urn entity URN being visited
   * @return true if under the cap (caller should proceed); false only when at cap (caller should
   *     short-circuit). Resolving multiple relationship types for the same entity in one request is
   *     allowed and does not short-circuit.
   */
  public boolean tryVisit(@Nonnull String urn) {
    if (_visitedUrns.size() >= _maxVisitedUrns) {
      return false;
    }
    _visitedUrns.add(urn);
    return true;
  }

  /** Returns whether the URN has already been visited in this request. */
  public boolean isVisited(@Nonnull String urn) {
    return _visitedUrns.contains(urn);
  }
}
