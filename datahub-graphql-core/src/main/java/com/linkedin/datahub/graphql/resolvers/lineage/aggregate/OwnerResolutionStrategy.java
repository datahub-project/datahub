package com.linkedin.datahub.graphql.resolvers.lineage.aggregate;

import com.linkedin.common.urn.Urn;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.Set;

/**
 * Maps a set of neighbour entity URNs to the URNs of their owning Domain or DataProduct.
 * Implementations: {@link DomainOwnerResolutionStrategy}, {@link
 * DataProductOwnerResolutionStrategy}.
 */
public interface OwnerResolutionStrategy {

  /**
   * @return a map from neighbour URN → set of owner URNs. Missing entries (or empty sets) mean "no
   *     resolvable owner"; the caller drops those hits and marks the response partial.
   */
  Map<Urn, Set<Urn>> resolveOwners(OperationContext opContext, Set<Urn> neighbourUrns);
}
