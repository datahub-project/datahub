package com.linkedin.datahub.graphql.resolvers.lineage.aggregate;

import com.linkedin.common.urn.Urn;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.Set;

/**
 * Strategy for mapping a set of neighbour entity URNs (the destinations of per-member {@code
 * searchAcrossLineage} hits) to the URNs of their owning Domain or DataProduct.
 *
 * <p>Implementations:
 *
 * <ul>
 *   <li>{@link DomainOwnerResolutionStrategy} — owners come from each neighbour's {@code Domains}
 *       aspect (batched).
 *   <li>{@link DataProductOwnerResolutionStrategy} — owners come from {@code DataProductContains}
 *       graph relationships (per-URN today; see class javadoc for the planned batched follow-up).
 * </ul>
 */
public interface OwnerResolutionStrategy {

  /**
   * @param opContext request operation context
   * @param neighbourUrns the distinct neighbour entity URNs to resolve
   * @return a map from neighbour URN → set of owner URNs. Missing entries (or empty sets) mean "no
   *     resolvable owner" and trigger the L2 filter (drop the hit + mark response partial).
   */
  Map<Urn, Set<Urn>> resolveOwners(OperationContext opContext, Set<Urn> neighbourUrns);
}
