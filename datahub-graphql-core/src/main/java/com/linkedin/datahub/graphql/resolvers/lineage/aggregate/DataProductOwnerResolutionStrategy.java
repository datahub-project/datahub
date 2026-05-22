package com.linkedin.datahub.graphql.resolvers.lineage.aggregate;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves neighbour assets to their owning DataProduct URNs via the {@code DataProductContains}
 * graph relationship.
 *
 * <p>Implementation: per-neighbour {@link GraphClient#getRelatedEntities} call with {@code
 * direction=INCOMING}. This mirrors the established pattern in {@code
 * DataProductService.unsetDataProduct} and works against the existing GMS surface without requiring
 * a new {@code GraphService} injection.
 *
 * <p>Performance caveat: O(N) ES queries where N = distinct neighbour URNs across all members.
 * Acceptable for v1 (a Domain with ~1000 members × ~100 hits/member dedupes to ~few thousand unique
 * neighbours in practice). The natural follow-up is to plumb {@link
 * com.linkedin.metadata.graph.GraphService} through {@code GmsGraphQLEngineArgs} and replace this
 * with a single batched {@code findRelatedEntities} call using a {@code Condition.IN} criterion
 * over the neighbour URN set.
 */
@Slf4j
@RequiredArgsConstructor
public class DataProductOwnerResolutionStrategy implements OwnerResolutionStrategy {

  private static final Set<String> DATA_PRODUCT_CONTAINS_REL =
      ImmutableSet.of("DataProductContains");

  /**
   * Upper bound on owners-per-neighbour. {@code MULTIPLE_DATA_PRODUCTS_PER_ASSET} is gated by a
   * feature flag, so in most deployments this is 1; we ask for a small margin so we don't silently
   * truncate when the flag is on.
   */
  private static final int MAX_OWNERS_PER_NEIGHBOUR = 10;

  private final GraphClient graphClient;

  @Override
  public Map<Urn, Set<Urn>> resolveOwners(
      final OperationContext opContext, final Set<Urn> neighbourUrns) {
    if (neighbourUrns == null || neighbourUrns.isEmpty()) {
      return Collections.emptyMap();
    }

    final String actorUrn = opContext.getSessionActorContext().getActorUrn().toString();
    final Map<Urn, Set<Urn>> ownersByNeighbour = new HashMap<>(neighbourUrns.size());

    for (final Urn neighbourUrn : neighbourUrns) {
      try {
        final EntityRelationships relationships =
            graphClient.getRelatedEntities(
                neighbourUrn.toString(),
                DATA_PRODUCT_CONTAINS_REL,
                RelationshipDirection.INCOMING,
                0,
                MAX_OWNERS_PER_NEIGHBOUR,
                actorUrn);
        if (relationships == null || relationships.getRelationships() == null) {
          continue;
        }
        final Set<Urn> dpUrns = new HashSet<>();
        for (final EntityRelationship rel : relationships.getRelationships()) {
          if (rel.getEntity() != null) {
            dpUrns.add(rel.getEntity());
          }
        }
        if (!dpUrns.isEmpty()) {
          ownersByNeighbour.put(neighbourUrn, dpUrns);
        }
      } catch (Exception e) {
        // Per-neighbour failure degrades to "no owner" for that one neighbour (so the hit gets
        // filtered + isPartial=true is set upstream) rather than failing the whole resolver.
        log.warn(
            "Failed to resolve DataProductContains owners for {}; treating as ownerless.",
            neighbourUrn,
            e);
      }
    }

    return ownersByNeighbour;
  }
}
