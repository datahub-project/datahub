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
 * Resolves neighbour assets to owning DataProduct URNs via the {@code DataProductContains} graph
 * relationship.
 *
 * <p>Performance: O(N) per-neighbour {@link GraphClient#getRelatedEntities} calls. The natural
 * batched follow-up is to plumb {@link com.linkedin.metadata.graph.GraphService} through and use a
 * single {@code findRelatedEntities} call with {@code Condition.IN} over the neighbour set; see the
 * roadmap's "Layer B" section.
 */
@Slf4j
@RequiredArgsConstructor
public class DataProductOwnerResolutionStrategy implements OwnerResolutionStrategy {

  private static final Set<String> DATA_PRODUCT_CONTAINS_REL =
      ImmutableSet.of("DataProductContains");

  // Small margin for the feature-flagged multi-DP-per-asset case; typically 1 in single-DP setups.
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
        log.warn(
            "Failed to resolve DataProductContains owners for {}; treating as ownerless.",
            neighbourUrn,
            e);
      }
    }

    return ownersByNeighbour;
  }
}
