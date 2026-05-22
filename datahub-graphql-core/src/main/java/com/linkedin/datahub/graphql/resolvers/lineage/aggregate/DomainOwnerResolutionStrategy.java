package com.linkedin.datahub.graphql.resolvers.lineage.aggregate;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves neighbour assets to their owning Domain URNs by batch-reading the {@code Domains}
 * aspect, one batched call per distinct neighbour entity type (typically 2–5: dataset, chart,
 * dashboard, ML*).
 */
@Slf4j
@RequiredArgsConstructor
public class DomainOwnerResolutionStrategy implements OwnerResolutionStrategy {

  private final EntityClient entityClient;

  @Override
  public Map<Urn, Set<Urn>> resolveOwners(
      final OperationContext opContext, final Set<Urn> neighbourUrns) {
    if (neighbourUrns == null || neighbourUrns.isEmpty()) {
      return Collections.emptyMap();
    }

    final Map<String, Set<Urn>> urnsByEntityType =
        neighbourUrns.stream()
            .collect(Collectors.groupingBy(Urn::getEntityType, Collectors.toSet()));

    final Map<Urn, Set<Urn>> ownersByNeighbour = new HashMap<>(neighbourUrns.size());

    for (final Map.Entry<String, Set<Urn>> entry : urnsByEntityType.entrySet()) {
      final String entityType = entry.getKey();
      final Set<Urn> urnsOfType = entry.getValue();
      try {
        final Map<Urn, EntityResponse> responses =
            entityClient.batchGetV2(
                opContext,
                entityType,
                urnsOfType,
                Collections.singleton(Constants.DOMAINS_ASPECT_NAME));
        for (final Map.Entry<Urn, EntityResponse> r : responses.entrySet()) {
          final Set<Urn> domainUrns = extractDomainUrns(r.getValue());
          if (!domainUrns.isEmpty()) {
            ownersByNeighbour.put(r.getKey(), domainUrns);
          }
        }
      } catch (Exception e) {
        // Degrade to "no owner" for these neighbours rather than failing the whole resolver.
        log.warn(
            "Failed to batch-fetch Domains aspect for entity type {} (count={}); treating as ownerless.",
            entityType,
            urnsOfType.size(),
            e);
      }
    }

    return ownersByNeighbour;
  }

  private static Set<Urn> extractDomainUrns(final EntityResponse response) {
    if (response == null
        || response.getAspects() == null
        || !response.getAspects().containsKey(Constants.DOMAINS_ASPECT_NAME)) {
      return Collections.emptySet();
    }
    final Domains domains =
        new Domains(response.getAspects().get(Constants.DOMAINS_ASPECT_NAME).getValue().data());
    final UrnArray domainUrns = domains.getDomains();
    if (domainUrns == null || domainUrns.isEmpty()) {
      return Collections.emptySet();
    }
    return new HashSet<>(domainUrns);
  }
}
