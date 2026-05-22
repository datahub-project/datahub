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
 * Resolves neighbour assets to their owning Domain URNs by batched reads of the {@code Domains}
 * aspect off each neighbour.
 *
 * <p>Why aspect read and not the indexed {@code domains} field on the search hit: every lineage hit
 * goes through {@code searchAcrossLineage}, which returns a {@link
 * com.linkedin.metadata.search.LineageSearchEntity}. That entity does carry {@code matchedFields}
 * populated from ES, but only for fields the query actually matched on — domains aren't matched
 * here (we run with {@code query="*"}), so we'd be relying on undocumented behaviour. Fetching the
 * aspect explicitly is a single batched call per entity type and is robust to that.
 *
 * <p>Performance: one {@link EntityClient#batchGetV2} call per distinct neighbour entity type
 * (typically 2–5: dataset, chart, dashboard, ML*). Acceptable for v1; if larger fan-outs ever
 * become a hot path the obvious next step is a multi-type batch API.
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
        // A failure for a single entity type degrades to "no owner for those neighbours" (so the
        // hits get filtered + isPartial=true is set upstream) rather than failing the whole
        // resolver. This matches the partial-result posture documented in the roadmap.
        log.warn(
            "Failed to batch-fetch Domains aspect for entity type {} (count={}); these neighbours"
                + " will be treated as ownerless.",
            entityType,
            urnsOfType.size(),
            e);
      }
    }

    return ownersByNeighbour;
  }

  private static Set<Urn> extractDomainUrns(final EntityResponse response) {
    if (response == null || response.getAspects() == null) {
      return Collections.emptySet();
    }
    if (!response.getAspects().containsKey(Constants.DOMAINS_ASPECT_NAME)) {
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
