package com.linkedin.metadata.graph;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Siblings;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.validation.ValidationUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class SiblingGraphService {

  private final EntityService _entityService;
  private final GraphService _graphService;

  @Nonnull
  public EntityLineageResult getLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageDirection direction,
      int offset,
      int count,
      int maxHops) {
    return getLineage(opContext, entityUrn, direction, offset, count, maxHops, false, false);
  }

  @Nonnull
  public EntityLineageResult getLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageDirection direction,
      int offset,
      int count,
      int maxHops,
      boolean separateSiblings,
      boolean includeGhostEntities) {
    return getLineage(
        opContext,
        entityUrn,
        direction,
        offset,
        count,
        maxHops,
        separateSiblings,
        includeGhostEntities,
        new HashSet<>());
  }

  /**
   * Traverse from the entityUrn towards the input direction up to maxHops number of hops Abstracts
   * away the concept of relationship types
   *
   * <p>Unless overridden, it uses the lineage registry to fetch valid edge types and queries for
   * them
   */
  @Nonnull
  public EntityLineageResult getLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageDirection direction,
      int offset,
      int count,
      int maxHops,
      boolean separateSiblings,
      boolean includeGhostEntities,
      @Nonnull Set<Urn> visitedUrns) {
    if (separateSiblings) {
      return ValidationUtils.validateEntityLineageResult(
          opContext,
          _graphService.getLineage(opContext, entityUrn, direction, offset, count, maxHops),
          _entityService,
          includeGhostEntities);
    }

    if (maxHops > 1) {
      throw new UnsupportedOperationException(
          String.format(
              "More than 1 hop is not supported for %s", this.getClass().getSimpleName()));
    }

    EntityLineageResult entityLineage =
        _graphService.getLineage(opContext, entityUrn, direction, offset, count, maxHops);

    Siblings siblingAspectOfEntity =
        (Siblings) _entityService.getLatestAspect(opContext, entityUrn, SIBLINGS_ASPECT_NAME);

    // if you have siblings, we want to fetch their lineage too and merge it in
    if (siblingAspectOfEntity != null && siblingAspectOfEntity.hasSiblings()) {
      UrnArray siblingUrns = siblingAspectOfEntity.getSiblings();
      Set<Urn> allSiblingsInGroup = new HashSet<>(siblingUrns);
      allSiblingsInGroup.add(entityUrn);

      // remove your siblings from your lineage
      entityLineage =
          filterLineageResultFromSiblings(
              opContext, entityUrn, allSiblingsInGroup, entityLineage, null, includeGhostEntities);

      // Update offset and count to fetch the correct number of edges from the next sibling node
      offset = Math.max(0, offset - entityLineage.getTotal());
      count = Math.max(0, count - entityLineage.getRelationships().size());

      visitedUrns.add(entityUrn);
      // iterate through each sibling and include their lineage in the bunch
      for (Urn siblingUrn : siblingUrns) {
        if (visitedUrns.contains(siblingUrn)) {
          continue;
        }
        // need to call siblingGraphService to get sibling results for this sibling entity in case
        // there is more than one sibling
        EntityLineageResult nextEntityLineage =
            filterLineageResultFromSiblings(
                opContext,
                siblingUrn,
                allSiblingsInGroup,
                getLineage(
                    opContext,
                    siblingUrn,
                    direction,
                    offset,
                    count,
                    maxHops,
                    false,
                    includeGhostEntities,
                    visitedUrns),
                entityLineage,
                includeGhostEntities);

        // Update offset and count to fetch the correct number of edges from the next sibling node
        offset = Math.max(0, offset - nextEntityLineage.getTotal());
        count = Math.max(0, count - nextEntityLineage.getCount() - entityLineage.getCount());

        entityLineage.setFiltered(getFiltered(entityLineage) + getFiltered(nextEntityLineage));
        entityLineage = nextEntityLineage;
      }
      ;
    }

    return ValidationUtils.validateEntityLineageResult(
        opContext, entityLineage, _entityService, includeGhostEntities);
  }

  private int getFiltered(@Nullable EntityLineageResult entityLineageResult) {
    return (entityLineageResult != null && entityLineageResult.getFiltered() != null
        ? entityLineageResult.getFiltered()
        : 0);
  }

  // takes a lineage result and removes any nodes that are siblings of some other node already in
  // the result
  private EntityLineageResult filterLineageResultFromSiblings(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final Set<Urn> allSiblingsInGroup,
      @Nonnull final EntityLineageResult entityLineageResult,
      @Nullable final EntityLineageResult existingResult,
      boolean includeGhostEntities) {
    int numFiltered = 0;

    // 1) remove the source entities siblings from this entity's downstreams
    final Map<Boolean, List<LineageRelationship>> partitionedFilteredRelationships =
        entityLineageResult.getRelationships().stream()
            .collect(
                Collectors.partitioningBy(
                    lineageRelationship ->
                        !allSiblingsInGroup.contains(lineageRelationship.getEntity())
                            || lineageRelationship.getEntity().equals(urn)));
    numFiltered += partitionedFilteredRelationships.get(Boolean.FALSE).size();

    final List<LineageRelationship> filteredRelationships =
        partitionedFilteredRelationships.get(Boolean.TRUE);

    // 2) filter out existing lineage to avoid duplicates in our combined result
    final Set<Urn> existingUrns =
        existingResult != null
            ? existingResult.getRelationships().stream()
                .map(LineageRelationship::getEntity)
                .collect(Collectors.toSet())
            : new HashSet<>();

    Map<Boolean, List<LineageRelationship>> partitionedUniqueFilteredRelationships =
        filteredRelationships.stream()
            .collect(
                Collectors.partitioningBy(
                    lineageRelationship ->
                        !existingUrns.contains(lineageRelationship.getEntity())));
    numFiltered += partitionedUniqueFilteredRelationships.get(Boolean.FALSE).size();

    List<LineageRelationship> uniqueFilteredRelationships =
        partitionedUniqueFilteredRelationships.get(Boolean.TRUE);

    // 3) combine this entity's lineage with the lineage we've already seen
    final List<LineageRelationship> combinedResults =
        Stream.concat(
                uniqueFilteredRelationships.stream(),
                existingResult != null
                    ? existingResult.getRelationships().stream()
                    : ImmutableList.<LineageRelationship>of().stream())
            .collect(Collectors.toList());

    // 4) fetch the siblings of each lineage result
    final Set<Urn> combinedResultUrns =
        combinedResults.stream().map(LineageRelationship::getEntity).collect(Collectors.toSet());

    final Map<Urn, List<RecordTemplate>> siblingAspects =
        _entityService.getLatestAspects(
            opContext, combinedResultUrns, ImmutableSet.of(SIBLINGS_ASPECT_NAME));

    // 5) if you are not primary & your sibling is in the results, filter yourself out of the return
    // set
    Map<Boolean, List<LineageRelationship>> partitionedFilteredSiblings =
        combinedResults.stream()
            .collect(
                Collectors.partitioningBy(
                    result -> {
                      Optional<RecordTemplate> optionalSiblingsAspect =
                          siblingAspects.get(result.getEntity()).stream()
                              .filter(aspect -> aspect instanceof Siblings)
                              .findAny();

                      if (optionalSiblingsAspect.isEmpty()) {
                        return true;
                      }

                      final Siblings siblingsAspect = (Siblings) optionalSiblingsAspect.get();

                      if (siblingsAspect.isPrimary()) {
                        return true;
                      }

                      // if you are not primary and your sibling exists in the result set, filter
                      // yourself out
                      return siblingsAspect.getSiblings().stream()
                          .noneMatch(combinedResultUrns::contains);
                    }));

    numFiltered += partitionedFilteredSiblings.get(Boolean.FALSE).size();
    uniqueFilteredRelationships = partitionedFilteredSiblings.get(Boolean.TRUE);

    EntityLineageResult combinedLineageResult = new EntityLineageResult();
    combinedLineageResult.setStart(entityLineageResult.getStart());
    combinedLineageResult.setRelationships(
        new LineageRelationshipArray(uniqueFilteredRelationships));
    combinedLineageResult.setTotal(
        entityLineageResult.getTotal() + (existingResult != null ? existingResult.getTotal() : 0));
    combinedLineageResult.setCount(uniqueFilteredRelationships.size());
    combinedLineageResult.setFiltered(
        numFiltered + getFiltered(existingResult) + getFiltered(entityLineageResult));
    return ValidationUtils.validateEntityLineageResult(
        opContext, combinedLineageResult, _entityService, includeGhostEntities);
  }
}
