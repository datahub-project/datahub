package com.linkedin.metadata.graph;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Siblings;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.entity.EntityService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;


@Slf4j
@RequiredArgsConstructor
public class SiblingGraphService {

  private final EntityService _entityService;
  private final GraphService _graphService;

  @Nonnull
  public EntityLineageResult getLineage(@Nonnull Urn entityUrn, @Nonnull LineageDirection direction, int offset,
      int count, int maxHops) {
    return getLineage(entityUrn, direction, offset, count, maxHops, false);
  }

  /**
   * Traverse from the entityUrn towards the input direction up to maxHops number of hops
   * Abstracts away the concept of relationship types
   *
   * Unless overridden, it uses the lineage registry to fetch valid edge types and queries for them
   */
  @Nonnull
  public EntityLineageResult getLineage(@Nonnull Urn entityUrn, @Nonnull LineageDirection direction,
     int offset, int count, int maxHops, boolean separateSiblings) {
    if (separateSiblings) {
      return _graphService.getLineage(entityUrn, direction, offset, count, maxHops);
    }

    if (maxHops > 1) {
      throw new UnsupportedOperationException(
          String.format("More than 1 hop is not supported for %s", this.getClass().getSimpleName()));
    }

    EntityLineageResult entityLineage = _graphService.getLineage(entityUrn, direction, offset, count, maxHops);

    Siblings siblingAspectOfEntity = (Siblings) _entityService.getLatestAspect(entityUrn, SIBLINGS_ASPECT_NAME);

    // if you have siblings, we want to fetch their lineage too and merge it in
    if (siblingAspectOfEntity != null && siblingAspectOfEntity.hasSiblings()) {
      UrnArray siblingUrns = siblingAspectOfEntity.getSiblings();
      Set<Urn> allSiblingsInGroup = siblingUrns.stream().collect(Collectors.toSet());
      allSiblingsInGroup.add(entityUrn);

      // remove your siblings from your lineage
      entityLineage =
          filterLineageResultFromSiblings(entityUrn, allSiblingsInGroup, entityLineage, null);

      // Update offset and count to fetch the correct number of edges from the next sibling node
      offset = Math.max(0, offset - entityLineage.getTotal());
      count = Math.max(0, count - entityLineage.getRelationships().size());

      // iterate through each sibling and include their lineage in the bunch
      for (Urn siblingUrn : siblingUrns) {
        EntityLineageResult nextEntityLineage = filterLineageResultFromSiblings(siblingUrn, allSiblingsInGroup,
            _graphService.getLineage(siblingUrn, direction, offset, count, maxHops), entityLineage);

        // Update offset and count to fetch the correct number of edges from the next sibling node
        offset = Math.max(0, offset - nextEntityLineage.getTotal());
        count = Math.max(0, count - nextEntityLineage.getCount() - entityLineage.getCount());

        entityLineage = nextEntityLineage;
      };
    }

    return entityLineage;
  }

  // takes a lineage result and removes any nodes that are siblings of some other node already in the result
  private EntityLineageResult filterLineageResultFromSiblings(
      Urn urn,
      Set<Urn> allSiblingsInGroup,
      EntityLineageResult entityLineageResult,
      EntityLineageResult existingResult
  ) {
    // 1) remove the source entities siblings from this entity's downstreams
    List<LineageRelationship> filteredRelationships = entityLineageResult.getRelationships()
        .stream()
        .filter(lineageRelationship -> !allSiblingsInGroup.contains(lineageRelationship.getEntity())
            || lineageRelationship.getEntity().equals(urn))
        .collect(Collectors.toList());

    // 2) combine this entity's lineage with the lineage we've already seen and remove duplicates
    List<LineageRelationship> combinedResults = new ArrayList<>(Stream.concat(
            filteredRelationships.stream(),
            existingResult != null ? existingResult.getRelationships().stream() : ImmutableList.<LineageRelationship>of().stream())
        .collect(Collectors.toSet()));

    // 3) fetch the siblings of each lineage result
    Set<Urn> combinedResultUrns = combinedResults.stream().map(result -> result.getEntity()).collect(Collectors.toSet());

    Map<Urn, List<RecordTemplate>> siblingAspects =
        _entityService.getLatestAspects(combinedResultUrns, ImmutableSet.of(SIBLINGS_ASPECT_NAME));

    // 4) if you are not primary & your sibling is in the results, filter yourself out of the return set
    filteredRelationships = combinedResults.stream().filter(result -> {
      Optional<RecordTemplate> optionalSiblingsAspect = siblingAspects.get(result.getEntity()).stream().filter(
          aspect -> aspect instanceof Siblings
      ).findAny();

      if (!optionalSiblingsAspect.isPresent()) {
        return true;
      }


      Siblings siblingsAspect = (Siblings) optionalSiblingsAspect.get();

      if (siblingsAspect.isPrimary()) {
        return true;
      }

      // if you are not primary and your sibling exists in the result set, filter yourself out
      if (siblingsAspect.getSiblings().stream().anyMatch(
          sibling -> combinedResultUrns.contains(sibling)
      )) {
        return false;
      }

      return true;
    }).collect(Collectors.toList());

    entityLineageResult.setRelationships(new LineageRelationshipArray(filteredRelationships));
    entityLineageResult.setTotal(entityLineageResult.getTotal() + (existingResult != null ? existingResult.getTotal() : 0));
    entityLineageResult.setCount(filteredRelationships.size());
    return entityLineageResult;
  }

}
