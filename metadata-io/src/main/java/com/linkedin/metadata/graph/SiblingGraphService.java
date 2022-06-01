package com.linkedin.metadata.graph;

import com.linkedin.common.Siblings;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import java.util.List;
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

  /**
   * Traverse from the entityUrn towards the input direction up to maxHops number of hops
   * Abstracts away the concept of relationship types
   *
   * Unless overridden, it uses the lineage registry to fetch valid edge types and queries for them
   */
  @Nonnull
  public EntityLineageResult getLineage(@Nonnull Urn entityUrn, @Nonnull LineageDirection direction, int offset,
      int count, int maxHops) {
    if (maxHops > 1) {
      throw new UnsupportedOperationException(
          String.format("More than 1 hop is not supported for %s", this.getClass().getSimpleName()));
    }

    EntityLineageResult entityLineage = _graphService.getLineage(entityUrn, direction, offset, count, maxHops);

    Siblings siblingAspectOfEntity = (Siblings) _entityService.getLatestAspect(entityUrn, Constants.SIBLINGS_ASPECT_NAME);

    // if you have siblings, we want to fetch their lineage too and merge it in
    if (siblingAspectOfEntity != null && siblingAspectOfEntity.hasSiblings()) {
      UrnArray siblingUrns = siblingAspectOfEntity.getSiblings();
      Set<Urn> allSiblingsInGroup = siblingUrns.stream().collect(Collectors.toSet());
      allSiblingsInGroup.add(entityUrn);

      entityLineage = filterLineageResultFromSiblings(entityUrn, allSiblingsInGroup, entityLineage);

      // Update offset and count to fetch the correct number of incoming edges below
      offset = Math.max(0, offset - entityLineage.getTotal());
      count = Math.max(0, count - entityLineage.getRelationships().size());

      for (Urn siblingUrn : siblingUrns) {
        EntityLineageResult siblingLineage = filterLineageResultFromSiblings(
            siblingUrn,
            allSiblingsInGroup,
            _graphService.getLineage(siblingUrn, direction, offset, count, maxHops)
        );

        entityLineage.getRelationships().addAll(siblingLineage.getRelationships());
        entityLineage.setTotal(entityLineage.getTotal() + siblingLineage.getTotal());

        // Update offset and count to fetch the correct number of incoming edges below
        offset = Math.max(0, offset - siblingLineage.getTotal());
        count = Math.max(0, count - siblingLineage.getRelationships().size());
      };
    }

    return entityLineage;
  }

  private EntityLineageResult filterLineageResultFromSiblings(Urn urn, Set<Urn> allSiblingsInGroup, EntityLineageResult entityLineageResult) {
    List<LineageRelationship> filteredRelationships = entityLineageResult.getRelationships()
        .stream()
        .filter(lineageRelationship -> !allSiblingsInGroup.contains(lineageRelationship.getEntity())
            || lineageRelationship.getEntity().equals(urn))
        .collect(Collectors.toList());
    entityLineageResult.setRelationships(new LineageRelationshipArray(filteredRelationships));
    entityLineageResult.setTotal(filteredRelationships.size());
    return entityLineageResult;
  }

}
