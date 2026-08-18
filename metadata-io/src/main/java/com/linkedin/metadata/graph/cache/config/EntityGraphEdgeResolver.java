package com.linkedin.metadata.graph.cache.config;

import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphEdgeTriplet;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.ResolvedGraphEdge;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EntityGraphEdgeResolver {

  private final EntityRegistry entityRegistry;

  public EntityGraphEdgeResolver(@Nonnull EntityRegistry entityRegistry) {
    this.entityRegistry = entityRegistry;
  }

  @Nonnull
  public Optional<ResolvedGraphEdge> resolve(@Nonnull GraphEdgeTriplet triplet) {
    String sourceEntityType = triplet.getSourceEntityType().toLowerCase(Locale.ROOT);
    EntitySpec sourceSpec = entityRegistry.getEntitySpecs().get(sourceEntityType);
    if (sourceSpec == null) {
      try {
        sourceSpec = entityRegistry.getEntitySpec(sourceEntityType);
      } catch (IllegalArgumentException e) {
        log.warn(
            "Unknown source entity type {} for triplet {}", triplet.getSourceEntityType(), triplet);
        return Optional.empty();
      }
    }
    if (sourceSpec == null) {
      log.warn(
          "Unknown source entity type {} for triplet {}", triplet.getSourceEntityType(), triplet);
      return Optional.empty();
    }

    for (RelationshipFieldSpec relationshipField : sourceSpec.getRelationshipFieldSpecs()) {
      if (!relationshipField
          .getRelationshipName()
          .equalsIgnoreCase(triplet.getRelationshipType())) {
        continue;
      }
      if (relationshipField.getValidDestinationTypes().stream()
          .noneMatch(dest -> dest.equalsIgnoreCase(triplet.getDestinationEntityType()))) {
        continue;
      }

      String aspectName = findAspectName(sourceSpec, relationshipField);
      String searchField = null;
      boolean searchable = false;
      Optional<SearchableFieldSpec> coLocated =
          findCoLocatedSearchable(sourceSpec, relationshipField);
      if (coLocated.isPresent()
          && coLocated.get().getSearchableAnnotation().getFieldType()
              == SearchableAnnotation.FieldType.URN) {
        searchField = relationshipField.getPath().toString();
        searchable = true;
      }

      return Optional.of(
          ResolvedGraphEdge.builder()
              .triplet(triplet)
              .aspectName(aspectName)
              .searchField(searchField)
              .searchable(searchable)
              .graphDirection(RelationshipDirection.OUTGOING)
              .build());
    }

    log.warn("No registry relationship for triplet {}", triplet);
    return Optional.empty();
  }

  private static String findAspectName(
      EntitySpec sourceSpec, RelationshipFieldSpec relationshipField) {
    for (AspectSpec aspectSpec : sourceSpec.getAspectSpecs()) {
      if (aspectSpec.getRelationshipFieldSpecs().stream()
          .anyMatch(r -> r.getPath().equals(relationshipField.getPath()))) {
        return aspectSpec.getName();
      }
    }
    return "";
  }

  private static Optional<SearchableFieldSpec> findCoLocatedSearchable(
      EntitySpec sourceSpec, RelationshipFieldSpec relationshipField) {
    for (AspectSpec aspectSpec : sourceSpec.getAspectSpecs()) {
      for (SearchableFieldSpec searchableFieldSpec : aspectSpec.getSearchableFieldSpecs()) {
        if (searchableFieldSpec.getPath().equals(relationshipField.getPath())) {
          return Optional.of(searchableFieldSpec);
        }
      }
    }
    return Optional.empty();
  }

  @Nonnull
  public List<ResolvedGraphEdge> resolveAll(@Nonnull List<GraphEdgeTriplet> triplets) {
    List<ResolvedGraphEdge> resolved = new ArrayList<>();
    for (GraphEdgeTriplet triplet : triplets) {
      resolve(triplet).ifPresent(resolved::add);
    }
    return resolved;
  }
}
