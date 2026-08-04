package com.linkedin.metadata.models;

import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.Value;

/**
 * Ensures each directed edge signature maps to exactly one aspect:
 *
 * <pre>(sourceEntityType, destinationEntityType, relationshipName) → aspectName</pre>
 *
 * Multiple fields within the same aspect may share a signature. The same aspect may be registered
 * on many source entity types. Two different aspects on the same source must not claim the same
 * destination + relationship name.
 *
 * <p>When {@code entityTypes} is empty on a {@code @Relationship}, destination is treated as {@code
 * *} and conflicts with any other aspect claiming the same source + relationship for any
 * destination.
 */
public final class RelationshipEdgeUniquenessValidator {

  public static final String WILDCARD_DESTINATION = "*";

  private RelationshipEdgeUniquenessValidator() {}

  @Nonnull
  public static List<Conflict> findConflicts(@Nonnull final EntityRegistry entityRegistry) {
    List<Conflict> conflicts = new ArrayList<>();
    for (EntitySpec entitySpec : entityRegistry.getEntitySpecs().values()) {
      conflicts.addAll(findConflicts(entitySpec));
    }
    return conflicts;
  }

  @Nonnull
  public static List<Conflict> findConflicts(@Nonnull final EntitySpec entitySpec) {
    String sourceEntity = normalize(entitySpec.getName());
    // edge key → first aspect that claimed it
    Map<EdgeKey, String> owners = new HashMap<>();
    List<Conflict> conflicts = new ArrayList<>();

    for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
      String aspectName = aspectSpec.getName();
      for (RelationshipFieldSpec fieldSpec : aspectSpec.getRelationshipFieldSpecs()) {
        String relationshipName = fieldSpec.getRelationshipName();
        List<String> destinations = fieldSpec.getValidDestinationTypes();
        if (destinations == null || destinations.isEmpty()) {
          destinations = Collections.singletonList(WILDCARD_DESTINATION);
        }
        for (String destination : destinations) {
          EdgeKey key = new EdgeKey(sourceEntity, normalize(destination), relationshipName);
          String existingAspect = owners.putIfAbsent(key, aspectName);
          if (existingAspect != null && !existingAspect.equals(aspectName)) {
            conflicts.add(
                new Conflict(
                    sourceEntity,
                    key.getDestination(),
                    relationshipName,
                    existingAspect,
                    aspectName));
          } else if (existingAspect == null) {
            // Also conflict with wildcard / concrete counterparts across aspects
            addCrossWildcardConflicts(
                owners, key, aspectName, conflicts, sourceEntity, relationshipName);
          }
        }
      }
    }
    return conflicts;
  }

  /**
   * When a concrete dest is claimed, conflict with an existing wildcard claim (and vice versa) from
   * a different aspect for the same source + relationship.
   */
  private static void addCrossWildcardConflicts(
      Map<EdgeKey, String> owners,
      EdgeKey newlyClaimed,
      String aspectName,
      List<Conflict> conflicts,
      String sourceEntity,
      String relationshipName) {
    if (WILDCARD_DESTINATION.equals(newlyClaimed.getDestination())) {
      for (Map.Entry<EdgeKey, String> entry : owners.entrySet()) {
        EdgeKey other = entry.getKey();
        if (other.equals(newlyClaimed)) {
          continue;
        }
        if (other.getSource().equals(sourceEntity)
            && other.getRelationshipName().equals(relationshipName)
            && !WILDCARD_DESTINATION.equals(other.getDestination())
            && !entry.getValue().equals(aspectName)) {
          conflicts.add(
              new Conflict(
                  sourceEntity,
                  other.getDestination(),
                  relationshipName,
                  entry.getValue(),
                  aspectName));
        }
      }
    } else {
      EdgeKey wildcardKey = new EdgeKey(sourceEntity, WILDCARD_DESTINATION, relationshipName);
      String wildcardOwner = owners.get(wildcardKey);
      if (wildcardOwner != null && !wildcardOwner.equals(aspectName)) {
        conflicts.add(
            new Conflict(
                sourceEntity,
                newlyClaimed.getDestination(),
                relationshipName,
                wildcardOwner,
                aspectName));
      }
    }
  }

  public static void validate(@Nonnull final EntitySpec entitySpec) {
    List<Conflict> conflicts = findConflicts(entitySpec);
    if (!conflicts.isEmpty()) {
      throw new ModelValidationException(formatConflicts(entitySpec.getName(), conflicts));
    }
  }

  public static void validate(@Nonnull final EntityRegistry entityRegistry) {
    List<Conflict> conflicts = findConflicts(entityRegistry);
    if (!conflicts.isEmpty()) {
      throw new ModelValidationException(formatConflicts("entity-registry", conflicts));
    }
  }

  @Nonnull
  public static String formatConflicts(
      @Nonnull final String context, @Nonnull final List<Conflict> conflicts) {
    StringBuilder sb = new StringBuilder();
    sb.append("Relationship edge uniqueness violated for ")
        .append(context)
        .append(
            ". Each (source, destination, relationship) may be owned by only one aspect. Found ")
        .append(conflicts.size())
        .append(" conflict(s):\n");
    for (Conflict conflict : conflicts) {
      sb.append("  - ")
          .append(conflict.getSourceEntity())
          .append(" --")
          .append(conflict.getRelationshipName())
          .append("--> ")
          .append(conflict.getDestinationEntity())
          .append(" claimed by aspects '")
          .append(conflict.getFirstAspect())
          .append("' and '")
          .append(conflict.getSecondAspect())
          .append("'\n");
    }
    return sb.toString();
  }

  private static String normalize(@Nonnull final String name) {
    return name.toLowerCase(Locale.ROOT);
  }

  @Value
  private static class EdgeKey {
    String source;
    String destination;
    String relationshipName;
  }

  @Value
  public static class Conflict {
    String sourceEntity;
    String destinationEntity;
    String relationshipName;
    String firstAspect;
    String secondAspect;
  }
}
