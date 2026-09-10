package com.linkedin.metadata.search.semantic;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Resolves semantic search entity names to their canonical entity-registry spelling. */
public final class SemanticSearchEntityUtils {

  private SemanticSearchEntityUtils() {}

  @Nonnull
  public static Set<String> canonicalizeEntityNames(
      @Nullable Set<String> entityNames, @Nonnull EntityRegistry entityRegistry) {
    if (entityNames == null || entityNames.isEmpty()) {
      return Collections.emptySet();
    }

    Map<String, String> canonicalNamesByLowercase = canonicalNamesByLowercase(entityRegistry);
    Set<String> canonicalNames = new LinkedHashSet<>();
    for (String entityName : entityNames) {
      canonicalizeEntityName(entityName, canonicalNamesByLowercase).ifPresent(canonicalNames::add);
    }
    return canonicalNames;
  }

  @Nonnull
  public static Optional<String> canonicalizeEntityName(
      @Nullable String entityName, @Nonnull EntityRegistry entityRegistry) {
    return canonicalizeEntityName(entityName, canonicalNamesByLowercase(entityRegistry));
  }

  public static boolean containsEntity(
      @Nullable Set<String> entityNames,
      @Nonnull EntityRegistry entityRegistry,
      @Nullable String entityName) {
    Optional<String> canonicalEntityName = canonicalizeEntityName(entityName, entityRegistry);
    return canonicalEntityName.isPresent()
        && canonicalizeEntityNames(entityNames, entityRegistry).contains(canonicalEntityName.get());
  }

  private static Optional<String> canonicalizeEntityName(
      @Nullable String entityName, @Nonnull Map<String, String> canonicalNamesByLowercase) {
    if (entityName == null || entityName.isBlank()) {
      return Optional.empty();
    }
    String trimmed = entityName.trim();
    return Optional.of(
        canonicalNamesByLowercase.getOrDefault(trimmed.toLowerCase(Locale.ROOT), trimmed));
  }

  private static Map<String, String> canonicalNamesByLowercase(
      @Nonnull EntityRegistry entityRegistry) {
    Map<String, String> names = new LinkedHashMap<>();
    entityRegistry.getEntitySpecs().values().stream()
        .map(EntitySpec::getName)
        .filter(name -> name != null && !name.isBlank())
        .forEach(name -> names.putIfAbsent(name.toLowerCase(Locale.ROOT), name));
    return names;
  }
}
