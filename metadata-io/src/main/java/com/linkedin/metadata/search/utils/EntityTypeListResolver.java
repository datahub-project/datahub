package com.linkedin.metadata.search.utils;

import com.linkedin.metadata.config.search.EntityTypeListConfig;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves an {@link EntityTypeListConfig} into an ordered list of registry entity names, with soft
 * validation against the entity registry. There is no code baseline — empty config yields an empty
 * list (production defaults come from {@code application.yaml}). An empty resolved list means
 * <em>search no entity types</em> at the GraphQL layer — it must not be treated as "search all".
 *
 * <p>Order from {@code value} then {@code add} is preserved; duplicates are dropped
 * case-insensitively (first occurrence wins). Unknown registry names are soft-dropped with a warn.
 */
@Slf4j
public final class EntityTypeListResolver {
  private EntityTypeListResolver() {}

  @Nonnull
  public static List<String> resolve(
      @Nullable EntityTypeListConfig config, @Nullable EntityRegistry entityRegistry) {
    if (config == null || config.isEmpty()) {
      return List.of();
    }

    return validateAndFilter(
        mergeOrdered(config.parsedValue(), config.parsedAdd(), config.parsedRemove()),
        entityRegistry);
  }

  /**
   * Merges value → add → remove into an ordered, case-insensitively unique list. First occurrence
   * wins for duplicates across value/add.
   */
  @Nonnull
  static List<String> mergeOrdered(
      @Nonnull List<String> value, @Nonnull List<String> add, @Nonnull List<String> remove) {
    final LinkedHashSet<String> merged = new LinkedHashSet<>();
    final Set<String> seenLower = new LinkedHashSet<>();
    appendUnique(merged, seenLower, value);
    appendUnique(merged, seenLower, add);

    Set<String> toRemove =
        remove.stream().map(name -> name.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
    merged.removeIf(name -> toRemove.contains(name.toLowerCase(Locale.ROOT)));
    return new ArrayList<>(merged);
  }

  private static void appendUnique(
      @Nonnull LinkedHashSet<String> merged,
      @Nonnull Set<String> seenLower,
      @Nonnull List<String> names) {
    for (String name : names) {
      String lower = name.toLowerCase(Locale.ROOT);
      if (seenLower.add(lower)) {
        merged.add(name);
      }
    }
  }

  private static List<String> validateAndFilter(
      @Nonnull List<String> entityNames, @Nullable EntityRegistry entityRegistry) {
    if (entityRegistry == null) {
      return List.copyOf(entityNames);
    }

    final Map<String, EntitySpec> specsByLowerName =
        entityRegistry.getEntitySpecs().values().stream()
            .collect(
                Collectors.toMap(
                    spec -> spec.getName().toLowerCase(Locale.ROOT),
                    Function.identity(),
                    (a, b) -> a));

    final Set<String> knownNames =
        specsByLowerName.keySet().stream().sorted().limit(40).collect(Collectors.toSet());

    final LinkedHashSet<String> result = new LinkedHashSet<>();
    final Set<String> seenCanonicalLower = new LinkedHashSet<>();
    final List<String> dropped = new ArrayList<>();
    for (String name : entityNames) {
      EntitySpec spec = specsByLowerName.get(name.toLowerCase(Locale.ROOT));
      if (spec == null) {
        dropped.add(name);
        log.warn(
            "Dropping configured entity type '{}': not found in entity registry "
                + "(typo or unknown type). Known entity types include: {}",
            name,
            knownNames.stream().sorted().collect(Collectors.joining(","))
                + (specsByLowerName.size() > 40 ? ",..." : ""));
        continue;
      }
      String canonical = spec.getName();
      if (seenCanonicalLower.add(canonical.toLowerCase(Locale.ROOT))) {
        result.add(canonical);
      }
    }
    if (!dropped.isEmpty()) {
      log.warn(
          "Soft-dropped {} unknown configured entity type(s) {}; effective list size {} → {}. "
              + "Fix typos in elasticsearch.search.*EntityTypes / SEARCH_*_ENTITY_TYPES.",
          dropped.size(),
          dropped,
          entityNames.size(),
          result.size());
    }
    if (result.isEmpty() && !entityNames.isEmpty()) {
      log.warn(
          "Configured entity-type list resolved to empty after registry validation "
              + "(input was {}). GraphQL search/autocomplete/browse will match no entity types "
              + "(not all indices).",
          entityNames);
    }
    return List.copyOf(result);
  }
}
