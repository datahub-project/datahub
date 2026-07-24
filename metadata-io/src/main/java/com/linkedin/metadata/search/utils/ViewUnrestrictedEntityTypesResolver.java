package com.linkedin.metadata.search.utils;

import com.datahub.authorization.config.ViewUnrestrictedEntityTypes;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collections;
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
 * Resolves {@link ViewUnrestrictedEntityTypes} into an effective set of registry entity names, with
 * soft validation against the entity registry. There is no code baseline — empty config yields an
 * empty set (production defaults come from {@code application.yaml}).
 *
 * <p>Order from {@code value} then {@code add} is preserved in the returned {@link LinkedHashSet};
 * duplicates are dropped case-insensitively (first occurrence wins).
 */
@Slf4j
public final class ViewUnrestrictedEntityTypesResolver {
  private ViewUnrestrictedEntityTypesResolver() {}

  @Nonnull
  public static Set<String> resolve(
      @Nullable ViewUnrestrictedEntityTypes config, @Nullable EntityRegistry entityRegistry) {
    if (config == null || config.isEmpty()) {
      return Set.of();
    }

    return Collections.unmodifiableSet(
        new LinkedHashSet<>(
            validateAndFilter(
                EntityTypeListResolver.mergeOrdered(
                    config.parsedValue(), config.parsedAdd(), config.parsedRemove()),
                entityRegistry)));
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
    for (String name : entityNames) {
      EntitySpec spec = specsByLowerName.get(name.toLowerCase(Locale.ROOT));
      if (spec == null) {
        log.warn(
            "Dropping view-unrestricted entity type '{}': not found in entity registry. "
                + "Known entity types include: {}",
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
    return result.isEmpty() ? Collections.emptyList() : List.copyOf(result);
  }
}
