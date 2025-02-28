package io.datahubproject.openapi.util;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class RequestInputUtil {
  private RequestInputUtil() {}

  public static List<String> resolveAspectNames(
      EntityRegistry entityRegistry, Urn urn, List<String> inputAspectNames, boolean expandEmpty) {
    return resolveAspectSpecs(entityRegistry, urn, inputAspectNames, expandEmpty).stream()
        .map(AspectSpec::getName)
        .toList();
  }

  /**
   * For a given urn and list of aspect names, resolve AspectSpecs
   *
   * @param entityRegistry
   * @param urn
   * @param inputAspectNames
   * @param expandEmpty if empty return all AspectSpecs
   * @return
   */
  public static List<AspectSpec> resolveAspectSpecs(
      EntityRegistry entityRegistry, Urn urn, List<String> inputAspectNames, boolean expandEmpty) {
    LinkedHashMap<String, Long> intermediateReq =
        inputAspectNames.stream()
            .map(name -> Map.entry(name, 0L))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (existing, replacement) -> existing,
                    LinkedHashMap::new));
    Map<Urn, Map<AspectSpec, Long>> intermediate =
        resolveAspectSpecs(
            entityRegistry, new LinkedHashMap<>(Map.of(urn, intermediateReq)), 0L, expandEmpty);
    return new ArrayList<>(intermediate.getOrDefault(urn, Map.of()).keySet());
  }

  /**
   * Given a map with aspect names from the API, normalized them into actual aspect names (casing
   * fixes)
   *
   * @param requestedAspectNames requested aspects
   * @param <T> map values
   * @param expandEmpty whether to expand empty aspect names to all aspect names
   * @return updated map
   */
  public static <T> LinkedHashMap<Urn, Map<AspectSpec, T>> resolveAspectSpecs(
      EntityRegistry entityRegistry,
      LinkedHashMap<Urn, Map<String, T>> requestedAspectNames,
      @Nonnull T defaultValue,
      boolean expandEmpty) {
    return requestedAspectNames.entrySet().stream()
        .map(
            entry -> {
              final Urn urn = entry.getKey();
              if (expandEmpty && (entry.getValue().isEmpty() || entry.getValue().containsKey(""))) {
                // All aspects specified
                Set<AspectSpec> allNames =
                    new HashSet<>(
                        entityRegistry.getEntitySpec(urn.getEntityType()).getAspectSpecs());
                return Map.entry(
                    urn,
                    allNames.stream()
                        .map(
                            aspectName ->
                                Map.entry(
                                    aspectName, entry.getValue().getOrDefault("", defaultValue)))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
              } else if (!entry.getValue().keySet().isEmpty()) {
                final Map<String, AspectSpec> normalizedNames =
                    entry.getValue().keySet().stream()
                        .map(
                            requestAspectName ->
                                Map.entry(
                                    requestAspectName,
                                    lookupAspectSpec(entityRegistry, urn, requestAspectName)))
                        .filter(aspectSpecEntry -> aspectSpecEntry.getValue().isPresent())
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));
                return Map.entry(
                    urn,
                    entry.getValue().entrySet().stream()
                        .filter(reqEntry -> normalizedNames.containsKey(reqEntry.getKey()))
                        .map(
                            reqEntry ->
                                Map.entry(
                                    normalizedNames.get(reqEntry.getKey()), reqEntry.getValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
              } else {
                return (Map.Entry<Urn, Map<AspectSpec, T>>) null;
              }
            })
        .filter(Objects::nonNull)
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (a, b) -> {
                  throw new IllegalStateException("Duplicate key");
                },
                LinkedHashMap::new));
  }

  private static Optional<AspectSpec> lookupAspectSpec(
      EntityRegistry entityRegistry, Urn urn, String aspectName) {
    return lookupAspectSpec(entityRegistry.getEntitySpec(urn.getEntityType()), aspectName);
  }

  /** Case-insensitive fallback */
  public static Optional<AspectSpec> lookupAspectSpec(EntitySpec entitySpec, String aspectName) {
    if (entitySpec == null) {
      return Optional.empty();
    }

    return entitySpec.getAspectSpec(aspectName) != null
        ? Optional.of(entitySpec.getAspectSpec(aspectName))
        : entitySpec.getAspectSpecs().stream()
            .filter(aspec -> aspec.getName().toLowerCase().equals(aspectName))
            .findFirst();
  }
}
