package com.linkedin.metadata.search.semantic.profile;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Aspects and dotted field paths that make one embedding profile stale ({@code aspectFields}), and
 * further fields its embedding reads without making it stale ({@code includedFields}). An empty
 * path set means the whole aspect. An empty trigger map means nothing dirties the profile.
 */
public record ContentBinding(
    @Nonnull Map<String, Set<String>> aspectFields,
    @Nonnull Map<String, Set<String>> includedFields) {

  public static final ContentBinding EXTERNAL = new ContentBinding(Map.of());

  public ContentBinding {
    aspectFields = Map.copyOf(aspectFields);
    includedFields = Map.copyOf(includedFields);
  }

  public ContentBinding(@Nonnull Map<String, Set<String>> aspectFields) {
    this(aspectFields, Map.of());
  }

  public boolean isExternal() {
    return aspectFields.isEmpty();
  }

  public boolean matches(@Nonnull String aspectName, @Nonnull Set<String> changedPaths) {
    if (!aspectFields.containsKey(aspectName)) {
      return false;
    }
    Set<String> paths = aspectFields.get(aspectName);
    if (paths == null || paths.isEmpty()) {
      return !changedPaths.isEmpty();
    }
    return !Collections.disjoint(paths, changedPaths);
  }

  /**
   * Every field an embedder reads: triggers and includes. An aspect read whole by either side is
   * read whole here.
   */
  @Nonnull
  public Map<String, Set<String>> inputFields() {
    Map<String, Set<String>> inputs = new LinkedHashMap<>();
    merge(inputs, aspectFields);
    merge(inputs, includedFields);
    return Map.copyOf(inputs);
  }

  private static void merge(
      @Nonnull Map<String, Set<String>> into, @Nonnull Map<String, Set<String>> fields) {
    fields.forEach(
        (aspect, paths) -> {
          Set<String> existing = into.get(aspect);
          if (existing != null && existing.isEmpty()) {
            return;
          }
          if (paths.isEmpty()) {
            into.put(aspect, Set.of());
            return;
          }
          Set<String> merged = existing == null ? new HashSet<>() : new HashSet<>(existing);
          merged.addAll(paths);
          into.put(aspect, Set.copyOf(merged));
        });
  }
}
