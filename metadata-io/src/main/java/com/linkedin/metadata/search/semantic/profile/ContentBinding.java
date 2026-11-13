package com.linkedin.metadata.search.semantic.profile;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Aspects and dotted field paths that make one embedding profile stale. An empty path set means any
 * change to that aspect. An empty aspect map means nothing dirties the profile.
 */
public record ContentBinding(@Nonnull Map<String, Set<String>> aspectFields) {

  public static final ContentBinding EXTERNAL = new ContentBinding(Map.of());

  public ContentBinding {
    aspectFields = Map.copyOf(aspectFields);
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
}
