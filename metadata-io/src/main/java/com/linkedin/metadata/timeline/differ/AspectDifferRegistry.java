package com.linkedin.metadata.timeline.differ;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;


/**
 * A registry that maps an aspect name to one or more {@link AspectDiffer}s.
 */
public class AspectDifferRegistry {

  private final Map<String, List<AspectDiffer<?>>> aspectDiffers = new HashMap<>();

  /**
   * Registers a new aspect differ.
   */
  public void register(@Nonnull final String aspectName, @Nonnull final AspectDiffer<?> differ) {
    Objects.requireNonNull(aspectName);
    Objects.requireNonNull(differ);
    aspectDiffers.putIfAbsent(aspectName, new ArrayList<>());
    aspectDiffers.get(aspectName).add(differ);
  }

  /**
   * Registers a new aspect differ, or null if one does not exist.
   */
  public List<AspectDiffer<?>> getAspectDiffers(@Nonnull final String aspectName) {
    final String key = Objects.requireNonNull(aspectName);
    return this.aspectDiffers.getOrDefault(key, Collections.emptyList());
  }
}
