package com.linkedin.metadata.entity.coordinator;

import javax.annotation.Nonnull;

/**
 * A proposed deletion of an aspect row. In the overlay a planned delete hides any database aspect
 * for its {@link AspectKey}.
 */
public record PlannedDelete(@Nonnull AspectKey key) implements PlannedMutation {

  @Nonnull
  public static PlannedDelete of(@Nonnull String urn, @Nonnull String aspectName) {
    return new PlannedDelete(AspectKey.latest(urn, aspectName));
  }
}
