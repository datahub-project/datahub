package com.linkedin.metadata.entity.coordinator;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Immutable identity of a single aspect row: {@code (urn, aspectName, version)}. Used as a value
 * key to resolve the coordination {@link ConflictKey} for each input aspect. Row-lock ordering is
 * the DAO's responsibility ({@code EbeanAspectDao} sorts by {@code PrimaryKey}), not this type's.
 */
public record AspectKey(@Nonnull String urn, @Nonnull String aspectName, long version) {

  public AspectKey {
    Objects.requireNonNull(urn, "urn");
    Objects.requireNonNull(aspectName, "aspectName");
  }

  /**
   * Key for the latest materialized row of an aspect (version {@link
   * Constants#ASPECT_LATEST_VERSION } == 0).
   */
  @Nonnull
  public static AspectKey latest(@Nonnull String urn, @Nonnull String aspectName) {
    return new AspectKey(urn, aspectName, Constants.ASPECT_LATEST_VERSION);
  }

  @Nonnull
  public static AspectKey latest(@Nonnull Urn urn, @Nonnull String aspectName) {
    return latest(urn.toString(), aspectName);
  }
}
