package com.linkedin.metadata.entity.coordinator;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import java.util.Comparator;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Immutable identity of a single aspect row: {@code (urn, aspectName, version)}.
 *
 * <p>The natural ordering is {@code urn -> aspectName -> version}. This intentionally mirrors the
 * {@code FOR UPDATE} lock-acquisition sort used in {@code EbeanAspectDao.batchGet} (sort by {@code
 * PrimaryKey.getUrn()}, then {@code getAspect()}, then {@code getVersion()}). Planned mutations are
 * kept in this order so that any row locks taken during apply are acquired in the same order as the
 * DAO, avoiding lock-order deadlocks between concurrent writers.
 */
public record AspectKey(@Nonnull String urn, @Nonnull String aspectName, long version)
    implements Comparable<AspectKey> {

  private static final Comparator<AspectKey> COMPARATOR =
      Comparator.comparing(AspectKey::urn)
          .thenComparing(AspectKey::aspectName)
          .thenComparingLong(AspectKey::version);

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

  @Override
  public int compareTo(@Nonnull AspectKey other) {
    return COMPARATOR.compare(this, other);
  }
}
