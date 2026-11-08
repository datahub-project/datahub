package com.linkedin.metadata.entity;

import java.util.Map;
import javax.annotation.Nonnull;

/**
 * The upper bound of a hard delete, captured from primary storage when the delete was requested
 * ({@code EntityService#captureDeleteCeiling}). Only what existed then is deleted; anything written
 * later survives.
 *
 * @param aspectVersions every aspect the entity had at capture, key included, mapped to the version
 *     a delete may remove up to (the {@code EntityService#DELETE_CONDITION_MAX_VERSION} condition).
 *     An aspect that is not listed was created after the capture and is never touched.
 * @param keyCreatedOnMillis when the key row was written: versions restart at 1 if the entity is
 *     deleted and recreated, so a different key row means a different entity, which is kept.
 */
public record DeleteCeiling(@Nonnull Map<String, Long> aspectVersions, long keyCreatedOnMillis) {

  public DeleteCeiling {
    aspectVersions = Map.copyOf(aspectVersions);
  }
}
