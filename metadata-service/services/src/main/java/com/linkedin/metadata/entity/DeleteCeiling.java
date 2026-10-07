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
 */
public record DeleteCeiling(@Nonnull Map<String, Long> aspectVersions) {

  public DeleteCeiling {
    aspectVersions = Map.copyOf(aspectVersions);
  }
}
