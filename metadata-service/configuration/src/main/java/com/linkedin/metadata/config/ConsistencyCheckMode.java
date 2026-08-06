package com.linkedin.metadata.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Per-check run mode for the entity-consistency upgrade job.
 *
 * <ul>
 *   <li>{@link #DISABLED} — check is not selected
 *   <li>{@link #DRY_RUN} — check runs; fixes are reported but not applied
 *   <li>{@link #ACTIVE} — check runs; fixes are applied
 * </ul>
 */
public enum ConsistencyCheckMode {
  DISABLED,
  DRY_RUN,
  ACTIVE;

  @JsonCreator
  @Nonnull
  public static ConsistencyCheckMode fromString(@Nullable String value) {
    if (value == null || value.isBlank()) {
      return DRY_RUN;
    }
    String normalized = value.trim().toLowerCase().replace('_', '-');
    return switch (normalized) {
      case "disabled" -> DISABLED;
      case "active" -> ACTIVE;
      case "dry-run", "dryrun" -> DRY_RUN;
      default -> throw new IllegalArgumentException(
          "Unknown consistency check mode '" + value + "'. Expected: disabled, dry-run, or active");
    };
  }

  public boolean isDisabled() {
    return this == DISABLED;
  }

  public boolean shouldApplyFixes() {
    return this == ACTIVE;
  }
}
