package com.linkedin.metadata.config.search;

import javax.annotation.Nonnull;

/** How {@code elasticsearch.clusterMigration.<family>.copy} moves a family's history. */
public enum CopyMode {
  /**
   * The upgrade process scrolls the source through its own client and bulk-creates on the target
   * through its own client. Works with every cluster connection configuration and needs nothing
   * from either cluster.
   */
  IN_PROCESS("inProcess"),

  /**
   * The target cluster's own {@code _reindex} pulls from the source. Server side, but the source
   * fetch is made by the target's nodes, so it can only use what the engine supports (basic auth,
   * or IAM pass-through on AWS public-endpoint pairs) and may need the allowlist node setting.
   */
  REMOTE_REINDEX("remoteReindex"),

  /**
   * No copy: the operator restored a snapshot into the target. The upgrade runs only the windowed
   * pass, delete-log replay, adoption and verification from {@code externalWatermarkEpochMs}.
   */
  EXTERNAL("external");

  private final String configValue;

  CopyMode(String configValue) {
    this.configValue = configValue;
  }

  public String getConfigValue() {
    return configValue;
  }

  @Nonnull
  public static CopyMode fromConfig(@Nonnull final String value) {
    final String trimmed = value.trim();
    for (CopyMode mode : values()) {
      if (mode.configValue.equalsIgnoreCase(trimmed) || mode.name().equalsIgnoreCase(trimmed)) {
        return mode;
      }
    }
    throw new IllegalArgumentException(
        "Unknown cluster migration copy mode '"
            + value
            + "'; expected one of inProcess, remoteReindex, external");
  }
}
