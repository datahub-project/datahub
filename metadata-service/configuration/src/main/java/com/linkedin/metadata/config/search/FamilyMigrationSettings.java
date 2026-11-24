package com.linkedin.metadata.config.search;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * {@code elasticsearch.clusterMigration.<family>}: moves one non-entity index family to another
 * named cluster while {@code componentCluster.<family>} keeps serving reads and primary writes.
 *
 * <p>A set {@link #targetCluster} mirrors the family's writes to that cluster. With {@code
 * copy.enabled} system-update also copies the family's history there and verifies it; with it off
 * the family is mirrored only, which is how a reverse mirror is kept during the bake after a flip.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class FamilyMigrationSettings {
  /** Name under {@code elasticsearch.clusters}; blank means no migration for this family. */
  private String targetCluster;

  private CopySettings copy;

  @Nullable
  public String getTargetCluster() {
    return SearchClusterSettings.blankToNull(targetCluster);
  }

  public boolean isConfigured() {
    return getTargetCluster() != null;
  }

  @Nonnull
  public CopySettings getCopy() {
    if (copy == null) {
      copy = CopySettings.builder().build();
    }
    return copy;
  }

  public boolean isCopyEnabled() {
    return isConfigured() && getCopy().isEnabled();
  }
}
