package com.linkedin.datahub.upgrade.system.sampledata;

import java.util.Collections;
import java.util.Set;
import lombok.Builder;
import lombok.Data;

/**
 * Represents the difference between two sample data manifests.
 *
 * <p>Used to determine what entities need to be deleted or updated during a sample data migration.
 *
 * <p>Example usage:
 *
 * <pre>
 * ManifestDiff diff = calculator.calculateDiff(previousManifest, currentManifest);
 * if (!diff.getHardDeleteUrns().isEmpty()) {
 *   // Delete entities that were removed or had aspects removed
 *   hardDelete(diff.getHardDeleteUrns());
 * }
 * </pre>
 */
@Data
@Builder
public class ManifestDiff {

  /**
   * URNs that exist in previous manifest but not in current manifest.
   *
   * <p>These entities have been completely removed from sample data and should be hard-deleted.
   *
   * <p>Example: A dataset that existed in v001 but was removed in v002.
   */
  @Builder.Default private Set<String> removedUrns = Collections.emptySet();

  /**
   * URNs that exist in both manifests but had aspects removed.
   *
   * <p>These entities need to be hard-deleted and re-ingested to ensure removed aspects are
   * properly cleaned up (DataHub upsert cannot delete individual aspects).
   *
   * <p>Example: An entity that had ["ownership", "schema", "properties"] in v001 but only
   * ["ownership", "schema"] in v002. The "properties" aspect needs to be removed.
   */
  @Builder.Default private Set<String> urnsWithRemovedAspects = Collections.emptySet();

  /**
   * Union of removedUrns and urnsWithRemovedAspects.
   *
   * <p>This is the complete set of URNs that should be hard-deleted during migration. After
   * deletion, urnsWithRemovedAspects will be re-ingested with the correct set of aspects.
   */
  @Builder.Default private Set<String> hardDeleteUrns = Collections.emptySet();
}
