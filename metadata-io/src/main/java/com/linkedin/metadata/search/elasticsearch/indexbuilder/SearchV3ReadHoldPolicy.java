package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nonnull;

/**
 * Decides which V3 indices still block reads from moving off V2 at startup.
 *
 * <p>A V2 entity index that exists can hold documents its paired V3 index does not have yet. Reads
 * move to V3 only after that V3 index is stamped {@link IndexMigrationStamp#COMPLETE}, which the
 * incremental reindex writes after the fill and its catch-up. Until then the process holds reads on
 * V2.
 */
public final class SearchV3ReadHoldPolicy {

  private SearchV3ReadHoldPolicy() {}

  /**
   * V3 indices, among those paired with an existing V2 index, that are not stamped complete.
   *
   * @param pairedV3Indices V3 indices whose paired V2 index exists
   * @param migrationStateByV3Index {@code _meta.migration} value for each V3 index that exists and
   *     carries a mark; an index with no entry is treated as incomplete
   */
  @Nonnull
  public static Set<String> incompleteV3Indices(
      @Nonnull Set<String> pairedV3Indices, @Nonnull Map<String, String> migrationStateByV3Index) {
    Set<String> incomplete = new TreeSet<>();
    for (String v3Index : pairedV3Indices) {
      if (!IndexMigrationStamp.COMPLETE.equals(migrationStateByV3Index.get(v3Index))) {
        incomplete.add(v3Index);
      }
    }
    return incomplete;
  }
}
