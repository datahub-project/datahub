package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nonnull;

/**
 * Decides which V3 indices still block reads from moving off V2 at startup, and whether a cutover
 * that already happened must stick.
 *
 * <p>A V2 entity index that exists can hold documents its paired V3 index does not have yet. Reads
 * move to V3 only after that V3 index is stamped {@link IndexMigrationStamp#COMPLETE}, which the
 * incremental reindex writes after the fill and its catch-up. Until then the process holds reads on
 * V2. Once every pair is complete the V2 indices are stamped retired, and a later start does not
 * hold even if a V3 mark is gone. A V3 index that is missing or whose mapping cannot be read after
 * that record exists stops startup instead of holding. An incomplete stamp is not unreadable.
 * Before the cutover it still holds reads on V2. After the cutover readers serve the alias, and
 * catch-up marks the index complete when the fill window is replayed. An index with no migration
 * mark and a positive count, including one whose mark a reindex dropped, is not unreadable.
 */
public final class SearchV3ReadHoldPolicy {

  private SearchV3ReadHoldPolicy() {}

  /** What the hold does with the indices it found. */
  public enum Decision {
    /** No V2 entity index. A fresh install reads V3. */
    LEAVE,
    /** Some paired V3 index is not complete, and the cutover is not recorded. */
    HOLD,
    /** Every paired V3 index is complete. Stamp the V2 indices retired and do not hold. */
    COMMIT,
    /** Every V2 index is already retired. Do not hold. */
    CUT_OVER,
    /** The cutover is recorded, and a paired V3 index cannot be read. */
    FAIL
  }

  /**
   * @param v2Indices V2 entity indices that exist
   * @param retiredV2Indices those of {@code v2Indices} stamped {@link SearchV3CutoverStamp#RETIRED}
   * @param pairedV3Indices V3 indices paired with {@code v2Indices}
   * @param migrationStateByV3Index {@code _meta.migration} for each V3 index that exists and
   *     carries a mark; an index with no entry is not complete
   * @param unreadableV3Indices paired V3 indices that are missing or whose mappings could not be
   *     read. An incomplete stamp is not included: before the cutover {@link #incompleteV3Indices}
   *     still holds, and after the cutover the index is served
   */
  @Nonnull
  public static Decision decide(
      @Nonnull Set<String> v2Indices,
      @Nonnull Set<String> retiredV2Indices,
      @Nonnull Set<String> pairedV3Indices,
      @Nonnull Map<String, String> migrationStateByV3Index,
      @Nonnull Set<String> unreadableV3Indices) {
    if (v2Indices.isEmpty()) {
      return Decision.LEAVE;
    }
    if (retiredV2Indices.containsAll(v2Indices)) {
      if (!unreadableV3Indices.isEmpty()) {
        return Decision.FAIL;
      }
      return Decision.CUT_OVER;
    }
    if (unreadableV3Indices.isEmpty()
        && incompleteV3Indices(pairedV3Indices, migrationStateByV3Index).isEmpty()) {
      return Decision.COMMIT;
    }
    return Decision.HOLD;
  }

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
