package com.linkedin.metadata.entity;

import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.validation.ConditionalWriteValidator;
import com.linkedin.mxe.SystemMetadata;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nonnull;

/**
 * Which rows a ceiling-bounded delete removes, decided from the latest rows read under lock. Pure:
 * no I/O. Per non-key aspect with a latest row:
 *
 * <ul>
 *   <li>not listed (created after the capture), or created after {@code capturedAtMillis}
 *       (hard-deleted and written again since the capture, so its version restarted at 1):
 *       untouched, survives;
 *   <li>listed, latest version at or below its ceiling: every row goes ({@link #deleteWhole});
 *   <li>listed, latest version above its ceiling: the latest survives, history rows {@code
 *       1..ceiling} go ({@link #deleteHistoryUpTo}; a history row is numbered by the version it had
 *       as latest).
 * </ul>
 *
 * The key aspect is never planned: it goes only when {@link #survivors} is empty.
 */
record CeilingDeletePlan(
    @Nonnull SortedSet<String> deleteWhole,
    @Nonnull SortedMap<String, Long> deleteHistoryUpTo,
    @Nonnull SortedSet<String> survivors) {

  /**
   * @param capturedAtMillis when the ceilings were captured; {@code Long.MAX_VALUE} when there was
   *     no capture (a caller-supplied single-aspect ceiling), which disables the creation-time rule
   */
  @Nonnull
  static CeilingDeletePlan of(
      @Nonnull String keyAspectName,
      @Nonnull Map<String, SystemAspect> latest,
      @Nonnull Map<String, Long> ceilings,
      long capturedAtMillis) {
    final SortedSet<String> deleteWhole = new TreeSet<>();
    final SortedMap<String, Long> deleteHistoryUpTo = new TreeMap<>();
    final SortedSet<String> survivors = new TreeSet<>();
    for (Map.Entry<String, SystemAspect> entry : latest.entrySet()) {
      final String aspectName = entry.getKey();
      if (aspectName.equals(keyAspectName)) {
        continue;
      }
      final Long ceiling = ceilings.get(aspectName);
      if (ceiling == null || createdAfter(entry.getValue(), capturedAtMillis)) {
        survivors.add(aspectName);
      } else if (ConditionalWriteValidator.resolveAspectVersion(entry.getValue()) <= ceiling) {
        deleteWhole.add(aspectName);
      } else {
        deleteHistoryUpTo.put(aspectName, ceiling);
        survivors.add(aspectName);
      }
    }
    return new CeilingDeletePlan(
        Collections.unmodifiableSortedSet(deleteWhole),
        Collections.unmodifiableSortedMap(deleteHistoryUpTo),
        Collections.unmodifiableSortedSet(survivors));
  }

  private static boolean createdAfter(@Nonnull SystemAspect row, long capturedAtMillis) {
    final SystemMetadata systemMetadata = row.getSystemMetadata();
    return systemMetadata != null
        && systemMetadata.hasAspectCreated()
        && systemMetadata.getAspectCreated().getTime() > capturedAtMillis;
  }
}
