package com.linkedin.metadata.entity;

import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.mxe.SystemMetadata;
import java.sql.Timestamp;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * The upper bound of a hard delete, captured from primary storage when the delete was requested.
 * {@code EntityService#deleteUrn(OperationContext, Urn, DeleteCeiling)} removes only what existed
 * at capture time; anything written later survives.
 *
 * @param aspectVersions every non-key, non-timeseries aspect the entity had at capture, mapped to
 *     its latest version per {@code ConditionalWriteValidator#resolveAspectVersion}. Rows of a
 *     listed aspect at or below the value are deleted; an aspect that is not listed is never
 *     touched. Never lists the key aspect.
 * @param keyCreatedMillis the key aspect's creation time at capture ({@link #keyCreatedMillisOf}).
 *     A different value at delete time means the urn was hard-deleted and recreated in between: the
 *     current entity is newer than the request, so nothing is deleted.
 * @param capturedAtMillis read before the capture. Timeseries aspects carry no version: the delete
 *     removes their documents with {@code timestampMillis} at or below this time. An aspect whose
 *     {@code systemMetadata.aspectCreated} is later than this time was created (or re-created after
 *     its own hard delete) after the capture, so it survives whatever its version.
 */
public record DeleteCeiling(
    @Nonnull Map<String, Long> aspectVersions, long keyCreatedMillis, long capturedAtMillis) {

  public DeleteCeiling {
    aspectVersions = Map.copyOf(aspectVersions);
    aspectVersions.forEach(
        (aspectName, version) -> {
          if (version < 1) {
            throw new IllegalArgumentException(
                String.format("Ceiling of %s must be at least 1, got %d", aspectName, version));
          }
        });
  }

  /**
   * Creation time of a key-aspect row: {@code systemMetadata.aspectCreated} when present (stamped
   * on the first write of the aspect), otherwise the row's {@code createdon} (legacy rows; a key
   * row is written once, so it is its creation time).
   */
  public static long keyCreatedMillisOf(@Nonnull SystemAspect keyAspect) {
    final SystemMetadata systemMetadata = keyAspect.getSystemMetadata();
    if (systemMetadata != null && systemMetadata.hasAspectCreated()) {
      return systemMetadata.getAspectCreated().getTime();
    }
    final Timestamp createdOn = keyAspect.getCreatedOn();
    if (createdOn == null) {
      throw new IllegalStateException(
          "Key aspect of " + keyAspect.getUrn() + " has neither aspectCreated nor createdon");
    }
    return createdOn.getTime();
  }

  /**
   * The aspects whose caller-supplied version is not exactly the captured one; an aspect absent at
   * capture never matches. A caller's {@code If-Version-Match} is a precondition on what it saw,
   * not a lower ceiling: deleting up to a version it saw while other aspects use the captured
   * versions would mix two points in time.
   */
  @Nonnull
  public SortedSet<String> callerMismatches(@Nonnull Map<String, Long> callerAspectVersions) {
    return callerAspectVersions.entrySet().stream()
        .filter(entry -> !entry.getValue().equals(aspectVersions.get(entry.getKey())))
        .map(Map.Entry::getKey)
        .collect(Collectors.toCollection(TreeSet::new));
  }
}
