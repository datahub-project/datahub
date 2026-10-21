package com.linkedin.metadata.service;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.DeleteCeiling;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * One hard delete, as a {@link HardDeleteDispatcher} is offered it: what to delete, the aspect
 * versions the entity had when the delete was requested, and, for {@link
 * DeleteKind#ENTITY_AND_TIMESERIES}, which timeseries aspect values to delete.
 *
 * @param versions required for the kinds that delete the entity, null for {@link
 *     DeleteKind#REFERENCES}
 * @param timeseriesAspects required for {@link DeleteKind#ENTITY_AND_TIMESERIES}, null otherwise
 * @param startTimeMillis the oldest timeseries value deleted; null deletes from the oldest
 * @param endTimeMillis the newest timeseries value deleted; null deletes up to the most recent
 */
public record HardDeleteRequest(
    @Nonnull Urn urn,
    @Nonnull DeleteKind kind,
    @Nullable DeleteCeiling versions,
    @Nullable List<String> timeseriesAspects,
    @Nullable Long startTimeMillis,
    @Nullable Long endTimeMillis) {

  /**
   * @throws IllegalArgumentException when a field is missing for {@code kind}, or given for a kind
   *     that does not take it
   */
  public HardDeleteRequest {
    Objects.requireNonNull(urn, "urn is required");
    Objects.requireNonNull(kind, "kind is required");
    if ((kind == DeleteKind.REFERENCES) != (versions == null)) {
      throw new IllegalArgumentException(
          kind + (versions == null ? " requires versions" : " takes no versions"));
    }
    final boolean timeseries = kind == DeleteKind.ENTITY_AND_TIMESERIES;
    if (timeseries != (timeseriesAspects != null)) {
      throw new IllegalArgumentException(
          kind
              + (timeseriesAspects == null
                  ? " requires timeseries aspects"
                  : " takes no timeseries aspects"));
    }
    if (!timeseries && (startTimeMillis != null || endTimeMillis != null)) {
      throw new IllegalArgumentException(kind + " takes no time window");
    }
    timeseriesAspects = timeseriesAspects == null ? null : List.copyOf(timeseriesAspects);
  }

  @Nonnull
  public static HardDeleteRequest entity(@Nonnull Urn urn, @Nonnull DeleteCeiling versions) {
    return new HardDeleteRequest(urn, DeleteKind.ENTITY, versions, null, null, null);
  }

  @Nonnull
  public static HardDeleteRequest entityAndReferences(
      @Nonnull Urn urn, @Nonnull DeleteCeiling versions) {
    return new HardDeleteRequest(urn, DeleteKind.ENTITY_AND_REFERENCES, versions, null, null, null);
  }

  @Nonnull
  public static HardDeleteRequest entityAndTimeseries(
      @Nonnull Urn urn,
      @Nonnull DeleteCeiling versions,
      @Nonnull List<String> timeseriesAspects,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis) {
    return new HardDeleteRequest(
        urn,
        DeleteKind.ENTITY_AND_TIMESERIES,
        versions,
        timeseriesAspects,
        startTimeMillis,
        endTimeMillis);
  }

  @Nonnull
  public static HardDeleteRequest references(@Nonnull Urn urn) {
    return new HardDeleteRequest(urn, DeleteKind.REFERENCES, null, null, null, null);
  }
}
