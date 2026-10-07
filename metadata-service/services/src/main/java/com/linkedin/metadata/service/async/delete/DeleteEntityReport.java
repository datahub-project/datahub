package com.linkedin.metadata.service.async.delete;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.ConditionalDeleteOutcome;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * What one reliable hard delete of an entity did.
 *
 * @param outcome {@code DELETED} (the entity is gone), {@code PARTIAL} (data written after the
 *     request survives, so the entity stays) or {@code ALREADY_DELETED} (absent, or recreated after
 *     the request)
 * @param rowsDeleted primary-storage rows removed
 * @param timeseriesRowsDeleted timeseries documents removed (timestamp at or before the request)
 * @param referencesRemoved referrers whose reference to the entity was removed
 */
public record DeleteEntityReport(
    @Nonnull String urn,
    @Nonnull ConditionalDeleteOutcome outcome,
    long rowsDeleted,
    long timeseriesRowsDeleted,
    int referencesRemoved) {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public DeleteEntityReport {
    Objects.requireNonNull(urn, "urn");
    Objects.requireNonNull(outcome, "outcome");
  }

  @Nonnull
  public static DeleteEntityReport alreadyDeleted(@Nonnull Urn urn) {
    return new DeleteEntityReport(
        urn.toString(), ConditionalDeleteOutcome.ALREADY_DELETED, 0L, 0L, 0);
  }

  @Nonnull
  public String toJson() {
    try {
      return OBJECT_MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      // Strings, an enum and numbers always serialize.
      throw new IllegalStateException(e);
    }
  }

  /**
   * @throws IllegalArgumentException when {@code json} is not a report
   */
  @Nonnull
  public static DeleteEntityReport fromJson(@Nonnull String json) {
    try {
      return OBJECT_MAPPER.readValue(json, DeleteEntityReport.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid delete report", e);
    }
  }
}
