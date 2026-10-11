package com.linkedin.metadata.systemmetadata.cache;

import java.io.Serializable;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class KeyAspectEntityCountInFlightStatus implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final String STATUS_BUILDING = "BUILDING";

  @Nonnull String status;
  long recordedAtMillis;
  @Nullable String claimantId;

  @Nonnull
  public static KeyAspectEntityCountInFlightStatus building(
      long recordedAtMillis, @Nullable String claimantId) {
    return KeyAspectEntityCountInFlightStatus.builder()
        .status(STATUS_BUILDING)
        .recordedAtMillis(recordedAtMillis)
        .claimantId(claimantId)
        .build();
  }

  public boolean isBuilding() {
    return STATUS_BUILDING.equals(status);
  }
}
