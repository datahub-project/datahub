package com.linkedin.metadata.timeseries;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class GenericTimeseriesDocument {
  @Nonnull private String urn;
  private long timestampMillis;

  @JsonProperty("@timestamp")
  private long timestamp;

  @Nonnull private Object event;
  @Nullable private String messageId;
  @Nullable private Object systemMetadata;
  @Nullable private String eventGranularity;
  private boolean isExploded;
  @Nullable private String runId;
  @Nullable private String partition;
  @Nullable private Object partitionSpec;
}
