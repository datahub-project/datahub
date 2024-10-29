package io.datahubproject.openapi.v2.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GenericTimeseriesAspect {
  private long timestampMillis;
  @Nonnull private String urn;
  @Nonnull private Object event;
  @Nullable private String messageId;
  @Nullable private Object systemMetadata;
}
