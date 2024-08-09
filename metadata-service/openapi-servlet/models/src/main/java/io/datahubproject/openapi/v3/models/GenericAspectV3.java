package io.datahubproject.openapi.v3.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.datahubproject.openapi.models.GenericAspect;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

@EqualsAndHashCode
@Value
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@AllArgsConstructor
public class GenericAspectV3 implements GenericAspect {
  @Nonnull Map<String, Object> value;
  @Nullable Map<String, Object> systemMetadata;
  @Nullable Map<String, String> headers;
  @Nullable Map<String, Object> auditStamp;
}
