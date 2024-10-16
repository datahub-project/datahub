package io.datahubproject.openapi.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.datahubproject.openapi.generated.EntityResponse;
import java.util.Map;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UrnResponseMap {
  @JsonProperty("responses")
  private Map<String, EntityResponse> responses;
}
