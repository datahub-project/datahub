package io.datahubproject.openapi.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.swagger.v3.oas.annotations.media.Schema;
import java.io.Serializable;
import java.util.List;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(builder = BatchGetUrnResponse.BatchGetUrnResponseBuilder.class)
public class BatchGetUrnResponse<T extends GenericEntity> implements Serializable {
  @JsonProperty("entities")
  @Schema(description = "List of entity responses")
  List<T> entities;
}
