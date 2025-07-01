package io.datahubproject.openapi.v2.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.swagger.v3.oas.annotations.media.Schema;
import java.io.Serializable;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode
@Builder
@JsonDeserialize(builder = BatchGetUrnRequestV2.BatchGetUrnRequestV2Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BatchGetUrnRequestV2 implements Serializable {
  @JsonProperty("urns")
  @Schema(required = true, description = "The list of urns to get.")
  List<String> urns;

  @JsonProperty("aspectNames")
  @Schema(required = true, description = "The list of aspect names to get")
  List<String> aspectNames;

  @JsonProperty("withSystemMetadata")
  @Schema(required = true, description = "Whether or not to retrieve system metadata")
  boolean withSystemMetadata;
}
