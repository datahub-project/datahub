package io.datahubproject.openapi.v2.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.datahubproject.openapi.models.GenericAspect;
import io.datahubproject.openapi.models.GenericEntity;
import io.swagger.v3.oas.annotations.media.Schema;
import java.io.Serializable;
import java.util.List;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(builder = BatchGetUrnResponseV2.BatchGetUrnResponseV2Builder.class)
public class BatchGetUrnResponseV2<A extends GenericAspect, T extends GenericEntity<A>>
    implements Serializable {
  @JsonProperty("entities")
  @Schema(description = "List of entity responses")
  List<T> entities;
}
