package com.linkedin.metadata.datahubusage.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@NoArgsConstructor(force = true, access = AccessLevel.PROTECTED)
@JsonIgnoreProperties(ignoreUnknown = true)
@SuperBuilder
@AllArgsConstructor
@Data
public class EntityEvent extends UsageEventResult {
  @JsonProperty("entityUrn")
  @Schema(description = "Entity urn being operated on.")
  protected String entityUrn;

  @JsonProperty("entityType")
  @Schema(description = "Type of the entity being operated on.")
  protected String entityType;

  @JsonProperty("aspectName")
  @Schema(description = "Aspect name of the entity being operated on.")
  protected String aspectName;
}
