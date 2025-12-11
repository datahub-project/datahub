/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
