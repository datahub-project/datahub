/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.event;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.datahubproject.openapi.generated.OneOfGenericAspectValue;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Value
@Builder
@JsonDeserialize(builder = UpsertAspectRequest.UpsertAspectRequestBuilder.class)
public class UpsertAspectRequest {

  @JsonProperty("entityType")
  @Schema(
      required = true,
      description = "The name of the entity matching with its definition in the entity registry")
  String entityType;

  @JsonProperty("entityUrn")
  @Schema(
      description =
          "Urn of the entity to be updated with the corresponding aspect, required if entityKey is null")
  String entityUrn;

  @JsonProperty("entityKeyAspect")
  @Schema(
      description =
          "A key aspect referencing the entity to be updated, required if entityUrn is null")
  OneOfGenericAspectValue entityKeyAspect;

  @JsonProperty("aspect")
  @Schema(required = true, description = "Aspect value to be upserted")
  OneOfGenericAspectValue aspect;

  @JsonPOJOBuilder(withPrefix = "")
  public static class UpsertAspectRequestBuilder {}
}
