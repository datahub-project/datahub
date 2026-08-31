package io.datahubproject.openapi.operations.k8.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request to update environment variables for a container in a deployment. All fields are optional.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Request to update deployment environment variables")
public class DeploymentEnvUpdateRequest {

  @Nullable
  @Schema(
      description = "Target container name. If not specified, uses the first container.",
      example = "datahub-gms",
      nullable = true)
  private String containerName;

  @Nullable
  @Schema(
      description = "Environment variables to add or update (name -> value)",
      example = "{\"NEW_VAR\": \"value\", \"EXISTING_VAR\": \"updated-value\"}",
      nullable = true)
  private Map<String, String> set;

  @Nullable
  @Schema(
      description = "Environment variable names to remove",
      example = "[\"OLD_VAR\", \"DEPRECATED_VAR\"]",
      nullable = true)
  private List<String> remove;
}
