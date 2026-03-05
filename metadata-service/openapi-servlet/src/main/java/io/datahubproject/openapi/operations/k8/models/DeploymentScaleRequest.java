package io.datahubproject.openapi.operations.k8.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request to scale a deployment's replicas and/or resources. All fields are optional, but at least
 * one of replicas or resources should be provided.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(
    description =
        "Request to scale deployment. At least one of replicas or resources should be provided.")
public class DeploymentScaleRequest {

  @Nullable
  @Min(0)
  @Schema(description = "Desired number of replicas", example = "3", nullable = true)
  private Integer replicas;

  @Nullable
  @Schema(
      description =
          "Target container name for resource changes. If not specified, uses first container.",
      example = "datahub-gms",
      nullable = true)
  private String containerName;

  @Nullable
  @Schema(description = "Resource requirements (limits/requests) to set", nullable = true)
  private ResourceRequirements resources;
}
