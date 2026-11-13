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
 * Request to scale a deployment's replicas and/or resources.
 *
 * <p>Autoscaling mode rules:
 *
 * <ul>
 *   <li>{@code replicas} set → scale the deployment; pauses autoscaling if configured
 *   <li>{@code autoscalingMode: "activate"} → resume autoscaling; {@code replicas} must be omitted
 *   <li>{@code autoscalingMode: "activate"} + {@code resources} → resume autoscaling and update
 *       container resource limits/requests in the same call
 *   <li>{@code resources} alone → update container resources without touching autoscaling
 * </ul>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(
    description =
        "Request to scale a deployment. Use replicas to scale (pauses autoscaling if configured), "
            + "autoscalingMode=active to resume autoscaling, "
            + "or resources to update container limits/requests.")
public class DeploymentScaleRequest {

  @Nullable
  @Min(0)
  @Schema(description = "Desired number of replicas", example = "3", nullable = true)
  private Integer replicas;

  @Nullable
  @Schema(
      description =
          "Autoscaling mode. Set to \"activate\" to resume autoscaling (replicas must be omitted).",
      example = "activate",
      allowableValues = {"activate"},
      nullable = true)
  private String autoscalingMode;

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
