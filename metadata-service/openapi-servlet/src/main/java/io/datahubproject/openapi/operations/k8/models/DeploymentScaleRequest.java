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
 *   <li>{@code replicas} set → pause autoscaling and hold at that replica count
 *   <li>{@code autoscalingMode: "active"} → resume autoscaling (replicas must be omitted)
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
        "Request to scale a deployment. Provide replicas to pause autoscaling at a fixed count, "
            + "autoscalingMode=active to resume autoscaling, or resources to update container limits/requests.")
public class DeploymentScaleRequest {

  @Nullable
  @Min(0)
  @Schema(description = "Desired number of replicas", example = "3", nullable = true)
  private Integer replicas;

  @Nullable
  @Schema(
      description =
          "Autoscaling mode. Set to \"active\" to resume autoscaling (replicas must be omitted). "
              + "Omit or set to \"paused\" when providing replicas to pause autoscaling at a fixed count.",
      example = "active",
      allowableValues = {"paused", "active"},
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
