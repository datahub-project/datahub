package io.datahubproject.openapi.operations.k8.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import java.util.List;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request to manually trigger a CronJob. The job name is required; other fields are optional and
 * default to the CronJob template values.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(
    description =
        "Request to trigger a CronJob. Job name is required; other fields default to the template.")
public class CronJobTriggerRequest {

  @NotBlank
  @Schema(
      description = "Name for the created Job",
      example = "datahub-system-update-manual-001",
      requiredMode = Schema.RequiredMode.REQUIRED)
  private String jobName;

  @Nullable
  @Schema(
      description = "Target container name. If not specified, uses the first container.",
      example = "datahub-upgrade",
      nullable = true)
  private String containerName;

  @Nullable
  @Schema(
      description = "Command override. If not specified, uses the template command.",
      example = "[\"/bin/sh\", \"-c\"]",
      nullable = true)
  private List<String> command;

  @Nullable
  @Schema(
      description = "Arguments override. If not specified, uses the template args.",
      example = "[\"datahub migrate --force\"]",
      nullable = true)
  private List<String> args;

  @Nullable
  @Schema(
      description =
          "Resource requirements override. If not specified, uses the template resources.",
      nullable = true)
  private ResourceRequirements resources;
}
