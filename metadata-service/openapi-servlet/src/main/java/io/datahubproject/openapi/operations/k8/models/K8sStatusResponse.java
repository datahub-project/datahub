package io.datahubproject.openapi.operations.k8.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Response containing Kubernetes integration status information. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Kubernetes integration status")
public class K8sStatusResponse {

  @Schema(description = "Whether Kubernetes integration is available")
  private boolean available;

  @Schema(description = "The namespace the application is running in")
  private String namespace;

  @Schema(description = "Reason why Kubernetes integration is not available")
  private String reason;
}
